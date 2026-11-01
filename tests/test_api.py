"""Comprehensive tests for the orchestration HTTP API endpoints, concurrency, and security."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.manager import RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.hitl.queue import _LOCAL as hitl_local_queue
from orchestrator.hitl.queue import ReviewItem, ReviewQueue
from orchestrator.main import cli
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_stores():
    """Reset memory and review queues before and after each test."""
    reset_stm()
    reset_ltm()
    hitl_local_queue.clear()
    yield
    reset_stm()
    reset_ltm()
    hitl_local_queue.clear()


@pytest.fixture
def api_client(tmp_path):
    """Provide an isolated TestClient with dedicated SQLite database and RunManager."""
    db_file = tmp_path / "test_api_checkpoints.db"
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)
    try:
        yield client, store, manager
    finally:
        manager.shutdown()
        store.close()


# -----------------------------------------------------------------------------
# 1. Health and Readiness Tests
# -----------------------------------------------------------------------------


def test_health_endpoint(api_client):
    """GET /health must return service health, version, provider, and active runs count."""
    client, store, _ = api_client
    resp = client.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "healthy"
    assert data["checkpoint_backend"] == "sqlite"
    assert data["persistence_enabled"] is True
    assert data["active_llm_provider"] in ("mock", "openai", "anthropic")
    assert data["active_runs_count"] == 0
    assert "version" in data
    assert "timestamp" in data


# -----------------------------------------------------------------------------
# 2. Workflow Creation and Request Validation Tests
# -----------------------------------------------------------------------------


def test_create_run_synchronous_success(api_client):
    """POST /runs with valid task must execute workflow and return 201 Created with full results."""
    client, store, _ = api_client
    payload = {
        "task": "Analyze latency and throughput bottlenecks in PostgreSQL.",
        "max_concurrency": 2,
    }

    resp = client.post("/runs", json=payload)
    assert resp.status_code == 201

    data = resp.json()
    assert data["run_id"].startswith("run-")
    assert data["task"] == payload["task"]
    assert data["status"] in ("completed", "approved", "edited")
    assert data["answer"] is not None
    assert len(data["completed_subtasks"]) >= 1
    assert data["review"] is not None
    assert data["review"]["quality"] is not None

    # Checkpoint store must have recorded the run
    summary = store.get_run_summary(data["run_id"])
    assert summary is not None
    assert summary["status"] == data["status"]


def test_create_run_validation_errors(api_client):
    """POST /runs must reject empty tasks and invalid concurrency with 400 Bad Request."""
    client, _, _ = api_client

    # 1. Empty task string
    resp_empty = client.post("/runs", json={"task": ""})
    assert resp_empty.status_code == 400
    assert "empty" in resp_empty.json()["detail"].lower()

    # 2. Whitespace-only task
    resp_whitespace = client.post("/runs", json={"task": "   "})
    assert resp_whitespace.status_code == 400

    # 3. Invalid max_concurrency < 1
    resp_concurrency = client.post("/runs", json={"task": "Valid task", "max_concurrency": 0})
    assert resp_concurrency.status_code == 400

    # 4. Exceeds max length
    resp_long = client.post("/runs", json={"task": "X" * 8005})
    assert resp_long.status_code == 400
    assert "exceeds maximum allowed length" in resp_long.json()["detail"]


def test_create_run_background_accepted(api_client):
    """POST /runs with background=True must return 202 Accepted immediately with status 'running'."""
    client, _, manager = api_client
    payload = {
        "task": "Analyze query performance in background",
        "background": True,
    }

    resp = client.post("/runs", json=payload)
    assert resp.status_code == 202

    data = resp.json()
    assert data["status"] in ("running", "completed")
    assert data["run_id"]


# -----------------------------------------------------------------------------
# 3. Run Status Retrieval and Unknown Run Tests
# -----------------------------------------------------------------------------


def test_get_run_status_success(api_client):
    """GET /runs/{run_id} must retrieve execution results and metadata for existing run."""
    client, store, _ = api_client

    # Create run first
    create_resp = client.post("/runs", json={"task": "Investigate memory leaks"})
    assert create_resp.status_code == 201
    run_id = create_resp.json()["run_id"]

    # Query status
    get_resp = client.get(f"/runs/{run_id}")
    assert get_resp.status_code == 200

    data = get_resp.json()
    assert data["run_id"] == run_id
    assert data["task"] == "Investigate memory leaks"
    assert data["status"] == create_resp.json()["status"]
    assert data["latest_step"] is not None


def test_get_run_unknown_returns_404(api_client):
    """GET /runs/{run_id} for a non-existent run ID must return 404 Not Found."""
    client, _, _ = api_client
    resp = client.get("/runs/non_existent_run_99999")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


# -----------------------------------------------------------------------------
# 4. Resume Behavior and Eligibility Tests
# -----------------------------------------------------------------------------


def test_resume_run_success(api_client):
    """POST /runs/{run_id}/resume must resume an eligible interrupted workflow."""
    client, store, _ = api_client
    run_id = "test-resume-flow"

    # Pre-populate an interrupted run where subtask s1 completed, s2 pending
    interrupted_state = {
        "run_id": run_id,
        "task_id": run_id,
        "task": "Analyze memory and draft report",
        "status": "pending",
        "plan": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Analyze memory profile",
                "status": "done",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "writing",
                "instruction": "Draft memory report",
                "status": "pending",
                "depends_on": ["s1"],
            },
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Analyze memory profile",
                "status": "done",
                "result": "Memory consumption is optimal.",
            }
        ],
        "failed": [],
        "blocked": [],
        "retries": 0,
        "trace": [],
    }
    store.save_checkpoint(run_id, interrupted_state, transition="init")

    # Resume the workflow
    resp = client.post(f"/runs/{run_id}/resume", json={"max_concurrency": 2})
    assert resp.status_code == 200

    data = resp.json()
    assert data["run_id"] == run_id
    assert data["status"] in ("completed", "approved", "edited")
    assert "s2" in data["completed_subtasks"]
    assert "s1" in data["completed_subtasks"]


def test_resume_completed_run_returns_409(api_client):
    """POST /runs/{run_id}/resume on an already completed run must return 409 Conflict."""
    client, _, _ = api_client

    # Run to completion
    create_resp = client.post("/runs", json={"task": "Analyze network traffic"})
    assert create_resp.status_code == 201
    run_id = create_resp.json()["run_id"]

    # Attempting to resume a completed run
    resume_resp = client.post(f"/runs/{run_id}/resume", json={})
    assert resume_resp.status_code == 409
    assert "already fully completed" in resume_resp.json()["detail"].lower()


def test_resume_unknown_run_returns_404(api_client):
    """POST /runs/{run_id}/resume on an unknown run ID must return 404 Not Found."""
    client, _, _ = api_client
    resp = client.post("/runs/unknown_resume_id/resume", json={})
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


# -----------------------------------------------------------------------------
# 5. Checkpoint Metadata Privacy Tests
# -----------------------------------------------------------------------------


def test_checkpoint_metadata_privacy_no_state_leak(api_client):
    """GET /runs/{run_id}/checkpoints must return metadata without leaking internal state or secrets."""
    client, store, _ = api_client

    # Create run
    create_resp = client.post("/runs", json={"task": "Benchmark caching performance"})
    assert create_resp.status_code == 201
    run_id = create_resp.json()["run_id"]

    # Retrieve checkpoints
    resp = client.get(f"/runs/{run_id}/checkpoints")
    assert resp.status_code == 200

    data = resp.json()
    assert data["run_id"] == run_id
    assert data["total_steps"] >= 1
    assert len(data["checkpoints"]) == data["total_steps"]

    # Strict privacy verification on metadata items
    for cp in data["checkpoints"]:
        assert "step" in cp
        assert "transition" in cp
        assert "schema_version" in cp
        assert "created_at" in cp

        # Sensitive state fields MUST NOT be present
        assert "state" not in cp
        assert "state_json" not in cp
        assert "task" not in cp
        assert "result" not in cp
        assert "context" not in cp
        assert "api_key" not in cp


def test_checkpoints_unknown_run_returns_404(api_client):
    """GET /runs/{run_id}/checkpoints on an unknown run must return 404 Not Found."""
    client, _, _ = api_client
    resp = client.get("/runs/unknown_checkpoints_id/checkpoints")
    assert resp.status_code == 404


# -----------------------------------------------------------------------------
# 6. Concurrency and Duplicate-Run Protection Tests
# -----------------------------------------------------------------------------


def test_duplicate_run_protection_in_flight(api_client):
    """Attempting to start or resume a run while it is currently in-flight must return 409 Conflict."""
    client, _, manager = api_client
    run_id = "test-concurrent-run-id"

    # Manually register the run as in-flight in manager
    manager._register_active(run_id, "In flight task")

    try:
        # Starting same run_id must conflict
        resp_start = client.post("/runs", json={"task": "In flight task", "run_id": run_id})
        assert resp_start.status_code == 409
        assert "already executing" in resp_start.json()["detail"]

        # Resuming same run_id must conflict
        resp_resume = client.post(f"/runs/{run_id}/resume", json={})
        assert resp_resume.status_code == 409
        assert "already executing" in resp_resume.json()["detail"]
    finally:
        manager._unregister_active(run_id)


# -----------------------------------------------------------------------------
# 7. HITL Escalation Read-Only Telemetry Tests
# -----------------------------------------------------------------------------


def test_hitl_escalation_status_exposed_read_only(api_client):
    """Escalated workflow must expose safe HITL status in GET /runs/{run_id} without leaking secrets."""
    client, store, _ = api_client
    run_id = "test-hitl-exposed-run"

    # Simulate an escalated state and review item
    escalated_state = {
        "run_id": run_id,
        "task_id": run_id,
        "task": "Rebalance portfolio under shifting rates",
        "status": "pending",
        "escalated": True,
        "escalation_reason": "low_confidence",
        "escalated_subtask_id": "s1",
        "completed": [
            {
                "id": "s1",
                "kind": "analysis",
                "status": "pending",
                "confidence": 0.35,
                "result": "Proposed rebalancing allocation.",
            }
        ],
        "failed": [],
        "blocked": [],
        "trace": [],
    }
    store.save_checkpoint(run_id, escalated_state, transition="escalate")

    # Enqueue item in ReviewQueue
    queue = ReviewQueue()
    queue.push(
        ReviewItem(
            task_id=run_id,
            task="Rebalance portfolio under shifting rates",
            reason="low_confidence",
            level="approve_action",
            proposed_action="Execute rebalancing",
            confidence=0.35,
            quality=0.5,
            subtask_id="s1",
        )
    )

    resp = client.get(f"/runs/{run_id}")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "pending"
    assert data["hitl"] is not None
    assert data["hitl"]["escalated"] is True
    assert data["hitl"]["escalation_reason"] == "low_confidence"
    assert data["hitl"]["escalated_subtask_id"] == "s1"
    assert data["hitl"]["pending_review"] is True
    assert data["hitl"]["approval_level"] == "approve_action"


# -----------------------------------------------------------------------------
# 8. Evaluation Endpoint and Tag Filtering Tests
# -----------------------------------------------------------------------------


def test_evaluation_endpoint_and_tag_filtering(api_client):
    """GET /evaluation must return versioned evaluation cases with optional tag filtering."""
    client, _, _ = api_client

    # 1. Full evaluation dataset
    resp_all = client.get("/evaluation")
    assert resp_all.status_code == 200

    data = resp_all.json()
    assert data["dataset_version"] == "1.0.0"
    assert data["total_cases"] == 9
    assert len(data["cases"]) == 9

    # 2. Filtered by tag
    resp_filtered = client.get("/evaluation?tag=concurrency")
    assert resp_filtered.status_code == 200

    filtered_data = resp_filtered.json()
    assert filtered_data["total_cases"] >= 1
    assert all("concurrency" in c["tags"] for c in filtered_data["cases"])


# -----------------------------------------------------------------------------
# 9. Clean JSON Responses without Raw Tracebacks or Secrets
# -----------------------------------------------------------------------------


def test_error_handling_sanitized_no_tracebacks(api_client):
    """Internal server errors must return clean JSON without leaking Python tracebacks."""
    client, _, _ = api_client

    # Force an unhandled error inside manager._run_worker
    with patch(
        "orchestrator.api.manager.run_task",
        side_effect=RuntimeError("Internal database connection failed with secret sk-1234567890abcdef"),
    ):
        resp = client.post("/runs", json={"task": "Failing task"})

        # Worker catches and marks failed status
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "failed"
        assert "sk-1234567890abcdef" not in data["answer"]
        assert "Traceback" not in data["answer"]


# -----------------------------------------------------------------------------
# 10. CLI Backward Compatibility Tests
# -----------------------------------------------------------------------------


def test_cli_serve_flag_backward_compatibility(capsys):
    """CLI parser must support --serve, --host, and --port while preserving all existing flags."""
    # Verify help output contains server options
    with pytest.raises(SystemExit):
        cli(["--help"])

    captured = capsys.readouterr().out
    assert "--serve" in captured
    assert "--host" in captured
    assert "--port" in captured

    # Verify standard CLI execution still functions
    rc = cli(["Analyze bottlenecks", "--json"])
    assert rc == 0
    raw = capsys.readouterr().out
    parsed = json.loads(raw)
    assert parsed.get("status") in ("completed", "pending")
