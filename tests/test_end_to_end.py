"""Comprehensive End-to-End Integration Test Suite for Multi-Agent Orchestrator.

Validates end-to-end execution across all system modules:
  - Scenario A: Successful Workflow (Planning, Execution, Assembly, Metadata, Persistence)
  - Scenario B: Partial Failure & Dependency Blocking (Deterministic failure, cascade prevention)
  - Scenario C: Retry & Recovery (Transient error recovery, retry bounds, memory isolation)
  - Scenario D: Human-in-the-Loop (Escalation, human resolution, identity preservation, audit)
  - Scenario E: Persistence & Resume (Checkpoint lifecycle, bypassing done subtasks)
  - Scenario F: API & CLI Contracts (Auth, payload limits, rate limits, clean stdout/stderr)
  - Scenario G: Evaluation & Dashboard (Offline evaluation, API client, UI helper resilience)
"""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import pytest
from fastapi.testclient import TestClient
from ui.dashboard_helpers import (
    check_resume_eligibility,
    classify_subtasks,
    format_cost_display,
    format_status_badge,
    sanitize_ui_error,
)

from orchestrator.api.client import (
    ApiPayloadTooLargeError,
    ApiRateLimitError,
    OrchestratorApiClient,
)
from orchestrator.api.limiter import get_rate_limiter
from orchestrator.api.manager import RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings, get_settings
from orchestrator.evaluation.cases import get_evaluation_cases
from orchestrator.evaluation.runner import EvaluationRunner
from orchestrator.graph.builder import resume_task, run_task
from orchestrator.graph.state import GraphState, Subtask
from orchestrator.hitl.audit import _LOCAL as audit_local_chain
from orchestrator.hitl.audit import AuditChain
from orchestrator.hitl.queue import _LOCAL as hitl_local_queue
from orchestrator.hitl.queue import ReviewQueue, apply_resolution
from orchestrator.llm import Completion
from orchestrator.main import cli
from orchestrator.memory.long_term import LongTermMemory
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import ShortTermMemory
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.observability.usage import clear_run_usage
from orchestrator.persistence.store import CheckpointStore

# ============================================================================
# Environment and Test Isolation Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def clean_environment():
    """Ensure complete isolation of memory, queues, audit logs, and rate limiters."""
    reset_stm()
    reset_ltm()
    hitl_local_queue.clear()
    audit_local_chain.clear()
    clear_run_usage()
    get_rate_limiter().reset()
    get_settings.cache_clear()
    yield
    reset_stm()
    reset_ltm()
    hitl_local_queue.clear()
    audit_local_chain.clear()
    clear_run_usage()
    get_rate_limiter().reset()
    get_settings.cache_clear()


@pytest.fixture
def temp_db(tmp_path):
    """Provide a path to an isolated SQLite checkpoint database."""
    return tmp_path / "e2e_checkpoints.db"


@pytest.fixture
def store(temp_db):
    """Provide an initialized CheckpointStore instance with temporary database."""
    return CheckpointStore(db_path=temp_db, enabled=True)


@pytest.fixture
def api_test_env(tmp_path):
    """Provide an isolated FastAPI TestClient with dedicated store and RunManager."""
    db_file = tmp_path / "api_e2e_checkpoints.db"
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


# ============================================================================
# Scenario A: Successful Workflow
# ============================================================================


def test_scenario_a_successful_workflow_core(store, monkeypatch):
    """Verify full orchestration lifecycle: planning, execution, assembly, metadata, and checkpoints."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(store.db_path),
            default_llm_provider="mock",
        ),
    )

    task_desc = "Research vector database indexing algorithms, analyze query latency, and write a summary"
    run_id = "e2e-run-success-01"

    result = run_task(task_desc, run_id=run_id, store=store)

    # 1. Status and Top-Level State
    assert result["status"] == "completed"
    assert result["run_id"] == run_id
    assert result["task_id"] == run_id
    assert result["task"] == task_desc

    # 2. Planning and Decomposition
    plan = result.get("plan", [])
    assert len(plan) >= 1
    for s in plan:
        assert "id" in s
        assert "kind" in s
        assert "instruction" in s

    # 3. Specialist Execution
    completed = result.get("completed", [])
    assert len(completed) == len(plan)
    for sub in completed:
        assert sub["status"] == "done"
        assert len(sub["result"]) > 0
        assert sub["confidence"] > 0.0

    # 4. Final Result Assembly
    answer = result.get("answer", "")
    assert len(answer) > 0
    # Reducer must combine specialist findings into labeled markdown sections
    for sub in completed:
        kind_title = sub["kind"].title()
        assert f"### {kind_title} specialist" in answer

    # 5. Token Usage and Cost Tracking
    usage = result.get("usage")
    assert usage is not None
    assert usage["total_tokens"] > 0
    assert usage["total_input_tokens"] > 0
    assert usage["total_output_tokens"] > 0
    assert usage["successful_requests"] >= 1
    assert "active_provider" in usage or "breakdown" in usage

    # 6. Checkpoint Persistence
    checkpoints = store.list_checkpoints(run_id)
    assert len(checkpoints) >= 4  # init, plan, subtask_complete(s), reduce
    transitions = [c.transition for c in checkpoints]
    assert "init" in transitions
    assert "plan" in transitions
    assert "subtask_complete" in transitions
    assert "reduce" in transitions

    # Steps must be strictly sequential
    steps = [c.step for c in checkpoints]
    assert steps == list(range(1, len(checkpoints) + 1))

    # Run summary reflects completed state
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "completed"
    assert summary["latest_step"] == len(checkpoints)


def test_scenario_a_successful_workflow_api(api_test_env):
    """Verify workflow submission and execution via the HTTP API layer."""
    client, store, _ = api_test_env
    task_text = "Analyze distributed cache synchronization strategies."

    resp = client.post("/runs", json={"task": task_text, "max_concurrency": 2})
    assert resp.status_code == 201

    data = resp.json()
    run_id = data["run_id"]
    assert run_id.startswith("run-")
    assert data["task"] == task_text
    assert data["status"] in ("completed", "approved")
    assert len(data["completed_subtasks"]) >= 1
    assert data["answer"] is not None

    # Usage metadata present
    assert "usage" in data
    assert data["usage"]["total_tokens"] > 0

    # Verify run record in persistence store
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] in ("completed", "approved")


# ============================================================================
# Scenario B: Partial Failure & Dependency Blocking
# ============================================================================


def test_scenario_b_partial_failure_and_dependency_blocking(store, monkeypatch):
    """Verify prerequisite failure blocks downstream subtasks while independent subtasks complete."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(store.db_path),
            default_llm_provider="mock",
        ),
    )

    plan = [
        {"id": "s1", "kind": "research", "instruction": "Fetch primary dataset", "depends_on": []},
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Draft report based on dataset",
            "depends_on": ["s1"],
        },
        {
            "id": "s3",
            "kind": "analysis",
            "instruction": "Analyze independent system telemetry",
            "depends_on": [],
        },
    ]

    invoked_specialists: list[str] = []

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "research" in system.lower():
            invoked_specialists.append("research")
            raise RuntimeError("Database connection timed out during research")
        if "analysis" in system.lower():
            invoked_specialists.append("analysis")
            return Completion("Telemetry analysis: latency p99 is 42ms.", "mock", "mock-1")
        if "writing" in system.lower():
            invoked_specialists.append("writing")
            return Completion("Writing report from data.", "mock", "mock-1")
        return Completion("Default output.", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Data fetch and reporting", plan=plan, run_id="e2e-run-partial-01", store=store)

    completed = result.get("completed", [])

    # 1. Prerequisite s1 failed
    s1_res = next((s for s in completed if s["id"] == "s1"), None)
    assert s1_res is not None
    assert s1_res["status"] == "failed"
    assert "Database connection timed out" in s1_res["error"]
    assert s1_res["result"] == ""

    # 2. Dependent s2 is blocked (never executed by specialist)
    s2_res = next((s for s in completed if s["id"] == "s2"), None)
    assert s2_res is not None
    assert s2_res["status"] == "blocked"
    assert "s1" in s2_res["error"]
    assert s2_res["result"] == ""
    assert "writing" not in invoked_specialists  # writing specialist must NEVER run!

    # 3. Independent s3 completed successfully
    s3_res = next((s for s in completed if s["id"] == "s3"), None)
    assert s3_res is not None
    assert s3_res["status"] == "done"
    assert "latency p99 is 42ms" in s3_res["result"]
    assert "analysis" in invoked_specialists

    # 4. Final Result Assembly
    assert result["status"] in ("partial", "failed", "pending")
    answer = result.get("answer", "")
    assert "### Analysis specialist" in answer
    assert "latency p99 is 42ms" in answer
    # Must NOT invent successful output for failed/blocked subtasks
    assert "### Writing specialist" not in answer

    # 5. Checkpoints include blocked transition
    checkpoints = store.list_checkpoints("e2e-run-partial-01")
    transitions = [c.transition for c in checkpoints]
    assert "blocked" in transitions


# ============================================================================
# Scenario C: Retry & Recovery
# ============================================================================


def test_scenario_c_transient_failure_recovers_on_retry():
    """Verify that a transient network failure retries boundedly and recovers successfully."""
    call_attempts = {"count": 0}

    def flaky_complete(system="", prompt="", *args, **kwargs):
        call_attempts["count"] += 1
        if call_attempts["count"] == 1:
            raise ConnectionResetError("Transient TCP reset from provider")
        return Completion("Successful specialist analysis after recovery.", "mock", "mock-1")

    plan = [{"id": "s1", "kind": "analysis", "instruction": "Analyze memory footprint"}]

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=flaky_complete):
        result = run_task("Check memory footprint", plan=plan, run_id="e2e-retry-recover-01")

    assert call_attempts["count"] == 2
    assert result["retries"] == 1
    assert result["status"] == "completed"

    s1_res = result["completed"][0]
    assert s1_res["status"] == "done"
    assert "Successful specialist analysis after recovery" in s1_res["result"]
    assert "Successful specialist analysis after recovery" in result["answer"]

    # Short-term working memory reflects success
    short = ShortTermMemory("e2e-retry-recover-01")
    assert short.get("status") == "completed"


def test_scenario_c_permanent_failure_exhausts_retries_and_escalates():
    """Verify persistent failure exhausts max_retries, escalates to HITL, and isolates memory."""
    settings = get_settings()

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Persistent fatal outage"),
    ):
        result = run_task("Compute mission-critical projection", run_id="e2e-retry-exhaust-01")

    # Retries reached limit
    assert result["retries"] >= settings.max_retries
    assert result["escalated"] is True
    assert result["escalation_reason"] == "repeated_failure"
    assert result["status"] == "pending"

    # Enqueued in HITL review queue
    items = ReviewQueue().pending()
    matching = [item for item in items if item.task_id == "e2e-retry-exhaust-01"]
    assert len(matching) == 1
    assert matching[0].reason == "repeated_failure"

    # Long-term semantic memory must NOT store failed or pending outcomes
    ltm = LongTermMemory()
    recalled = ltm.recall("Compute mission-critical projection")
    assert not recalled


# ============================================================================
# Scenario D: Human-in-the-Loop (HITL)
# ============================================================================


def test_scenario_d_hitl_escalation_and_authoritative_resolution():
    """Verify escalation trigger, review queue enqueuing, human takeover, and audit verification."""
    # 1. Trigger sensitive command escalation
    result = run_task("Execute DROP TABLE production_users;", run_id="e2e-hitl-01")
    assert result["escalated"] is True
    assert result["approval"] == "take_over"
    assert result["status"] == "pending"
    assert "Human Review Required" in result["answer"]

    # 2. Check ReviewQueue
    queue = ReviewQueue()
    pending = queue.pending()
    matching = [p for p in pending if p.task_id == "e2e-hitl-01"]
    assert len(matching) == 1
    item = matching[0]
    assert item.level == "take_over"

    # 3. Human reviewer takes over with authoritative correction
    resolution = queue.resolve(
        identifier=item.item_id,
        decision="take_over",
        approver="sec-ops-lead@company.com",
        note="Destructive database commands disallowed; schema retention policy enforced.",
    )
    expected_note = "Destructive database commands disallowed; schema retention policy enforced."
    assert resolution.decision == "take_over"
    assert resolution.approver == "sec-ops-lead@company.com"
    assert resolution.final_result == expected_note

    # 4. Apply resolution to state
    resolved_state = apply_resolution(result, resolution)
    assert resolved_state["status"] == "taken_over"
    assert resolved_state["escalated"] is False
    assert resolved_state["answer"] == expected_note
    assert resolved_state["task_id"] == "e2e-hitl-01"

    # 5. Duplicate resolution prevention
    with pytest.raises(ValueError, match="already resolved"):
        apply_resolution(resolved_state, resolution)

    # 6. Audit Trail Verification
    audit_records = AuditChain().records()
    assert len(audit_records) >= 1
    audit_entry = next(r for r in audit_records if r["task_id"] == "e2e-hitl-01")
    assert audit_entry["decision"] == "take_over"
    assert audit_entry["approver"] == "sec-ops-lead@company.com"
    is_valid, err = AuditChain().verify()
    assert is_valid is True


# ============================================================================
# Scenario E: Persistence & Resume
# ============================================================================


def test_scenario_e_persistence_and_resume_bypasses_completed_subtasks(store, monkeypatch):
    """Verify resuming an interrupted workflow skips already-done subtasks and completes remainder."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(store.db_path),
            default_llm_provider="mock",
        ),
    )

    run_id = "e2e-resume-skip-01"
    plan: list[Subtask] = [
        {"id": "s1", "kind": "research", "instruction": "Collect network metrics", "status": "pending"},
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Compile report",
            "status": "pending",
            "depends_on": ["s1"],
        },
    ]

    # Simulate an interrupted state where s1 finished and s2 is pending
    s1_done: Subtask = {
        "id": "s1",
        "kind": "research",
        "instruction": "Collect network metrics",
        "status": "done",
        "result": "Network metrics: RTT 12ms, loss 0.001%, verified with comprehensive documentation.",
        "confidence": 0.98,
        "tools_used": ["metrics_tool"],
    }

    initial_checkpoint: GraphState = {
        "run_id": run_id,
        "task_id": run_id,
        "task": "Collect metrics and write report",
        "plan": plan,
        "completed": [s1_done],
        "failed": [],
        "blocked": [],
        "tools_used": ["metrics_tool"],
        "retries": 0,
        "status": "pending",
        "trace": [],
    }

    store.save_checkpoint(run_id, initial_checkpoint, transition="subtask_complete")

    # Track specialist invocations to verify s1 is never re-executed
    invoked_subtask_ids: list[str] = []

    def mock_invoke_specialist(kind, state, subtask):
        invoked_subtask_ids.append(subtask["id"])
        return {
            "completed": [{
                **subtask,
                "status": "done",
                "result": (
                    f"Executed {subtask['id']} with comprehensive detailed documentation, "
                    f"benchmark metrics, and verified testing."
                ),
                "confidence": 0.95,
            }],
            "tools_used": [],
            "trace": [],
        }

    monkeypatch.setattr("orchestrator.graph.builder._invoke_specialist", mock_invoke_specialist)

    # Resume the workflow
    resumed_result = resume_task(run_id, store=store)

    assert resumed_result["status"] == "completed"
    assert resumed_result["run_id"] == run_id

    # s1 must NOT have been re-invoked; only s2 should execute
    assert "s1" not in invoked_subtask_ids
    assert "s2" in invoked_subtask_ids

    # Final answer combines prior s1 result and new s2 result
    assert "Network metrics: RTT 12ms" in resumed_result["answer"]
    assert "Executed s2 with comprehensive detailed documentation" in resumed_result["answer"]

    # Checkpoint store transitions include resume and reduce
    transitions = [c.transition for c in store.list_checkpoints(run_id)]
    assert "resume" in transitions
    assert "reduce" in transitions


def test_scenario_e_resume_already_completed_workflow_returns_cached_state(store):
    """Resuming an already-completed run returns terminal state without re-execution."""
    run_id = "e2e-already-done-01"
    completed_state: GraphState = {
        "run_id": run_id,
        "task_id": run_id,
        "task": "Pre-completed task",
        "plan": [{"id": "s1", "kind": "analysis", "instruction": "Analyze", "status": "pending"}],
        "completed": [{
            "id": "s1",
            "kind": "analysis",
            "instruction": "Analyze",
            "status": "done",
            "result": "Analysis complete.",
        }],
        "status": "completed",
        "answer": "Cached terminal answer.",
        "trace": [],
    }

    store.save_checkpoint(run_id, completed_state, transition="reduce")

    resumed = resume_task(run_id, store=store)
    assert resumed["status"] == "completed"
    assert resumed["answer"] == "Cached terminal answer."


# ============================================================================
# Scenario F: API & CLI Contracts
# ============================================================================


def test_scenario_f_api_contracts_security_and_protections(tmp_path, monkeypatch):
    """Verify API authentication, payload limits, and rate limits."""
    # Set explicit token in settings
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "secret-test-token-77")
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "500")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "2")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "30.0")
    get_settings.cache_clear()

    db_file = tmp_path / "sec_api_checkpoints.db"
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)
    try:
        # 1. Unauthorized request returns 401 with WWW-Authenticate header
        resp_unauth = client.post("/runs", json={"task": "Short task"})
        assert resp_unauth.status_code == 401
        assert "Bearer" in resp_unauth.headers.get("www-authenticate", "")

        # 2. Authorized request under limits succeeds
        auth_headers = {"Authorization": "Bearer secret-test-token-77"}
        resp_ok = client.post("/runs", json={"task": "Valid task 1"}, headers=auth_headers)
        assert resp_ok.status_code == 201

        # 3. Payload size enforcement (413 Payload Too Large)
        oversized_payload = {"task": "A" * 1000}
        resp_413 = client.post("/runs", json=oversized_payload, headers=auth_headers)
        assert resp_413.status_code == 413
        assert "exceeds maximum allowed size" in resp_413.json()["detail"].lower()

        # 4. Rate limiting: quota is 2 requests per window; request 2 succeeds, request 3 is throttled
        resp_ok2 = client.post("/runs", json={"task": "Valid task 2"}, headers=auth_headers)
        assert resp_ok2.status_code == 201

        resp_throttled = client.post("/runs", json={"task": "Valid task 3"}, headers=auth_headers)
        assert resp_throttled.status_code == 429
        assert "retry-after" in resp_throttled.headers
        assert "rate limit exceeded" in resp_throttled.json()["detail"].lower()

        # 5. Health and polling are never throttled
        resp_health = client.get("/health")
        assert resp_health.status_code == 200
    finally:
        manager.shutdown()
        store.close()


def test_scenario_f_cli_contracts_stdout_json_isolation(capsys):
    """Verify CLI stdout emits strictly parseable JSON with diagnostics kept on stderr."""
    rc = cli(["Analyze distributed transaction semantics", "--json"])
    assert rc == 0

    captured = capsys.readouterr()
    raw_stdout = captured.out.strip()

    # Must be valid single JSON object parseable from stdout
    data = json.loads(raw_stdout)
    assert data["status"] in ("completed", "approved")
    assert "answer" in data
    assert len(data["answer"]) > 0

    # Ensure stdout is free of non-JSON diagnostic noise
    assert "=== ANSWER ===" not in raw_stdout
    assert "provider=" not in raw_stdout

    # Diagnostics are routed to stderr
    assert "provider=" in captured.err or "task=" in captured.err


def test_scenario_f_cli_diagnostics_and_invalid_arguments(capsys):
    """Verify CLI --diagnostics output and graceful handling of invalid arguments."""
    # Diagnostics text mode
    rc_diag = cli(["--diagnostics"])
    assert rc_diag == 0
    captured_diag = capsys.readouterr()
    assert "Diagnostics" in captured_diag.err

    # Diagnostics JSON mode
    rc_diag_json = cli(["--diagnostics", "--json"])
    assert rc_diag_json == 0
    diag_data = json.loads(capsys.readouterr().out)
    assert "version" in diag_data
    assert "api" in diag_data
    assert "llm" in diag_data
    assert "persistence" in diag_data

    # Invalid concurrency argument rejected cleanly with exit code 1
    rc_bad_arg = cli(["Test task", "--concurrency", "0"])
    assert rc_bad_arg == 1
    captured_bad = capsys.readouterr()
    assert captured_bad.out == ""
    assert "error: --concurrency must be 1..128" in captured_bad.err.lower()


# ============================================================================
# Scenario G: Evaluation & Dashboard Compatibility
# ============================================================================


def test_scenario_g_offline_evaluation_runner_and_metrics():
    """Verify offline evaluation framework executes dataset cases and calculates metrics."""
    cases = get_evaluation_cases()
    assert len(cases) >= 5

    # Run single representative case via EvaluationRunner.run_all
    runner = EvaluationRunner()
    report = runner.run_all(case_ids=["case_01_single_specialist"])

    assert report.total_cases == 1
    assert report.passed_cases == 1
    assert report.pass_rate == 1.0

    res = report.case_results[0]
    assert res.case_id == "case_01_single_specialist"
    assert res.passed is True
    assert res.status in ("completed", "approved")

    # Verify metrics computation
    assert len(report.metrics) == 10
    completion_rate = report.metrics["task_completion_rate"]
    assert completion_rate.applicable is True
    assert completion_rate.value == 1.0

    # Test CLI evaluation invocation
    rc_eval = cli(["--eval", "--eval-cases", "case_01_single_specialist"])
    assert rc_eval == 0


def test_scenario_g_dashboard_api_client_and_helpers():
    """Verify Dashboard OrchestratorApiClient and UI helper resilience with mock transport."""

    def mock_transport_handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return httpx.Response(
                200,
                json={
                    "status": "healthy",
                    "version": "0.1.0",
                    "timestamp": "2026-10-22T00:00:00Z",
                    "checkpoint_backend": "sqlite",
                    "persistence_enabled": True,
                    "active_llm_provider": "mock",
                    "active_runs_count": 0,
                },
            )
        if path == "/runs" and request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "run_id": "mock-run-123",
                    "task": "Test task",
                    "status": "completed",
                    "answer": "Mock output answer.",
                    "completed_subtasks": ["s1"],
                    "review": {"quality": 0.95, "confidence": 0.95},
                    "usage": {"total_tokens": 150, "estimated_cost_usd": 0.002},
                },
            )
        if path == "/runs/mock-run-123":
            return httpx.Response(
                200,
                json={
                    "run_id": "mock-run-123",
                    "task": "Test task",
                    "status": "completed",
                    "answer": "Mock output answer.",
                    "completed_subtasks": ["s1"],
                    "review": {"quality": 0.95, "confidence": 0.95},
                    "usage": {"total_tokens": 150, "estimated_cost_usd": 0.002},
                },
            )
        if path == "/runs/mock-run-123/checkpoints":
            return httpx.Response(
                200,
                json={
                    "run_id": "mock-run-123",
                    "total_steps": 1,
                    "checkpoints": [{
                        "step": 1,
                        "transition": "reduce",
                        "schema_version": 1,
                        "created_at": "2026-10-22T00:00:00Z",
                    }],
                },
            )
        if path == "/runs/throttle-test":
            return httpx.Response(429, headers={"Retry-After": "45"}, json={"detail": "Rate limit exceeded."})
        if path == "/runs/oversized-test":
            return httpx.Response(413, json={"detail": "Payload too large."})

        return httpx.Response(404, json={"detail": "Not found"})

    transport = httpx.MockTransport(mock_transport_handler)
    http_client = httpx.Client(transport=transport, base_url="http://dashboard.test")
    client = OrchestratorApiClient(base_url="http://dashboard.test", client=http_client)

    # 1. Test Client API queries
    health = client.get_health()
    assert health.status == "healthy"
    assert health.active_llm_provider == "mock"

    run_resp = client.create_run("Test task", max_concurrency=2)
    assert run_resp.run_id == "mock-run-123"
    assert run_resp.status == "completed"

    poll_resp = client.get_run("mock-run-123")
    assert poll_resp.run_id == "mock-run-123"

    ckpts = client.get_checkpoints("mock-run-123")
    assert ckpts.total_steps == 1
    assert len(ckpts.checkpoints) == 1

    # 2. Test Client Error Handling
    with pytest.raises(ApiRateLimitError) as exc_rate:
        client._client.post("http://dashboard.test/runs/throttle-test")
        client._handle_response_error(
            httpx.Response(429, headers={"Retry-After": "45"}, json={"detail": "Rate limit exceeded."})
        )
    assert exc_rate.value.retry_after == 45

    with pytest.raises(ApiPayloadTooLargeError):
        client._handle_response_error(httpx.Response(413, json={"detail": "Payload too large."}))

    # 3. Test Dashboard UI Helpers
    badge = format_status_badge("completed")
    assert "Completed" in str(badge)

    classified = classify_subtasks({
        "completed_subtasks": ["s1"],
        "failed_subtasks": ["s2"],
        "blocked_subtasks": ["s3"],
    })
    assert len(classified["completed"]) == 1
    assert len(classified["failed"]) == 1
    assert len(classified["blocked"]) == 1

    eligible, reason = check_resume_eligibility({"status": "failed"})
    assert eligible is True

    cost_info = format_cost_display({"estimated_cost_usd": 0.0042, "has_unknown_cost": False})
    assert "$0.0042" in cost_info["cost_display"]

    sanitized = sanitize_ui_error("Database error with key=sk-secrettoken123456")
    assert "sk-secrettoken123456" not in sanitized
