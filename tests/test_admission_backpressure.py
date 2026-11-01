"""Deterministic tests for Bounded Workflow Admission & Backpressure (Task 36)."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiCapacityExhaustedError,
    ApiRateLimitError,
    OrchestratorApiClient,
)
from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import RunManager
from orchestrator.api.models import DiagnosticsResponse
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import (
    Settings,
    get_settings,
    get_startup_diagnostics,
    reset_settings,
    validate_configuration,
)
from orchestrator.persistence.store import CheckpointStore

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def clean_environment(monkeypatch, tmp_path):
    """Ensure clean isolated configuration and database for every test."""
    db_file = str(tmp_path / "test_admission.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.setenv("ORCHESTRATOR_IDEMPOTENCY_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_MAX_ACTIVE_RUNS", "10")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield db_file
    reset_settings()
    reset_rate_limiter()


@pytest.fixture
def test_app(clean_environment):
    """Provide a configured test application, CheckpointStore, and RunManager."""
    db_file = clean_environment
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager(max_background_workers=8, max_active_runs=10)

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)
    try:
        yield client, store, manager
    finally:
        manager.shutdown(grace_period=0.1)
        store.close()


# -----------------------------------------------------------------------------
# 1. Configuration & Validation Tests
# -----------------------------------------------------------------------------


def test_config_max_active_runs_validation(monkeypatch):
    """Verify max_active_runs bounds, validators, and diagnostic exposure."""
    # Defaults
    s = Settings()
    assert s.max_active_runs == 10

    # Valid bounds
    s_min = Settings(max_active_runs=1)
    assert s_min.max_active_runs == 1
    assert validate_configuration(s_min).max_active_runs == 1

    s_max = Settings(max_active_runs=1024)
    assert s_max.max_active_runs == 1024
    assert validate_configuration(s_max).max_active_runs == 1024

    # Invalid bounds (< 1)
    with pytest.raises(ValueError, match="max_active_runs must be an integer >= 1"):
        Settings(max_active_runs=0)

    # Invalid bounds (> 1024)
    with pytest.raises(ValueError, match="max_active_runs cannot exceed 1024"):
        Settings(max_active_runs=1025)

    # Environment variable aliases
    monkeypatch.setenv("ORCHESTRATOR_MAX_ACTIVE_RUNS", "25")
    reset_settings()
    assert get_settings().max_active_runs == 25

    monkeypatch.delenv("ORCHESTRATOR_MAX_ACTIVE_RUNS", raising=False)
    monkeypatch.setenv("MAX_ACTIVE_RUNS", "30")
    reset_settings()
    assert get_settings().max_active_runs == 30

    monkeypatch.delenv("MAX_ACTIVE_RUNS", raising=False)
    monkeypatch.setenv("API_MAX_ACTIVE_RUNS", "15")
    reset_settings()
    assert get_settings().max_active_runs == 15

    # Startup diagnostics exposure
    diag = get_startup_diagnostics(Settings(max_active_runs=15))
    assert diag["execution"]["max_active_runs"] == 15


# -----------------------------------------------------------------------------
# 2. Admission & Rejection Tests
# -----------------------------------------------------------------------------


def test_submission_below_capacity(test_app):
    """Submissions below configured capacity are admitted and executed."""
    client, store, manager = test_app
    manager.max_active_runs = 2

    hold_event = threading.Event()

    def mock_run_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {
            "status": "completed",
            "task": "Test task",
            "answer": "Completed answer",
            "completed": [],
        }

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        resp = client.post("/runs", json={"task": "Task 1", "background": True})
        assert resp.status_code == 202
        assert manager.active_runs_count() == 1
        assert manager.available_capacity == 1

        # Release worker and verify capacity restores
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0
        assert manager.available_capacity == 2


def test_rejection_at_capacity(test_app):
    """Submissions exceeding active capacity are rejected with HTTP 429 and Retry-After."""
    client, store, manager = test_app
    manager.max_active_runs = 2

    hold_event = threading.Event()

    def mock_run_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "task": "Task", "answer": "Done", "completed": []}

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        # 1. Fill capacity (2 slots)
        resp1 = client.post("/runs", json={"task": "Task 1", "background": True})
        resp2 = client.post("/runs", json={"task": "Task 2", "background": True})
        assert resp1.status_code == 202
        assert resp2.status_code == 202
        assert manager.active_runs_count() == 2
        assert manager.available_capacity == 0

        # 2. Attempt 3rd submission -> Rejected with 429
        resp3 = client.post("/runs", json={"task": "Task 3", "background": True})
        assert resp3.status_code == 429
        assert resp3.headers.get("retry-after") == "2"
        data = resp3.json()
        assert "capacity limit of 2 active runs reached" in data["detail"]

        # Capacity was not over-consumed
        assert manager.active_runs_count() == 2

        # Release workers
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0


def test_concurrent_requests_racing_for_last_available_slot(test_app):
    """When multiple threads race for the last slot, exactly one wins and others get 429."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    hold_event = threading.Event()

    def mock_run_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "task": "Winner", "answer": "Done", "completed": []}

    num_threads = 10
    barrier = threading.Barrier(num_threads)
    results: list[int] = []
    results_lock = threading.Lock()

    def submit_worker():
        barrier.wait()
        resp = client.post("/runs", json={"task": "Racing task", "background": True})
        with results_lock:
            results.append(resp.status_code)

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        threads = [threading.Thread(target=submit_worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly 1 request accepted (202), and exactly 9 rejected (429)
        assert results.count(202) == 1
        assert results.count(429) == 9
        assert manager.active_runs_count() == 1

        # Release winning worker
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0


# -----------------------------------------------------------------------------
# 3. Capacity Release Tests
# -----------------------------------------------------------------------------


def test_capacity_release_after_success(test_app):
    """Capacity slot is released immediately upon successful workflow completion."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    # Synchronous successful run
    resp = client.post("/runs", json={"task": "Successful task", "background": False})
    assert resp.status_code == 201
    assert manager.active_runs_count() == 0
    assert manager.available_capacity == 1

    # Next run can immediately be accepted
    resp2 = client.post("/runs", json={"task": "Next task", "background": False})
    assert resp2.status_code == 201
    assert manager.active_runs_count() == 0


def test_capacity_release_after_failure(test_app):
    """Capacity slot is released when a workflow run raises an unhandled exception."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    def failing_run_task(*args, **kwargs):
        raise RuntimeError("Specialist execution fatal crash")

    with patch("orchestrator.api.manager.run_task", side_effect=failing_run_task):
        # Run fails and is recorded as failed in results_cache and store
        resp = client.post("/runs", json={"task": "Crashing task", "background": False})
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "failed"
        assert "Specialist execution fatal crash" in data["answer"]

        # Slot must be released despite failure
        assert manager.active_runs_count() == 0
        assert manager.available_capacity == 1

    # Subsequent submission succeeds
    resp2 = client.post("/runs", json={"task": "Recovery task", "background": False})
    assert resp2.status_code == 201
    assert resp2.json()["status"] == "completed"


def test_capacity_release_after_cancellation(test_app):
    """Capacity slot is released when an in-flight run is cooperatively cancelled."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    hold_event = threading.Event()

    def mock_run_task(task, run_id, *args, **kwargs):
        from orchestrator.graph.cancellation import get_cancellation_registry

        reg = get_cancellation_registry()
        # Wait until cancelled or hold_event is set
        while True:
            token = reg.get(run_id)
            if token and token.is_cancelled:
                break
            if hold_event.wait(timeout=0.05):
                break
        return {
            "status": "cancelled",
            "task": task,
            "answer": "Cancelled",
            "cancellation_reason": "User cancelled",
            "completed": [],
        }

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        resp = client.post("/runs", json={"task": "Cancellable task", "background": True})
        assert resp.status_code == 202
        run_id = resp.json()["run_id"]
        assert manager.active_runs_count() == 1

        # Cancel the active run
        cancel_resp = client.post(f"/runs/{run_id}/cancel")
        assert cancel_resp.status_code == 200
        assert cancel_resp.json()["status"] == "cancelled"

        # Signal thread exit and wait for worker completion
        hold_event.set()
        time.sleep(0.1)

        assert manager.active_runs_count() == 0
        assert manager.available_capacity == 1


def test_capacity_release_after_timeout(test_app):
    """Capacity slot is released when a workflow run encounters an execution timeout."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    def timeout_run_task(task, run_id, *args, **kwargs):
        return {
            "status": "timed_out",
            "task": task,
            "answer": "Execution deadline exceeded",
            "cancellation_reason": "Timeout exceeded",
            "completed": [],
        }

    with patch("orchestrator.api.manager.run_task", side_effect=timeout_run_task):
        resp = client.post(
            "/runs",
            json={"task": "Timing out task", "background": False, "timeout_seconds": 1.0},
        )
        assert resp.status_code == 201
        assert resp.json()["status"] == "timed_out"

        assert manager.active_runs_count() == 0
        assert manager.available_capacity == 1


# -----------------------------------------------------------------------------
# 4. Resume & Recovery Interactions
# -----------------------------------------------------------------------------


def test_resume_behavior_at_capacity(test_app):
    """Resuming an interrupted run is subject to capacity limits and rejected when full."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    # 1. Seed an interrupted run in the store
    run_id = "run-interrupted-01"
    state = {
        "run_id": run_id,
        "task": "Interrupted research workflow",
        "status": "interrupted",
        "plan": [
            {"id": "s1", "kind": "research", "instruction": "Step 1", "status": "done"},
            {"id": "s2", "kind": "writing", "instruction": "Step 2", "status": "pending"},
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "research",
                "instruction": "Step 1",
                "status": "done",
                "result": "Step 1 completed research output.",
            }
        ],
    }
    store.save_checkpoint(run_id, state, transition="interrupt")

    hold_event = threading.Event()

    def mock_run_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "task": "Active", "answer": "Done", "completed": []}

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        # Occupy the only available slot
        resp_occ = client.post("/runs", json={"task": "Occupier task", "background": True})
        assert resp_occ.status_code == 202
        assert manager.active_runs_count() == 1

        # Attempt to resume interrupted run -> rejected with 429
        resp_resume = client.post(f"/runs/{run_id}/resume", json={"background": True})
        assert resp_resume.status_code == 429
        assert "capacity limit of 1 active runs reached" in resp_resume.json()["detail"]

        # Free the occupied slot
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0

        # Now resumption succeeds
        resp_resume2 = client.post(f"/runs/{run_id}/resume", json={"background": False})
        assert resp_resume2.status_code == 200
        assert resp_resume2.json()["status"] == "completed"
        assert manager.active_runs_count() == 0


def test_startup_recovery_does_not_consume_capacity(test_app):
    """Interrupted runs recovered at startup are not active in memory and do not consume capacity."""
    client, store, manager = test_app
    manager.max_active_runs = 3

    # Seed 3 lingering runs marked 'running' in SQLite from a crashed previous process
    for i in range(1, 4):
        rid = f"crashed-run-{i}"
        store.save_checkpoint(
            rid,
            {"run_id": rid, "status": "running", "task": f"Task {i}"},
            transition="init",
        )
        with store._get_db_lock(store.db_path):
            conn = store._get_connection()
            try:
                with conn:
                    conn.execute("UPDATE runs SET status = 'running' WHERE run_id = ?", (rid,))
            finally:
                conn.close()

    # Execute startup recovery
    recovered = manager.recover_interrupted_runs(store)
    assert len(recovered) == 3

    # Active runs count in memory must be 0
    assert manager.active_runs_count() == 0
    assert manager.available_capacity == 3

    # Normal submissions up to capacity proceed without rejection
    for i in range(3):
        resp = client.post("/runs", json={"task": f"Fresh task {i}", "background": False})
        assert resp.status_code == 201


# -----------------------------------------------------------------------------
# 5. Shutdown & Idempotency Interactions
# -----------------------------------------------------------------------------


def test_shutdown_precedes_capacity_rejection(test_app):
    """During shutdown, requests are rejected with 503 (ShutdownError), not 429."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    with manager._lock:
        manager._is_shutting_down = True

    # Even with capacity available (active_runs == 0), shutdown returns 503
    resp = client.post("/runs", json={"task": "New task during shutdown"})
    assert resp.status_code == 503
    assert "Application is shutting down" in resp.json()["detail"]

    # Resume during shutdown also returns 503
    resp_res = client.post("/runs/run-any/resume", json={})
    assert resp_res.status_code == 503
    assert "Application is shutting down" in resp_res.json()["detail"]


def test_idempotent_replay_while_capacity_is_full(test_app):
    """Replaying an existing completed run via Idempotency-Key succeeds even when capacity is full."""
    client, store, manager = test_app
    manager.max_active_runs = 1
    key = "idem-replay-full-cap"

    # 1. Submit initial run with Idempotency-Key and wait for completion
    payload = {"task": "Analyze caching architectures."}
    resp1 = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    original_run_id = resp1.json()["run_id"]
    assert manager.active_runs_count() == 0

    # 2. Start background occupier that fills capacity
    hold_event = threading.Event()

    def mock_bg_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "task": "Occupier", "answer": "Done", "completed": []}

    with patch("orchestrator.api.manager.run_task", side_effect=mock_bg_task):
        resp_bg = client.post("/runs", json={"task": "Occupier", "background": True})
        assert resp_bg.status_code == 202
        assert manager.active_runs_count() == 1
        assert manager.available_capacity == 0

        # Verify a new request would be rejected with 429
        resp_rej = client.post("/runs", json={"task": "New task"})
        assert resp_rej.status_code == 429

        # 3. Retrying with original Idempotency-Key succeeds immediately with 201 and replayed header
        resp_replayed = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
        assert resp_replayed.status_code == 201
        assert resp_replayed.headers.get("Idempotent-Replayed") == "true"
        assert resp_replayed.json()["run_id"] == original_run_id

        # Release occupier
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0


def test_failed_admission_does_not_orphan_idempotency_key(test_app):
    """When admission is rejected at capacity, the idempotency key is marked failed and can be retried."""
    client, store, manager = test_app
    manager.max_active_runs = 1
    key = "idem-retry-after-cap"

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {
            "run_id": run_id,
            "status": "completed",
            "task": task,
            "answer": "Done",
            "completed": [],
        }

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        # 1. Fill capacity
        client.post("/runs", json={"task": "Occupier", "background": True})
        assert manager.active_runs_count() == 1

        # 2. Submit with Idempotency-Key -> Rejected with 429
        payload = {"task": "Important task to be retried."}
        resp = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
        assert resp.status_code == 429

        # Record in DB must be marked 'failed', NOT left stuck in 'in_progress'
        rec = store.get_idempotency_record(key=key)
        assert rec is not None
        assert rec["status"] == "failed"

        # 3. Release occupier so slot becomes available
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0

        # 4. Client retries with the SAME Idempotency-Key -> now succeeds!
        resp2 = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
        assert resp2.status_code == 201
        assert resp2.json()["run_id"].startswith("run-")

        # Record transitioned to 'completed'
        rec2 = store.get_idempotency_record(key=key)
        assert rec2["status"] == "completed"


# -----------------------------------------------------------------------------
# 6. Diagnostics & Client SDK Tests
# -----------------------------------------------------------------------------


def test_diagnostics_reflects_capacity_limits_and_active_usage(test_app):
    """GET /diagnostics accurately reports max_active_runs and available_capacity."""
    client, store, manager = test_app
    manager.max_active_runs = 4

    # 1. Idle state
    diag_resp1 = client.get("/diagnostics")
    assert diag_resp1.status_code == 200
    diag1 = DiagnosticsResponse.model_validate(diag_resp1.json())
    assert diag1.execution.max_active_runs == 4
    assert diag1.execution.active_runs_count == 0
    assert diag1.execution.available_capacity == 4
    assert diag1.limits.max_active_runs == 4

    hold_event = threading.Event()

    def mock_run_task(*args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "task": "Active", "answer": "Done", "completed": []}

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        # 2. Start 2 active runs
        client.post("/runs", json={"task": "Task 1", "background": True})
        client.post("/runs", json={"task": "Task 2", "background": True})
        assert manager.active_runs_count() == 2

        diag_resp2 = client.get("/diagnostics")
        assert diag_resp2.status_code == 200
        diag2 = DiagnosticsResponse.model_validate(diag_resp2.json())
        assert diag2.execution.max_active_runs == 4
        assert diag2.execution.active_runs_count == 2
        assert diag2.execution.available_capacity == 2

        # Release workers
        hold_event.set()
        time.sleep(0.1)
        assert manager.active_runs_count() == 0


def test_client_sdk_capacity_exhausted_error(test_app):
    """OrchestratorApiClient translates HTTP 429 capacity error into ApiCapacityExhaustedError."""
    client, store, manager = test_app
    manager.max_active_runs = 1

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {
            "run_id": run_id,
            "status": "completed",
            "task": task,
            "answer": "Done",
            "completed": [],
        }

    with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
        # Fill capacity
        client.post("/runs", json={"task": "Occupier", "background": True})
        assert manager.active_runs_count() == 1

        # Use OrchestratorApiClient wrapping the TestClient
        api_client = OrchestratorApiClient(base_url="http://testserver", timeout=5.0)
        api_client._client = client

        with pytest.raises(ApiCapacityExhaustedError) as exc_info:
            api_client.create_run(task="Task rejected at capacity")

        exc = exc_info.value
        assert exc.status_code == 429
        assert exc.retry_after == 2
        assert "capacity limit of 1 active runs reached" in str(exc)
        assert isinstance(exc, ApiRateLimitError)

        hold_event.set()
        time.sleep(0.1)
