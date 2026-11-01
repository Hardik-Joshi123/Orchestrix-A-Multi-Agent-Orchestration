"""Deterministic tests for Task 30: Graceful Shutdown & Interrupted Run Recovery.

Verifies:
1. Graceful shutdown stops accepting new start/resume requests (503 Service Unavailable).
2. Active workers receive cooperative cancellation and finish within grace period.
3. Bounded grace period expiry prevents indefinite process hangs on blocked workers.
4. Startup recovery identifies runs left active/running and marks them 'interrupted'.
5. Startup recovery is strictly idempotent across repeated invocations.
6. Startup recovery never overwrites terminal statuses (completed, failed, cancelled, timed_out).
7. Recovered 'interrupted' runs resume safely, executing only unfinished subtasks.
8. Resume fails safely with clear error on corrupt or empty plan without corrupting persistence.
9. FastAPI lifespan executes startup recovery and graceful shutdown end-to-end.
10. Health endpoint reports 'shutting_down' status during shutdown.
"""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.manager import (
    RunManager,
    ShutdownError,
    get_run_manager,
)
from orchestrator.api.server import create_app
from orchestrator.config import Settings
from orchestrator.graph.cancellation import get_cancellation_registry
from orchestrator.llm import Completion
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_cancellation_registry():
    """Ensure cancellation registry is clean before and after each test."""
    reg = get_cancellation_registry()
    reg.clear()
    yield
    reg.clear()


@pytest.fixture
def tmp_store(tmp_path):
    """Provide a thread-safe isolated CheckpointStore backed by a temporary file."""
    db_file = tmp_path / "test_shutdown_recovery.db"
    return CheckpointStore(db_path=str(db_file), enabled=True)


# ============================================================================
# 1. Graceful Shutdown Stops Accepting New Runs
# ============================================================================


def test_graceful_shutdown_stops_accepting_new_runs(tmp_store, monkeypatch):
    """Once shutdown is initiated, start_run and resume_run reject requests with ShutdownError."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
            api_shutdown_grace_seconds=1.0,
        ),
    )

    manager = RunManager()
    assert not manager.is_shutting_down

    # Trigger shutdown
    manager.shutdown(grace_period=0.1, store=tmp_store)
    assert manager.is_shutting_down

    # Starting a new run must raise ShutdownError
    with pytest.raises(ShutdownError) as exc_start:
        manager.start_run(
            task="New task after shutdown",
            store=tmp_store,
        )
    assert "shutting down" in str(exc_start.value).lower()

    # Resuming a run must raise ShutdownError
    with pytest.raises(ShutdownError) as exc_resume:
        manager.resume_run(
            run_id="any-run-id",
            store=tmp_store,
        )
    assert "shutting down" in str(exc_resume.value).lower()


def test_api_returns_503_during_shutdown(tmp_store, monkeypatch):
    """FastAPI POST /runs and POST /runs/{run_id}/resume return HTTP 503 when shutting down."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
            api_shutdown_grace_seconds=1.0,
        ),
    )

    manager = get_run_manager()
    # Trigger shutdown on default manager
    manager.shutdown(grace_period=0.1, store=tmp_store)

    app = create_app()
    with TestClient(app) as client:
        # POST /runs returns 503
        resp_create = client.post("/runs", json={"task": "Task during shutdown"})
        assert resp_create.status_code == 503
        assert "shutting down" in resp_create.json()["detail"].lower()

        # POST /runs/{run_id}/resume returns 503
        resp_resume = client.post("/runs/test-run-1/resume", json={})
        assert resp_resume.status_code == 503
        assert "shutting down" in resp_resume.json()["detail"].lower()

    # Reset default manager for other tests
    import orchestrator.api.manager as mgr_mod

    mgr_mod._DEFAULT_MANAGER = None


# ============================================================================
# 2. Cooperative Worker Completion During Graceful Shutdown
# ============================================================================


def test_graceful_shutdown_cooperative_worker_completion(tmp_store, monkeypatch):
    """Active background worker receives cancellation during shutdown and finalizes cleanly."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
            api_shutdown_grace_seconds=5.0,
        ),
    )

    manager = RunManager()
    worker_started = threading.Event()
    worker_can_finish = threading.Event()

    def mock_complete(*args, **kwargs):
        worker_started.set()
        worker_can_finish.wait(timeout=5.0)
        return Completion(
            "Worker output completed with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    plan = [
        {"id": "s1", "kind": "analysis", "instruction": "Subtask 1", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Subtask 2", "depends_on": []},
    ]

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        # 1. Start background run
        resp = manager.start_run(
            task="Shutdown cooperative test",
            plan=plan,
            run_id="run-coop-shutdown",
            background=True,
            store=tmp_store,
        )
        assert resp.status == "running"
        assert worker_started.wait(timeout=5.0)

        # 2. Initiate shutdown in a separate thread with a 5s grace period
        shutdown_thread = threading.Thread(
            target=manager.shutdown,
            kwargs={"grace_period": 5.0, "store": tmp_store},
        )
        shutdown_thread.start()

        # Allow in-flight worker to finish its current subtask
        worker_can_finish.set()
        shutdown_thread.join(timeout=5.0)

    # 3. Manager should have 0 active runs and the run should be cancelled
    assert manager.active_runs_count() == 0
    final_run = manager.get_run("run-coop-shutdown", store=tmp_store)
    assert final_run.status == "cancelled"

    summary = tmp_store.get_run_summary("run-coop-shutdown")
    assert summary["status"] == "cancelled"


# ============================================================================
# 3. Grace Period Expiry Prevents Indefinite Hangs
# ============================================================================


def test_graceful_shutdown_grace_period_expiry(tmp_store, monkeypatch):
    """When a worker hangs past the grace period, shutdown exits boundedly and marks run interrupted."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
            api_shutdown_grace_seconds=0.2,
        ),
    )

    manager = RunManager()
    worker_started = threading.Event()
    worker_never_returns = threading.Event()

    def mock_hang(*args, **kwargs):
        worker_started.set()
        # Simulate blocked specialist waiting on stalled external resource
        worker_never_returns.wait(timeout=10.0)
        return Completion("Never reached.", "mock", "mock-1")

    plan = [{"id": "s1", "kind": "analysis", "instruction": "Hanging subtask", "depends_on": []}]

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_hang):
        # Start hanging run
        manager.start_run(
            task="Hanging task",
            plan=plan,
            run_id="run-hanging-shutdown",
            background=True,
            store=tmp_store,
        )
        assert worker_started.wait(timeout=5.0)

        # Call shutdown with a tight 0.2s grace period
        start_t = time.monotonic()
        manager.shutdown(grace_period=0.2, store=tmp_store)
        elapsed = time.monotonic() - start_t

        # Bounded wait: shutdown must not hang indefinitely
        assert elapsed < 3.0, f"Shutdown took too long: {elapsed:.2f}s"

        # Release blocked thread
        worker_never_returns.set()

    # The lingering run was marked 'interrupted' in persistence
    summary = tmp_store.get_run_summary("run-hanging-shutdown")
    assert summary is not None
    assert summary["status"] in ("interrupted", "cancelled")


# ============================================================================
# 4. Startup Recovery Identifies and Marks Interrupted Runs
# ============================================================================


def test_startup_recovery_identifies_and_marks_interrupted(tmp_store):
    """Runs left active/running before process exit are safely recovered to 'interrupted' status."""
    # Seed an in-flight run simulating a crash during step 2
    plan = [
        {
            "id": "s1",
            "kind": "research",
            "instruction": "Step 1",
            "status": "done",
            "result": "Done 1 with complete evidence and rigorous analysis.",
            "confidence": 0.9,
        },
        {"id": "s2", "kind": "writing", "instruction": "Step 2", "status": "pending", "depends_on": ["s1"]},
    ]
    initial_running_state = {
        "run_id": "crashed-run-01",
        "task_id": "crashed-run-01",
        "task": "Two step workflow that crashed",
        "plan": plan,
        "completed": [plan[0]],
        "failed": [],
        "blocked": [],
        "status": "running",
    }
    tmp_store.save_checkpoint("crashed-run-01", initial_running_state, transition="subtask_complete")

    # Verify initial persisted status is 'running'
    assert tmp_store.get_run_summary("crashed-run-01")["status"] == "running"

    # Execute recovery
    recovered = tmp_store.recover_interrupted_runs(reason="Host process terminated unexpectedly")
    assert "crashed-run-01" in recovered

    # Verify run summary status is now 'interrupted'
    summary = tmp_store.get_run_summary("crashed-run-01")
    assert summary["status"] == "interrupted"

    # Verify checkpoint history contains 'interrupted' transition
    latest_rec = tmp_store.load_latest_checkpoint("crashed-run-01")
    assert latest_rec.transition == "interrupted"
    assert latest_rec.state["status"] == "interrupted"
    assert "Host process terminated" in latest_rec.state.get("interrupted_reason", "")
    assert len(latest_rec.state["completed"]) == 1
    assert latest_rec.state["completed"][0]["id"] == "s1"


# ============================================================================
# 5. Startup Recovery is Idempotent
# ============================================================================


def test_startup_recovery_is_idempotent(tmp_store):
    """Calling recovery multiple times does not duplicate checkpoints or alter state."""
    # Seed running run
    tmp_store.save_checkpoint(
        "idempotent-run",
        {"run_id": "idempotent-run", "task": "Idempotent task", "status": "running", "plan": []},
        transition="init",
    )

    # First recovery
    rec1 = tmp_store.recover_interrupted_runs()
    assert rec1 == ["idempotent-run"]
    step_after_first = tmp_store.get_run_summary("idempotent-run")["latest_step"]

    # Second recovery
    rec2 = tmp_store.recover_interrupted_runs()
    assert rec2 == []

    # Third recovery
    rec3 = tmp_store.recover_interrupted_runs()
    assert rec3 == []

    # Verify latest_step and status remain unchanged
    summary_final = tmp_store.get_run_summary("idempotent-run")
    assert summary_final["status"] == "interrupted"
    assert summary_final["latest_step"] == step_after_first


# ============================================================================
# 6. Startup Recovery Never Overwrites Terminal Statuses
# ============================================================================


def test_startup_recovery_never_overwrites_terminal_status(tmp_store):
    """Terminal statuses (completed, failed, cancelled, timed_out) are never modified by recovery."""
    terminal_cases = [
        ("term-completed", "completed"),
        ("term-failed", "failed"),
        ("term-cancelled", "cancelled"),
        ("term-timed-out", "timed_out"),
    ]

    for rid, term_status in terminal_cases:
        tmp_store.save_checkpoint(
            rid,
            {"run_id": rid, "task": f"Task {rid}", "status": term_status, "plan": []},
            transition=term_status,
        )

    # Run recovery
    recovered = tmp_store.recover_interrupted_runs()
    assert recovered == []

    # Verify every terminal run preserved its exact status
    for rid, term_status in terminal_cases:
        summary = tmp_store.get_run_summary(rid)
        assert summary["status"] == term_status


# ============================================================================
# 7. Resuming a Recovered Interrupted Run
# ============================================================================


def test_resume_recovered_interrupted_run(tmp_store, monkeypatch):
    """A run recovered into 'interrupted' status can be resumed, executing only remaining subtasks."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    plan = [
        {
            "id": "s1",
            "kind": "research",
            "instruction": "Phase 1 research",
            "status": "done",
            "result": "Phase 1 research findings with extensive documentation and thorough analysis.",
            "confidence": 0.95,
        },
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Phase 2 report",
            "status": "pending",
            "depends_on": ["s1"],
        },
    ]

    # Seed run that crashed while running
    tmp_store.save_checkpoint(
        "resumable-interrupted-run",
        {
            "run_id": "resumable-interrupted-run",
            "task_id": "resumable-interrupted-run",
            "task": "Comprehensive multi-phase report",
            "plan": plan,
            "completed": [plan[0]],
            "status": "running",
        },
        transition="subtask_1_done",
    )

    # 1. Simulate process startup recovery
    manager = RunManager()
    recovered = manager.recover_interrupted_runs(store=tmp_store)
    assert "resumable-interrupted-run" in recovered

    summary_recovered = tmp_store.get_run_summary("resumable-interrupted-run")
    assert summary_recovered["status"] == "interrupted"

    # 2. Resume execution
    invoked_subtasks = []

    def mock_complete(system="", prompt="", *args, **kwargs):
        invoked_subtasks.append(prompt)
        return Completion(
            "Phase 2 report synthesized with comprehensive verification and thorough data analysis.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        resume_res = manager.resume_run("resumable-interrupted-run", background=False, store=tmp_store)

    # 3. Resumed run completed successfully
    assert resume_res.status == "completed"
    assert "Phase 1 research findings" in resume_res.answer
    assert "Phase 2 report synthesized" in resume_res.answer

    # Verify persistence updated to completed
    final_summary = tmp_store.get_run_summary("resumable-interrupted-run")
    assert final_summary["status"] == "completed"


# ============================================================================
# 8. Resume Fails Safely on Missing or Corrupt Plan
# ============================================================================


def test_resume_fails_safely_on_missing_or_empty_plan(tmp_store, monkeypatch):
    """Resuming a checkpoint that has an empty plan raises a clear ValueError without corrupting history."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    tmp_store.save_checkpoint(
        "empty-plan-run",
        {
            "run_id": "empty-plan-run",
            "task_id": "empty-plan-run",
            "task": "Task with no plan",
            "plan": [],
            "status": "interrupted",
        },
        transition="interrupted",
    )

    manager = RunManager()
    with pytest.raises(ValueError) as exc_info:
        manager.resume_run("empty-plan-run", background=False, store=tmp_store)

    assert "cannot be safely resumed" in str(exc_info.value)
    assert "no execution plan" in str(exc_info.value)

    # Verify persistence status is preserved and not overwritten
    summary = tmp_store.get_run_summary("empty-plan-run")
    assert summary["status"] == "interrupted"


# ============================================================================
# 9. FastAPI Lifespan Startup and Shutdown End-to-End
# ============================================================================


def test_fastapi_lifespan_startup_and_shutdown(tmp_store, monkeypatch):
    """TestClient context manager triggers startup recovery on entry and graceful shutdown on exit."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
            api_shutdown_grace_seconds=1.0,
            api_recovery_on_startup=True,
        ),
    )

    # Seed a crashed run in persistence before app start
    tmp_store.save_checkpoint(
        "crashed-before-startup",
        {
            "run_id": "crashed-before-startup",
            "task": "Crashed run",
            "plan": [{"id": "s1", "kind": "analysis", "instruction": "Do", "status": "pending"}],
            "status": "running",
        },
        transition="init",
    )
    assert tmp_store.get_run_summary("crashed-before-startup")["status"] == "running"

    import orchestrator.api.manager as mgr_mod

    mgr_mod._DEFAULT_MANAGER = None

    app = create_app()

    # Enter lifespan: executes startup recovery
    with TestClient(app) as client:
        # Check run was recovered on startup
        resp = client.get("/runs/crashed-before-startup")
        assert resp.status_code == 200
        assert resp.json()["status"] == "interrupted"

        # Check health
        health_resp = client.get("/health")
        assert health_resp.status_code == 200
        assert health_resp.json()["status"] == "healthy"

    # Exit lifespan: graceful shutdown executed
    mgr = mgr_mod.get_run_manager()
    assert mgr.is_shutting_down

    mgr_mod._DEFAULT_MANAGER = None


# ============================================================================
# 10. Health Endpoint Reports Shutting Down
# ============================================================================


def test_health_endpoint_reports_shutting_down(tmp_store, monkeypatch):
    """When manager is in shutting down state, GET /health returns status='shutting_down'."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    import orchestrator.api.manager as mgr_mod

    mgr = mgr_mod.get_run_manager()
    mgr.shutdown(grace_period=0.1, store=tmp_store)

    app = create_app()
    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "shutting_down"

    mgr_mod._DEFAULT_MANAGER = None
