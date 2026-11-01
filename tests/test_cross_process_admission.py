"""Deterministic tests for Cross-Process Workflow Admission Coordination (Task 37)."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import CapacityExhaustedError, RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import (
    Settings,
    reset_settings,
    validate_configuration,
)
from orchestrator.persistence.store import CheckpointStore

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture
def clean_env(monkeypatch, tmp_path):
    """Provide isolated SQLite database and reset settings."""
    db_file = str(tmp_path / "test_cross_process.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.setenv("ORCHESTRATOR_MAX_ACTIVE_RUNS", "3")
    monkeypatch.setenv("ORCHESTRATOR_ADMISSION_LEASE_TTL_SECONDS", "30.0")
    monkeypatch.setenv("ORCHESTRATOR_ADMISSION_HEARTBEAT_INTERVAL_SECONDS", "5.0")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield db_file
    reset_settings()
    reset_rate_limiter()


# -----------------------------------------------------------------------------
# 1. Configuration & Lease Settings Tests
# -----------------------------------------------------------------------------


def test_lease_configuration_validation():
    """Verify lease TTL and heartbeat interval bounds and validators."""
    s = Settings(
        max_active_runs=5,
        admission_lease_ttl_seconds=15.0,
        admission_heartbeat_interval_seconds=3.0,
    )
    validated = validate_configuration(s)
    assert validated.admission_lease_ttl_seconds == 15.0
    assert validated.admission_heartbeat_interval_seconds == 3.0

    # Invalid TTL (< 1.0)
    with pytest.raises(ValueError, match="admission_lease_ttl_seconds must be a number >= 1.0"):
        Settings(admission_lease_ttl_seconds=0.5)

    # Invalid Heartbeat (< 0.1)
    with pytest.raises(ValueError, match="admission_heartbeat_interval_seconds must be a number >= 0.1"):
        Settings(admission_heartbeat_interval_seconds=0.05)

    # Heartbeat >= TTL
    with pytest.raises(ValueError, match="must be strictly less than"):
        validate_configuration(
            Settings(
                admission_lease_ttl_seconds=10.0,
                admission_heartbeat_interval_seconds=10.0,
            )
        )


# -----------------------------------------------------------------------------
# 2. Multi-Worker Shared Capacity Enforcement
# -----------------------------------------------------------------------------


def test_cross_process_capacity_enforcement(clean_env):
    """Verify multiple worker instances sharing a database enforce a single global limit."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(
        max_background_workers=4,
        max_active_runs=3,
        owner_id="worker-proc-A",
        store=store,
    )
    worker_b = RunManager(
        max_background_workers=4,
        max_active_runs=3,
        owner_id="worker-proc-B",
        store=store,
    )

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
            # Worker A admits run 1
            res_a1 = worker_a.start_run(task="Task A1", background=True, store=store)
            assert res_a1.status == "running"
            assert worker_a.local_active_runs_count() == 1
            assert worker_b.local_active_runs_count() == 0
            assert store.count_active_admissions() == 1

            # Worker B admits run 2
            res_b1 = worker_b.start_run(task="Task B1", background=True, store=store)
            assert res_b1.status == "running"
            assert worker_a.local_active_runs_count() == 1
            assert worker_b.local_active_runs_count() == 1
            assert store.count_active_admissions() == 2

            # Worker A admits run 3 (total reaches global limit of 3)
            res_a2 = worker_a.start_run(task="Task A2", background=True, store=store)
            assert res_a2.status == "running"
            assert store.count_active_admissions() == 3

            # Now global capacity is exhausted (3/3).
            # Neither Worker A nor Worker B should be able to admit run 4!
            with pytest.raises(CapacityExhaustedError) as exc_info_a:
                worker_a.start_run(task="Task A3 (Excess)", background=True, store=store)
            assert exc_info_a.value.max_active_runs == 3
            assert exc_info_a.value.active_runs == 3

            with pytest.raises(CapacityExhaustedError) as exc_info_b:
                worker_b.start_run(task="Task B2 (Excess)", background=True, store=store)
            assert exc_info_b.value.max_active_runs == 3
            assert exc_info_b.value.active_runs == 3

            # Database admission count remains strictly 3
            assert store.count_active_admissions() == 3
    finally:
        hold_event.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 3. Atomic Race for Last Available Slot
# -----------------------------------------------------------------------------


def test_atomic_race_between_processes(clean_env):
    """Verify simultaneous processes racing for the final admission slot cannot both acquire it."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(
        max_background_workers=4,
        max_active_runs=2,
        owner_id="worker-proc-A",
        store=store,
    )
    worker_b = RunManager(
        max_background_workers=4,
        max_active_runs=2,
        owner_id="worker-proc-B",
        store=store,
    )

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
            # Pre-fill 1 slot
            worker_a.start_run(task="Pre-fill 1", background=True, store=store)
            assert store.count_active_admissions() == 1

            # Now exactly 1 slot remains. Worker A and Worker B race simultaneously
            results: dict[str, str] = {}
            barrier = threading.Barrier(2)

            def worker_a_attempt():
                barrier.wait()
                try:
                    worker_a.start_run(task="Race Run A", background=True, store=store)
                    results["A"] = "admitted"
                except CapacityExhaustedError:
                    results["A"] = "exhausted"

            def worker_b_attempt():
                barrier.wait()
                try:
                    worker_b.start_run(task="Race Run B", background=True, store=store)
                    results["B"] = "admitted"
                except CapacityExhaustedError:
                    results["B"] = "exhausted"

            t1 = threading.Thread(target=worker_a_attempt)
            t2 = threading.Thread(target=worker_b_attempt)
            t1.start()
            t2.start()
            t1.join()
            t2.join()

            # Exactly one must succeed, and one must be rejected
            statuses = list(results.values())
            assert statuses.count("admitted") == 1
            assert statuses.count("exhausted") == 1

            # Total admissions in DB never exceeded 2
            assert store.count_active_admissions() == 2
    finally:
        hold_event.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 4. Capacity Release Across Processes
# -----------------------------------------------------------------------------


def test_capacity_release_allows_other_worker(clean_env):
    """Verify that when worker A finishes a run, worker B can immediately acquire the freed slot."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(max_active_runs=1, owner_id="worker-A", store=store)
    worker_b = RunManager(max_active_runs=1, owner_id="worker-B", store=store)

    hold_a = threading.Event()
    hold_b = threading.Event()

    def mock_run_tasks(task, run_id="", *args, **kwargs):
        if "Task B" in task:
            hold_b.wait(timeout=5.0)
        else:
            hold_a.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_tasks):
            # Worker A acquires the only slot
            worker_a.start_run(task="Task to complete", background=True, store=store)
            assert store.count_active_admissions() == 1

            # Worker B is rejected
            with pytest.raises(CapacityExhaustedError):
                worker_b.start_run(task="Task B", background=True, store=store)

            # Let Worker A complete
            hold_a.set()
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline and store.count_active_admissions() > 0:
                time.sleep(0.02)
            assert store.count_active_admissions() == 0

            # Now Worker B can acquire the slot
            run_b = worker_b.start_run(task="Task B Accepted", background=True, store=store)
            assert run_b.status == "running"
            assert store.count_active_admissions() == 1
    finally:
        hold_a.set()
        hold_b.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 5. Heartbeat Renewal and Stale Lease Expiration
# -----------------------------------------------------------------------------


def test_heartbeat_renewal_and_stale_reclaim(clean_env):
    """Verify that active leases are renewed, and dead process abandoned leases expire."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    # Worker A with short heartbeat and lease TTL
    worker_a = RunManager(
        max_active_runs=1,
        owner_id="worker-A",
        store=store,
        lease_ttl_seconds=0.10,
        heartbeat_interval_seconds=0.02,
    )

    hold_a = threading.Event()

    def mock_run_a(task, run_id="", *args, **kwargs):
        hold_a.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_a):
            worker_a.start_run(task="Heartbeated Task", background=True, store=store)
            admissions = store.list_active_admissions(lease_ttl_seconds=1.0)
            assert len(admissions) == 1
            first_heartbeat = admissions[0]["heartbeat_at"]

            # Wait enough for heartbeat loop to fire at least once
            time.sleep(0.06)
            admissions_updated = store.list_active_admissions(lease_ttl_seconds=1.0)
            assert len(admissions_updated) == 1
            second_heartbeat = admissions_updated[0]["heartbeat_at"]
            assert second_heartbeat >= first_heartbeat

            # Now simulate Worker A abruptly crashing (stop heartbeat loop without clean shutdown)
            worker_a._heartbeat_stop_event.set()
            if worker_a._heartbeat_thread.is_alive():
                worker_a._heartbeat_thread.join(timeout=0.2)

            # Wait for lease to expire (> 0.10s)
            time.sleep(0.15)

            # Worker B should now be able to acquire capacity because Worker A's lease is stale
            worker_b = RunManager(
                max_active_runs=1,
                owner_id="worker-B",
                store=store,
                lease_ttl_seconds=10.0,
            )
            try:
                run_b = worker_b.start_run(task="Worker B Reclaim", background=True, store=store)
                assert run_b.status == "running"
                active_list = store.list_active_admissions(lease_ttl_seconds=10.0)
                assert len(active_list) == 1
                assert active_list[0]["owner_id"] == "worker-B"
            finally:
                worker_b.shutdown(grace_period=0.1, store=store)
    finally:
        hold_a.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 6. Live Process Protection During Startup Recovery
# -----------------------------------------------------------------------------


def test_startup_recovery_shields_live_process_workflows(clean_env):
    """Verify that startup recovery run by process B never marks process A's active runs as interrupted."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(
        max_active_runs=5,
        owner_id="worker-live-A",
        store=store,
        lease_ttl_seconds=30.0,
    )
    worker_b = RunManager(
        max_active_runs=5,
        owner_id="worker-starting-B",
        store=store,
        lease_ttl_seconds=30.0,
    )

    try:
        # Worker A has an active run saved in checkpoints and registered in admissions
        store.save_checkpoint("run-live-1", {"status": "running", "task": "Live task A"}, transition="start")
        admitted = store.acquire_workflow_admission("run-live-1", "worker-live-A", 5, lease_ttl_seconds=30.0)
        assert admitted is True

        # There is also an abandoned run from an old dead process with no lease
        store.save_checkpoint("run-abandoned", {"status": "running", "task": "Dead task"}, transition="start")

        # Worker B runs startup recovery
        recovered = worker_b.recover_interrupted_runs(store=store)

        # Only the abandoned run should be recovered
        assert "run-abandoned" in recovered
        assert "run-live-1" not in recovered

        # Worker A's run is STILL running and completely protected!
        rec_live = store.load_latest_checkpoint("run-live-1")
        assert rec_live.state["status"] == "running"

        # The abandoned run is now interrupted
        rec_dead = store.load_latest_checkpoint("run-abandoned")
        assert rec_dead.state["status"] == "interrupted"
    finally:
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 7. Resumption Obeys Global Capacity Limit
# -----------------------------------------------------------------------------


def test_resume_obeys_cross_process_capacity(clean_env):
    """Verify that workflow resumption is bounded by cross-process capacity limits."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(max_active_runs=2, owner_id="worker-A", store=store)
    worker_b = RunManager(max_active_runs=2, owner_id="worker-B", store=store)

    hold_a = threading.Event()

    def mock_run_a(task, run_id="", *args, **kwargs):
        hold_a.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        # Create an interrupted run ready to be resumed
        interrupted_state = {
            "status": "interrupted",
            "task": "Resumable Task",
            "plan": [{"id": "s1", "task": "Subtask 1"}],
            "completed": [],
        }
        store.save_checkpoint("run-to-resume", interrupted_state, transition="interrupted")

        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_a):
            # Worker A starts 2 active runs to fill global capacity (2/2)
            worker_a.start_run(task="Run A1", background=True, store=store)
            worker_a.start_run(task="Run A2", background=True, store=store)
            assert store.count_active_admissions() == 2

            # Worker B attempts to resume 'run-to-resume'
            with pytest.raises(CapacityExhaustedError) as exc_info:
                worker_b.resume_run("run-to-resume", background=True, store=store)
            assert exc_info.value.max_active_runs == 2
            assert exc_info.value.active_runs == 2

            # Free 1 slot: let run A1 complete
            run_a1_id = list(worker_a._active_runs.keys())[0]
            worker_a._unregister_active(run_a1_id)
            store.release_workflow_admission(run_a1_id, "worker-A")
            assert store.count_active_admissions() == 1

            # Now Worker B can successfully resume!
            resume_res = worker_b.resume_run("run-to-resume", background=True, store=store)
            assert resume_res.status == "running"
            assert store.count_active_admissions() == 2
    finally:
        hold_a.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 8. Idempotency Replay Does Not Consume Admission Slot
# -----------------------------------------------------------------------------


def test_idempotent_replay_at_capacity(clean_env):
    """Verify that replaying a completed idempotent request succeeds even when global capacity is full."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager(max_active_runs=1, owner_id="worker-main", store=store)

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager
    client = TestClient(app)

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        if task == "Saturating Task":
            hold_event.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
            headers = {"Idempotency-Key": "idemp-key-100"}
            # Initial submission completes
            res1 = client.post("/runs", json={"task": "Initial Task", "background": False}, headers=headers)
            assert res1.status_code == 201
            run_id = res1.json()["run_id"]

            # Now saturate capacity with an active background run
            res_bg = client.post("/runs", json={"task": "Saturating Task", "background": True})
            assert res_bg.status_code == 202
            assert store.count_active_admissions() == 1

            # New submission is rejected due to capacity
            res_reject = client.post("/runs", json={"task": "Excess Task", "background": True})
            assert res_reject.status_code == 429

            # But replaying the idempotent key succeeds without consuming capacity!
            res_replay = client.post(
                "/runs",
                json={"task": "Initial Task", "background": False},
                headers=headers,
            )
            assert res_replay.status_code == 201
            assert res_replay.headers.get("Idempotent-Replayed") == "true"
            assert res_replay.json()["run_id"] == run_id
            assert store.count_active_admissions() == 1
    finally:
        hold_event.set()
        manager.shutdown(grace_period=0.1, store=store)
        store.close()


# -----------------------------------------------------------------------------
# 9. Diagnostics Endpoint Reports Global Multi-Worker Usage
# -----------------------------------------------------------------------------


def test_diagnostics_reports_global_multi_worker_state(clean_env):
    """Verify /diagnostics returns global active runs and capacity across all workers."""
    db_file = clean_env
    store = CheckpointStore(db_path=db_file, enabled=True)

    worker_a = RunManager(max_active_runs=4, owner_id="worker-A", store=store)
    worker_b = RunManager(max_active_runs=4, owner_id="worker-B", store=store)

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: worker_a
    client = TestClient(app)

    hold_event = threading.Event()

    def mock_run_task(task, run_id="", *args, **kwargs):
        hold_event.wait(timeout=5.0)
        return {"status": "completed", "run_id": run_id, "task": task, "completed": []}

    try:
        with patch("orchestrator.api.manager.run_task", side_effect=mock_run_task):
            # Worker A has 1 active run
            worker_a.start_run(task="Run A", background=True, store=store)
            # Worker B has 1 active run
            worker_b.start_run(task="Run B", background=True, store=store)

            res = client.get("/diagnostics")
            assert res.status_code == 200
            data = res.json()

            exec_diag = data["execution"]
            # Global active count across workers = 2
            assert exec_diag["active_runs_count"] == 2
            assert exec_diag["max_active_runs"] == 4
            # Available capacity = 4 - 2 = 2
            assert exec_diag["available_capacity"] == 2
            assert exec_diag["global_active_runs_count"] == 2
            assert exec_diag["global_available_capacity"] == 2
            assert exec_diag["owner_id"] == "worker-A"

            limits_diag = data["limits"]
            assert limits_diag["admission_lease_ttl_seconds"] == 30.0
            assert limits_diag["admission_heartbeat_interval_seconds"] == 5.0
    finally:
        hold_event.set()
        worker_a.shutdown(grace_period=0.1, store=store)
        worker_b.shutdown(grace_period=0.1, store=store)
        store.close()
