"""Deterministic test suite for Task 39: Atomic Fenced Workflow State Transitions."""

from __future__ import annotations

import datetime
import threading

import pytest

from orchestrator.api.manager import RunConflictError, RunManager
from orchestrator.persistence.store import (
    AdmissionFencingError,
    CheckpointConflictError,
    CheckpointStore,
)


def _make_store(tmp_path, name: str = "test_fenced_transitions.db") -> CheckpointStore:
    db_file = tmp_path / name
    return CheckpointStore(db_path=str(db_file), enabled=True)


def test_stale_worker_attempting_all_terminal_transitions(tmp_path):
    """Verify a stale worker cannot transition a run to any terminal or running status."""
    store = _make_store(tmp_path)
    run_id = "run-stale-terminal"
    owner_a = "worker-proc-A"
    owner_b = "worker-proc-B"

    # Worker A acquires generation 1
    assert store.acquire_workflow_admission(run_id, owner_id=owner_a, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {"run_id": run_id, "status": "running", "task": "Stale worker test"},
        transition="init",
        owner_id=owner_a,
        fence_token=1,
    )

    # Worker B supersedes Worker A with generation 2
    # Simulate lease expiry and reclamation by Worker B
    store.release_workflow_admission(run_id, owner_id=owner_a, fence_token=1)
    assert store.acquire_workflow_admission(run_id, owner_id=owner_b, max_active_runs=10)
    summary = store.get_run_summary(run_id)
    assert summary["fence_token"] == 2

    # Worker A (stale generation 1) attempts terminal state transitions:
    # 1. Attempt 'completed'
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "completed", "answer": "stale answer"},
            transition="reduce",
            owner_id=owner_a,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # 2. Attempt 'failed'
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "failed", "error": "stale error"},
            transition="error",
            owner_id=owner_a,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # 3. Attempt 'cancelled'
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "cancelled", "cancellation_reason": "stale cancel"},
            transition="cancelled",
            owner_id=owner_a,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # 4. Attempt 'timed_out'
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "timed_out"},
            transition="timed_out",
            owner_id=owner_a,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # 5. Attempt 'running' subtask update
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "running", "completed": [{"id": "s1"}]},
            transition="subtask_complete",
            owner_id=owner_a,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # Verify database was never polluted by Worker A
    latest_rec = store.load_latest_checkpoint(run_id)
    assert latest_rec.step == 1
    assert latest_rec.state["status"] == "running"
    assert "answer" not in latest_rec.state
    assert "error" not in latest_rec.state


def test_completion_racing_with_cancellation(tmp_path):
    """Verify that if completion commits first, administrative cancellation is rejected."""
    store = _make_store(tmp_path)
    run_id = "run-completion-race"
    owner_id = "worker-winner"

    # Worker acquires and completes the run
    assert store.acquire_workflow_admission(run_id, owner_id=owner_id, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {
            "run_id": run_id,
            "status": "completed",
            "task": "Racing task",
            "answer": "Legitimate final answer",
        },
        transition="reduce",
        owner_id=owner_id,
        fence_token=1,
    )
    store.release_workflow_admission(run_id, owner_id=owner_id, fence_token=1)

    # Database is now completed
    summary = store.get_run_summary(run_id)
    assert summary["status"] == "completed"

    # An administrative cancellation attempt must fail
    with pytest.raises(CheckpointConflictError) as exc_info:
        store.cancel_run_administratively(run_id, reason="Late cancel")
    assert "already reached terminal status 'completed'" in str(exc_info.value)

    # Verify manager cancel_run maps to RunConflictError
    manager = RunManager(store=store)
    with pytest.raises(RunConflictError) as exc_mgr:
        manager.cancel_run(run_id, reason="Manager late cancel", store=store)
    assert "terminal status 'completed'" in str(exc_mgr.value)

    # Ensure run remains completed with answer intact
    latest = store.load_latest_checkpoint(run_id)
    assert latest.state["status"] == "completed"
    assert latest.state["answer"] == "Legitimate final answer"


def test_cancellation_racing_with_completion(tmp_path):
    """Verify that if cancellation commits first, a delayed worker cannot complete the run."""
    store = _make_store(tmp_path)
    run_id = "run-cancel-first"
    owner_id = "worker-slow"

    # Worker starts run at generation 1
    assert store.acquire_workflow_admission(run_id, owner_id=owner_id, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {"run_id": run_id, "status": "running", "task": "Cancel first task"},
        transition="init",
        owner_id=owner_id,
        fence_token=1,
    )

    # Administrative cancellation commits first
    cancel_rec = store.cancel_run_administratively(run_id, reason="User cancelled early")
    assert cancel_rec.state["status"] == "cancelled"

    # DB now reflects cancelled and fence_token has been bumped to 2
    summary = store.get_run_summary(run_id)
    assert summary["status"] == "cancelled"
    assert summary["fence_token"] == 2

    # Delayed worker (generation 1) attempts to write completed
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "completed", "answer": "I finished anyway!"},
            transition="reduce",
            owner_id=owner_id,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # Verify status remains cancelled and corrupted answer was not saved
    final_rec = store.load_latest_checkpoint(run_id)
    assert final_rec.state["status"] == "cancelled"
    assert "I finished anyway!" not in str(final_rec.state)


def test_completion_racing_with_timeout(tmp_path):
    """Verify that once a run is completed, a delayed timeout cannot mark it timed_out."""
    store = _make_store(tmp_path)
    run_id = "run-timeout-race"
    owner_id = "worker-fast"

    assert store.acquire_workflow_admission(run_id, owner_id=owner_id, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {
            "run_id": run_id,
            "status": "completed",
            "task": "Timeout race task",
            "answer": "Fast result",
        },
        transition="reduce",
        owner_id=owner_id,
        fence_token=1,
    )
    store.release_workflow_admission(run_id, owner_id=owner_id, fence_token=1)

    # Delayed timeout attempts to record timed_out
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "timed_out", "error": "Execution deadline exceeded"},
            transition="timed_out",
        )
    assert "already reached terminal status 'completed'" in str(exc_info.value)

    # Checkpoint and runs record remain completed
    rec = store.load_latest_checkpoint(run_id)
    assert rec.state["status"] == "completed"
    assert rec.state["answer"] == "Fast result"


def test_resume_racing_with_delayed_old_worker_update(tmp_path):
    """Verify resume assigns a new generation, fencing out any delayed writes from previous worker."""
    store = _make_store(tmp_path)
    run_id = "run-resume-race"
    owner_1 = "worker-1"
    owner_2 = "worker-2"

    # Worker 1 acquires generation 1 and runs subtask 1
    assert store.acquire_workflow_admission(run_id, owner_id=owner_1, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {
            "run_id": run_id,
            "status": "running",
            "task": "Resumable task",
            "completed": [{"id": "s1", "result": "ok"}],
        },
        transition="subtask_complete",
        owner_id=owner_1,
        fence_token=1,
    )

    # Process crashes or stops: recovered as interrupted (generation 2)
    rec = store.recover_run_as_interrupted(
        run_id,
        reason="Process terminated unexpectedly",
        force=True,
    )
    assert rec is not None
    assert rec.state["status"] == "interrupted"
    summary = store.get_run_summary(run_id)
    assert summary["fence_token"] == 2

    # Worker 2 resumes the run, acquiring generation 3
    assert store.acquire_workflow_admission(run_id, owner_id=owner_2, max_active_runs=10)
    summary2 = store.get_run_summary(run_id)
    assert summary2["fence_token"] == 3

    # Worker 2 saves resume transition
    store.save_checkpoint(
        run_id,
        {
            "run_id": run_id,
            "status": "running",
            "task": "Resumable task",
            "completed": [{"id": "s1", "result": "ok"}],
        },
        transition="resume",
        owner_id=owner_2,
        fence_token=3,
    )

    # Old Worker 1 wakes up from delayed thread/pause and attempts delayed subtask write
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {
                "run_id": run_id,
                "status": "running",
                "task": "Resumable task",
                "completed": [{"id": "s1", "result": "stale"}],
            },
            transition="subtask_complete",
            owner_id=owner_1,
            fence_token=1,
        )
    assert "superseded" in str(exc_info.value) or "lease" in str(exc_info.value)

    # Worker 2 safely finishes subtask 2 and completes the run
    store.save_checkpoint(
        run_id,
        {
            "run_id": run_id,
            "status": "completed",
            "task": "Resumable task",
            "completed": [{"id": "s1", "result": "ok"}, {"id": "s2", "result": "ok"}],
            "answer": "Resumed final answer",
        },
        transition="reduce",
        owner_id=owner_2,
        fence_token=3,
    )
    store.release_workflow_admission(run_id, owner_id=owner_2, fence_token=3)

    final = store.load_latest_checkpoint(run_id)
    assert final.state["status"] == "completed"
    assert final.state["answer"] == "Resumed final answer"
    assert len(final.state["completed"]) == 2


def test_atomic_ownership_validation_and_write(tmp_path):
    """Verify that ownership validation and state update occur atomically inside the transaction."""
    store = _make_store(tmp_path)
    run_id = "run-atomic-val"
    owner_1 = "worker-atomic-1"

    assert store.acquire_workflow_admission(run_id, owner_id=owner_1, max_active_runs=10)
    store.save_checkpoint(
        run_id,
        {"run_id": run_id, "status": "running", "task": "Atomic check"},
        transition="init",
        owner_id=owner_1,
        fence_token=1,
    )

    # Run concurrent threads: one attempting to complete under owner_1, another cancelling administratively
    results: list[str] = []
    barrier = threading.Barrier(2)

    def do_complete():
        barrier.wait()
        try:
            store.save_checkpoint(
                run_id,
                {"run_id": run_id, "status": "completed", "answer": "Done"},
                transition="reduce",
                owner_id=owner_1,
                fence_token=1,
            )
            results.append("completed")
        except Exception as exc:
            results.append(f"complete_err:{type(exc).__name__}")

    def do_cancel():
        barrier.wait()
        try:
            store.cancel_run_administratively(run_id, reason="Concurrent cancel")
            results.append("cancelled")
        except Exception as exc:
            results.append(f"cancel_err:{type(exc).__name__}")

    t1 = threading.Thread(target=do_complete)
    t2 = threading.Thread(target=do_cancel)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Exactly one must succeed and the other must be rejected cleanly
    assert len(results) == 2
    final_summary = store.get_run_summary(run_id)
    assert final_summary["status"] in ("completed", "cancelled")
    if final_summary["status"] == "completed":
        assert any("cancel_err" in r for r in results)
    else:
        assert any("complete_err" in r for r in results)


def test_legitimate_shutdown_and_recovery_transitions(tmp_path):
    """Verify shutdown recovery bumps fence tokens, shields live workers, and cleanly transitions
    dead runs.
    """
    store = _make_store(tmp_path)
    run_dead = "run-abandoned"
    run_live = "run-healthy"
    owner_dead = "worker-dead"
    owner_live = "worker-live"

    # Dead run: acquired, but lease expired
    assert store.acquire_workflow_admission(run_dead, owner_id=owner_dead, max_active_runs=10)
    store.save_checkpoint(
        run_dead,
        {"run_id": run_dead, "status": "running", "task": "Dead run task"},
        transition="init",
        owner_id=owner_dead,
        fence_token=1,
    )

    # Live run: acquired with fresh heartbeat
    assert store.acquire_workflow_admission(run_live, owner_id=owner_live, max_active_runs=10)
    store.save_checkpoint(
        run_live,
        {"run_id": run_live, "status": "running", "task": "Live run task"},
        transition="init",
        owner_id=owner_live,
        fence_token=1,
    )

    # Expire dead run's lease in database directly
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            past_iso = (
                datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60)
            ).isoformat()
            conn.execute(
                "UPDATE workflow_admissions SET expires_at = ? WHERE run_id = ?;",
                (past_iso, run_dead),
            )
            conn.commit()
        finally:
            conn.close()

    # Run recovery
    recovered_ids = store.recover_interrupted_runs(lease_ttl_seconds=30.0)
    assert run_dead in recovered_ids
    assert run_live not in recovered_ids

    # Verify dead run is now interrupted with fence_token=2
    dead_summary = store.get_run_summary(run_dead)
    assert dead_summary["status"] == "interrupted"
    assert dead_summary["fence_token"] == 2

    # Verify live run remains running with fence_token=1
    live_summary = store.get_run_summary(run_live)
    assert live_summary["status"] == "running"
    assert live_summary["fence_token"] == 1


def test_terminal_state_cannot_be_regressed_by_unfenced_calls(tmp_path):
    """Verify that even unfenced calls cannot regress a completed workflow back to running."""
    store = _make_store(tmp_path)
    run_id = "run-terminal-regression"

    # Complete the run
    store.save_checkpoint(
        run_id,
        {"run_id": run_id, "status": "completed", "answer": "Final output"},
        transition="reduce",
    )

    # Attempt to overwrite with running status via raw unfenced save_checkpoint
    with pytest.raises(AdmissionFencingError) as exc_info:
        store.save_checkpoint(
            run_id,
            {"run_id": run_id, "status": "running", "task": "Attempted regression"},
            transition="state_update",
        )
    assert "already reached terminal status 'completed'" in str(exc_info.value)

    # Verify run remains completed
    summary = store.get_run_summary(run_id)
    assert summary["status"] == "completed"
