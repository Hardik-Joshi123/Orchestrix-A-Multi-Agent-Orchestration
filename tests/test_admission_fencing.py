"""Deterministic tests for Admission Lease Fencing & Ownership Safety (Task 38)."""

from __future__ import annotations

import datetime
import sqlite3
import threading
import time
from unittest.mock import patch

import pytest

from orchestrator.api.manager import RunManager
from orchestrator.graph.cancellation import get_cancellation_registry
from orchestrator.persistence.store import (
    AdmissionFencingError,
    CheckpointStore,
)

# -----------------------------------------------------------------------------
# Unit & Integration Tests: Lease Ownership & Fencing Safety
# -----------------------------------------------------------------------------


def test_expired_owner_cannot_renew_or_release_newer_lease(tmp_path) -> None:
    """A stale worker whose lease expired must not renew or release a newer worker's reservation."""
    db_file = tmp_path / "fencing_renewal.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    try:
        run_id = "run-fencing-race-1"
        store.save_checkpoint(run_id, {"status": "pending", "task": "Fencing Task"}, transition="init")

        # Worker A acquires initial admission -> fence_token = 1
        admitted_a = store.acquire_workflow_admission(
            run_id, "worker-A", max_active_runs=5, lease_ttl_seconds=30.0
        )
        assert admitted_a is True

        adm_a = store.get_workflow_admission(run_id)
        assert adm_a is not None
        assert adm_a["owner_id"] == "worker-A"
        assert adm_a["fence_token"] == 1

        # Simulate Worker A's lease expiration by backdating timestamps in DB
        past_iso = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60.0)
        ).isoformat()
        with store._get_db_lock(store.db_path):
            conn = store._get_connection()
            try:
                with conn:
                    conn.execute(
                        "UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                        (past_iso, past_iso, run_id),
                    )
            finally:
                conn.close()

        # Clean stale admissions: Worker A's lease is purged
        stale = store.clean_stale_admissions(lease_ttl_seconds=30.0)
        assert run_id in stale

        # Worker B acquires admission -> fence_token = 2
        admitted_b = store.acquire_workflow_admission(
            run_id, "worker-B", max_active_runs=5, lease_ttl_seconds=30.0
        )
        assert admitted_b is True

        adm_b = store.get_workflow_admission(run_id)
        assert adm_b is not None
        assert adm_b["owner_id"] == "worker-B"
        assert adm_b["fence_token"] == 2

        # 1. Stale Worker A attempts unconditional heartbeat: updates 0 rows
        cnt = store.heartbeat_workflow_admissions("worker-A", [run_id], lease_ttl_seconds=30.0)
        assert cnt == 0

        # 2. Stale Worker A attempts fenced heartbeat: reports failure
        _cnt, renewed, failed = store.heartbeat_active_runs(
            "worker-A",
            {run_id: 1},
            lease_ttl_seconds=30.0,
        )
        assert cnt == 0
        assert run_id not in renewed
        assert run_id in failed

        # 3. Stale Worker A attempts to release with old fence token: fails, does NOT delete Worker B's lease
        released_a = store.release_workflow_admission(run_id, owner_id="worker-A", fence_token=1)
        assert released_a is False

        adm_still_b = store.get_workflow_admission(run_id)
        assert adm_still_b is not None
        assert adm_still_b["owner_id"] == "worker-B"
        assert adm_still_b["fence_token"] == 2

        # 4. Worker B cleanly releases its valid reservation
        released_b = store.release_workflow_admission(run_id, owner_id="worker-B", fence_token=2)
        assert released_b is True
        assert store.get_workflow_admission(run_id) is None
    finally:
        store.close()


def test_delayed_heartbeat_racing_with_lease_reclamation(tmp_path) -> None:
    """A delayed heartbeat from an expired owner must not resurrect a reclaimed lease."""
    db_file = tmp_path / "delayed_heartbeat.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    try:
        run_id = "run-delayed-hb"
        store.save_checkpoint(run_id, {"status": "pending", "task": "Delayed HB Task"}, transition="init")

        store.acquire_workflow_admission(run_id, "worker-slow", max_active_runs=5, lease_ttl_seconds=0.01)

        # Backdate to simulate time passing past TTL
        past_iso = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10.0)
        ).isoformat()
        with store._get_db_lock(store.db_path):
            conn = store._get_connection()
            try:
                with conn:
                    conn.execute(
                        "UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                        (past_iso, past_iso, run_id),
                    )
            finally:
                conn.close()

        # Stale cleaner deletes the reservation
        stale = store.clean_stale_admissions(lease_ttl_seconds=0.01)
        assert run_id in stale
        assert store.get_workflow_admission(run_id) is None

        # Delayed heartbeat arrives
        _cnt, renewed, failed = store.heartbeat_active_runs(
            "worker-slow", {run_id: 1}, lease_ttl_seconds=30.0
        )
        assert run_id in failed
        assert run_id not in renewed

        # Lease must NOT be resurrected
        assert store.get_workflow_admission(run_id) is None
    finally:
        store.close()


def test_temporary_sqlite_errors_during_heartbeat(tmp_path) -> None:
    """Heartbeat loop tolerates temporary SQLite errors within lease TTL,
    but triggers cancellation when TTL expires.
    """
    db_file = tmp_path / "sqlite_errors.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    manager = RunManager(
        max_active_runs=5,
        owner_id="worker-resilient",
        store=store,
        lease_ttl_seconds=2.0,
        heartbeat_interval_seconds=0.05,
    )

    try:
        run_id = "run-db-err"
        store.save_checkpoint(run_id, {"status": "pending", "task": "Resilience Task"}, transition="init")
        store.acquire_workflow_admission(run_id, "worker-resilient", max_active_runs=5, lease_ttl_seconds=2.0)

        cancel_reg = get_cancellation_registry()
        token = cancel_reg.get_or_create(run_id)

        # Register run in manager active runs
        with manager._lock:
            manager._active_runs[run_id] = {
                "run_id": run_id,
                "fence_token": 1,
                "last_heartbeat_at": time.monotonic(),
                "lease_lost": False,
            }

        # Simulate transient database error (e.g. database locked)
        call_count = 0

        def mock_heartbeat_error(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise sqlite3.OperationalError("database is locked")

        with patch.object(store, "heartbeat_active_runs", side_effect=mock_heartbeat_error):
            # Run heartbeat iteration while within lease TTL
            manager._heartbeat_loop_pulse(store)

            # Worker is still within 2.0s lease TTL, so lease_lost is NOT set
            with manager._lock:
                assert manager._active_runs[run_id]["lease_lost"] is False
            assert token.is_cancelled is False

            # Simulate passage of time beyond lease TTL
            with manager._lock:
                manager._active_runs[run_id]["last_heartbeat_at"] = time.monotonic() - 5.0

            # Run heartbeat iteration now that lease TTL has expired without confirmed heartbeat
            manager._heartbeat_loop_pulse(store)

            # Worker detects lease expiration and triggers cooperative cancellation
            with manager._lock:
                assert manager._active_runs[run_id]["lease_lost"] is True
            assert token.is_cancelled is True
            assert "unconfirmed for 2.0s" in token.reason
    finally:
        manager.shutdown(grace_period=0.1, store=store)
        store.close()


def test_worker_losing_ownership_before_state_transition(tmp_path) -> None:
    """A worker whose admission lease is superseded or expired is fenced out from writing checkpoints."""
    db_file = tmp_path / "state_transition_fencing.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    try:
        run_id = "run-fenced-write"
        store.save_checkpoint(run_id, {"status": "pending", "task": "Fenced Write Task"}, transition="init")

        # Worker A acquires admission -> fence_token = 1
        store.acquire_workflow_admission(run_id, "worker-A", max_active_runs=5, lease_ttl_seconds=30.0)
        fenced_store_a = store.with_fencing("worker-A", fence_token=1)

        # Worker A can save checkpoint while owning valid lease
        step1 = fenced_store_a.save_checkpoint(
            run_id, {"status": "running", "task": "Fenced Write Task"}, transition="plan"
        )
        assert step1 > 0

        # Now Worker B takes over (simulating Worker A stall/expiry) -> fence_token = 2
        # First expire Worker A's lease in DB
        past_iso = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60.0)
        ).isoformat()
        with store._get_db_lock(store.db_path):
            conn = store._get_connection()
            try:
                with conn:
                    conn.execute(
                        "UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                        (past_iso, past_iso, run_id),
                    )
            finally:
                conn.close()

        store.clean_stale_admissions(lease_ttl_seconds=30.0)
        store.acquire_workflow_admission(run_id, "worker-B", max_active_runs=5, lease_ttl_seconds=30.0)
        fenced_store_b = store.with_fencing("worker-B", fence_token=2)

        # Worker B saves checkpoint successfully with generation 2
        step2 = fenced_store_b.save_checkpoint(
            run_id,
            {"status": "running", "progress": "subtask 1 done"},
            transition="subtask_complete",
        )
        assert step2 > step1

        # Stale Worker A wakes up and attempts to write state with generation 1 -> Fenced out!
        with pytest.raises(AdmissionFencingError) as exc_info:
            fenced_store_a.save_checkpoint(
                run_id,
                {"status": "failed", "error": "Stale worker failed"},
                transition="error",
            )
        assert "superseded" in str(exc_info.value).lower() or "conflict" in str(exc_info.value).lower()

        # Database remains uncorrupted: latest checkpoint is Worker B's progress
        rec = store.load_latest_checkpoint(run_id)
        assert rec.state["status"] == "running"
        assert rec.state.get("progress") == "subtask 1 done"
    finally:
        store.close()


def test_recovery_and_resume_races_across_separate_managers(tmp_path) -> None:
    """Startup recovery and resumption properly fence older managers attempting delayed state writes."""
    db_file = tmp_path / "recovery_resume_race.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    manager_a = RunManager(
        max_active_runs=5, owner_id="worker-crash-A", store=store, lease_ttl_seconds=30.0
    )
    manager_b = RunManager(
        max_active_runs=5, owner_id="worker-recovery-B", store=store, lease_ttl_seconds=30.0
    )

    try:
        run_id = "run-crash-recover"
        store.save_checkpoint(
            run_id,
            {
                "status": "running",
                "task": "Crash recovery task",
                "plan": [{"id": "s1", "kind": "analysis", "status": "pending"}],
            },
            transition="init",
        )
        store.acquire_workflow_admission(run_id, "worker-crash-A", max_active_runs=5, lease_ttl_seconds=30.0)

        # Manager A crashes: heartbeat stops, lease expires
        past_iso = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60.0)
        ).isoformat()
        with store._get_db_lock(store.db_path):
            conn = store._get_connection()
            try:
                with conn:
                    conn.execute(
                        "UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                        (past_iso, past_iso, run_id),
                    )
            finally:
                conn.close()

        # Manager B boots up and runs recovery: marks run as interrupted
        recovered = manager_b.recover_interrupted_runs(store=store)
        assert run_id in recovered

        rec_interrupted = store.load_latest_checkpoint(run_id)
        assert rec_interrupted.state["status"] == "interrupted"

        # Manager B resumes the run: acquires new admission with fence_token = 2
        def mock_resume_exec(run_id, max_concurrency, store, timeout):
            completed_state = {
                "status": "completed",
                "run_id": run_id,
                "task": "Crash recovery task",
                "completed": [{"id": "s1", "status": "done"}],
            }
            store.save_checkpoint(run_id, completed_state, transition="reduce")
            return completed_state

        with patch("orchestrator.api.manager.resume_task", side_effect=mock_resume_exec):
            res = manager_b.resume_run(run_id, background=False, store=store)
            assert res.status == "completed"

        # Zombie Manager A attempts to persist failure with old fence_token = 1
        with pytest.raises(AdmissionFencingError):
            store.save_checkpoint(
                run_id,
                {"status": "failed", "error": "Late zombie failure"},
                transition="error",
                owner_id="worker-crash-A",
                fence_token=1,
            )

        # Verify final state is completed by Manager B
        rec_final = store.load_latest_checkpoint(run_id)
        assert rec_final.state["status"] == "completed"
    finally:
        manager_a.shutdown(grace_period=0.1, store=store)
        manager_b.shutdown(grace_period=0.1, store=store)
        store.close()


def test_normal_lifecycle_completion_cancellation_timeout_shutdown(tmp_path) -> None:
    """Verifies clean admission lease acquisition, renewal, and release under normal lifecycle events."""
    db_file = tmp_path / "lifecycle_clean.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    manager = RunManager(
        max_active_runs=5, owner_id="worker-lifecycle", store=store, lease_ttl_seconds=30.0
    )

    try:
        # 1. Normal Completion
        def mock_complete(*args, **kwargs):
            return {
                "status": "completed",
                "run_id": "run-complete",
                "task": "Complete task",
                "completed": [],
            }

        with patch("orchestrator.api.manager.run_task", side_effect=mock_complete):
            resp = manager.start_run(
                task="Complete task", run_id="run-complete", background=False, store=store
            )
            assert resp.status == "completed"
            # Lease must be cleanly released upon completion
            assert store.get_workflow_admission("run-complete") is None
            assert store.count_active_admissions() == 0

        # 2. Cancellation
        store.save_checkpoint(
            "run-to-cancel", {"status": "pending", "task": "Cancel task"}, transition="init"
        )
        store.acquire_workflow_admission(
            "run-to-cancel", "worker-lifecycle", max_active_runs=5, lease_ttl_seconds=30.0
        )
        assert store.count_active_admissions() == 1

        cancel_resp = manager.cancel_run("run-to-cancel", reason="User cancel test", store=store)
        assert cancel_resp.status == "cancelled"
        # Lease released upon cancellation
        assert store.get_workflow_admission("run-to-cancel") is None
        assert store.count_active_admissions() == 0

        # 3. Graceful Shutdown
        store.save_checkpoint(
            "run-lingering", {"status": "running", "task": "Lingering task"}, transition="init"
        )
        store.acquire_workflow_admission(
            "run-lingering", "worker-lifecycle", max_active_runs=5, lease_ttl_seconds=30.0
        )
        with manager._lock:
            manager._active_runs["run-lingering"] = {
                "run_id": "run-lingering",
                "fence_token": 1,
                "last_heartbeat_at": time.monotonic(),
                "lease_lost": False,
            }

        # Shutdown cleans up lingering admissions
        manager.shutdown(grace_period=0.1, store=store)
        assert store.get_workflow_admission("run-lingering") is None
        assert store.count_active_admissions() == 0
    finally:
        store.close()


def test_worker_fencing_during_graph_execution(tmp_path) -> None:
    """Mid-workflow fencing loss halts graph execution immediately
    and suppresses persistent failure overwrite.
    """
    db_file = tmp_path / "mid_graph_fencing.db"
    store = CheckpointStore(db_path=str(db_file), enabled=True)

    manager = RunManager(
        max_active_runs=5,
        owner_id="worker-exec-A",
        store=store,
        lease_ttl_seconds=30.0,
    )

    try:
        run_id = "run-mid-fencing"
        takeover_happened = threading.Event()

        def mock_execute_subtask(
            subtask, state_snapshot, settings, run_id, deadline=None, cancellation_token=None
        ):
            # Mid-execution, Worker B preempts and acquires admission lease with fence_token 2
            past_iso = (
                datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60.0)
            ).isoformat()
            with store._get_db_lock(store.db_path):
                conn = store._get_connection()
                try:
                    with conn:
                        conn.execute(
                            "UPDATE workflow_admissions "
                            "SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                            (past_iso, past_iso, run_id),
                        )
                finally:
                    conn.close()

            store.clean_stale_admissions(lease_ttl_seconds=30.0)
            store.acquire_workflow_admission(
                run_id, "worker-exec-B", max_active_runs=5, lease_ttl_seconds=30.0
            )
            fenced_b = store.with_fencing("worker-exec-B", fence_token=2)
            fenced_b.save_checkpoint(
                run_id,
                {"status": "running", "task": "Task", "progress": "Worker B took over"},
                transition="takeover",
            )
            takeover_happened.set()
            return {
                "subtask": {**subtask, "status": "done", "result": "done", "tools_used": []},
                "quality": 0.9,
                "confidence": 0.9,
                "tools_used": [],
                "trace": [],
                "retries": 0,
            }

        with patch(
            "orchestrator.graph.builder._execute_subtask_with_review",
            side_effect=mock_execute_subtask,
        ):
            plan = [{"id": "s1", "kind": "analysis", "status": "pending", "instruction": "Analyze data"}]
            resp = manager.start_run(
                task="Analyze data",
                run_id=run_id,
                plan=plan,
                background=False,
                store=store,
            )
            # Manager A returns failed status in-process due to fencing error
            assert resp.status == "failed"
            assert takeover_happened.is_set()

            # The persistent database was NOT overwritten with Manager A's failed state!
            rec = store.load_latest_checkpoint(run_id)
            assert rec.state["status"] == "running"
            assert rec.state["progress"] == "Worker B took over"
    finally:
        manager.shutdown(grace_period=0.1, store=store)
        store.close()
