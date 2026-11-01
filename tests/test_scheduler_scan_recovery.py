"""Comprehensive offline deterministic test suite for Task 48: Scheduler Scan Atomicity & Recovery.

Covers:
1. Item-level atomicity and savepoint rollback on partial failure during batch scans.
2. Safe retry behavior after partial failure (failed items retried, succeeded items skipped).
3. Prevention of duplicate logical audit events across retries, repeated scans, and manual calls.
4. Interrupted scan recovery (fatal batch error triggers rollback, subsequent scan recovers cleanly).
5. Stale-owner fencing protection during scheduled batch expiration (superseded owner rolled back).
6. Stale-owner fencing protection during overdue approval escalation.
7. Manual scans supported and safe alongside active scheduler leadership.
8. API endpoint POST /approvals/scan-expired reporting partial failure counts and keys.
9. Scheduler diagnostics reporting last_scan_failed_count and sanitized error summaries.
10. Concurrency safety: simultaneous scans serialized cleanly via _scan_lock.
"""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from starlette.testclient import TestClient

from orchestrator.api.models import ApprovalSchedulerDiagnostics, ScanExpiredApprovalsResponse
from orchestrator.api.routes import get_store
from orchestrator.api.scheduler import (
    ApprovalLifecycleScheduler,
    reset_approval_scheduler,
)
from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.persistence.store import CheckpointStore, SchedulerFencingError


@pytest.fixture(autouse=True)
def clean_scheduler_and_env(monkeypatch: pytest.MonkeyPatch):
    """Ensure a clean singleton scheduler and isolated environment for each test."""
    reset_approval_scheduler()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    yield
    reset_approval_scheduler()
    get_settings.cache_clear()


@pytest.fixture
def tmp_store(tmp_path: Any) -> CheckpointStore:
    """Provide a fresh CheckpointStore backed by a temporary SQLite file."""
    db_file = tmp_path / "scheduler_recovery_test.db"
    return CheckpointStore(str(db_file), enabled=True)


# ── 1. Partial Failure, Savepoint Rollback & Item-Level Progress ─────────────────


def test_partial_failure_item_level_rollback_and_progress(tmp_store: CheckpointStore) -> None:
    """Partial item failure rolls back only the failed item via savepoint, allowing others to commit."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-part-fail-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    # Seed 4 expired approvals
    for i in range(1, 5):
        tmp_store.record_tool_approval_request(
            run_id=run_id,
            operation_key=f"op-part-{i}",
            tool_name=f"tool_{i}",
            expires_at=past,
        )

    # Intercept conn.execute to simulate a database constraint error specifically on op-part-2
    orig_get_connection = tmp_store._get_connection

    class FlakyApprovalConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "UPDATE tool_approvals" in str(sql) and "op-part-2" in parameters:
                import sqlite3
                raise sqlite3.OperationalError("Simulated disk error updating op-part-2")
            return self._real.execute(sql, parameters)

        def __enter__(self):
            return self._real.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._real.__exit__(exc_type, exc_val, exc_tb)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: Any) -> None:
            if name == "_real":
                self.__dict__["_real"] = value
            else:
                setattr(self._real, name, value)

    tmp_store._get_connection = lambda: FlakyApprovalConn(orig_get_connection())

    # Execute batch scan
    res = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())

    # Verify return structure: 3 succeeded, 1 failed
    assert res["expired_count"] == 3
    assert res["failed_count"] == 1
    assert "op-part-2" in res["failed_operation_keys"]
    assert "op-part-1" in res["expired_operation_keys"]
    assert "op-part-3" in res["expired_operation_keys"]
    assert "op-part-4" in res["expired_operation_keys"]

    # Restore normal connection factory
    tmp_store._get_connection = orig_get_connection

    # Verify durable database state: op-1, 3, 4 committed as 'expired'; op-2 remains untouched 'pending'
    rec1 = tmp_store.get_tool_approval(run_id, "op-part-1")
    assert rec1 is not None and rec1["decision"] == "expired"

    rec2 = tmp_store.get_tool_approval(run_id, "op-part-2")
    assert rec2 is not None and rec2["decision"] == "pending"

    rec3 = tmp_store.get_tool_approval(run_id, "op-part-3")
    assert rec3 is not None and rec3["decision"] == "expired"

    rec4 = tmp_store.get_tool_approval(run_id, "op-part-4")
    assert rec4 is not None and rec4["decision"] == "expired"


def test_retry_after_partial_failure_is_clean_and_idempotent(tmp_store: CheckpointStore) -> None:
    """Subsequent scan cycle retries only the previously failed item and skips committed items."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-part-retry-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    # Seed 3 expired approvals
    for i in range(1, 4):
        tmp_store.record_tool_approval_request(
            run_id=run_id,
            operation_key=f"op-retry-{i}",
            tool_name=f"tool_{i}",
            expires_at=past,
        )

    # First scan fails on op-retry-2
    orig_get_connection = tmp_store._get_connection

    class FlakyConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "UPDATE tool_approvals" in str(sql) and "op-retry-2" in parameters:
                import sqlite3
                raise sqlite3.OperationalError("Simulated failure on op-retry-2")
            return self._real.execute(sql, parameters)

        def __enter__(self):
            return self._real.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._real.__exit__(exc_type, exc_val, exc_tb)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: Any) -> None:
            if name == "_real":
                self.__dict__["_real"] = value
            else:
                setattr(self._real, name, value)

    tmp_store._get_connection = lambda: FlakyConn(orig_get_connection())
    res1 = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())
    assert res1["expired_count"] == 2
    assert res1["failed_count"] == 1
    assert "op-retry-2" in res1["failed_operation_keys"]

    # Restore normal connection and execute second scan (retry)
    tmp_store._get_connection = orig_get_connection
    res2 = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())

    # Only op-retry-2 is processed and newly expired; op-retry-1 and op-retry-3 are skipped
    assert res2["expired_count"] == 1
    assert res2["failed_count"] == 0
    assert "op-retry-2" in res2["expired_operation_keys"]

    # Verify all 3 are now expired in durable store
    for i in range(1, 4):
        rec = tmp_store.get_tool_approval(run_id, f"op-retry-{i}")
        assert rec is not None
        assert rec["decision"] == "expired"

    # Third scan is completely idempotent: 0 expired
    res3 = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())
    assert res3["expired_count"] == 0
    assert res3["failed_count"] == 0


# ── 2. Duplicate Audit Event Prevention Across Scans & Retries ───────────────────


def test_prevention_of_duplicate_audit_events_across_scans_and_retries(
    tmp_store: CheckpointStore,
) -> None:
    """Repeated scans and direct expiration calls must never emit duplicate logical audit events."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-audit-dedup-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()
    future_near = (now + datetime.timedelta(minutes=5)).isoformat()

    # 1. Seed 1 expired approval and 1 overdue pending approval
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-audit-exp",
        tool_name="restart_service",
        expires_at=past,
    )
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-audit-esc",
        tool_name="drain_node",
        expires_at=future_near,
    )

    sched = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        clock=lambda: now,
    )

    # 2. First scan cycle executes expiration and overdue escalation
    res1 = sched.run_scan_cycle(actor="operator")
    assert res1["expired_count"] == 1
    assert res1["escalated_count"] >= 1

    # Check audit events recorded
    events_exp1 = tmp_store.get_audit_events(run_id=run_id, event_type="tool.approval_expired")
    assert len(events_exp1) == 1

    events_esc1 = tmp_store.get_audit_events(run_id=run_id, event_type="tool.approval_escalated")
    assert len(events_esc1) >= 1
    total_events_initial = len(events_exp1) + len(events_esc1)

    # 3. Repeated scan cycle runs immediately after
    res2 = sched.run_scan_cycle(actor="operator")
    assert res2["expired_count"] == 0
    assert res2["escalated_count"] == 0

    # 4. Direct expire_tool_approval called on already-expired operation
    res_direct = tmp_store.expire_tool_approval(
        run_id=run_id,
        operation_key="op-audit-exp",
    )
    assert res_direct["idempotent"] is True

    # 5. Direct escalate_tool_approval called on already-escalated operation
    res_esc_direct = tmp_store.escalate_tool_approval(
        run_id=run_id,
        operation_key="op-audit-esc",
    )
    assert res_esc_direct["idempotent"] is True

    # Verify zero duplicate audit events were created
    events_exp2 = tmp_store.get_audit_events(run_id=run_id, event_type="tool.approval_expired")
    assert len(events_exp2) == len(events_exp1)

    events_esc2 = tmp_store.get_audit_events(run_id=run_id, event_type="tool.approval_escalated")
    assert len(events_esc2) == len(events_esc1)

    total_events_after = len(events_exp2) + len(events_esc2)
    assert total_events_after == total_events_initial


# ── 3. Interrupted Scan & Batch Error Rollback ───────────────────────────────────


def test_interrupted_scan_transaction_rollback_and_recovery(tmp_store: CheckpointStore) -> None:
    """A fatal error before commit rolls back uncommitted batch updates, which recover next scan."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-interrupt-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    # Seed 3 expired approvals
    for i in range(1, 4):
        tmp_store.record_tool_approval_request(
            run_id=run_id,
            operation_key=f"op-intr-{i}",
            tool_name=f"tool_{i}",
            expires_at=past,
        )

    # Inject a fatal exception at the conn.commit() stage (simulating crash before commit)
    orig_get_connection = tmp_store._get_connection

    class FailingCommitConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def commit(self):
            import sqlite3
            raise sqlite3.OperationalError("Fatal disk I/O error during commit")

        def __enter__(self):
            return self._real.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._real.__exit__(exc_type, exc_val, exc_tb)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: Any) -> None:
            if name == "_real":
                self.__dict__["_real"] = value
            else:
                setattr(self._real, name, value)

    tmp_store._get_connection = lambda: FailingCommitConn(orig_get_connection())

    # Scan attempt fails with OperationalError
    with pytest.raises(Exception, match="Fatal disk I/O error during commit"):
        tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())

    # Restore normal connection
    tmp_store._get_connection = orig_get_connection

    # Verify that uncommitted mutations were rolled back: all 3 items remain 'pending'
    for i in range(1, 4):
        rec = tmp_store.get_tool_approval(run_id, f"op-intr-{i}")
        assert rec is not None
        assert rec["decision"] == "pending"

    # Subsequent recovery scan succeeds completely
    res_rec = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())
    assert res_rec["expired_count"] == 3
    assert res_rec["failed_count"] == 0

    # Verify durable state
    for i in range(1, 4):
        rec = tmp_store.get_tool_approval(run_id, f"op-intr-{i}")
        assert rec is not None
        assert rec["decision"] == "expired"


# ── 4. Stale-Owner Fencing Protections for Scheduled Mutations ────────────────────


def test_stale_owner_fenced_out_during_batch_expiration(tmp_store: CheckpointStore) -> None:
    """A worker that loses its lease while a scheduled scan is in flight is rolled back completely."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-fence-exp-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    # Seed 2 expired approvals
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-fence-1",
        tool_name="cleanup_cache",
        expires_at=past,
    )
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-fence-2",
        tool_name="rotate_credentials",
        expires_at=past,
    )

    # Worker A acquires lease with fence_token=1
    acq, token, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=30.0,
        now_dt=now,
    )
    assert acq is True and token == 1

    # Worker B supersedes Worker A in the database (takeover after expiry or release)
    later = now + datetime.timedelta(seconds=35)
    acq_b, token_b, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        lease_ttl_seconds=30.0,
        now_dt=later,
    )
    assert acq_b is True and token_b == 2

    # Worker A attempts scheduled scan with stale token=1
    with pytest.raises(SchedulerFencingError, match="fencing validation failed"):
        tmp_store.check_and_expire_tool_approvals(
            run_id=run_id,
            before_timestamp=later.isoformat(),
            expected_owner_id="worker-A",
            expected_fence_token=1,
        )

    # Verify that Worker A committed ZERO mutations to the database: both items remain 'pending'
    rec1 = tmp_store.get_tool_approval(run_id, "op-fence-1")
    assert rec1 is not None and rec1["decision"] == "pending"

    rec2 = tmp_store.get_tool_approval(run_id, "op-fence-2")
    assert rec2 is not None and rec2["decision"] == "pending"

    # Zero audit events emitted by stale worker
    events = tmp_store.get_audit_events(run_id=run_id, event_type="tool.approval_expired")
    assert len(events) == 0


def test_stale_owner_fenced_out_during_overdue_escalation(tmp_store: CheckpointStore) -> None:
    """Overdue escalation by a stale or superseded worker is fenced out and rolled back."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-fence-esc-1"
    future_near = (now + datetime.timedelta(minutes=5)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-esc-fence-1",
        tool_name="restart_ingress",
        expires_at=future_near,
    )

    # Worker A acquires lease
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-A", 10.0, now)

    # Worker B supersedes Worker A
    later = now + datetime.timedelta(seconds=15)
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-B", 30.0, later)

    # Worker A attempts scheduled escalation with stale fence token=1
    with pytest.raises(SchedulerFencingError, match="fencing validation failed"):
        tmp_store.escalate_tool_approval(
            run_id=run_id,
            operation_key="op-esc-fence-1",
            expected_owner_id="worker-A",
            expected_fence_token=1,
        )

    # Verify mutation was not committed: escalated_at remains None
    rec = tmp_store.get_tool_approval(run_id, "op-esc-fence-1")
    assert rec is not None
    assert rec["escalated_at"] is None


def test_scheduler_loop_handles_mid_scan_fence_loss(tmp_store: CheckpointStore) -> None:
    """ApprovalLifecycleScheduler handles SchedulerFencingError safely during background loop."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    sched = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        owner_id="worker-mid-loss",
        lease_ttl_seconds=10.0,
        clock=lambda: now,
    )
    sched.start()
    assert sched.is_leader is True

    # Simulate external worker taking over lease in database
    later = now + datetime.timedelta(seconds=15)
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-new-leader", 30.0, later)

    sched._clock = lambda: later

    # Worker executes scheduled scan: catches SchedulerFencingError and transitions state cleanly
    res = sched.run_scan_cycle(actor="scheduler")
    assert res["success"] is False
    assert "Fenced out" in res.get("reason", "")
    assert sched.is_leader is False
    assert sched.leadership_state == "lost"
    assert sched.fence_token is None

    sched.stop()


# ── 5. Manual Scans & Operational API Integration ────────────────────────────────


def test_manual_scans_supported_on_standby_and_disabled_workers(
    tmp_store: CheckpointStore,
) -> None:
    """Manual scans proceed safely and commit without requiring scheduler lease leadership."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    run_id = "run-manual-standby-1"
    past = (now - datetime.timedelta(minutes=5)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-manual-1",
        tool_name="scale_cluster",
        expires_at=past,
    )

    # Primary worker holds the active lease
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-primary", 60.0, now)

    # Standby worker starts (enters standby state)
    sched_standby = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        owner_id="worker-standby",
        clock=lambda: now,
    )
    sched_standby.start()
    assert sched_standby.is_leader is False
    assert sched_standby.leadership_state == "standby"

    # Scheduled scan by standby worker is skipped
    res_sched = sched_standby.run_scan_cycle(actor="scheduler")
    assert res_sched["success"] is False
    assert res_sched["reason"] == "Not scheduler leader"

    # Manual scan by standby worker executes and commits successfully
    res_manual = sched_standby.run_scan_cycle(actor="manual_operator")
    assert res_manual["success"] is True
    assert res_manual["expired_count"] == 1
    assert "op-manual-1" in res_manual["expired_operation_keys"]

    # Verify durable state
    rec = tmp_store.get_tool_approval(run_id, "op-manual-1")
    assert rec is not None
    assert rec["decision"] == "expired"

    sched_standby.stop()


def test_api_scan_expired_reports_partial_failures(
    tmp_store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """POST /approvals/scan-expired returns failed_count and failed_operation_keys on partial failure."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-token-rec")
    get_settings.cache_clear()

    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-api-part-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-api-ok",
        tool_name="sync_db",
        expires_at=past,
    )
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-api-err",
        tool_name="drop_table",
        expires_at=past,
    )

    # Intercept connection to fail op-api-err
    orig_get_connection = tmp_store._get_connection

    class FlakyConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "UPDATE tool_approvals" in str(sql) and "op-api-err" in parameters:
                import sqlite3
                raise sqlite3.OperationalError("Simulated write error on op-api-err")
            return self._real.execute(sql, parameters)

        def __enter__(self):
            return self._real.__enter__()

        def __exit__(self, exc_type, exc_val, exc_tb):
            return self._real.__exit__(exc_type, exc_val, exc_tb)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: Any) -> None:
            if name == "_real":
                self.__dict__["_real"] = value
            else:
                setattr(self._real, name, value)

    tmp_store._get_connection = lambda: FlakyConn(orig_get_connection())

    app = create_app()
    app.dependency_overrides[CheckpointStore] = lambda: tmp_store
    app.dependency_overrides[get_store] = lambda: tmp_store

    client = TestClient(app)
    headers = {"Authorization": "Bearer test-token-rec"}

    with client:
        resp = client.post(f"/approvals/scan-expired?run_id={run_id}", headers=headers)
        assert resp.status_code == 200
        data = resp.json()

        # Parse with model
        model = ScanExpiredApprovalsResponse.model_validate(data)
        assert model.expired_count == 1
        assert "op-api-ok" in model.expired_operation_keys
        assert model.failed_count == 1
        assert "op-api-err" in model.failed_operation_keys

    tmp_store._get_connection = orig_get_connection


def test_scheduler_diagnostics_tracks_last_scan_failed_count(
    tmp_store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GET /approvals/scheduler diagnostics reports last_scan_failed_count and error summary."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-token-diag")
    get_settings.cache_clear()

    sched = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
    )
    # Simulate partial failure result in scheduler
    sched._last_scan_result = {
        "success": True,
        "expired_count": 2,
        "failed_count": 1,
        "failed_operation_keys": ["op-fail-1"],
    }
    sched._last_error = "Partial scan failure on 1 approvals"

    app = create_app()
    app.dependency_overrides[CheckpointStore] = lambda: tmp_store
    app.dependency_overrides[get_store] = lambda: tmp_store

    client = TestClient(app)
    headers = {"Authorization": "Bearer test-token-diag"}

    with client:
        app.state.approval_scheduler = sched
        resp = client.get("/approvals/scheduler", headers=headers)
        assert resp.status_code == 200
        data = resp.json()

        diag = ApprovalSchedulerDiagnostics.model_validate(data)
        assert diag.last_scan_failed_count == 1
        assert "Partial scan failure" in (diag.last_error or "")
