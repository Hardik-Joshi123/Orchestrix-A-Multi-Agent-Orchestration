"""Tests for Task 49: Atomic Approval State & Audit Persistence.

Verifies:
1. Audit-write failure rolls back single approval expiration.
2. Audit-write failure rolls back individual batch scan items via savepoints without aborting the batch.
3. Audit-write failure rolls back approval decisions (approved / rejected).
4. Audit-write failure rolls back revocation.
5. Audit-write failure rolls back recovery.
6. Successful atomic commit persists both approval state and audit event simultaneously.
7. Retrying after transient audit failure recovers state cleanly and preserves audit deduplication.
8. Legacy records with state transitions but missing audit events are handled safely without fake backfills.
9. Stale-owner fencing validation rolls back both approval state mutations and audit events.
10. Concurrently executed approval lifecycle transitions maintain atomic consistency.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import sqlite3
from typing import Any

import pytest

from orchestrator.api.scheduler import ApprovalLifecycleScheduler, reset_approval_scheduler
from orchestrator.config import get_settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import (
    CheckpointStore,
    SchedulerFencingError,
)


@pytest.fixture(autouse=True)
def clean_scheduler_and_env(monkeypatch: pytest.MonkeyPatch):
    """Ensure clean singleton scheduler and isolated environment for each test."""
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
    db_file = tmp_path / "atomic_approval_audit_test.db"
    return CheckpointStore(str(db_file), enabled=True)


# ── 1. Audit Write Failure & Single Expiration Rollback ──────────────────────────


def test_audit_write_failure_rolls_back_single_approval_expiration(
    tmp_store: CheckpointStore,
) -> None:
    """When audit write fails during expire_tool_approval, the approval update is rolled back."""
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-atomic-exp-1"
    op_key = "op-atomic-exp-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="database_migration",
        expires_at=past,
    )

    orig_get_connection = tmp_store._get_connection

    class FailingAuditConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql):
                raise sqlite3.OperationalError("Simulated disk error writing audit event")
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

    tmp_store._get_connection = lambda: FailingAuditConn(orig_get_connection())

    # Expiration attempt must fail due to audit write failure
    with pytest.raises(sqlite3.OperationalError, match="Simulated disk error writing audit event"):
        tmp_store.expire_tool_approval(run_id=run_id, operation_key=op_key)

    # Restore normal connection
    tmp_store._get_connection = orig_get_connection

    # State must be rolled back: decision remains pending, expired_at remains None
    record = tmp_store.get_tool_approval(run_id, op_key)
    assert record is not None
    assert record["decision"] == "pending"
    assert record["expired_at"] is None

    # Zero audit events persisted
    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_EXPIRED)
    assert len(events) == 0


# ── 2. Audit Write Failure & Batch Savepoint Rollback ────────────────────────────


def test_audit_write_failure_rolls_back_batch_item_in_scheduler_scan(
    tmp_store: CheckpointStore,
) -> None:
    """When audit write fails on an item during batch scan, only that item rolls back via savepoint."""
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-batch-audit-fail"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    # Seed 3 expired approvals
    for i in range(1, 4):
        tmp_store.record_tool_approval_request(
            run_id=run_id,
            operation_key=f"op-batch-audit-{i}",
            tool_name=f"service_job_{i}",
            expires_at=past,
        )

    orig_get_connection = tmp_store._get_connection

    class FailingAuditItemConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql) and "op-batch-audit-2" in str(parameters):
                raise sqlite3.OperationalError("Simulated audit write failure on op-batch-audit-2")
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

    tmp_store._get_connection = lambda: FailingAuditItemConn(orig_get_connection())

    res = tmp_store.check_and_expire_tool_approvals(run_id=run_id, before_timestamp=now.isoformat())

    # 2 succeeded, 1 failed
    assert res["expired_count"] == 2
    assert res["failed_count"] == 1
    assert "op-batch-audit-2" in res["failed_operation_keys"]
    assert "op-batch-audit-1" in res["expired_operation_keys"]
    assert "op-batch-audit-3" in res["expired_operation_keys"]

    tmp_store._get_connection = orig_get_connection

    # In database: items 1 and 3 are expired; item 2 was rolled back and remains pending
    rec1 = tmp_store.get_tool_approval(run_id, "op-batch-audit-1")
    assert rec1 is not None and rec1["decision"] == "expired"

    rec2 = tmp_store.get_tool_approval(run_id, "op-batch-audit-2")
    assert rec2 is not None and rec2["decision"] == "pending"

    rec3 = tmp_store.get_tool_approval(run_id, "op-batch-audit-3")
    assert rec3 is not None and rec3["decision"] == "expired"

    # Audit events: items 1 and 3 have exactly 1 event; item 2 has zero
    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_EXPIRED)
    matched_keys = [str(e.get("metadata") or e.get("metadata_json") or "") for e in events]
    assert any("op-batch-audit-1" in m for m in matched_keys)
    assert any("op-batch-audit-3" in m for m in matched_keys)
    assert not any("op-batch-audit-2" in m for m in matched_keys)


# ── 3. Audit Write Failure & Decision Rollback ───────────────────────────────────


def test_audit_write_failure_rolls_back_approval_decision(
    tmp_store: CheckpointStore,
) -> None:
    """When audit write fails during record_tool_approval, decision is rolled back to pending."""
    run_id = "run-dec-audit-fail"
    op_key = "op-dec-audit-1"

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="grant_admin_access",
    )

    orig_get_connection = tmp_store._get_connection

    class FailingAuditConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql):
                raise sqlite3.OperationalError("Simulated write error inserting approval audit event")
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

    tmp_store._get_connection = lambda: FailingAuditConn(orig_get_connection())

    with pytest.raises(
        sqlite3.OperationalError, match="Simulated write error inserting approval audit event"
    ):
        tmp_store.record_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name="grant_admin_access",
            approver="secops_lead",
            decision="approved",
            reason="Approved after review",
        )

    tmp_store._get_connection = orig_get_connection

    # Approval was rolled back; remains pending
    rec = tmp_store.get_tool_approval(run_id, op_key)
    assert rec is not None
    assert rec["decision"] == "pending"

    # Zero approved audit events
    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVED)
    assert len(events) == 0


# ── 4. Audit Write Failure & Revocation Rollback ─────────────────────────────────


def test_audit_write_failure_rolls_back_revocation(
    tmp_store: CheckpointStore,
) -> None:
    """When audit write fails during revoke_tool_approval, decision is rolled back to approved."""
    run_id = "run-rev-audit-fail"
    op_key = "op-rev-audit-1"

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="restart_gateway",
    )
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="restart_gateway",
        approver="operator_1",
        decision="approved",
    )

    orig_get_connection = tmp_store._get_connection

    class FailingAuditConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql) and "revoked" in str(parameters):
                raise sqlite3.OperationalError("Simulated disk error writing revocation audit event")
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

    tmp_store._get_connection = lambda: FailingAuditConn(orig_get_connection())

    with pytest.raises(sqlite3.OperationalError, match="Simulated disk error writing revocation"):
        tmp_store.revoke_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name="restart_gateway",
            revoked_by="secops_admin",
            reason="Compromised token",
        )

    tmp_store._get_connection = orig_get_connection

    # State was rolled back: decision remains approved, not revoked
    rec = tmp_store.get_tool_approval(run_id, op_key)
    assert rec is not None
    assert rec["decision"] == "approved"
    assert rec["revoked_at"] is None

    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_REVOKED)
    assert len(events) == 0


# ── 5. Audit Write Failure & Recovery Rollback ───────────────────────────────────


def test_audit_write_failure_rolls_back_recovery(
    tmp_store: CheckpointStore,
) -> None:
    """When audit write fails during recover_tool_approval, state is rolled back to expired."""
    run_id = "run-recov-audit-fail"
    op_key = "op-recov-audit-1"
    now = datetime.datetime.now(datetime.timezone.utc)
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="rebalance_partitions",
        expires_at=past,
    )
    tmp_store.expire_tool_approval(run_id, op_key)

    rec_initial = tmp_store.get_tool_approval(run_id, op_key)
    assert rec_initial is not None and rec_initial["decision"] == "expired"

    orig_get_connection = tmp_store._get_connection

    class FailingAuditConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql) and "recovered" in str(parameters):
                raise sqlite3.OperationalError("Simulated disk error on recovery audit event")
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

    tmp_store._get_connection = lambda: FailingAuditConn(orig_get_connection())

    with pytest.raises(sqlite3.OperationalError, match="Simulated disk error on recovery"):
        tmp_store.recover_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name="rebalance_partitions",
            recovered_by="admin_ops",
            reason="Partition rebalance approved for second attempt",
        )

    tmp_store._get_connection = orig_get_connection

    # State remains expired, recovered_by remains None
    rec_after = tmp_store.get_tool_approval(run_id, op_key)
    assert rec_after is not None
    assert rec_after["decision"] == "expired"
    assert rec_after["recovered_by"] is None

    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_RECOVERED)
    assert len(events) == 0


# ── 6. Successful Atomic Commit of State & Audit Event ───────────────────────────


def test_successful_atomic_commit_persists_both_state_and_audit(
    tmp_store: CheckpointStore,
) -> None:
    """Successful state transition persists approval state and audit event in a single transaction."""
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-atomic-success-1"
    op_key = "op-atomic-success-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="rotate_kms_key",
        expires_at=past,
    )

    res = tmp_store.expire_tool_approval(run_id=run_id, operation_key=op_key, actor="operator_bob")
    assert res["decision"] == "expired"
    assert res["audit_event_id"] is not None

    # Verify both exist durably in SQLite
    approval = tmp_store.get_tool_approval(run_id, op_key)
    assert approval is not None
    assert approval["decision"] == "expired"

    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_EXPIRED)
    assert len(events) == 1
    assert events[0]["event_id"] == res["audit_event_id"]

    # Consistency check helper confirms consistency
    diag = tmp_store.verify_approval_audit_consistency(op_key)
    assert diag["consistent"] is True
    assert diag["has_audit_event"] is True
    assert diag["is_legacy_uncommitted_audit"] is False


# ── 7. Retry After Transient Audit Failure & Deduplication ───────────────────────


def test_retry_after_transient_audit_failure_recovers_and_deduplicates(
    tmp_store: CheckpointStore,
) -> None:
    """When a transient audit write error clears on retry, state commits and deduplication holds."""
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-retry-dedup-1"
    op_key = "op-retry-dedup-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="flush_queue",
        expires_at=past,
    )

    # 1. Attempt 1: Transient audit write failure
    orig_get_connection = tmp_store._get_connection

    class FailingAuditConn:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            if "INSERT OR IGNORE INTO audit_events" in str(sql):
                raise sqlite3.OperationalError("Simulated transient lock on audit_events")
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

    tmp_store._get_connection = lambda: FailingAuditConn(orig_get_connection())
    with pytest.raises(sqlite3.OperationalError):
        tmp_store.expire_tool_approval(run_id, op_key)

    # 2. Attempt 2: Error clears; retry succeeds
    tmp_store._get_connection = orig_get_connection
    res_retry = tmp_store.expire_tool_approval(run_id, op_key)
    assert res_retry["decision"] == "expired"

    # 3. Attempt 3: Idempotent repeat
    res_repeat = tmp_store.expire_tool_approval(run_id, op_key)
    assert res_repeat["idempotent"] is True

    # Exactly 1 audit event in total
    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_EXPIRED)
    assert len(events) == 1


# ── 8. Safe Handling of Legacy Records Without Audit Events ──────────────────────


def test_legacy_record_without_audit_event_safe_and_not_retroactively_invented(
    tmp_store: CheckpointStore,
) -> None:
    """Legacy records without audit events are queryable and not retrofitted with fake historical events."""
    run_id = "run-legacy-1"
    op_key = "op-legacy-expired-1"
    now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Directly seed an approval in SQLite without creating an audit event (simulating legacy data)
    with tmp_store._get_db_lock(tmp_store.db_path):
        conn = tmp_store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO tool_approvals (
                        operation_key, run_id, tool_name, approver, decision, reason, created_at,
                        argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                        revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    ) VALUES (
                        ?, ?, 'legacy_tool', '', 'expired', 'Expired in Task 45', ?,
                        NULL, 'high', NULL, NULL, NULL, NULL, NULL, ?, ?, NULL, NULL
                    );
                    """,
                    (op_key, run_id, now_str, now_str, now_str),
                )
        finally:
            if not tmp_store._is_memory:
                conn.close()

    # Verify diagnostic detects legacy status without modifying records
    diag = tmp_store.verify_approval_audit_consistency(op_key)
    assert diag["exists"] is True
    assert diag["decision"] == "expired"
    assert diag["has_audit_event"] is False
    assert diag["is_legacy_uncommitted_audit"] is True
    assert diag["consistent"] is False

    # Scheduler scan does not touch already-expired legacy records or invent fake events
    res_scan = tmp_store.check_and_expire_tool_approvals(run_id=run_id)
    assert res_scan["expired_count"] == 0

    # Zero audit events were fabricated
    events = tmp_store.get_audit_events(run_id=run_id)
    assert len(events) == 0


# ── 9. Stale-Owner Fencing Rollback of State & Audit Mutations ────────────────────


def test_stale_scheduled_worker_fenced_out_rolls_back_both_state_and_audit(
    tmp_store: CheckpointStore,
) -> None:
    """When a worker loses lease leadership, fencing rollback reverts both state changes and audit events."""
    now = datetime.datetime.now(datetime.timezone.utc)
    run_id = "run-fence-atomic-1"
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-fence-atomic-1",
        tool_name="kill_pod",
        expires_at=past,
    )

    # Worker A acquires lease
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-A", 10.0, now)

    # Worker B supersedes Worker A
    later = now + datetime.timedelta(seconds=15)
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-B", 30.0, later)

    # Worker A attempts scheduled scan with stale token
    with pytest.raises(SchedulerFencingError, match="fencing validation failed"):
        tmp_store.check_and_expire_tool_approvals(
            run_id=run_id,
            before_timestamp=later.isoformat(),
            expected_owner_id="worker-A",
            expected_fence_token=1,
        )

    # In database: Worker A committed ZERO mutations
    rec = tmp_store.get_tool_approval(run_id, "op-fence-atomic-1")
    assert rec is not None and rec["decision"] == "pending"

    # Worker A committed ZERO audit events
    events = tmp_store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_APPROVAL_EXPIRED)
    assert len(events) == 0


# ── 10. Concurrently Executed Approval Transitions ───────────────────────────────


def test_concurrent_scans_and_manual_approvals_maintain_atomic_consistency(
    tmp_store: CheckpointStore,
) -> None:
    """Concurrent threads executing scans and approvals commit atomic state and audit events safely."""
    run_id = "run-concurrent-atomic"
    num_items = 8
    now = datetime.datetime.now(datetime.timezone.utc)
    past = (now - datetime.timedelta(minutes=10)).isoformat()

    for i in range(num_items):
        tmp_store.record_tool_approval_request(
            run_id=run_id,
            operation_key=f"op-conc-{i}",
            tool_name=f"task_{i}",
            expires_at=past,
        )

    def worker_scan():
        sched = ApprovalLifecycleScheduler(store=tmp_store, enabled=True)
        return sched.run_scan_cycle(actor="worker_scanner")

    def worker_approve(idx: int):
        try:
            return tmp_store.record_tool_approval(
                run_id=run_id,
                operation_key=f"op-conc-{idx}",
                tool_name=f"task_{idx}",
                approver=f"approver_{idx}",
                decision="approved",
            )
        except Exception as e:
            return {"error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        f_scan = executor.submit(worker_scan)
        f_apps = [executor.submit(worker_approve, i) for i in range(num_items)]
        f_scan.result()
        for f in f_apps:
            f.result()

    # For every item in database, check that its state and audit events are strictly consistent
    for i in range(num_items):
        op_key = f"op-conc-{i}"
        rec = tmp_store.get_tool_approval(run_id, op_key)
        assert rec is not None
        assert rec["decision"] in ("approved", "expired")

        # Consistency check confirms no un-audited state exists
        diag = tmp_store.verify_approval_audit_consistency(op_key)
        assert diag["consistent"] is True
        assert diag["has_audit_event"] is True
        assert diag["audit_event_count"] >= 1
