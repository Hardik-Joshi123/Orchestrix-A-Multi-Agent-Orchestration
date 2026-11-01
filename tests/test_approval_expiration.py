"""Deterministic offline tests for Task 45: Approval Expiration, Timeouts & Escalation."""

from __future__ import annotations

import datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import (
    CheckpointStore,
    ToolApprovalConflictError,
    parse_utc_iso,
)
from orchestrator.tools import (
    IdempotencyMode,
    RiskLevel,
    Tool,
    ToolApprovalExpiredError,
    ToolExecutionContext,
    ToolPolicy,
    compute_argument_fingerprint,
    derive_operation_key,
    diagnose_tool_approval,
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Ensure clean API authorization and settings."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "valid-secret-token")
    monkeypatch.setenv("ORCHESTRATOR_TOOL_APPROVAL_TIMEOUT_SECONDS", "3600.0")
    monkeypatch.setenv("ORCHESTRATOR_TOOL_APPROVAL_ESCALATION_THRESHOLD_SECONDS", "1800.0")
    monkeypatch.setenv("ORCHESTRATOR_TOOL_APPROVAL_ESCALATION_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_TOOL_APPROVAL_ALLOWED_APPROVERS", "admin,secops,lead_bob")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
def tmp_store(tmp_path: Any) -> CheckpointStore:
    """Return a fresh SQLite CheckpointStore on disk."""
    db_file = tmp_path / "test_approval_expiry.db"
    return CheckpointStore(db_path=str(db_file), enabled=True)


@pytest.fixture
def test_app_and_client(
    clean_env: None,
    tmp_store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TestClient, CheckpointStore, OrchestratorApiClient]:
    """Provide a TestClient and OrchestratorApiClient wired to tmp_store."""
    app = create_app()
    app.dependency_overrides[CheckpointStore] = lambda: tmp_store
    from orchestrator.api.routes import get_store

    app.dependency_overrides[get_store] = lambda: tmp_store
    client = TestClient(app)
    sdk_client = OrchestratorApiClient(
        base_url="http://testserver",
        api_token="valid-secret-token",
        http_client=client,
    )
    return client, tmp_store, sdk_client


# ==============================================================================
# 1. Configuration & Validation Tests
# ==============================================================================


def test_approval_timeout_config_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify validation bounds for timeout and escalation threshold settings."""
    from pydantic import ValidationError

    from orchestrator.config import Settings

    # Valid config
    s = Settings(
        checkpoint_db_path=":memory:",
        tool_approval_timeout_seconds=7200.0,
        tool_approval_escalation_threshold_seconds=3600.0,
    )
    assert s.tool_approval_timeout_seconds == 7200.0
    assert s.tool_approval_escalation_threshold_seconds == 3600.0

    # Bounds: timeout < 1.0 rejected
    with pytest.raises(ValidationError):
        Settings(checkpoint_db_path=":memory:", tool_approval_timeout_seconds=0.5)

    # Bounds: timeout > 604800.0 rejected
    with pytest.raises(ValidationError):
        Settings(checkpoint_db_path=":memory:", tool_approval_timeout_seconds=700000.0)

    # Bounds: threshold > 604800.0 rejected
    with pytest.raises(ValidationError):
        Settings(
            checkpoint_db_path=":memory:",
            tool_approval_escalation_threshold_seconds=800000.0,
        )


def test_tool_policy_per_tool_timeout() -> None:
    """Verify ToolPolicy supports per-tool approval_timeout_seconds and hash updates."""
    p1 = ToolPolicy(tool_name="sensitive_tool", risk_level=RiskLevel.HIGH)
    assert p1.approval_timeout_seconds is None

    p2 = ToolPolicy(
        tool_name="sensitive_tool",
        risk_level=RiskLevel.HIGH,
        approval_timeout_seconds=900.0,
    )
    assert p2.approval_timeout_seconds == 900.0

    from orchestrator.tools import compute_policy_hash

    h1 = compute_policy_hash(p1)
    h2 = compute_policy_hash(p2)
    assert h1 != h2, "Policy hash must reflect per-tool timeout changes"


# ==============================================================================
# 2. Durable Expiration Timestamps & Persistence Across Restart
# ==============================================================================


def test_durable_expiration_timestamps_survive_store_restart(tmp_path: Any) -> None:
    """Verify expires_at, expired_at, and escalation persist across process/connection restarts."""
    db_file = tmp_path / "durable_restart.db"
    store1 = CheckpointStore(db_path=str(db_file), enabled=True)

    run_id = "run-durable-exp"
    op_key = "op-durable-1"
    tool_name = "reboot_host"

    exp_time = (
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=600)
    ).isoformat()

    store1.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        expires_at=exp_time,
    )

    rec1 = store1.get_tool_approval(op_key)
    assert rec1 is not None
    assert rec1["expires_at"] == exp_time
    assert rec1["decision"] == "pending"

    # Escalate
    store1.escalate_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        reason="Nearing deadline",
        escalation_level="director",
        actor="monitor_service",
    )

    # Simulate process restart by opening fresh store instance on same file
    store2 = CheckpointStore(db_path=str(db_file), enabled=True)
    rec2 = store2.get_tool_approval(op_key)
    assert rec2 is not None
    assert rec2["expires_at"] == exp_time
    assert rec2["escalation_reason"] == "Nearing deadline"
    assert rec2["escalated_at"] is not None

    # Now expire
    store2.expire_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        reason="Time elapsed",
        force=True,
        actor="timeout_reaper",
    )

    store3 = CheckpointStore(db_path=str(db_file), enabled=True)
    rec3 = store3.get_tool_approval(op_key)
    assert rec3 is not None
    assert rec3["decision"] == "expired"
    assert rec3["expired_at"] is not None


def test_legacy_database_migration_adds_expiry_columns(tmp_path: Any) -> None:
    """Verify opening a legacy database lacking Task 45 columns non-destructively migrates."""
    import sqlite3

    db_file = str(tmp_path / "legacy.db")
    conn = sqlite3.connect(db_file)
    # Create legacy table without expires_at, expired_at, escalated_at, escalation_reason
    conn.execute(
        """
        CREATE TABLE tool_approvals (
            operation_key TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            tool_name TEXT NOT NULL,
            approver TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            argument_fingerprint TEXT,
            risk_level TEXT,
            policy_hash TEXT,
            revoked_by TEXT,
            revocation_reason TEXT,
            revoked_at TEXT,
            recovered_by TEXT
        );
        """
    )
    conn.execute(
        """
        INSERT INTO tool_approvals VALUES (
            'legacy-op-1', 'run-leg', 'delete_table', 'admin', 'approved',
            'ok', '2026-01-01T00:00:00Z', 'fp1', 'high', 'phash1', NULL, NULL, NULL, NULL
        );
        """
    )
    conn.commit()
    conn.close()

    # Open with CheckpointStore (triggers migrations)
    store = CheckpointStore(db_path=db_file, enabled=True)
    rec = store.get_tool_approval("legacy-op-1")
    assert rec is not None
    assert rec["decision"] == "approved"
    assert rec.get("expires_at") is None
    assert rec.get("expired_at") is None
    assert rec.get("escalated_at") is None


# ==============================================================================
# 3. Expiry Enforcement Pre-Dispatch and During Retry/Resume
# ==============================================================================


def test_expired_approval_blocks_tool_execution(tmp_store: CheckpointStore) -> None:
    """Expired approval strictly cannot authorize tool execution and raises ToolApprovalExpiredError."""
    called = []

    def perform_action(param: str) -> str:
        called.append(param)
        return f"done {param}"

    tool = Tool(
        name="quarantine_vm",
        fn=perform_action,
        schema={"type": "object", "properties": {"param": {"type": "string"}}},
        risk_level=RiskLevel.HIGH,
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
    )

    run_id = "run-exp-enforce"
    subtask_id = "sub-1"
    args = {"param": "vm-99"}
    op_key = derive_operation_key(run_id, subtask_id, tool.name, args, fence_token=1)

    tmp_store.acquire_workflow_admission(run_id=run_id, owner_id="worker-1", max_active_runs=10)

    # Set an already expired timestamp
    past_time = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
    ).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool.name,
        expires_at=past_time,
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="code",
        store=tmp_store,
        owner_id="worker-1",
    )

    # Tool invocation must detect expired approval and raise ToolApprovalExpiredError
    with pytest.raises(ToolApprovalExpiredError) as exc_info:
        tool(context=ctx, **args)

    assert "expired" in str(exc_info.value).lower()
    assert len(called) == 0, "Tool body must never be dispatched on expired approval"


def test_concurrent_expiry_before_dispatch_in_retry_loop(tmp_store: CheckpointStore) -> None:
    """Revalidation immediately before dispatch catches approval expiration occurring after wait/retry."""
    called_count = 0

    def flaky_tool(target: str) -> str:
        nonlocal called_count
        called_count += 1
        return "success"

    tool = Tool(
        name="rotate_credentials",
        fn=flaky_tool,
        schema={"type": "object", "properties": {"target": {"type": "string"}}},
        risk_level=RiskLevel.CRITICAL,
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        max_retries=2,
    )

    run_id = "run-retry-exp"
    subtask_id = "sub-retry"
    args = {"target": "db-prod"}
    op_key = derive_operation_key(run_id, subtask_id, tool.name, args, fence_token=1)

    tmp_store.acquire_workflow_admission(run_id=run_id, owner_id="worker-1", max_active_runs=10)

    # Initial valid approval with short remaining expiry
    short_expiry = (
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3600)
    ).isoformat()

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool.name,
        approver="admin",
        decision="approved",
        expires_at=short_expiry,
        argument_fingerprint=compute_argument_fingerprint(args),
    )

    # Now simulate reaper expiring it before dispatch
    tmp_store.expire_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        reason="Expired right before worker dispatch",
        force=True,
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="admin",
        store=tmp_store,
        owner_id="worker-1",
    )

    with pytest.raises(ToolApprovalExpiredError):
        tool(context=ctx, **args)

    assert called_count == 0, "Dispatched must be aborted when approval expires before dispatch"


# ==============================================================================
# 4. Diagnostics & State Distinction
# ==============================================================================


def test_diagnostics_distinguish_expired_from_rejected_and_revoked(tmp_store: CheckpointStore) -> None:
    """Diagnostics clearly distinguish expired_approval from revoked_approval and rejected_approval."""
    run_id = "run-diag-comp"

    # 1. Expired record
    op_exp = "op-diag-expired"
    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=50)).isoformat()
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_exp,
        tool_name="tool_exp",
        expires_at=past,
    )
    diag_exp = diagnose_tool_approval(run_id=run_id, operation_key=op_exp, store=tmp_store)
    assert diag_exp["diagnostic_code"] == "expired_approval"
    assert diag_exp["state"] == "expired"
    assert diag_exp["is_expired"] is True
    assert diag_exp["is_blocked"] is True
    assert diag_exp["can_execute"] is False

    # 2. Revoked record
    op_rev = "op-diag-revoked"
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_rev,
        tool_name="tool_rev",
        approver="admin",
        decision="approved",
    )
    tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_rev,
        tool_name="tool_rev",
        revoked_by="secops",
        reason="compromised",
    )
    diag_rev = diagnose_tool_approval(run_id=run_id, operation_key=op_rev, store=tmp_store)
    assert diag_rev["diagnostic_code"] == "revoked_approval"
    assert diag_rev["state"] == "revoked"

    # 3. Rejected record
    op_rej = "op-diag-rejected"
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_rej,
        tool_name="tool_rej",
        approver="admin",
        decision="rejected",
        reason="too dangerous",
    )
    diag_rej = diagnose_tool_approval(run_id=run_id, operation_key=op_rej, store=tmp_store)
    assert diag_rej["diagnostic_code"] == "rejected_approval"
    assert diag_rej["state"] == "rejected"


# ==============================================================================
# 5. Safe Timeout Behavior & Operational Recovery
# ==============================================================================


def test_approving_expired_operation_rejected_with_conflict(tmp_store: CheckpointStore) -> None:
    """Cannot approve an already expired approval without first performing operational recovery."""
    run_id = "run-no-direct-app"
    op_key = "op-exp-direct"
    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="purge_cache",
        expires_at=past,
    )

    with pytest.raises(ToolApprovalConflictError) as exc_info:
        tmp_store.record_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name="purge_cache",
            approver="admin",
            decision="approved",
        )

    assert "expired" in str(exc_info.value).lower()


def test_operational_recovery_of_expired_approval_resets_to_pending(tmp_store: CheckpointStore) -> None:
    """Recovery safely resets expired approval to pending with fresh expiration timestamp."""
    run_id = "run-rec-exp"
    op_key = "op-rec-1"
    tool_name = "resize_cluster"
    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=100)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        expires_at=past,
    )

    # Perform operational recovery
    rec_res = tmp_store.recover_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        recovered_by="lead_bob",
        reason="Customer requested re-evaluation after window passed",
        timeout_seconds=1200.0,
    )

    assert rec_res["decision"] == "pending"
    assert rec_res["recovered_by"] == "lead_bob"
    assert rec_res["expires_at"] is not None
    exp_dt = parse_utc_iso(rec_res["expires_at"])
    assert exp_dt is not None
    assert exp_dt > datetime.datetime.now(datetime.timezone.utc)

    # Fresh approval now succeeds
    app_res = tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="lead_bob",
        decision="approved",
        reason="Approved upon re-review",
    )
    assert app_res["decision"] == "approved"


# ==============================================================================
# 6. Escalation, Scanning & Batch Expiry
# ==============================================================================


def test_check_overdue_approvals_identifies_stale_requests(tmp_store: CheckpointStore) -> None:
    """check_overdue_approvals detects pending requests exceeding the escalation threshold."""
    run_id = "run-escalate"
    op1 = "op-fresh"
    op2 = "op-overdue"

    now = datetime.datetime.now(datetime.timezone.utc)
    old_created = (now - datetime.timedelta(seconds=2000)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op1,
        tool_name="t1",
    )

    # Insert older request manually or via request then update created_at
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op2,
        tool_name="t2",
    )
    conn = tmp_store._get_connection()
    conn.execute(
        "UPDATE tool_approvals SET created_at = ? WHERE operation_key = ?",
        (old_created, op2),
    )
    conn.commit()
    if not tmp_store._is_memory:
        conn.close()

    overdue = tmp_store.check_overdue_approvals(run_id=run_id, escalation_threshold_seconds=1800.0)
    overdue_ops = [r["operation_key"] for r in overdue]
    assert op2 in overdue_ops
    assert op1 not in overdue_ops


def test_batch_scan_and_expire_tool_approvals(tmp_store: CheckpointStore) -> None:
    """check_and_expire_tool_approvals scans, marks expired, and auto-escalates."""
    run_id = "run-scan-batch"
    op_exp1 = "op-scan-exp1"
    op_valid = "op-scan-valid"

    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=10)).isoformat()
    future = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3600)).isoformat()

    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_exp1,
        tool_name="scan_tool_1",
        expires_at=past,
    )
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_valid,
        tool_name="scan_tool_2",
        expires_at=future,
    )

    res = tmp_store.check_and_expire_tool_approvals(run_id=run_id, auto_escalate=True)
    assert res["expired_count"] == 1
    assert op_exp1 in res["expired_operation_keys"]
    assert op_valid not in res["expired_operation_keys"]

    # Verify status in store
    rec1 = tmp_store.get_tool_approval(op_exp1)
    assert rec1 is not None
    assert rec1["decision"] == "expired"
    assert rec1["expired_at"] is not None

    # Idempotent second scan
    res2 = tmp_store.check_and_expire_tool_approvals(run_id=run_id)
    assert res2["expired_count"] == 0


# ==============================================================================
# 7. REST API & SDK Client Integration
# ==============================================================================


def test_api_expire_tool_approval(test_app_and_client: tuple) -> None:
    """POST /runs/{run_id}/tools/{operation_key}/expire marks approval expired and emits audit event."""
    client, store, _ = test_app_and_client
    run_id = "run-api-exp"
    op_key = "op-api-1"
    tool_name = "drop_database"

    # Seed pending approval
    store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
    )

    # Call expire endpoint
    resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/expire",
        headers={"Authorization": "Bearer valid-secret-token"},
        json={
            "tool_name": tool_name,
            "reason": "Test manual expiration",
            "actor": "admin",
            "force": True,
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is True
    assert data["decision"] == "expired"
    assert data["expired_at"] != ""
    assert data["audit_event_id"] is not None

    # Verify audit event in store
    events = store.get_audit_events(run_id=run_id)
    exp_evs = [e for e in events if e["event_type"] == AuditEventType.TOOL_APPROVAL_EXPIRED]
    assert len(exp_evs) >= 1
    assert exp_evs[-1]["metadata"]["reason"] == "Test manual expiration"


def test_api_escalate_tool_approval(test_app_and_client: tuple) -> None:
    """POST /runs/{run_id}/tools/{operation_key}/escalate records escalation."""
    client, store, _ = test_app_and_client
    run_id = "run-api-esc"
    op_key = "op-api-esc-1"
    tool_name = "change_dns"

    store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
    )

    resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/escalate",
        headers={"Authorization": "Bearer valid-secret-token"},
        json={
            "tool_name": tool_name,
            "reason": "Overdue by 30 mins",
            "escalation_level": "vp_engineering",
            "actor": "alertmanager",
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["success"] is True
    assert data["escalation_reason"] == "Overdue by 30 mins"
    assert data["escalated_at"] != ""


def test_api_scan_expired_approvals(test_app_and_client: tuple) -> None:
    """POST /runs/{run_id}/approvals/scan-expired triggers batch scan."""
    client, store, _ = test_app_and_client
    run_id = "run-api-scan"
    op_key = "op-api-scan-1"

    past = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=20)).isoformat()
    store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="batch_tool",
        expires_at=past,
    )

    resp = client.post(
        f"/runs/{run_id}/approvals/scan-expired",
        headers={"Authorization": "Bearer valid-secret-token"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["expired_count"] >= 1
    assert op_key in data["expired_operation_keys"]


def test_sdk_client_expire_and_escalate_methods(test_app_and_client: tuple) -> None:
    """Verify OrchestratorApiClient methods for expire, escalate, and scan."""
    _, store, sdk = test_app_and_client
    run_id = "run-sdk-exp"
    op_key = "op-sdk-exp-1"
    tool_name = "purge_audit"

    store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
    )

    # 1. Escalate via SDK
    esc_resp = sdk.escalate_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        reason="Escalating via SDK",
        escalation_level="director",
        actor="lead_bob",
    )
    assert esc_resp.success is True
    assert esc_resp.escalation_reason == "Escalating via SDK"

    # 2. Expire via SDK
    exp_resp = sdk.expire_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        reason="Expiring via SDK",
        actor="lead_bob",
        force=True,
    )
    assert exp_resp.success is True
    assert exp_resp.decision == "expired"

    # 3. Scan via SDK
    scan_resp = sdk.scan_expired_approvals(run_id=run_id)
    assert scan_resp.run_id == run_id
    assert scan_resp.scanned_at != ""
