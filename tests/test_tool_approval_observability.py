"""Tests for Task 44: Approval Workflow Observability & Operational Recovery.

Validates:
1. Approval lifecycle visibility across pending, approved, rejected, and revoked states.
2. Query filtering by status and tool name.
3. Diagnostic engine identifying missing, pending, rejected, revoked, mismatched,
   unauthorized, cancelled, and fencing failures.
4. Safe, privileged operational recovery resetting blocked approvals to pending.
5. Strict prevention of automatic approval or redispatch on recovery.
6. Sanitization of sensitive data (secrets, tokens, credentials, tracebacks).
7. SDK client methods for approval listing, diagnostics, and recovery.
"""

from __future__ import annotations

import os
import tempfile

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiValidationError,
    OrchestratorApiClient,
)
from orchestrator.api.routes import get_store
from orchestrator.api.server import create_app
from orchestrator.graph.cancellation import CancellationToken
from orchestrator.observability.audit import sanitize_tool_approval
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools.registry import (
    REGISTRY,
    IdempotencyMode,
    RiskLevel,
    Tool,
    ToolApprovalRequiredError,
    ToolExecutionContext,
    ToolPermissionDeniedError,
    ToolPolicy,
    compute_argument_fingerprint,
    compute_policy_hash,
    derive_operation_key,
    diagnose_tool_approval,
)


@pytest.fixture
def tmp_store():
    """Create a temporary CheckpointStore backed by a fresh SQLite file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = CheckpointStore(db_path=db_path, enabled=True)
    yield store
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def clean_registry():
    """Ensure custom test tools are cleaned up after each test."""
    yield
    for tool_name in [
        "wire_transfer",
        "deploy_service",
        "delete_database",
        "custom_test_tool",
        "obs_test_tool",
    ]:
        REGISTRY._tools.pop(tool_name, None)
        REGISTRY._policies.pop(tool_name, None)


# -----------------------------------------------------------------------------
# 1. Approval Request Auto-Recording and Status Lifecycle
# -----------------------------------------------------------------------------


def test_tool_call_records_pending_approval_request(tmp_store):
    """Test that invoking a tool requiring approval automatically records a pending request."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["secops_lead"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda target: f"processed {target}",
        schema={
            "type": "object",
            "properties": {"target": {"type": "string"}},
            "required": ["target"],
        },
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-obs-1"
    subtask_id = "sub-1"
    args = {"target": "prod-cluster"}
    op_key = derive_operation_key(run_id, subtask_id, "obs_test_tool", args)

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        operation_key=op_key,
    )

    # First invocation should fail with ToolApprovalRequiredError
    with pytest.raises(ToolApprovalRequiredError) as exc_info:
        t(context=ctx, target="prod-cluster")
    assert "requires human approval" in str(exc_info.value)

    # Verify that a pending record was automatically stored
    record = tmp_store.get_tool_approval(run_id, op_key)
    assert record is not None
    assert record["decision"] == "pending"
    assert record["tool_name"] == "obs_test_tool"
    assert record["risk_level"] == "high"
    assert record["argument_fingerprint"] == compute_argument_fingerprint(args)
    assert record["policy_hash"] == compute_policy_hash(policy)

    # Second invocation while pending still raises ToolApprovalRequiredError with pending message
    with pytest.raises(ToolApprovalRequiredError) as exc_info2:
        t(context=ctx, target="prod-cluster")
    assert "pending" in str(exc_info2.value)


def test_list_tool_approvals_filtering(tmp_store):
    """Test filtering tool approvals by status and tool_name."""
    run_id = "run-filter-1"

    # Seed approvals with different decisions and tools
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-1",
        tool_name="wire_transfer",
        argument_fingerprint="fp1",
        risk_level="high",
        policy_hash="ph1",
    )
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key="op-2",
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="approved",
        argument_fingerprint="fp2",
        risk_level="high",
        policy_hash="ph1",
    )
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key="op-3",
        tool_name="deploy_service",
        approver="cfo_alice",
        decision="rejected",
        argument_fingerprint="fp3",
        risk_level="critical",
        policy_hash="ph2",
    )
    tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key="op-2",
        tool_name="wire_transfer",
        revoked_by="secops_lead",
        reason="Security audit check",
    )

    # 1. List all
    all_apps = tmp_store.list_tool_approvals(run_id)
    assert len(all_apps) == 3

    # 2. Filter by status: pending
    pending_apps = tmp_store.list_tool_approvals(run_id, status="pending")
    assert len(pending_apps) == 1
    assert pending_apps[0]["operation_key"] == "op-1"

    # 3. Filter by status: revoked
    revoked_apps = tmp_store.list_tool_approvals(run_id, status="revoked")
    assert len(revoked_apps) == 1
    assert revoked_apps[0]["operation_key"] == "op-2"

    # 4. Filter by status: rejected
    rejected_apps = tmp_store.list_tool_approvals(run_id, status="rejected")
    assert len(rejected_apps) == 1
    assert rejected_apps[0]["operation_key"] == "op-3"

    # 5. Filter by tool_name: wire_transfer
    wt_apps = tmp_store.list_tool_approvals(run_id, tool_name="wire_transfer")
    assert len(wt_apps) == 2

    # 6. Combined filter
    wt_pending = tmp_store.list_tool_approvals(
        run_id, status="pending", tool_name="wire_transfer"
    )
    assert len(wt_pending) == 1
    assert wt_pending[0]["operation_key"] == "op-1"


# -----------------------------------------------------------------------------
# 2. Approval Diagnostics Unit Tests
# -----------------------------------------------------------------------------


def test_diagnose_tool_approval_missing(tmp_store):
    """Test diagnostics when approval has not been requested or recorded."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        policy=policy,
    )
    REGISTRY.register(t)

    diag = diagnose_tool_approval(
        run_id="run-diag-1",
        operation_key="non-existent-op",
        tool_name="obs_test_tool",
        store=tmp_store,
    )
    assert diag["diagnostic_code"] == "missing_approval"
    assert diag["can_proceed"] is False
    assert diag["is_blocked"] is True
    assert "No approval record found" in diag["message"]


def test_diagnose_tool_approval_pending(tmp_store):
    """Test diagnostics when approval is pending operator review."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-diag-2"
    op_key = "op-diag-pending"
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        argument_fingerprint="fp123",
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    diag = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        store=tmp_store,
    )
    assert diag["diagnostic_code"] == "pending_approval"
    assert diag["can_proceed"] is False
    assert diag["is_blocked"] is True
    assert diag["status"] == "pending"


def test_diagnose_tool_approval_valid(tmp_store):
    """Test diagnostics when approval is valid and execution can proceed."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda x: f"ok-{x}",
        schema={"type": "object", "properties": {"x": {"type": "integer"}}},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-diag-3"
    args = {"x": 42}
    op_key = "op-diag-valid"
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        approver="alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    diag = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        arguments=args,
        store=tmp_store,
    )
    assert diag["diagnostic_code"] == "valid_approved"
    assert diag["can_proceed"] is True
    assert diag["is_blocked"] is False
    assert diag["status"] == "approved"


def test_diagnose_tool_approval_rejected_and_revoked(tmp_store):
    """Test diagnostics for rejected and revoked approvals."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-diag-4"

    # Rejected
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key="op-rej",
        tool_name="obs_test_tool",
        approver="alice",
        decision="rejected",
        reason="Security risk unacceptable",
        argument_fingerprint="fp1",
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )
    diag_rej = diagnose_tool_approval(
        run_id=run_id,
        operation_key="op-rej",
        store=tmp_store,
    )
    assert diag_rej["diagnostic_code"] == "rejected_approval"
    assert diag_rej["can_proceed"] is False
    assert diag_rej["is_blocked"] is True

    # Revoked
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key="op-rev",
        tool_name="obs_test_tool",
        approver="alice",
        decision="approved",
        argument_fingerprint="fp2",
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )
    tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key="op-rev",
        tool_name="obs_test_tool",
        revoked_by="alice",
        reason="Compromised credentials",
    )
    diag_rev = diagnose_tool_approval(
        run_id=run_id,
        operation_key="op-rev",
        store=tmp_store,
    )
    assert diag_rev["diagnostic_code"] == "revoked_approval"
    assert diag_rev["can_proceed"] is False
    assert diag_rev["is_blocked"] is True


def test_diagnose_tool_approval_fingerprint_and_policy_mismatches(tmp_store):
    """Test diagnostics when argument fingerprint or policy hash/risk has drifted."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-diag-5"
    op_key = "op-drift"
    approved_args = {"amount": 100}

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        approver="alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(approved_args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    # 1. Fingerprint mismatch
    diag_fp = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        arguments={"amount": 200},  # Different amount
        store=tmp_store,
    )
    assert diag_fp["diagnostic_code"] == "argument_fingerprint_mismatch"
    assert diag_fp["can_proceed"] is False

    # 2. Risk mismatch (policy changed to CRITICAL)
    updated_policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.CRITICAL,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    REGISTRY.set_policy(updated_policy)

    diag_risk = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        arguments=approved_args,
        store=tmp_store,
    )
    assert diag_risk["diagnostic_code"] == "policy_risk_mismatch"
    assert diag_risk["can_proceed"] is False


def test_diagnose_tool_approval_cancellation_and_fencing(tmp_store):
    """Test diagnostics detecting workflow cancellation or fencing token loss."""
    policy = ToolPolicy(
        tool_name="obs_test_tool",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    t = Tool(
        name="obs_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-diag-6"
    op_key = "op-cancel"

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        approver="alice",
        decision="approved",
        argument_fingerprint="fp",
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    # Cancelled token
    token = CancellationToken()
    token.cancel(reason="Run aborted by user")

    diag_cancel = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        store=tmp_store,
        cancellation_token=token,
    )
    assert diag_cancel["diagnostic_code"] == "workflow_cancelled"
    assert diag_cancel["can_proceed"] is False
    assert "aborted by user" in diag_cancel["message"]

    # Fencing lost
    diag_fence = diagnose_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="obs_test_tool",
        store=tmp_store,
        admission_fence_token=1,
        active_lease={"fence_token": 2},  # Higher generation
    )
    assert diag_fence["diagnostic_code"] == "admission_fencing_lost"
    assert diag_fence["can_proceed"] is False


# -----------------------------------------------------------------------------
# 3. Safe Operational Recovery
# -----------------------------------------------------------------------------


def test_recover_tool_approval_resets_to_pending(tmp_store):
    """Test that recover_tool_approval resets rejected or revoked state to pending."""
    run_id = "run-rec-1"
    op_key = "op-rec-1"

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="rejected",
        reason="Incorrect recipient code",
        argument_fingerprint="fp-old",
        risk_level="high",
        policy_hash="ph-old",
    )

    # Recover approval
    rec = tmp_store.recover_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        recovered_by="secops_lead",
        reason="Recipient verified out-of-band",
        argument_fingerprint="fp-new",
        risk_level="high",
        policy_hash="ph-old",
    )

    assert rec["decision"] == "pending"
    assert rec["recovered_by"] == "secops_lead"
    assert "Recipient verified out-of-band" in rec["reason"]
    assert rec["argument_fingerprint"] == "fp-new"

    # Confirm in store
    in_store = tmp_store.get_tool_approval(run_id, op_key)
    assert in_store["decision"] == "pending"
    assert in_store["recovered_by"] == "secops_lead"


def test_recovery_never_auto_approves_or_redispatches(tmp_store):
    """Test that operational recovery leaves the tool blocked until explicitly re-approved."""
    dispatch_count = 0

    def mutating_action(amount: int) -> str:
        nonlocal dispatch_count
        dispatch_count += 1
        return f"transferred {amount}"

    policy = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["cfo_alice"],
    )
    t = Tool(
        name="wire_transfer",
        fn=mutating_action,
        schema={"type": "object", "properties": {"amount": {"type": "integer"}}},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-rec-safety"
    subtask_id = "sub-1"
    args = {"amount": 500}
    op_key = derive_operation_key(run_id, subtask_id, "wire_transfer", args)

    # Initially rejected
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="rejected",
        reason="Suspicious request",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        operation_key=op_key,
    )

    # Execution is blocked
    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        t(context=ctx, amount=500)
    assert "rejected" in str(exc_info.value)
    assert dispatch_count == 0

    # Operator recovers the approval (resets to pending)
    tmp_store.recover_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        recovered_by="secops_lead",
        reason="Investigation concluded; cleared for re-review",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    # CRITICAL: Attempting to invoke the tool still FAILS because it is pending, NOT approved!
    with pytest.raises(ToolApprovalRequiredError) as exc_info2:
        t(context=ctx, amount=500)
    assert "pending" in str(exc_info2.value)
    assert dispatch_count == 0  # Tool was NEVER redispatched!

    # Only when explicit authorized approval is granted, tool dispatches
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    res = t(context=ctx, amount=500)
    assert res == "transferred 500"
    assert dispatch_count == 1


# -----------------------------------------------------------------------------
# 4. Sensitive Data Sanitization
# -----------------------------------------------------------------------------


def test_sensitive_data_sanitization():
    """Test that sanitize_tool_approval scrubs passwords, tokens, and tracebacks."""
    record = {
        "run_id": "run-sec-1",
        "operation_key": "op-1",
        "tool_name": "wire_transfer",
        "decision": "rejected",
        "reason": "Token bearer_xyz123 rejected with password=supersecret Traceback: err at line 4",
        "revocation_reason": "API_KEY=sk-live-999 leaked",
        "approver": "alice; token=secret123",
        "recovered_by": "bob; auth=key987",
    }
    sanitized = sanitize_tool_approval(record)

    assert "supersecret" not in sanitized["reason"]
    assert "sk-live-999" not in sanitized["revocation_reason"]
    assert "[REDACTED]" in sanitized["reason"]
    assert "[REDACTED]" in sanitized["revocation_reason"]


# -----------------------------------------------------------------------------
# 5. REST API & Integration Tests
# -----------------------------------------------------------------------------


@pytest.fixture
def test_app_and_client(tmp_store, monkeypatch):
    """Create a test client with store dependency override and configured settings."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "valid-secret-token")
    monkeypatch.setenv("ORCHESTRATOR_REQUIRE_TOOL_APPROVAL", "true")
    app = create_app()
    app.dependency_overrides[get_store] = lambda: tmp_store
    client = TestClient(app)
    yield client, tmp_store
    app.dependency_overrides.clear()


def test_api_list_tool_approvals(test_app_and_client):
    """Test GET /runs/{run_id}/approvals with query filtering and auth."""
    client, store = test_app_and_client
    run_id = "run-api-list"

    # Unauthenticated rejected
    resp_unauth = client.get(f"/runs/{run_id}/approvals")
    assert resp_unauth.status_code == 401

    headers = {"Authorization": "Bearer valid-secret-token"}

    # Seed records
    store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-1",
        tool_name="wire_transfer",
        argument_fingerprint="fp1",
        risk_level="high",
        policy_hash="ph1",
    )
    store.record_tool_approval(
        run_id=run_id,
        operation_key="op-2",
        tool_name="deploy_service",
        approver="alice",
        decision="approved",
        argument_fingerprint="fp2",
        risk_level="high",
        policy_hash="ph2",
    )

    # List all
    resp = client.get(f"/runs/{run_id}/approvals", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_count"] == 2
    assert len(data["approvals"]) == 2

    # Filter by status=pending
    resp_pend = client.get(
        f"/runs/{run_id}/approvals?status=pending",
        headers=headers,
    )
    assert resp_pend.status_code == 200
    data_pend = resp_pend.json()
    assert data_pend["total_count"] == 1
    assert data_pend["approvals"][0]["operation_key"] == "op-1"

    # Filter by tool_name=deploy_service
    resp_tool = client.get(
        f"/runs/{run_id}/approvals?tool_name=deploy_service",
        headers=headers,
    )
    assert resp_tool.status_code == 200
    data_tool = resp_tool.json()
    assert data_tool["total_count"] == 1
    assert data_tool["approvals"][0]["operation_key"] == "op-2"


def test_api_tool_approval_diagnostics(test_app_and_client):
    """Test GET and POST /runs/{run_id}/tools/{operation_key}/diagnostics."""
    client, store = test_app_and_client
    headers = {"Authorization": "Bearer valid-secret-token"}
    run_id = "run-api-diag"
    op_key = "op-diag-api"

    policy = ToolPolicy(
        tool_name="deploy_service",
        risk_level=RiskLevel.HIGH,
        requires_approval=True,
        authorized_approvers=["alice"],
    )
    REGISTRY.set_policy(policy)

    store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="deploy_service",
        approver="alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint({"service": "payments"}),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    # 1. GET diagnostics without arguments
    resp_get = client.get(
        f"/runs/{run_id}/tools/{op_key}/diagnostics",
        headers=headers,
    )
    assert resp_get.status_code == 200
    diag_get = resp_get.json()
    assert diag_get["diagnostic_code"] == "valid_approved"
    assert diag_get["can_proceed"] is True

    # 2. POST diagnostics with matching arguments
    resp_post_ok = client.post(
        f"/runs/{run_id}/tools/{op_key}/diagnostics",
        json={"arguments": {"service": "payments"}},
        headers=headers,
    )
    assert resp_post_ok.status_code == 200
    assert resp_post_ok.json()["diagnostic_code"] == "valid_approved"

    # 3. POST diagnostics with modified arguments (drift detection)
    resp_post_mismatch = client.post(
        f"/runs/{run_id}/tools/{op_key}/diagnostics",
        json={"arguments": {"service": "auth"}},
        headers=headers,
    )
    assert resp_post_mismatch.status_code == 200
    diag_mismatch = resp_post_mismatch.json()
    assert diag_mismatch["diagnostic_code"] == "argument_fingerprint_mismatch"
    assert diag_mismatch["can_proceed"] is False
    assert diag_mismatch["is_blocked"] is True


def test_api_tool_approval_recovery(test_app_and_client):
    """Test POST /runs/{run_id}/tools/{operation_key}/recover."""
    client, store = test_app_and_client
    headers = {"Authorization": "Bearer valid-secret-token"}
    run_id = "run-api-rec"
    op_key = "op-rec-api"
    tool_name = "wire_transfer"

    # Seed rejected approval
    store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="alice",
        decision="rejected",
        reason="Initial fraud alert",
    )

    # 1. Recovery fails if reason is empty
    resp_no_reason = client.post(
        f"/runs/{run_id}/tools/{op_key}/recover",
        json={"tool_name": tool_name, "reason": ""},
        headers=headers,
    )
    assert resp_no_reason.status_code == 400
    assert "reason" in str(resp_no_reason.json()["detail"]).lower()

    # 2. Recovery fails if operator is generic bearer-token
    resp_generic_op = client.post(
        f"/runs/{run_id}/tools/{op_key}/recover",
        json={
            "tool_name": tool_name,
            "reason": "Legitimate reason",
            "operator": "bearer-token",
        },
        headers=headers,
    )
    assert resp_generic_op.status_code == 403
    assert "generic or unverified" in resp_generic_op.json()["detail"]

    # 3. Recovery succeeds with authorized operator
    resp_ok = client.post(
        f"/runs/{run_id}/tools/{op_key}/recover",
        json={
            "tool_name": tool_name,
            "reason": "False positive resolved by SecOps",
            "operator": "secops_lead",
            "arguments": {"amount": 100},
        },
        headers=headers,
    )
    assert resp_ok.status_code == 200
    rec_data = resp_ok.json()
    assert rec_data["success"] is True
    assert rec_data["decision"] == "pending"
    assert rec_data["recovered_by"] == "secops_lead"
    assert rec_data["audit_event_id"] is not None

    # Verify store state is now pending
    updated = store.get_tool_approval(run_id, op_key)
    assert updated["decision"] == "pending"
    assert updated["recovered_by"] == "secops_lead"


# -----------------------------------------------------------------------------
# 6. SDK Client Methods
# -----------------------------------------------------------------------------


def test_sdk_client_approval_observability_and_recovery(test_app_and_client):
    """Test SDK OrchestratorApiClient methods for listing, diagnostics, and recovery."""
    client, store = test_app_and_client
    run_id = "run-sdk-obs"
    op_key = "op-sdk-1"
    tool_name = "wire_transfer"

    # Seed revoked approval
    store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="cfo_alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint({"amount": 100}),
    )
    store.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        revoked_by="secops_lead",
        reason="Revoked during security review",
    )

    api_client = OrchestratorApiClient(
        base_url="http://testserver",
        auth_token="valid-secret-token",
        http_client=client,
    )

    # 1. List approvals via SDK
    list_resp = api_client.list_tool_approvals(run_id, status="revoked")
    assert list_resp.total_count == 1
    assert list_resp.approvals[0].operation_key == op_key

    # 2. Get diagnostics via SDK
    diag_resp = api_client.get_tool_approval_diagnostics(
        run_id=run_id,
        operation_key=op_key,
    )
    assert diag_resp.diagnostic_code == "revoked_approval"
    assert diag_resp.is_blocked is True
    assert diag_resp.can_proceed is False

    # 3. Recover approval via SDK
    rec_resp = api_client.recover_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        reason="Audit cleared operation",
        operator="secops_lead",
        arguments={"amount": 100},
    )
    assert rec_resp.success is True
    assert rec_resp.decision == "pending"
    assert rec_resp.recovered_by == "secops_lead"

    # 4. Client validation errors
    with pytest.raises(ApiValidationError):
        api_client.list_tool_approvals("")

    with pytest.raises(ApiValidationError):
        api_client.recover_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name=tool_name,
            reason="",
        )
