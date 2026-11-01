"""Tests for Task 43: Approval Integrity, Authorization & Revocation.

Validates exact operation and argument fingerprint binding, approver authorization policies,
safe state transitions, audited revocation, pre-dispatch revalidation, and API integration.
"""

from __future__ import annotations

import tempfile

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import ApiConflictError, OrchestratorApiClient
from orchestrator.api.routes import get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import (
    CheckpointStore,
    ToolApprovalConflictError,
    ToolApprovalNotFoundError,
)
from orchestrator.tools.registry import (
    REGISTRY,
    IdempotencyMode,
    RiskLevel,
    Tool,
    ToolApprovalRequiredError,
    ToolApproverUnauthorizedError,
    ToolExecutionContext,
    ToolExecutionError,
    ToolFencingError,
    ToolPermissionDeniedError,
    ToolPolicy,
    compute_argument_fingerprint,
    compute_policy_hash,
    derive_operation_key,
    verify_approver_authorization,
)


@pytest.fixture
def tmp_store():
    """Create a temporary CheckpointStore backed by a fresh SQLite file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = CheckpointStore(db_path=db_path, enabled=True)
    yield store
    try:
        import os

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
        "integrity_tool",
    ]:
        REGISTRY._tools.pop(tool_name, None)
        REGISTRY._policies.pop(tool_name, None)


# -----------------------------------------------------------------------------
# 1. Argument Fingerprinting & Policy Hashing Unit Tests
# -----------------------------------------------------------------------------


def test_argument_fingerprint_deterministic_and_filtered():
    """Test that compute_argument_fingerprint is deterministic and ignores context and idempotency_key."""
    args1 = {"amount": 1000, "recipient": "acct_42", "currency": "USD"}
    args2 = {"currency": "USD", "amount": 1000, "recipient": "acct_42"}
    args3 = {
        "amount": 1000,
        "recipient": "acct_42",
        "currency": "USD",
        "context": None,
        "idempotency_key": "x",
    }

    fp1 = compute_argument_fingerprint(args1)
    fp2 = compute_argument_fingerprint(args2)
    fp3 = compute_argument_fingerprint(args3)

    assert fp1 == fp2
    assert fp1 == fp3
    assert len(fp1) == 64  # SHA-256 hex string

    # Changing an argument changes the fingerprint
    args_diff = {"amount": 2000, "recipient": "acct_42", "currency": "USD"}
    assert compute_argument_fingerprint(args_diff) != fp1


def test_compute_policy_hash_deterministic():
    """Test that compute_policy_hash changes when policy rules change."""
    pol1 = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["cfo", "secops"],
    )
    pol2 = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["secops", "cfo"],  # different order, same content
    )
    assert compute_policy_hash(pol1) == compute_policy_hash(pol2)

    pol3 = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.CRITICAL,  # escalated risk
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["cfo", "secops"],
    )
    assert compute_policy_hash(pol1) != compute_policy_hash(pol3)


# -----------------------------------------------------------------------------
# 2. Approver Authorization Policy Tests
# -----------------------------------------------------------------------------


def test_verify_approver_authorization_rejects_generic():
    """Test that generic and unverified approver identities are rejected."""
    policy = ToolPolicy(tool_name="t1", authorized_approvers=["*"])

    for generic in ["", "  ", None, "bearer-token", "authenticated-client", "unknown", "anonymous"]:
        with pytest.raises(ToolApproverUnauthorizedError):
            verify_approver_authorization(generic, "t1", policy)


def test_verify_approver_authorization_tool_policy():
    """Test that approver authorization checks tool policy authorized_approvers."""
    policy = ToolPolicy(
        tool_name="wire_transfer",
        authorized_approvers=["cfo_alice", "treasury_bob"],
    )
    # Authorized approvers succeed
    verify_approver_authorization("cfo_alice", "wire_transfer", policy)
    verify_approver_authorization("treasury_bob", "wire_transfer", policy)

    # Unauthorized approver raises ToolApproverUnauthorizedError
    with pytest.raises(ToolApproverUnauthorizedError) as exc_info:
        verify_approver_authorization("intern_eve", "wire_transfer", policy)
    assert "not authorized to approve tool 'wire_transfer'" in str(exc_info.value)


def test_verify_approver_authorization_global_settings(monkeypatch):
    """Test that approver authorization respects Settings.tool_approval_allowed_approvers."""
    from orchestrator import config

    # Mock global settings to allow only secops and lead
    monkeypatch.setattr(
        config,
        "get_settings",
        lambda: Settings(tool_approval_allowed_approvers="secops_lead,audit_manager"),
    )
    policy = ToolPolicy(tool_name="t1", authorized_approvers=["*"])

    # Permitted in global policy
    verify_approver_authorization("secops_lead", "t1", policy)
    verify_approver_authorization("audit_manager", "t1", policy)

    # Not permitted in global policy
    with pytest.raises(ToolApproverUnauthorizedError) as exc_info:
        verify_approver_authorization("developer_dan", "t1", policy)
    assert "not in globally configured allowed approvers" in str(exc_info.value)


# -----------------------------------------------------------------------------
# 3. Store State Transitions and Conflict Handling Tests
# -----------------------------------------------------------------------------


def test_store_approval_idempotent_and_conflict(tmp_store):
    """Test approval recording idempotency and conflict rejections."""
    run_id = "run-trans-1"
    op_key = "toolop:run-trans-1:s1:wire_transfer:11112222"
    tool_name = "wire_transfer"

    # 1. Initial approval
    res1 = tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="cfo_alice",
        decision="approved",
        reason="Budget approved",
        argument_fingerprint="fp123456",
        risk_level="high",
        policy_hash="phash123",
    )
    assert res1["decision"] == "approved"
    assert res1["idempotent"] is False

    # 2. Duplicate identical approval is idempotent
    res2 = tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="cfo_alice",
        decision="approved",
        reason="Budget approved again",
        argument_fingerprint="fp123456",
        risk_level="high",
        policy_hash="phash123",
    )
    assert res2["decision"] == "approved"
    assert res2["idempotent"] is True

    # 3. Conflicting decision (approved -> rejected) raises ToolApprovalConflictError
    with pytest.raises(ToolApprovalConflictError) as exc_info:
        tmp_store.record_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name=tool_name,
            approver="cfo_alice",
            decision="rejected",
            reason="Changed my mind",
        )
    assert "existing decision is 'approved', cannot change to 'rejected'" in str(exc_info.value)


def test_store_revocation_lifecycle(tmp_store):
    """Test revoking an approved operation and revocation idempotency."""
    run_id = "run-trans-2"
    op_key = "toolop:run-trans-2:s1:wire_transfer:33334444"
    tool_name = "wire_transfer"

    # Approve
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        approver="cfo_alice",
        decision="approved",
        reason="Initial go-ahead",
    )

    # Revoke
    rev = tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        revoked_by="secops_lead",
        reason="Compromised credentials detected",
    )
    assert rev["decision"] == "revoked"
    assert rev["revoked_by"] == "secops_lead"
    assert rev["revocation_reason"] == "Compromised credentials detected"
    assert rev["idempotent"] is False
    assert rev["revoked_at"] is not None

    # Idempotent re-revocation
    rev_idem = tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        revoked_by="secops_lead",
        reason="Compromised credentials detected",
    )
    assert rev_idem["idempotent"] is True

    # Cannot approve after revocation
    with pytest.raises(ToolApprovalConflictError) as exc_info:
        tmp_store.record_tool_approval(
            run_id=run_id,
            operation_key=op_key,
            tool_name=tool_name,
            approver="cfo_alice",
            decision="approved",
            reason="Trying to un-revoke",
        )
    assert "has been revoked" in str(exc_info.value)

    # Cannot revoke an explicitly rejected execution
    op_rej = "toolop:run-trans-2:s1:wire_transfer:55556666"
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_rej,
        tool_name=tool_name,
        approver="cfo_alice",
        decision="rejected",
        reason="Denied",
    )
    with pytest.raises(ToolApprovalConflictError):
        tmp_store.revoke_tool_approval(
            run_id=run_id,
            operation_key=op_rej,
            tool_name=tool_name,
            revoked_by="secops_lead",
            reason="Cannot revoke a rejection",
        )


def test_store_revocation_not_found(tmp_store):
    """Test that revoking a non-existent approval raises ToolApprovalNotFoundError."""
    with pytest.raises(ToolApprovalNotFoundError):
        tmp_store.revoke_tool_approval(
            run_id="run-fake",
            operation_key="toolop:run-fake:s1:tool:9999",
            tool_name="fake_tool",
            revoked_by="secops_lead",
            reason="Does not exist",
        )


# -----------------------------------------------------------------------------
# 4. Tool Execution Enforcement: Fingerprint and Policy Freshness Tests
# -----------------------------------------------------------------------------


def test_tool_execution_fingerprint_mismatch_rejected(tmp_store):
    """Test that calling an approved tool with altered arguments is rejected."""
    policy = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["cfo_alice"],
    )

    t = Tool(
        name="wire_transfer",
        fn=lambda amount, recipient: f"sent {amount} to {recipient}",
        schema={
            "type": "object",
            "properties": {"amount": {"type": "integer"}, "recipient": {"type": "string"}},
            "required": ["amount", "recipient"],
        },
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-fp-1"
    subtask_id = "sub-1"
    original_args = {"amount": 100, "recipient": "alice"}
    op_key = derive_operation_key(run_id, subtask_id, "wire_transfer", original_args)
    correct_fp = compute_argument_fingerprint(original_args)

    # Record approval for original_args
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="approved",
        argument_fingerprint=correct_fp,
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        agent_id="finance_specialist",
        operation_key=op_key,
    )

    # Invocation with altered argument (amount=500 instead of 100) must be rejected
    with pytest.raises(ToolApprovalRequiredError) as exc_info:
        t(context=ctx, amount=500, recipient="alice")
    assert "approval argument fingerprint mismatch" in str(exc_info.value)

    # Invocation with exact matching argument succeeds
    res = t(context=ctx, amount=100, recipient="alice")
    assert res == "sent 100 to alice"


def test_tool_execution_policy_change_invalidates_approval(tmp_store):
    """Test that modifying tool policy after approval invalidates stale approval."""
    policy = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["cfo_alice"],
    )

    t = Tool(
        name="wire_transfer",
        fn=lambda amount, recipient: f"sent {amount} to {recipient}",
        schema={
            "type": "object",
            "properties": {"amount": {"type": "integer"}, "recipient": {"type": "string"}},
            "required": ["amount", "recipient"],
        },
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-pol-1"
    subtask_id = "sub-1"
    args = {"amount": 100, "recipient": "alice"}
    op_key = derive_operation_key(run_id, subtask_id, "wire_transfer", args)

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

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        agent_id="finance_specialist",
        operation_key=op_key,
    )

    # Update tool policy (e.g. escalate to CRITICAL)
    updated_policy = ToolPolicy(
        tool_name="wire_transfer",
        risk_level=RiskLevel.CRITICAL,
        allowed_agents=["finance_specialist"],
        requires_approval=True,
        authorized_approvers=["cfo_alice", "ceo_carol"],
    )
    REGISTRY.set_policy(updated_policy)

    # Stale approval must be rejected due to risk level / policy hash mismatch
    with pytest.raises(ToolApprovalRequiredError) as exc_info:
        t(context=ctx, amount=100, recipient="alice")
    assert "policy" in str(exc_info.value).lower()


# -----------------------------------------------------------------------------
# 5. Revocation and Pre-Dispatch Revalidation Tests
# -----------------------------------------------------------------------------


def test_tool_execution_revoked_approval_rejected(tmp_store):
    """Test that invoking a tool whose approval was revoked raises ToolPermissionDeniedError."""
    policy = ToolPolicy(
        tool_name="deploy_service",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["deploy_specialist"],
        requires_approval=True,
        authorized_approvers=["release_manager"],
    )

    t = Tool(
        name="deploy_service",
        fn=lambda service: f"deployed {service}",
        schema={"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-rev-1"
    subtask_id = "sub-1"
    args = {"service": "payments"}
    op_key = derive_operation_key(run_id, subtask_id, "deploy_service", args)

    # Approve
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="deploy_service",
        approver="release_manager",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    # Revoke
    tmp_store.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="deploy_service",
        revoked_by="secops_lead",
        reason="Security vulnerability discovered in payments service",
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        agent_id="deploy_specialist",
        operation_key=op_key,
    )

    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        t(context=ctx, service="payments")
    assert "approval was revoked by 'secops_lead'" in str(exc_info.value)


def test_tool_pre_dispatch_concurrent_revocation_during_retry(tmp_store):
    """Test that pre-dispatch revalidation prevents retry dispatch if revoked during wait."""
    attempt_count = 0

    def flaky_deploy(service: str) -> str:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            # Revoke approval in the store before attempt 2
            tmp_store.revoke_tool_approval(
                run_id="run-retry-1",
                operation_key=op_key,
                tool_name="deploy_service",
                revoked_by="secops_lead",
                reason="Revoked during retry wait",
            )
            raise RuntimeError("temporary processing error")
        return f"deployed {service}"

    policy = ToolPolicy(
        tool_name="deploy_service",
        risk_level=RiskLevel.HIGH,
        allowed_agents=["deploy_specialist"],
        requires_approval=True,
        authorized_approvers=["release_manager"],
    )

    t = Tool(
        name="deploy_service",
        fn=flaky_deploy,
        schema={"type": "object", "properties": {"service": {"type": "string"}}, "required": ["service"]},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        max_retries=2,
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-retry-1"
    subtask_id = "sub-1"
    args = {"service": "api"}
    op_key = derive_operation_key(run_id, subtask_id, "deploy_service", args)

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="deploy_service",
        approver="release_manager",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(args),
        risk_level="high",
        policy_hash=compute_policy_hash(policy),
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        agent_id="deploy_specialist",
        operation_key=op_key,
    )

    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        t(context=ctx, service="api")
    assert "approval was revoked before dispatch" in str(exc_info.value)
    assert attempt_count == 1  # Retry was aborted before second execution!


def test_tool_pre_dispatch_cancellation_and_fencing(tmp_store):
    """Test that cancellation and admission fencing are revalidated immediately post-approval."""
    from orchestrator.graph.cancellation import CancellationToken

    policy = ToolPolicy(tool_name="wire_transfer", risk_level=RiskLevel.HIGH, requires_approval=True)
    t = Tool(
        name="wire_transfer",
        fn=lambda amount: f"sent {amount}",
        schema={"type": "object", "properties": {"amount": {"type": "integer"}}, "required": ["amount"]},
        policy=policy,
    )
    REGISTRY.register(t)

    run_id = "run-pre-1"
    op_key = "toolop:run-pre-1:s1:wire_transfer:9999"
    args = {"amount": 500}

    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name="wire_transfer",
        approver="cfo_alice",
        decision="approved",
        argument_fingerprint=compute_argument_fingerprint(args),
    )

    # 1. Cancellation pre-dispatch
    token = CancellationToken()
    token.cancel("User cancelled workflow")
    ctx_cancelled = ToolExecutionContext(
        run_id=run_id,
        subtask_id="s1",
        store=tmp_store,
        operation_key=op_key,
        cancellation_token=token,
    )
    with pytest.raises(ToolExecutionError) as exc_info:
        t(context=ctx_cancelled, amount=500)
    assert "Tool execution cancelled" in str(exc_info.value)

    # 2. Lost fencing pre-dispatch
    ctx_stale = ToolExecutionContext(
        run_id=run_id,
        subtask_id="s1",
        store=tmp_store,
        owner_id="worker_stale",
        fence_token=1,
        operation_key=op_key,
    )
    with pytest.raises(ToolFencingError) as exc_info:
        t(context=ctx_stale, amount=500)
    assert "admission ownership" in str(exc_info.value)


# -----------------------------------------------------------------------------
# 6. REST API Endpoints & Audit Logging Tests
# -----------------------------------------------------------------------------


def test_api_approval_authorization_and_revocation(tmp_store):
    """Test HTTP API approve, conflict, revocation, and audit trail."""
    app = create_app()
    app.dependency_overrides[get_store] = lambda: tmp_store
    client = TestClient(app)

    run_id = "run-api-auth-1"
    op_key = "toolop:run-api-auth-1:sub-1:wire_transfer:8888"
    tool_name = "wire_transfer"
    args = {"amount": 1000, "recipient": "bob"}

    policy = ToolPolicy(
        tool_name=tool_name,
        risk_level=RiskLevel.HIGH,
        authorized_approvers=["cfo_alice", "treasury_bob"],
    )
    REGISTRY.set_policy(policy)

    # 1. Unauthorized approver (generic bearer-token) rejected with 403
    resp_unauth = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": tool_name,
            "decision": "approved",
            "approver": "bearer-token",
        },
    )
    assert resp_unauth.status_code == 403
    assert "generic or unverified" in resp_unauth.json()["detail"]

    # 2. Unauthorized approver not in policy rejected with 403
    resp_unauth2 = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": tool_name,
            "decision": "approved",
            "approver": "intern_eve",
        },
    )
    assert resp_unauth2.status_code == 403
    assert "not authorized to approve tool" in resp_unauth2.json()["detail"]

    # 3. Authorized approver succeeds with argument fingerprint binding
    resp_ok = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": tool_name,
            "decision": "approved",
            "reason": "Payment authorized",
            "approver": "cfo_alice",
            "arguments": args,
        },
    )
    assert resp_ok.status_code == 200
    app_data = resp_ok.json()
    assert app_data["success"] is True
    assert app_data["decision"] == "approved"
    assert app_data["approver"] == "cfo_alice"
    assert app_data["argument_fingerprint"] == compute_argument_fingerprint(args)
    assert app_data["idempotent"] is False

    # 4. Duplicate identical approval is idempotent
    resp_dup = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": tool_name,
            "decision": "approved",
            "reason": "Payment authorized",
            "approver": "cfo_alice",
            "arguments": args,
        },
    )
    assert resp_dup.status_code == 200
    assert resp_dup.json()["idempotent"] is True

    # 5. Conflicting decision (approved -> rejected) rejected with 409 Conflict
    resp_conflict = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": tool_name,
            "decision": "rejected",
            "reason": "Changed my mind",
            "approver": "cfo_alice",
        },
    )
    assert resp_conflict.status_code == 409
    assert "Conflicting approval decision" in resp_conflict.json()["detail"]

    # 6. Revocation endpoint POST /revoke
    resp_rev = client.post(
        f"/runs/{run_id}/tools/{op_key}/revoke",
        json={
            "tool_name": tool_name,
            "reason": "Suspected fraudulent transfer",
            "revoked_by": "treasury_bob",
        },
    )
    assert resp_rev.status_code == 200
    rev_data = resp_rev.json()
    assert rev_data["success"] is True
    assert rev_data["decision"] == "revoked"
    assert rev_data["revoked_by"] == "treasury_bob"
    assert rev_data["reason"] == "Suspected fraudulent transfer"
    assert rev_data["idempotent"] is False

    # 7. GET /approval returns full revocation metadata
    resp_get = client.get(f"/runs/{run_id}/tools/{op_key}/approval")
    assert resp_get.status_code == 200
    get_data = resp_get.json()
    assert get_data["decision"] == "revoked"
    assert get_data["revoked_by"] == "treasury_bob"
    assert get_data["revocation_reason"] == "Suspected fraudulent transfer"
    assert get_data["argument_fingerprint"] == compute_argument_fingerprint(args)

    # 8. Check audit trail
    events = tmp_store.get_audit_events(run_id=run_id)
    event_types = [e["event_type"] for e in events]
    assert AuditEventType.TOOL_APPROVER_DENIED in event_types
    assert AuditEventType.TOOL_APPROVED in event_types
    assert AuditEventType.TOOL_APPROVAL_REVOKED in event_types


# -----------------------------------------------------------------------------
# 7. Python SDK Client Integration Tests
# -----------------------------------------------------------------------------


def test_api_client_approve_and_revoke_methods(tmp_store):
    """Test OrchestratorApiClient approve_tool_execution and revoke_tool_approval."""
    app = create_app()
    app.dependency_overrides[get_store] = lambda: tmp_store
    client = TestClient(app)

    api_client = OrchestratorApiClient(base_url="http://testserver")
    api_client._client = client

    run_id = "run-client-1"
    op_key = "toolop:run-client-1:s1:wire_transfer:7777"
    tool_name = "wire_transfer"
    args = {"amount": 500}

    policy = ToolPolicy(
        tool_name=tool_name,
        risk_level=RiskLevel.HIGH,
        authorized_approvers=["cfo_alice"],
    )
    REGISTRY.set_policy(policy)

    # 1. Approve via SDK
    app_res = api_client.approve_tool_execution(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        decision="approved",
        reason="Approved by CFO",
        approver="cfo_alice",
        arguments=args,
    )
    assert app_res.success is True
    assert app_res.decision == "approved"
    assert app_res.argument_fingerprint == compute_argument_fingerprint(args)

    # 2. Conflicting rejection via SDK raises ApiConflictError
    with pytest.raises(ApiConflictError):
        api_client.approve_tool_execution(
            run_id=run_id,
            operation_key=op_key,
            tool_name=tool_name,
            decision="rejected",
            reason="Conflict",
            approver="cfo_alice",
        )

    # 3. Revoke via SDK
    rev_res = api_client.revoke_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool_name,
        reason="Revoking for compliance check",
        revoked_by="cfo_alice",
    )
    assert rev_res.success is True
    assert rev_res.decision == "revoked"
    assert rev_res.revoked_by == "cfo_alice"

    # 4. Get approval via SDK
    get_res = api_client.get_tool_approval(run_id=run_id, operation_key=op_key)
    assert get_res.decision == "revoked"
    assert get_res.revoked_by == "cfo_alice"
    assert get_res.revocation_reason == "Revoking for compliance check"
