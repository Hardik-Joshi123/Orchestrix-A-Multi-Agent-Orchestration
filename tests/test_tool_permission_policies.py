"""Tests for Task 42: Tool Permission Policies & Risk-Based Approval.

Verifies:
- Agent authorization gates (allowed vs denied agent identities)
- Risk-based approval requirements (LOW, MEDIUM, HIGH, CRITICAL)
- Post-approval revalidation of cancellation, authorization, and fencing
- Persisted approvals in SQLite store across retries/resumes
- Non-bypassability via direct registry calls or specialist dispatch
- Audit logging for policy denials, approval requirements, and approvals
- API endpoints and client SDK for tool approval workflow
"""

import tempfile
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.routes import get_store
from orchestrator.api.server import create_app
from orchestrator.graph.cancellation import CancellationToken
from orchestrator.graph.specialists import _run_specialist
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools import (
    REGISTRY,
    IdempotencyMode,
    RiskLevel,
    Tool,
    ToolApproval,
    ToolApprovalRequiredError,
    ToolExecutionContext,
    ToolExecutionError,
    ToolFencingError,
    ToolNotFoundError,
    ToolPermissionDeniedError,
    ToolPolicy,
    derive_operation_key,
)


@pytest.fixture
def tmp_store():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = CheckpointStore(db_path=db_path)
    yield store
    try:
        import os

        os.unlink(db_path)
    except Exception:
        pass


def test_allowed_agent_can_invoke_tool():
    """Agents in allowed_agents list can invoke the tool successfully."""
    called = []

    def sample_code_fn(code: str) -> str:
        called.append(code)
        return f"executed: {code}"

    tool = Tool(
        name="execute_python_code",
        fn=sample_code_fn,
        schema={"type": "object", "properties": {"code": {"type": "string"}}},
        allowed_agents=["code", "analysis"],
        risk_level=RiskLevel.LOW,
    )

    ctx = ToolExecutionContext(
        run_id="run-p1",
        subtask_id="sub-1",
        agent_id="code",
    )
    result = tool(context=ctx, code="print('hello')")
    assert result == "executed: print('hello')"
    assert called == ["print('hello')"]


def test_unauthorized_agent_rejected_before_dispatch():
    """Agents not in allowed_agents list are rejected before tool execution."""
    called = []

    def restricted_fn(cmd: str) -> str:
        called.append(cmd)
        return "done"

    tool = Tool(
        name="run_terminal_cmd",
        fn=restricted_fn,
        schema={"type": "object", "properties": {"cmd": {"type": "string"}}},
        allowed_agents=["code"],
        risk_level=RiskLevel.MEDIUM,
    )

    ctx = ToolExecutionContext(
        run_id="run-p2",
        subtask_id="sub-1",
        agent_id="research",
    )

    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        tool(context=ctx, cmd="rm -rf /")

    assert "Agent 'research' is not authorized" in str(exc_info.value)
    assert len(called) == 0  # fn was never called


def test_unspecified_agent_rejected_when_agents_restricted():
    """Context without an agent_id is rejected when allowed_agents is restricted."""
    tool = Tool(
        name="deploy_service",
        fn=lambda service: "deployed",
        schema={"type": "object", "properties": {"service": {"type": "string"}}},
        allowed_agents=["ops"],
        risk_level=RiskLevel.LOW,
    )

    ctx = ToolExecutionContext(run_id="run-p3", subtask_id="sub-1", agent_id=None)
    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        tool(context=ctx, service="api")

    assert "Agent 'unspecified' is not authorized" in str(exc_info.value)


def test_wildcard_allowed_agents_permits_any_agent():
    """Default wildcard allowed_agents=['*'] permits any agent or unspecified agent."""
    tool = Tool(
        name="read_documentation",
        fn=lambda topic: f"docs for {topic}",
        schema={"type": "object", "properties": {"topic": {"type": "string"}}},
        allowed_agents=["*"],
    )

    ctx1 = ToolExecutionContext(run_id="run-p4", subtask_id="sub-1", agent_id="research")
    assert tool(context=ctx1, topic="raft") == "docs for raft"

    ctx2 = ToolExecutionContext(run_id="run-p4", subtask_id="sub-2", agent_id=None)
    assert tool(context=ctx2, topic="paxos") == "docs for paxos"


def test_high_and_critical_risk_tools_require_approval_by_default():
    """Tools with HIGH or CRITICAL risk levels require human approval by default."""
    called = []

    def mutate_account(account_id: str, amount: int) -> str:
        called.append((account_id, amount))
        return f"debited {amount} from {account_id}"

    high_risk_tool = Tool(
        name="transfer_funds",
        fn=mutate_account,
        schema={
            "type": "object",
            "properties": {"account_id": {"type": "string"}, "amount": {"type": "integer"}},
        },
        risk_level=RiskLevel.HIGH,
    )
    crit_risk_tool = Tool(
        name="wipe_cluster",
        fn=lambda: "wiped",
        schema={"type": "object"},
        risk_level=RiskLevel.CRITICAL,
    )

    ctx = ToolExecutionContext(run_id="run-risk-1", subtask_id="sub-1", agent_id="code")

    with pytest.raises(ToolApprovalRequiredError) as exc_info1:
        high_risk_tool(context=ctx, account_id="acc-123", amount=500)
    assert "requires human approval" in str(exc_info1.value)
    assert len(called) == 0

    with pytest.raises(ToolApprovalRequiredError) as exc_info2:
        crit_risk_tool(context=ctx)
    assert "requires human approval" in str(exc_info2.value)


def test_explicit_in_memory_approval_satisfies_requirement():
    """ToolExecutionContext carrying ToolApproval with decision='approved' allows execution."""
    called = []

    def write_payload(payload: str) -> str:
        called.append(payload)
        return f"saved: {payload}"

    tool = Tool(
        name="write_storage_block",
        fn=write_payload,
        schema={"type": "object", "properties": {"payload": {"type": "string"}}},
        risk_level=RiskLevel.HIGH,
    )

    approval = ToolApproval(approver="alice@company.com", decision="approved", reason="Verified by ops")
    ctx = ToolExecutionContext(
        run_id="run-app-1",
        subtask_id="sub-1",
        agent_id="code",
        approval=approval,
    )

    res = tool(context=ctx, payload="block-101")
    assert res == "saved: block-101"
    assert called == ["block-101"]


def test_explicit_in_memory_rejection_raises_permission_denied():
    """ToolExecutionContext carrying ToolApproval with decision='rejected' blocks execution."""
    called = []

    tool = Tool(
        name="restart_gateway",
        fn=lambda: called.append(1) or "restarted",
        schema={"type": "object"},
        risk_level=RiskLevel.HIGH,
    )

    approval = ToolApproval(
        approver="security@company.com", decision="rejected", reason="Maintenance window closed"
    )
    ctx = ToolExecutionContext(
        run_id="run-app-2",
        subtask_id="sub-1",
        agent_id="ops",
        approval=approval,
    )

    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        tool(context=ctx)
    assert "explicitly rejected" in str(exc_info.value)
    assert "Maintenance window closed" in str(exc_info.value)
    assert len(called) == 0


def test_persisted_approval_in_store_satisfies_requirement(tmp_store):
    """Approval recorded in CheckpointStore satisfies the approval requirement."""
    called = []

    def deploy_image(image_tag: str) -> str:
        called.append(image_tag)
        return f"deployed {image_tag}"

    tool = Tool(
        name="deploy_container_image",
        fn=deploy_image,
        schema={"type": "object", "properties": {"image_tag": {"type": "string"}}},
        risk_level=RiskLevel.HIGH,
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
    )

    run_id = "run-store-app"
    subtask_id = "sub-1"
    args = {"image_tag": "v1.4.2"}
    op_key = derive_operation_key(run_id, subtask_id, tool.name, args, fence_token=1)

    # Worker-1 acquires admission first
    tmp_store.acquire_workflow_admission(run_id=run_id, owner_id="worker-1", max_active_runs=10)

    # Initial invocation without store approval raises ToolApprovalRequiredError
    ctx_unapproved = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="code",
        store=tmp_store,
        owner_id="worker-1",
    )
    with pytest.raises(ToolApprovalRequiredError):
        tool(context=ctx_unapproved, **args)
    assert len(called) == 0

    # Human operator records approval in database
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool.name,
        approver="lead-dev@company.com",
        decision="approved",
        reason="Release 1.4.2 approved",
    )

    # Re-invocation with same store and op_key succeeds
    ctx_approved = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="code",
        store=tmp_store,
        owner_id="worker-1",
    )
    res = tool(context=ctx_approved, **args)
    assert res == "deployed v1.4.2"
    assert called == ["v1.4.2"]


def test_persisted_rejection_in_store_blocks_execution(tmp_store):
    """Rejection recorded in CheckpointStore prevents tool execution."""
    called = []
    tool = Tool(
        name="delete_table",
        fn=lambda table: called.append(table) or "dropped",
        schema={"type": "object", "properties": {"table": {"type": "string"}}},
        risk_level=RiskLevel.HIGH,
    )

    run_id = "run-store-rej"
    subtask_id = "sub-1"
    args = {"table": "customers"}
    op_key = derive_operation_key(run_id, subtask_id, tool.name, args, fence_token=1)

    tmp_store.acquire_workflow_admission(run_id=run_id, owner_id="worker-1", max_active_runs=10)
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool.name,
        approver="compliance@company.com",
        decision="rejected",
        reason="Customer tables cannot be dropped",
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="code",
        store=tmp_store,
        owner_id="worker-1",
    )

    with pytest.raises(ToolPermissionDeniedError) as exc_info:
        tool(context=ctx, **args)
    assert "Customer tables cannot be dropped" in str(exc_info.value)
    assert len(called) == 0


def test_post_approval_revalidation_detects_cancellation():
    """Cancellation token cancelled immediately post-approval aborts before dispatch."""
    called = []
    tool = Tool(
        name="critical_step",
        fn=lambda: called.append(1) or "done",
        schema={"type": "object"},
        risk_level=RiskLevel.HIGH,
    )

    token = CancellationToken()
    token.cancel(reason="Workflow cancelled by user before dispatch")

    approval = ToolApproval(approver="lead", decision="approved")
    ctx = ToolExecutionContext(
        run_id="run-cancel-1",
        subtask_id="sub-1",
        cancellation_token=token,
        approval=approval,
    )

    with pytest.raises(ToolExecutionError) as exc_info:
        tool(context=ctx)
    assert "cancelled" in str(exc_info.value).lower()
    assert len(called) == 0


def test_post_approval_revalidation_detects_agent_auth_revocation():
    """Post-approval revalidation prevents unauthorized agent from executing approved operation."""
    called = []
    tool = Tool(
        name="special_task",
        fn=lambda: called.append(1) or "ok",
        schema={"type": "object"},
        allowed_agents=["code"],
        risk_level=RiskLevel.HIGH,
    )

    # Approval was issued, but invoking agent is "research"
    approval = ToolApproval(approver="admin", decision="approved")
    ctx = ToolExecutionContext(
        run_id="run-auth-rev",
        subtask_id="sub-1",
        agent_id="research",
        approval=approval,
    )

    with pytest.raises(ToolPermissionDeniedError):
        tool(context=ctx)
    assert len(called) == 0


def test_post_approval_revalidation_detects_admission_lease_loss(tmp_store):
    """Post-approval revalidation aborts with ToolFencingError if worker lost admission ownership."""
    called = []
    tool = Tool(
        name="fenced_high_risk",
        fn=lambda: called.append(1) or "ok",
        schema={"type": "object"},
        risk_level=RiskLevel.HIGH,
    )

    run_id = "run-fence-loss"
    subtask_id = "sub-1"
    args = {}
    op_key = derive_operation_key(run_id, subtask_id, tool.name, args, fence_token=1)

    # Worker-1 acquires admission
    tmp_store.acquire_workflow_admission(run_id=run_id, owner_id="worker-1", max_active_runs=10)
    tmp_store.record_tool_approval(
        run_id=run_id,
        operation_key=op_key,
        tool_name=tool.name,
        approver="lead",
        decision="approved",
    )

    # Worker-1 loses admission lease
    tmp_store.release_workflow_admission(run_id=run_id, owner_id="worker-1")

    # Worker-1 attempts dispatch with stale ownership
    ctx_stale = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        store=tmp_store,
        owner_id="worker-1",
        fence_token=1,
    )

    with pytest.raises(ToolFencingError):
        tool(context=ctx_stale, **args)
    assert len(called) == 0


def test_cannot_bypass_approval_via_direct_registry_call():
    """Direct registry calls via REGISTRY.call enforce risk-based approval."""
    tool = Tool(
        name="payment_transfer_direct",
        fn=lambda amt: f"transferred {amt}",
        schema={"type": "object", "properties": {"amt": {"type": "integer"}}},
        risk_level=RiskLevel.HIGH,
    )
    REGISTRY.register(tool)

    ctx = ToolExecutionContext(run_id="run-direct-1", subtask_id="sub-1")
    with pytest.raises(ToolApprovalRequiredError):
        REGISTRY.call("payment_transfer_direct", context=ctx, amt=100)


def test_unknown_tool_requires_approval_default_deny():
    """Unknown tools default to requiring approval and raise ToolNotFoundError on call."""
    assert REGISTRY.requires_approval("completely_unknown_tool_xyz") is True

    with pytest.raises(ToolNotFoundError):
        REGISTRY.call("completely_unknown_tool_xyz")


def test_set_policy_dynamically_updates_tool_enforcement():
    """Updating a policy via ToolRegistry.set_policy immediately adjusts tool enforcement."""
    tool = Tool(
        name="dynamic_sandbox_tool",
        fn=lambda: "success",
        schema={"type": "object"},
        risk_level=RiskLevel.LOW,
        allowed_agents=["*"],
    )
    REGISTRY.register(tool)

    # Initially allowed for research
    ctx_res = ToolExecutionContext(run_id="run-dyn-1", subtask_id="sub-1", agent_id="research")
    assert REGISTRY.call("dynamic_sandbox_tool", context=ctx_res) == "success"

    # Restrict to code only and require approval
    REGISTRY.set_policy(
        ToolPolicy(
            tool_name="dynamic_sandbox_tool",
            risk_level=RiskLevel.HIGH,
            allowed_agents=["code"],
            requires_approval=True,
        )
    )

    # Now research is denied permission
    with pytest.raises(ToolPermissionDeniedError):
        REGISTRY.call("dynamic_sandbox_tool", context=ctx_res)

    # Code agent is permitted agent, but requires approval
    ctx_code = ToolExecutionContext(run_id="run-dyn-1", subtask_id="sub-2", agent_id="code")
    with pytest.raises(ToolApprovalRequiredError):
        REGISTRY.call("dynamic_sandbox_tool", context=ctx_code)

    # Code agent with approval succeeds
    ctx_code_approved = ToolExecutionContext(
        run_id="run-dyn-1",
        subtask_id="sub-2",
        agent_id="code",
        approval=ToolApproval(approver="sec-lead", decision="approved"),
    )
    assert REGISTRY.call("dynamic_sandbox_tool", context=ctx_code_approved) == "success"


def test_audit_logging_on_policy_denial_and_approval(tmp_store):
    """Audit events are recorded for policy denial and approval requirement without leaking secrets."""
    tool = Tool(
        name="provision_api_key",
        fn=lambda secret_token: "token-ok",
        schema={"type": "object", "properties": {"secret_token": {"type": "string"}}},
        allowed_agents=["code"],
        risk_level=RiskLevel.HIGH,
    )

    run_id = "run-audit-tool"
    subtask_id = "sub-1"
    ctx_denied = ToolExecutionContext(
        run_id=run_id,
        subtask_id=subtask_id,
        agent_id="research",
        store=tmp_store,
    )

    with pytest.raises(ToolPermissionDeniedError):
        tool(context=ctx_denied, secret_token="super-secret-key-999")

    # Verify audit event in tmp_store
    events = tmp_store.get_audit_events(run_id=run_id)
    denied_events = [e for e in events if e["event_type"] == AuditEventType.TOOL_POLICY_DENIED]
    assert len(denied_events) >= 1
    ev = denied_events[0]
    assert ev["actor"] == "research"
    assert "super-secret-key-999" not in str(ev["metadata"])
    assert ev["metadata"]["tool_name"] == "provision_api_key"


def test_api_tool_approval_and_retrieval(tmp_store):
    """Test API endpoints POST and GET for tool approval workflow."""
    app = create_app()
    app.dependency_overrides[get_store] = lambda: tmp_store
    client = TestClient(app)
    run_id = "run-api-approve-1"
    op_key = "toolop:run-api-approve-1:sub-1:sample_tool:abc12345"

    # 1. Post approval decision
    post_resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/approve",
        json={
            "tool_name": "sample_tool",
            "decision": "approved",
            "reason": "Authorized by security officer",
            "approver": "sec_officer_42",
        },
    )
    assert post_resp.status_code == 200
    data = post_resp.json()
    assert data["success"] is True
    assert data["decision"] == "approved"
    assert data["approver"] == "sec_officer_42"
    assert data["operation_key"] == op_key

    # 2. Get approval decision
    get_resp = client.get(f"/runs/{run_id}/tools/{op_key}/approval")
    assert get_resp.status_code == 200
    get_data = get_resp.json()
    assert get_data["decision"] == "approved"
    assert get_data["approver"] == "sec_officer_42"
    assert get_data["reason"] == "Authorized by security officer"

    # 3. Test OrchestratorApiClient methods
    op_key_2 = "toolop:run-api-approve-1:sub-2:sample_tool:def67890"
    api_client = OrchestratorApiClient(base_url="http://testserver")
    api_client._client = client

    client_app_resp = api_client.approve_tool_execution(
        run_id=run_id,
        operation_key=op_key_2,
        tool_name="sample_tool",
        decision="rejected",
        reason="Rejected decision",
        approver="manager_1",
    )
    assert client_app_resp.decision == "rejected"
    assert client_app_resp.approver == "manager_1"

    client_get_resp = api_client.get_tool_approval(run_id=run_id, operation_key=op_key_2)
    assert client_get_resp.decision == "rejected"
    assert client_get_resp.reason == "Rejected decision"


def test_specialist_execution_passes_agent_id_and_enforces_policy(monkeypatch):
    """Specialists pass agent_id=kind; unauthorized tools produce clean error notes without crashing."""
    from orchestrator.llm import Completion

    mock_llm = MagicMock()
    mock_llm.complete.return_value = Completion(
        text="Specialist response", provider="mock", model="mock-model"
    )
    monkeypatch.setattr("orchestrator.graph.specialists.LLMClient", lambda: mock_llm)

    orig_calc = REGISTRY._tools.get("calculator")
    orig_policy = REGISTRY._policies.get("calculator")

    try:
        # Register calculator with allowed_agents=["analysis"] only
        calc_tool = Tool(
            name="calculator",
            fn=lambda expression: "42",
            schema={"type": "object", "properties": {"expression": {"type": "string"}}},
            allowed_agents=["analysis"],
            risk_level=RiskLevel.LOW,
        )
        REGISTRY.register(calc_tool)

        # 1. 'analysis' specialist can use calculator
        state_analysis = {
            "run_id": "run-spec-1",
            "plan": [{"id": "s1", "kind": "analysis", "instruction": "calculate: 20 + 22"}],
            "completed": [],
        }
        out_analysis = _run_specialist(state_analysis, "analysis")
        assert out_analysis["completed"][0]["status"] == "done"
        assert "calculator" in out_analysis["completed"][0]["tools_used"]
        assert "[Calculator output: 42]" in mock_llm.complete.call_args[1]["prompt"]

        # 2. 'code' specialist is unauthorized for calculator
        state_code = {
            "run_id": "run-spec-2",
            "plan": [{"id": "s2", "kind": "code", "instruction": "calculate: 10 * 10"}],
            "completed": [],
        }
        out_code = _run_specialist(state_code, "code")
        assert out_code["completed"][0]["status"] == "done"
        assert "calculator" in out_code["completed"][0]["tools_used"]
        assert "not authorized to invoke tool 'calculator'" in mock_llm.complete.call_args[1]["prompt"]
    finally:
        if orig_calc is not None:
            REGISTRY.register(orig_calc)
        if orig_policy is not None:
            REGISTRY.set_policy(orig_policy)
