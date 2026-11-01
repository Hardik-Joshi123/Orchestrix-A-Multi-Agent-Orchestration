"""Tests for tool execution monitoring, safe reconciliation, audit logging, and immutability."""

from __future__ import annotations

import concurrent.futures

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiValidationError,
    OrchestratorApiClient,
)
from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import reset_settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools import (
    IdempotencyMode,
    Tool,
    ToolExecutionContext,
    ToolExecutionError,
    derive_operation_key,
)


@pytest.fixture
def clean_environment(monkeypatch, tmp_path):
    """Ensure clean isolated configuration and database for every test."""
    db_file = str(tmp_path / "test_reconciliation.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield db_file
    reset_settings()
    reset_rate_limiter()


def _setup_app_and_store(db_path: str):
    store = CheckpointStore(db_path=db_path, enabled=True)
    manager = RunManager()
    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)
    return app, store, manager, client


def _insert_run(store: CheckpointStore, run_id: str, status: str = "running"):
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Test reconciliation workflow", "status": status, "results": {}},
        transition="init",
    )


# ---------------------------------------------------------------------------
# 1. Authentication & Authorization
# ---------------------------------------------------------------------------


def test_inspect_tool_executions_authentication(clean_environment, monkeypatch):
    """Verify that tool inspection endpoints enforce API token authentication when configured."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "secret-token-xyz")
    reset_settings()

    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-auth-test"
    _insert_run(store, run_id)

    # 1. Unauthenticated request rejected
    resp_no_auth = client.get(f"/runs/{run_id}/tools")
    assert resp_no_auth.status_code == 401

    # 2. Invalid token rejected
    resp_bad_auth = client.get(
        f"/runs/{run_id}/tools",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert resp_bad_auth.status_code == 401

    # 3. Valid token accepted
    resp_valid = client.get(
        f"/runs/{run_id}/tools",
        headers={"Authorization": "Bearer secret-token-xyz"},
    )
    assert resp_valid.status_code == 200
    assert resp_valid.json()["run_id"] == run_id


def test_reconcile_authentication(clean_environment, monkeypatch):
    """Verify that reconciliation endpoints enforce API token authentication."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "reconcile-secret-token")
    reset_settings()

    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-auth"
    op_key = "toolop:run-rec-auth:s1:charge:1234567890abcdef"
    _insert_run(store, run_id)
    store.record_tool_dispatch(op_key, run_id, "s1", "charge", 1, {})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    payload = {"outcome": "completed", "reason": "Verified out of band"}

    # Unauthenticated rejected
    resp = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp.status_code == 401

    # Authenticated accepted
    resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/reconcile",
        json=payload,
        headers={"Authorization": "Bearer reconcile-secret-token"},
    )
    assert resp.status_code == 200
    assert resp.json()["success"] is True


# ---------------------------------------------------------------------------
# 2. Argument Redaction & Privacy Shielding
# ---------------------------------------------------------------------------


def test_inspect_tool_executions_redaction(clean_environment):
    """Verify that sensitive arguments (passwords, tokens, api keys) and tracebacks are redacted."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-redaction-test"
    _insert_run(store, run_id)

    raw_args = {
        "api_key": "sk-1234567890abcdef12345",
        "auth_token": "bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        "password": "super-confidential-password",
        "secret_param": "classified-value",
        "customer_id": "cust-882",
        "amount_cents": 4900,
    }

    op_key = derive_operation_key(run_id, "sub-redact", "payment_tool", raw_args)
    store.record_tool_dispatch(op_key, run_id, "sub-redact", "payment_tool", 1, raw_args)
    error_msg = (
        "Traceback (most recent call last):\n"
        "  File '/app/specialists.py', line 45, in call\n"
        "HTTPError: 401 Unauthorized for Bearer sk-secret-token-value-1234"
    )
    store.record_tool_failure(op_key, error_msg, uncertain=True)

    resp = client.get(f"/runs/{run_id}/tools")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total_executions"] == 1
    execution = data["executions"][0]

    # Verify sensitive dictionary keys are redacted
    clean_args = execution["args"]
    assert clean_args["api_key"] == "[REDACTED]"
    assert clean_args["auth_token"] == "[REDACTED]"
    assert clean_args["password"] == "[REDACTED]"
    assert clean_args["secret_param"] == "[REDACTED]"

    # Verify non-sensitive arguments remain intact
    assert clean_args["customer_id"] == "cust-882"
    assert clean_args["amount_cents"] == 4900

    # Verify tracebacks are stripped and tokens redacted in error text
    err = execution["error_text"]
    assert "Traceback" not in err
    assert "sk-secret-token-value-1234" not in err
    assert "[REDACTED_KEY]" in err or "Bearer [REDACTED_TOKEN]" in err


# ---------------------------------------------------------------------------
# 3. Validation & Edge Cases
# ---------------------------------------------------------------------------


def test_inspect_invalid_run_not_found(clean_environment):
    """Verify 404 response when querying a non-existent run ID."""
    _, _, _, client = _setup_app_and_store(clean_environment)
    resp = client.get("/runs/run-does-not-exist/tools")
    assert resp.status_code == 404


def test_inspect_single_tool_execution_not_found(clean_environment):
    """Verify 404 response when querying a non-existent operation key."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-single-404"
    _insert_run(store, run_id)

    resp = client.get(f"/runs/{run_id}/tools/toolop:invalid:key")
    assert resp.status_code == 404


def test_reconcile_missing_reason_rejected(clean_environment):
    """Verify 400 response when reconciliation payload omits an explanation."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-val"
    op_key = "toolop:val:key:1"
    _insert_run(store, run_id)
    store.record_tool_dispatch(op_key, run_id, "s1", "tool_x", 1, {})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    # Missing reason
    resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/reconcile",
        json={"outcome": "completed", "reason": "   "},
    )
    assert resp.status_code == 400


def test_reconcile_invalid_outcome_rejected(clean_environment):
    """Verify 400 response when outcome is neither 'completed' nor 'failed'."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-outcome"
    op_key = "toolop:val:key:2"
    _insert_run(store, run_id)
    store.record_tool_dispatch(op_key, run_id, "s1", "tool_x", 1, {})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    resp = client.post(
        f"/runs/{run_id}/tools/{op_key}/reconcile",
        json={"outcome": "in_progress", "reason": "Trying to set invalid status"},
    )
    assert resp.status_code == 400


# ---------------------------------------------------------------------------
# 4. Valid Reconciliation State Transitions & Audit Trails
# ---------------------------------------------------------------------------


def test_reconcile_uncertain_to_completed(clean_environment):
    """Verify operator can successfully reconcile an uncertain execution to completed."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-reconcile-complete"
    _insert_run(store, run_id)
    op_key = "toolop:rec-comp:s1:charge:1"

    store.record_tool_dispatch(op_key, run_id, "s1", "charge_customer", 1, {"amount": 100})
    store.record_tool_failure(op_key, "Socket connection dropped", uncertain=True)

    payload = {
        "outcome": "completed",
        "reason": "Verified charge ch_98721 exists in Stripe production dashboard",
        "result_text": "ch_98721:paid",
        "operator": "ops-lead-carol",
    }

    resp = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp.status_code == 200
    res_data = resp.json()
    assert res_data["success"] is True
    assert res_data["idempotent"] is False
    assert res_data["previous_status"] == "uncertain"
    assert res_data["status"] == "completed"
    assert res_data["reconciled_by"] == "ops-lead-carol"
    assert "Stripe production dashboard" in res_data["reconciliation_reason"]

    # Verify SQLite record updated
    record = store.get_tool_execution(op_key)
    assert record["status"] == "completed"
    assert record["result_text"] == "ch_98721:paid"
    assert record["reconciled_by"] == "ops-lead-carol"

    # Verify audit event persisted
    audit_events = store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_RECONCILED)
    assert len(audit_events) == 1
    event = audit_events[0]
    assert event["outcome"] == "completed"
    assert event["actor"] == "ops-lead-carol"
    assert event["metadata"]["operation_key"] == op_key
    assert event["metadata"]["previous_status"] == "uncertain"
    assert event["metadata"]["new_status"] == "completed"


def test_reconcile_uncertain_to_failed(clean_environment):
    """Verify operator can successfully reconcile an uncertain execution to failed."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-reconcile-failed"
    _insert_run(store, run_id)
    op_key = "toolop:rec-fail:s1:email:2"

    store.record_tool_dispatch(op_key, run_id, "s1", "send_mail", 1, {"to": "user@example.com"})
    store.record_tool_failure(op_key, "Timeout 30s", uncertain=True)

    payload = {
        "outcome": "failed",
        "reason": "Verified message was dropped and not delivered by upstream MTA",
        "error_text": "Upstream MTA rejected envelope",
        "operator": "ops-lead-bob",
    }

    resp = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp.status_code == 200
    res_data = resp.json()
    assert res_data["success"] is True
    assert res_data["idempotent"] is False
    assert res_data["status"] == "failed"
    assert res_data["reconciled_by"] == "ops-lead-bob"

    # Verify SQLite record updated
    record = store.get_tool_execution(op_key)
    assert record["status"] == "failed"
    assert "Upstream MTA rejected" in record["error_text"]
    assert record["reconciled_by"] == "ops-lead-bob"

    # Verify audit event persisted
    audit_events = store.get_audit_events(run_id=run_id, event_type=AuditEventType.TOOL_RECONCILED)
    assert len(audit_events) == 1
    assert audit_events[0]["outcome"] == "failed"


# ---------------------------------------------------------------------------
# 5. Invalid State Transitions & Conflict Rejection
# ---------------------------------------------------------------------------


def test_reconcile_pending_rejected(clean_environment):
    """Verify that in-flight tool executions (status='pending') cannot be reconciled."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-pending"
    _insert_run(store, run_id)
    op_key = "toolop:rec-pending:s1:vm:3"

    store.record_tool_dispatch(op_key, run_id, "s1", "create_vm", 1, {})

    payload = {"outcome": "completed", "reason": "Attempting to reconcile in-flight"}
    resp = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp.status_code == 409
    assert "pending" in resp.json()["detail"].lower()


def test_reconcile_conflicting_terminal_outcomes_rejected(clean_environment):
    """Verify that an already reconciled execution cannot be modified to a conflicting status."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-conflict"
    _insert_run(store, run_id)
    op_key = "toolop:rec-conflict:s1:db:4"

    store.record_tool_dispatch(op_key, run_id, "s1", "mutate_db", 1, {})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    # 1. Reconcile to completed
    resp1 = client.post(
        f"/runs/{run_id}/tools/{op_key}/reconcile",
        json={"outcome": "completed", "reason": "Initial verified completion"},
    )
    assert resp1.status_code == 200

    # 2. Conflicting attempt to reconcile to failed must be rejected with 409 Conflict
    resp2 = client.post(
        f"/runs/{run_id}/tools/{op_key}/reconcile",
        json={"outcome": "failed", "reason": "Conflicting reversal attempt"},
    )
    assert resp2.status_code == 409
    assert "conflicting outcome" in resp2.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 6. Idempotency on Repeated Requests
# ---------------------------------------------------------------------------


def test_reconcile_idempotent_repeated_requests(clean_environment):
    """Verify repeated reconciliation requests with identical outcome succeed idempotently."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-rec-idempotent"
    _insert_run(store, run_id)
    op_key = "toolop:rec-idem:s1:refund:5"

    store.record_tool_dispatch(op_key, run_id, "s1", "issue_refund", 1, {})
    store.record_tool_failure(op_key, "Network timeout", uncertain=True)

    payload = {
        "outcome": "completed",
        "reason": "Refund ref_1102 verified in ledger",
        "operator": "finance-admin",
    }

    # First submission
    resp1 = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp1.status_code == 200
    assert resp1.json()["idempotent"] is False

    # Second submission (identical outcome)
    resp2 = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
    assert resp2.status_code == 200
    res_data2 = resp2.json()
    assert res_data2["idempotent"] is True
    assert res_data2["status"] == "completed"
    assert res_data2["reconciled_by"] == "finance-admin"


# ---------------------------------------------------------------------------
# 7. Concurrent Reconciliation Race Safety
# ---------------------------------------------------------------------------


def test_concurrent_reconciliation_race(clean_environment):
    """Verify that concurrent conflicting reconciliation requests resolve safely with no corruption."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-race-rec"
    _insert_run(store, run_id)
    op_key = "toolop:race:s1:action:6"

    store.record_tool_dispatch(op_key, run_id, "s1", "critical_side_effect", 1, {})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    results: list[int] = []

    def submit_reconciliation(outcome: str, reason: str):
        payload = {"outcome": outcome, "reason": reason}
        r = client.post(f"/runs/{run_id}/tools/{op_key}/reconcile", json=payload)
        results.append(r.status_code)

    # Launch 4 'completed' attempts and 4 'failed' attempts concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = []
        for i in range(4):
            futures.append(pool.submit(submit_reconciliation, "completed", f"Completed attempt {i}"))
            futures.append(pool.submit(submit_reconciliation, "failed", f"Failed attempt {i}"))
        concurrent.futures.wait(futures)

    # One outcome type must succeed (200), the other outcome type must be rejected (409)
    assert all(code in (200, 409) for code in results)
    assert 200 in results
    assert 409 in results

    final_row = store.get_tool_execution(op_key)
    assert final_row["status"] in ("completed", "failed")


# ---------------------------------------------------------------------------
# 8. Workflow Resumption & Execution Guarantees
# ---------------------------------------------------------------------------


def test_resumed_run_with_reconciled_completed_tool(clean_environment):
    """Verify that a resumed run short-circuits on a reconciled completed tool and does not redispatch."""
    _, store, _, _ = _setup_app_and_store(clean_environment)
    run_id = "run-resume-completed"
    op_key = "toolop:resume-comp:s1:charge:7"

    dispatch_count = {"count": 0}

    def charge_card(amount: int, idempotency_key: str = "") -> str:
        dispatch_count["count"] += 1
        return "charge_ok"

    tool = Tool(
        name="charge_card",
        fn=charge_card,
        schema={"type": "object", "properties": {"amount": {"type": "integer"}}},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        supports_idempotency_key=True,
    )

    # Setup uncertain execution, then reconcile to completed
    store.acquire_workflow_admission(run_id, owner_id="worker-resume-1", max_active_runs=10)
    store.record_tool_dispatch(op_key, run_id, "s1", "charge_card", 1, {"amount": 50})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)
    store.reconcile_tool_execution(
        run_id=run_id,
        operation_key=op_key,
        outcome="completed",
        reason="Verified transaction on card network",
        result_text="verified_charge_tx_999",
        actor="ops-auditor",
    )

    # Execute tool with workflow context
    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id="s1",
        fence_token=1,
        store=store,
        owner_id="worker-resume-1",
        operation_key=op_key,
    )

    result = tool(amount=50, context=ctx)
    # Result returns the reconciled result text and did NOT call charge_card
    assert result == "verified_charge_tx_999"
    assert dispatch_count["count"] == 0


def test_resumed_run_with_reconciled_failed_tool(clean_environment):
    """Verify that a resumed run halts and does not redispatch a tool reconciled to failed."""
    _, store, _, _ = _setup_app_and_store(clean_environment)
    run_id = "run-resume-failed"
    op_key = "toolop:resume-fail:s1:sms:8"

    dispatch_count = {"count": 0}

    def send_sms(phone: str) -> str:
        dispatch_count["count"] += 1
        return "sent"

    tool = Tool(
        name="send_sms",
        fn=send_sms,
        schema={"type": "object", "properties": {"phone": {"type": "string"}}},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
    )

    # Setup uncertain execution, then reconcile to failed
    store.acquire_workflow_admission(run_id, owner_id="worker-resume-2", max_active_runs=10)
    store.record_tool_dispatch(op_key, run_id, "s1", "send_sms", 1, {"phone": "+1234567890"})
    store.record_tool_failure(op_key, "Connection dropped", uncertain=True)
    store.reconcile_tool_execution(
        run_id=run_id,
        operation_key=op_key,
        outcome="failed",
        reason="Carrier confirmed number is deactivated",
        error_text="Carrier error: number deactivated",
        actor="ops-auditor",
    )

    ctx = ToolExecutionContext(
        run_id=run_id,
        subtask_id="s1",
        fence_token=1,
        store=store,
        owner_id="worker-resume-2",
        operation_key=op_key,
    )

    # Must raise ToolExecutionError without redispatching
    with pytest.raises(ToolExecutionError, match="Carrier confirmed number is deactivated"):
        tool(phone="+1234567890", context=ctx)

    assert dispatch_count["count"] == 0


# ---------------------------------------------------------------------------
# 9. Filtering & Client SDK Integration
# ---------------------------------------------------------------------------


def test_filter_by_status_and_tool_name(clean_environment):
    """Verify querying executions by status and tool_name filters."""
    _, store, _, client = _setup_app_and_store(clean_environment)
    run_id = "run-filter-test"
    _insert_run(store, run_id)

    store.record_tool_dispatch("op:1", run_id, "s1", "tool_a", 1, {})
    store.record_tool_completion("op:1", "ok")

    store.record_tool_dispatch("op:2", run_id, "s2", "tool_b", 1, {})
    store.record_tool_failure("op:2", "fail", uncertain=False)

    store.record_tool_dispatch("op:3", run_id, "s3", "tool_a", 1, {})
    store.record_tool_failure("op:3", "drop", uncertain=True)

    # Filter status=uncertain
    resp_uncertain = client.get(f"/runs/{run_id}/tools?status=uncertain")
    assert resp_uncertain.status_code == 200
    items = resp_uncertain.json()["executions"]
    assert len(items) == 1
    assert items[0]["operation_key"] == "op:3"

    # Filter tool_name=tool_a
    resp_tool_a = client.get(f"/runs/{run_id}/tools?tool_name=tool_a")
    assert resp_tool_a.status_code == 200
    assert len(resp_tool_a.json()["executions"]) == 2


def test_client_sdk_tool_methods(clean_environment):
    """Verify OrchestratorApiClient methods for tool execution inspection and reconciliation."""
    app, store, _, test_http = _setup_app_and_store(clean_environment)
    run_id = "run-sdk-tools"
    _insert_run(store, run_id)
    op_key = "toolop:sdk:s1:api:9"

    store.record_tool_dispatch(op_key, run_id, "s1", "remote_api", 1, {"query": "hello"})
    store.record_tool_failure(op_key, "Timeout", uncertain=True)

    api_client = OrchestratorApiClient(base_url="http://testserver", client=test_http)

    # 1. get_run_tools
    tools_resp = api_client.get_run_tools(run_id)
    assert tools_resp.total_executions == 1
    assert tools_resp.executions[0].operation_key == op_key

    # 2. get_tool_execution
    exec_item = api_client.get_tool_execution(run_id, op_key)
    assert exec_item.status == "uncertain"
    assert exec_item.tool_name == "remote_api"

    # 3. reconcile_tool_execution
    rec_resp = api_client.reconcile_tool_execution(
        run_id=run_id,
        operation_key=op_key,
        outcome="completed",
        reason="Verified by automated reconcile script",
        result_text="result_from_script",
        operator="bot-reconciler",
    )
    assert rec_resp.success is True
    assert rec_resp.status == "completed"
    assert rec_resp.reconciled_by == "bot-reconciler"

    # 4. Client validation check
    with pytest.raises(ApiValidationError, match="reason is required"):
        api_client.reconcile_tool_execution(run_id, op_key, outcome="completed", reason="")
