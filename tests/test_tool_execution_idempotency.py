"""Tests for safe tool execution, idempotency keys, fencing gates, and uncertainty handling."""

from __future__ import annotations

import time

import pytest

from orchestrator.graph.cancellation import CancellationToken
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools import (
    IdempotencyMode,
    Tool,
    ToolDuplicateExecutionError,
    ToolExecutionContext,
    ToolExecutionError,
    ToolFencingError,
    ToolUncertainOutcomeError,
    derive_operation_key,
)


def _make_store(tmp_path) -> CheckpointStore:
    db_file = tmp_path / "test_tool_idempotency.db"
    return CheckpointStore(db_path=str(db_file), enabled=True)


# ---------------------------------------------------------------------------
# 1. READ_ONLY tool retries transient network errors up to max_retries
# ---------------------------------------------------------------------------


def test_read_only_tool_retries_transient_failures_and_succeeds(tmp_path):
    """Verify read-only tool retries transient network errors up to max_retries and succeeds."""
    store = _make_store(tmp_path)
    attempts = {"count": 0}

    def flaky_search(query: str) -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("Temporary DNS failure")
        return f"results for {query}"

    tool = Tool(
        name="test_flaky_search",
        fn=flaky_search,
        schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
        idempotency_mode=IdempotencyMode.READ_ONLY,
        max_retries=2,
    )

    ctx = ToolExecutionContext(run_id="run-ro-1", subtask_id="s1", store=store)
    result = tool(context=ctx, query="distributed systems")

    assert result == "results for distributed systems"
    assert attempts["count"] == 2


# ---------------------------------------------------------------------------
# 2. MUTATING_NON_IDEMPOTENT tool is never automatically retried
# ---------------------------------------------------------------------------


def test_mutating_non_idempotent_tool_never_retried_automatically(tmp_path):
    """Verify mutating non-idempotent tool is never automatically retried even if max_retries > 0."""
    store = _make_store(tmp_path)
    attempts = {"count": 0}

    def charge_account(amount: int) -> str:
        attempts["count"] += 1
        raise RuntimeError("Payment gateway network error")

    tool = Tool(
        name="test_charge_account",
        fn=charge_account,
        schema={"type": "object", "properties": {"amount": {"type": "integer"}}, "required": ["amount"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
        max_retries=3,  # Should be forced to 0 retries
    )

    ctx = ToolExecutionContext(run_id="run-mut-1", subtask_id="s1", store=store)
    with pytest.raises(RuntimeError, match="Payment gateway network error"):
        tool(context=ctx, amount=100)

    assert attempts["count"] == 1

    # Verify failure recorded in persistence
    op_key = derive_operation_key("run-mut-1", "s1", "test_charge_account", {"amount": 100})
    rec = store.get_tool_execution(op_key)
    assert rec is not None
    assert rec["status"] == "failed"
    assert "Payment gateway network error" in str(rec["error_text"])


# ---------------------------------------------------------------------------
# 3. MUTATING_NON_IDEMPOTENT tool timeout surfaces ToolUncertainOutcomeError
# ---------------------------------------------------------------------------


def test_mutating_non_idempotent_tool_timeout_surfaces_uncertain_outcome(tmp_path):
    """Verify timeout on a mutating tool produces ToolUncertainOutcomeError and records uncertain status."""
    store = _make_store(tmp_path)

    def slow_external_action(target: str) -> str:
        time.sleep(0.3)
        return f"acted on {target}"

    tool = Tool(
        name="test_slow_action",
        fn=slow_external_action,
        schema={"type": "object", "properties": {"target": {"type": "string"}}, "required": ["target"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
        timeout_seconds=0.05,
    )

    ctx = ToolExecutionContext(run_id="run-timeout-1", subtask_id="s1", store=store)
    with pytest.raises(ToolUncertainOutcomeError) as exc_info:
        tool(context=ctx, target="server-1")

    assert "uncertain" in str(exc_info.value).lower()

    # Verify marked as uncertain in database
    op_key = derive_operation_key("run-timeout-1", "s1", "test_slow_action", {"target": "server-1"})
    rec = store.get_tool_execution(op_key)
    assert rec is not None
    assert rec["status"] == "uncertain"
    assert "uncertain outcome" in rec["error_text"]


# ---------------------------------------------------------------------------
# 4. MUTATING_IDEMPOTENT tool receives operation key downstream
# ---------------------------------------------------------------------------


def test_mutating_idempotent_tool_receives_operation_key_downstream(tmp_path):
    """Verify tool with provider idempotency receives the derived operation_key and completes."""
    store = _make_store(tmp_path)
    received_keys = []

    def deploy_release(version: str, idempotency_key: str | None = None) -> str:
        received_keys.append(idempotency_key)
        return f"deployed {version}"

    tool = Tool(
        name="test_deploy_release",
        fn=deploy_release,
        schema={"type": "object", "properties": {"version": {"type": "string"}}, "required": ["version"]},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        supports_idempotency_key=True,
    )

    ctx = ToolExecutionContext(run_id="run-deploy-1", subtask_id="sub-10", fence_token=1, store=store)
    res = tool(context=ctx, version="v2.1.0")

    assert res == "deployed v2.1.0"
    op_key = derive_operation_key("run-deploy-1", "sub-10", "test_deploy_release", {"version": "v2.1.0"})
    assert received_keys == [op_key]

    rec = store.get_tool_execution(op_key)
    assert rec is not None
    assert rec["status"] == "completed"
    assert rec["result_text"] == "deployed v2.1.0"


# ---------------------------------------------------------------------------
# 5. Short-circuit on previously completed operation
# ---------------------------------------------------------------------------


def test_short_circuit_on_previously_completed_operation(tmp_path):
    """Verify that an operation key with status completed returns cached result without calling fn."""
    store = _make_store(tmp_path)
    calls = {"count": 0}

    def idempotent_action(payload: str, idempotency_key: str | None = None) -> str:
        calls["count"] += 1
        return f"processed: {payload}"

    tool = Tool(
        name="test_idempotent_action",
        fn=idempotent_action,
        schema={"type": "object", "properties": {"payload": {"type": "string"}}, "required": ["payload"]},
        idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
        supports_idempotency_key=True,
    )

    ctx = ToolExecutionContext(run_id="run-cache-1", subtask_id="s1", store=store)

    # First call: executes action
    res1 = tool(context=ctx, payload="data-abc")
    assert res1 == "processed: data-abc"
    assert calls["count"] == 1

    # Second call with identical arguments: short-circuits to cached result!
    res2 = tool(context=ctx, payload="data-abc")
    assert res2 == "processed: data-abc"
    assert calls["count"] == 1  # Did NOT call fn a second time!


# ---------------------------------------------------------------------------
# 6. Block on uncertain execution
# ---------------------------------------------------------------------------


def test_block_on_uncertain_prior_execution(tmp_path):
    """Verify re-invoking a tool whose operation key is in uncertain state
    raises ToolUncertainOutcomeError.
    """
    store = _make_store(tmp_path)
    calls = {"count": 0}

    def unsafe_action(item_id: str) -> str:
        calls["count"] += 1
        return f"item: {item_id}"

    tool = Tool(
        name="test_unsafe_action",
        fn=unsafe_action,
        schema={"type": "object", "properties": {"item_id": {"type": "string"}}, "required": ["item_id"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
        retry_on_uncertain=False,
    )

    op_key = derive_operation_key("run-uncert-1", "s1", "test_unsafe_action", {"item_id": "item-99"})
    # Pre-record an uncertain execution
    store.record_tool_dispatch(op_key, "run-uncert-1", "s1", "test_unsafe_action", 1, {"item_id": "item-99"})
    store.record_tool_failure(op_key, "Upstream connection dropped", uncertain=True)

    ctx = ToolExecutionContext(run_id="run-uncert-1", subtask_id="s1", store=store)
    with pytest.raises(ToolUncertainOutcomeError) as exc_info:
        tool(context=ctx, item_id="item-99")

    assert "uncertain prior execution" in str(exc_info.value)
    assert calls["count"] == 0  # fn was not called!

    # When retry_on_uncertain is explicitly enabled, re-execution proceeds
    tool_retry = Tool(
        name="test_unsafe_action_retry",
        fn=unsafe_action,
        schema={"type": "object", "properties": {"item_id": {"type": "string"}}, "required": ["item_id"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
        retry_on_uncertain=True,
    )
    # Different op key for retry tool
    op_key_retry = derive_operation_key(
        "run-uncert-1", "s1", "test_unsafe_action_retry", {"item_id": "item-99"}
    )
    store.record_tool_dispatch(
        op_key_retry, "run-uncert-1", "s1", "test_unsafe_action_retry", 1, {"item_id": "item-99"}
    )
    store.record_tool_failure(op_key_retry, "Upstream timeout", uncertain=True)

    res = tool_retry(context=ctx, item_id="item-99")
    assert res == "item: item-99"
    assert calls["count"] == 1


# ---------------------------------------------------------------------------
# 7. Fencing rejection before dispatch
# ---------------------------------------------------------------------------


def test_fencing_rejection_before_dispatch(tmp_path):
    """Verify that a worker whose admission lease expired or was superseded fails before calling the tool."""
    store = _make_store(tmp_path)
    run_id = "run-fence-predispatch"
    owner_id = "worker-stale"

    # Worker acquires admission but lease expires or is not valid
    assert store.acquire_workflow_admission(
        run_id, owner_id=owner_id, max_active_runs=10, lease_ttl_seconds=-10.0
    )

    tool_called = {"called": False}

    def sensitive_write(data: str) -> str:
        tool_called["called"] = True
        return "written"

    tool = Tool(
        name="test_sensitive_write",
        fn=sensitive_write,
        schema={"type": "object", "properties": {"data": {"type": "string"}}, "required": ["data"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
    )

    fenced_store = store.with_fencing(owner_id=owner_id, fence_token=1)
    ctx = ToolExecutionContext(run_id=run_id, subtask_id="s1", store=fenced_store)

    with pytest.raises(ToolFencingError) as exc_info:
        tool(context=ctx, data="critical-data")

    assert "admission" in str(exc_info.value).lower()
    assert tool_called["called"] is False


# ---------------------------------------------------------------------------
# 8. Fencing rejection before completion
# ---------------------------------------------------------------------------


def test_fencing_rejection_before_completion(tmp_path):
    """Verify that if a worker's lease is lost during tool execution,
    completion is rejected with ToolFencingError.
    """
    store = _make_store(tmp_path)
    run_id = "run-fence-postdispatch"
    owner_id = "worker-slow"

    assert store.acquire_workflow_admission(
        run_id, owner_id=owner_id, max_active_runs=10, lease_ttl_seconds=30.0
    )

    def write_and_lose_lease(text: str) -> str:
        # Simulate worker superseded mid-execution: release old lease and acquire new worker
        store.release_workflow_admission(run_id, owner_id=owner_id, fence_token=1)
        store.acquire_workflow_admission(run_id, owner_id="worker-new", max_active_runs=10)
        return f"result: {text}"

    tool = Tool(
        name="test_race_tool",
        fn=write_and_lose_lease,
        schema={"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
    )

    fenced_store = store.with_fencing(owner_id=owner_id, fence_token=1)
    ctx = ToolExecutionContext(run_id=run_id, subtask_id="s1", store=fenced_store)

    with pytest.raises(ToolFencingError) as exc_info:
        tool(context=ctx, text="racing-write")

    assert "lost admission ownership" in str(exc_info.value)

    # Verify tool execution record was NOT marked completed
    op_key = derive_operation_key(run_id, "s1", "test_race_tool", {"text": "racing-write"})
    rec = store.get_tool_execution(op_key)
    assert rec is not None
    assert rec["status"] != "completed"


# ---------------------------------------------------------------------------
# 9. Cooperative cancellation before dispatch
# ---------------------------------------------------------------------------


def test_cooperative_cancellation_before_dispatch(tmp_path):
    """Verify tool execution is rejected immediately if cancellation_token is cancelled."""
    store = _make_store(tmp_path)
    token = CancellationToken(run_id="run-cancelled-tool")
    token.cancel("User aborted workflow")

    called = {"count": 0}

    def dummy_tool(arg: str) -> str:
        called["count"] += 1
        return "done"

    tool = Tool(
        name="test_dummy_tool",
        fn=dummy_tool,
        schema={"type": "object", "properties": {"arg": {"type": "string"}}, "required": ["arg"]},
    )

    ctx = ToolExecutionContext(
        run_id="run-cancelled-tool",
        subtask_id="s1",
        cancellation_token=token,
        store=store,
    )

    with pytest.raises(ToolExecutionError) as exc_info:
        tool(context=ctx, arg="hello")

    assert "cancelled" in str(exc_info.value).lower()
    assert called["count"] == 0


# ---------------------------------------------------------------------------
# 10. In-flight duplicate dispatch rejection for non-idempotent tool
# ---------------------------------------------------------------------------


def test_in_flight_duplicate_dispatch_rejection(tmp_path):
    """Verify that a mutating non-idempotent tool rejects re-dispatch when pending."""
    store = _make_store(tmp_path)
    op_key = derive_operation_key("run-dup-1", "s1", "test_dup_tool", {"val": "1"})

    store.record_tool_dispatch(op_key, "run-dup-1", "s1", "test_dup_tool", 1, {"val": "1"})

    tool = Tool(
        name="test_dup_tool",
        fn=lambda val: f"val={val}",
        schema={"type": "object", "properties": {"val": {"type": "string"}}, "required": ["val"]},
        idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
    )

    ctx = ToolExecutionContext(run_id="run-dup-1", subtask_id="s1", store=store)
    with pytest.raises(ToolDuplicateExecutionError) as exc_info:
        tool(context=ctx, val="1")

    assert "in-flight non-idempotent execution" in str(exc_info.value)


# ---------------------------------------------------------------------------
# 11. Run recovery marks pending tool executions as uncertain
# ---------------------------------------------------------------------------


def test_run_recovery_marks_pending_tool_executions_uncertain(tmp_path):
    """Verify that recovering an interrupted run marks all in-flight pending tool executions as uncertain."""
    store = _make_store(tmp_path)
    run_id = "run-recover-tools"
    owner_id = "worker-crashed"

    assert store.acquire_workflow_admission(
        run_id, owner_id=owner_id, max_active_runs=10, lease_ttl_seconds=30.0
    )

    # Save initial running checkpoint
    store.save_checkpoint(
        run_id,
        {"run_id": run_id, "task": "Crash recovery task", "status": "running"},
        transition="init",
        owner_id=owner_id,
        fence_token=1,
    )

    # Record 2 pending tool dispatches
    op1 = derive_operation_key(run_id, "s1", "action_1", {"x": 10})
    op2 = derive_operation_key(run_id, "s2", "action_2", {"y": 20})
    store.record_tool_dispatch(op1, run_id, "s1", "action_1", 1, {"x": 10})
    store.record_tool_dispatch(op2, run_id, "s2", "action_2", 1, {"y": 20})

    # One completes, one remains pending
    store.record_tool_completion(op1, "result-10", owner_id=owner_id, fence_token=1)

    assert store.get_tool_execution(op1)["status"] == "completed"
    assert store.get_tool_execution(op2)["status"] == "pending"

    # Simulate process termination & startup recovery
    rec = store.recover_run_as_interrupted(run_id, force=True)
    assert rec is not None
    assert rec.state["status"] == "interrupted"

    # Completed remains completed; pending transitioned to uncertain!
    assert store.get_tool_execution(op1)["status"] == "completed"
    assert store.get_tool_execution(op2)["status"] == "uncertain"
    assert "interrupted" in store.get_tool_execution(op2)["error_text"].lower()


# ---------------------------------------------------------------------------
# 12. Specialist execution propagates tool context
# ---------------------------------------------------------------------------


def test_specialist_execution_passes_tool_context(tmp_path):
    """Verify that specialist execution constructs and passes ToolExecutionContext to tools."""
    from orchestrator.graph.specialists import _run_specialist

    store = _make_store(tmp_path)
    run_id = "run-spec-ctx"
    subtask = {"id": "sub-spec-1", "kind": "research", "instruction": "Research quantum algorithms"}
    state = {
        "run_id": run_id,
        "task_id": run_id,
        "task": "Quantum research",
        "plan": [subtask],
        "completed": [],
        "_store": store,
    }

    res = _run_specialist(state, "research", target_subtask=subtask)
    assert res is not None
    assert "web_search" in res.get("tools_used", [])
    assert len(res.get("completed", [])) == 1
    assert res["completed"][0]["id"] == "sub-spec-1"
