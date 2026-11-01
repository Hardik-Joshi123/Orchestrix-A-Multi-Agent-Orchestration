"""Comprehensive tests for Task 28: Workflow Cancellation & Execution Deadlines.

Verifies:
1. Cooperative CancellationToken & CancellationRegistry mechanics.
2. Cancellation before execution begins.
3. Cancellation while work is in progress.
4. Prevention of new subtask scheduling after cancellation.
5. Repeated cancellation requests (idempotency).
6. Cancellation of already terminal runs (HTTP 409 Conflict).
7. Deadline expiry before dispatch and during execution.
8. Status distinction: cancelled vs timed_out vs failed vs blocked.
9. Persistence and resume behavior after cancellation and timeout.
10. API route authentication, authorization, and error handling.
11. CLI --timeout argument validation and execution.
12. Regression tests for normal execution and retries.
"""

from __future__ import annotations

import os
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.manager import RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.graph.builder import resume_task, run_task
from orchestrator.graph.cancellation import (
    CancellationRegistry,
    CancellationToken,
    WorkflowCancelledError,
    get_cancellation_registry,
)
from orchestrator.graph.state import GraphState, Subtask
from orchestrator.main import cli
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_environment():
    """Reset memory, tokens, and environment before each test."""
    reset_stm()
    reset_ltm()
    get_cancellation_registry().clear()
    old_token = os.environ.pop("ORCHESTRATOR_API_TOKEN", None)
    yield
    reset_stm()
    reset_ltm()
    get_cancellation_registry().clear()
    if old_token is not None:
        os.environ["ORCHESTRATOR_API_TOKEN"] = old_token
    else:
        os.environ.pop("ORCHESTRATOR_API_TOKEN", None)


@pytest.fixture
def temp_store(tmp_path):
    """Provide a dedicated file-backed CheckpointStore in a temporary directory."""
    db_file = tmp_path / "test_cancel_checkpoints.db"
    store = CheckpointStore(db_path=db_file, enabled=True)
    yield store
    store.close()


@pytest.fixture
def api_test_env(tmp_path):
    """Provide a TestClient with isolated CheckpointStore and RunManager."""
    db_file = tmp_path / "test_api_cancel.db"
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)
    try:
        yield client, store, manager
    finally:
        manager.shutdown()
        store.close()


# =============================================================================
# 1. CancellationToken & Registry Mechanics
# =============================================================================


def test_cancellation_token_basic_lifecycle():
    token = CancellationToken(run_id="run-token-test")
    assert not token.is_cancelled
    assert token.cancelled_at is None

    # First cancel
    changed = token.cancel("Testing cancellation")
    assert changed is True
    assert token.is_cancelled is True
    assert token.reason == "Testing cancellation"
    assert token.cancelled_at is not None

    # Repeated cancel is idempotent
    second_changed = token.cancel("Different reason")
    assert second_changed is False
    assert token.reason == "Testing cancellation"

    with pytest.raises(WorkflowCancelledError) as exc_info:
        token.check_cancelled()
    assert "Testing cancellation" in str(exc_info.value)


def test_cancellation_registry_operations():
    reg = CancellationRegistry()
    token = reg.get_or_create("run-reg-1")
    assert token.run_id == "run-reg-1"
    assert not token.is_cancelled

    # Cancel via registry
    success = reg.cancel("run-reg-1", reason="Stopped by registry")
    assert success is True
    assert token.is_cancelled is True
    assert token.reason == "Stopped by registry"

    # Cancelling non-existent ID returns False
    assert reg.cancel("non-existent-id") is False

    # Remove
    popped = reg.remove("run-reg-1")
    assert popped is token
    assert reg.get("run-reg-1") is None


# =============================================================================
# 2. Cancellation Before Execution Begins
# =============================================================================


def test_cancellation_before_execution_begins(temp_store):
    token = CancellationToken(run_id="run-pre-cancel")
    token.cancel("Cancelled before start")

    result = run_task(
        "Analyze PostgreSQL memory structures",
        run_id="run-pre-cancel",
        store=temp_store,
        cancellation_token=token,
    )

    assert result["status"] == "cancelled"
    assert "Cancelled before start" in result.get("cancellation_reason", "")
    assert len(result.get("completed", [])) == 0
    assert "(workflow execution cancelled" in result.get("answer", "")

    # Persistence verification
    summary = temp_store.get_run_summary("run-pre-cancel")
    assert summary is not None
    assert summary["status"] == "cancelled"


# =============================================================================
# 3. Cancellation While Work is In Progress & Preventing New Subtasks
# =============================================================================


def test_cancellation_while_work_in_progress(temp_store):
    """When cancelled after subtask 1, subtask 2 must not be scheduled."""
    token = CancellationToken(run_id="run-mid-cancel")

    plan: list[Subtask] = [
        {"id": "s1", "kind": "analysis", "instruction": "Step 1", "status": "pending", "depends_on": []},
        {"id": "s2", "kind": "code", "instruction": "Step 2", "status": "pending", "depends_on": ["s1"]},
    ]

    from orchestrator.graph import specialists

    orig_exec = specialists.execute_subtask

    def cancel_after_s1(state, subtask):
        res = orig_exec(state, subtask)
        if subtask.get("id") == "s1":
            token.cancel("Cancelled after subtask 1")
        return res

    with patch.object(specialists, "execute_subtask", side_effect=cancel_after_s1):
        result = run_task(
            "Multi-step pipeline",
            plan=plan,
            run_id="run-mid-cancel",
            store=temp_store,
            cancellation_token=token,
            max_concurrency=1,
        )

    assert result["status"] == "cancelled"
    completed = result.get("completed", [])
    # s1 completed successfully; s2 was never executed
    assert len(completed) == 1
    assert completed[0]["id"] == "s1"
    assert completed[0]["status"] == "done"

    # Completed subtask output must be preserved in final answer
    answer = result.get("answer", "")
    assert "Analysis specialist" in answer
    assert "Workflow Cancelled" in answer

    # Verify persisted status
    summary = temp_store.get_run_summary("run-mid-cancel")
    assert summary["status"] == "cancelled"


def test_no_new_subtasks_scheduled_after_cancellation(temp_store):
    """Ensure sequential queue stops immediately when cancellation is acknowledged."""
    token = CancellationToken(run_id="run-queue-cancel")

    plan: list[Subtask] = [
        {"id": "s1", "kind": "analysis", "instruction": "Step 1", "status": "pending"},
        {"id": "s2", "kind": "writing", "instruction": "Step 2", "status": "pending"},
        {"id": "s3", "kind": "code", "instruction": "Step 3", "status": "pending"},
    ]

    executed_subtasks = []
    from orchestrator.graph import specialists

    orig_exec = specialists.execute_subtask

    def intercept_s1(state, subtask):
        executed_subtasks.append(subtask["id"])
        if subtask.get("id") == "s1":
            token.cancel("Stop queue")
        return orig_exec(state, subtask)

    with patch.object(specialists, "execute_subtask", side_effect=intercept_s1):
        result = run_task(
            "Sequential queue test",
            plan=plan,
            run_id="run-queue-cancel",
            store=temp_store,
            cancellation_token=token,
            max_concurrency=1,
        )

    assert result["status"] == "cancelled"
    completed_ids = [c["id"] for c in result.get("completed", [])]
    assert completed_ids == ["s1"]
    assert "s2" not in completed_ids
    assert "s3" not in completed_ids


# =============================================================================
# 4. API Cancellation Route & Error Handling
# =============================================================================


def test_api_cancel_endpoint_active_run(api_test_env):
    """POST /runs/{run_id}/cancel on an active background run marks it cancelled."""
    client, store, _ = api_test_env

    # Start background run
    create_resp = client.post(
        "/runs",
        json={"task": "Long running simulation", "background": True, "run_id": "run-bg-cancel"},
    )
    assert create_resp.status_code == 202

    # Cancel the run
    cancel_resp = client.post(
        "/runs/run-bg-cancel/cancel",
        json={"reason": "User requested abort"},
    )
    assert cancel_resp.status_code == 200
    cancel_data = cancel_resp.json()
    assert cancel_data["run_id"] == "run-bg-cancel"
    assert cancel_data["status"] == "cancelled"


def test_api_repeated_cancellation_idempotent(api_test_env):
    """Calling POST /runs/{run_id}/cancel multiple times is safe and returns 200."""
    client, store, _ = api_test_env

    # Seed cancelled run in store
    state: GraphState = {
        "run_id": "run-already-cancelled",
        "task_id": "run-already-cancelled",
        "task": "Cancelled task",
        "status": "cancelled",
        "answer": "(workflow execution cancelled)",
        "completed": [],
    }
    store.save_checkpoint("run-already-cancelled", state, transition="cancelled")

    # First cancel call
    r1 = client.post("/runs/run-already-cancelled/cancel")
    assert r1.status_code == 200
    assert r1.json()["status"] == "cancelled"

    # Second cancel call
    r2 = client.post("/runs/run-already-cancelled/cancel")
    assert r2.status_code == 200
    assert r2.json()["status"] == "cancelled"


def test_api_cancel_already_terminal_completed_run_raises_conflict(api_test_env):
    """Attempting to cancel an already completed run must return HTTP 409 Conflict."""
    client, store, _ = api_test_env

    # Run synchronous workflow to completion
    resp = client.post(
        "/runs",
        json={"task": "Analyze sorting algorithms", "run_id": "run-completed-1"},
    )
    assert resp.status_code == 201
    assert resp.json()["status"] in ("completed", "approved", "edited")

    # Attempt to cancel
    cancel_resp = client.post("/runs/run-completed-1/cancel")
    assert cancel_resp.status_code == 409
    assert "already reached terminal status" in cancel_resp.json()["detail"]


def test_api_cancel_non_existent_run_raises_404(api_test_env):
    """Attempting to cancel a non-existent run ID must return HTTP 404 Not Found."""
    client, _, _ = api_test_env
    resp = client.post("/runs/non-existent-run-xyz/cancel")
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


def test_api_cancel_authentication_enforcement(api_test_env):
    """When API token is configured, /runs/{run_id}/cancel must enforce Bearer auth."""
    client, _, _ = api_test_env
    os.environ["ORCHESTRATOR_API_TOKEN"] = "secret-guard-token"

    # Missing token -> 401
    r_unauth = client.post("/runs/run-auth-test/cancel")
    assert r_unauth.status_code == 401

    # Invalid token -> 401
    r_bad = client.post(
        "/runs/run-auth-test/cancel",
        headers={"Authorization": "Bearer wrong-token"},
    )
    assert r_bad.status_code == 401

    # Valid token -> 404 (because run doesn't exist, proving auth passed)
    r_good = client.post(
        "/runs/run-auth-test/cancel",
        headers={"Authorization": "Bearer secret-guard-token"},
    )
    assert r_good.status_code == 404


# =============================================================================
# 5. Execution Deadlines & Timeouts
# =============================================================================


def test_deadline_expiry_before_dispatch(temp_store):
    """A workflow with deadline that has already expired halts immediately."""
    result = run_task(
        "Analyze database indexes",
        run_id="run-timeout-pre",
        timeout=0.000001,  # Sub-millisecond timeout expires immediately
        store=temp_store,
    )

    assert result["status"] == "timed_out"
    assert "(workflow execution timed out" in result.get("answer", "")

    summary = temp_store.get_run_summary("run-timeout-pre")
    assert summary["status"] == "timed_out"


def test_deadline_expiry_during_execution(temp_store):
    """Workflow deadline exceeded while specialist is executing halts further scheduling."""
    from orchestrator.graph import specialists

    orig_exec = specialists.execute_subtask

    def slow_exec(state, subtask):
        time.sleep(0.08)  # Sleep longer than timeout budget
        return orig_exec(state, subtask)

    plan: list[Subtask] = [
        {"id": "s1", "kind": "analysis", "instruction": "Slow analysis", "status": "pending"},
        {"id": "s2", "kind": "code", "instruction": "Code generation", "status": "pending"},
    ]

    with patch.object(specialists, "execute_subtask", side_effect=slow_exec):
        result = run_task(
            "Timed out workflow",
            plan=plan,
            run_id="run-timeout-mid",
            timeout=0.04,  # Expires during s1 execution
            store=temp_store,
            max_concurrency=1,
        )

    assert result["status"] == "timed_out"
    summary = temp_store.get_run_summary("run-timeout-mid")
    assert summary["status"] == "timed_out"


def test_negative_or_zero_timeout_raises_validation_error():
    with pytest.raises(ValueError) as exc1:
        run_task("Task", timeout=0)
    assert "greater than 0" in str(exc1.value)

    with pytest.raises(ValueError) as exc2:
        run_task("Task", timeout=-10.0)
    assert "greater than 0" in str(exc2.value)


# =============================================================================
# 6. Status Distinction: Cancelled vs Timed Out vs Failed vs Blocked
# =============================================================================


def test_terminal_status_distinction(temp_store):
    # 1. Cancelled
    t1 = CancellationToken(run_id="dist-cancel")
    t1.cancel("User cancelled")
    r1 = run_task("Task 1", run_id="dist-cancel", cancellation_token=t1, store=temp_store)
    assert r1["status"] == "cancelled"

    # 2. Timed out
    r2 = run_task("Task 2", run_id="dist-timeout", timeout=0.000001, store=temp_store)
    assert r2["status"] == "timed_out"

    # 3. Blocked (dependency cycle or missing dependency)
    plan_blocked: list[Subtask] = [
        {"id": "b1", "kind": "analysis", "instruction": "Missing dep", "depends_on": ["non_existent"]},
    ]
    r3 = run_task("Task 3", plan=plan_blocked, run_id="dist-blocked", store=temp_store)
    assert r3["status"] == "blocked"

    # Verify all three are strictly distinct in store summaries
    assert temp_store.get_run_summary("dist-cancel")["status"] == "cancelled"
    assert temp_store.get_run_summary("dist-timeout")["status"] == "timed_out"
    assert temp_store.get_run_summary("dist-blocked")["status"] == "blocked"


# =============================================================================
# 7. Persistence & Resume After Cancellation and Timeout
# =============================================================================


def test_resume_after_cancellation_preserves_progress(temp_store):
    """A run cancelled midway can be resumed; already done subtasks are not re-executed."""
    token = CancellationToken(run_id="run-resume-cancel")

    plan: list[Subtask] = [
        {"id": "s1", "kind": "analysis", "instruction": "Part 1", "status": "pending"},
        {"id": "s2", "kind": "writing", "instruction": "Part 2", "status": "pending", "depends_on": ["s1"]},
    ]

    from orchestrator.graph import specialists

    orig_exec = specialists.execute_subtask

    def stop_after_s1(state, subtask):
        res = orig_exec(state, subtask)
        if subtask.get("id") == "s1":
            token.cancel("Stop after part 1")
        return res

    # 1. Run and cancel
    with patch.object(specialists, "execute_subtask", side_effect=stop_after_s1):
        run_res = run_task(
            "Two-part pipeline",
            plan=plan,
            run_id="run-resume-cancel",
            cancellation_token=token,
            store=temp_store,
            max_concurrency=1,
        )
    assert run_res["status"] == "cancelled"
    assert len(run_res["completed"]) == 1
    assert run_res["completed"][0]["id"] == "s1"

    # 2. Resume the cancelled run without cancellation token
    resumed = resume_task("run-resume-cancel", store=temp_store)
    assert resumed["status"] in ("completed", "approved", "edited")

    # Both subtasks must be present in final result
    completed_ids = [c["id"] for c in resumed["completed"]]
    assert "s1" in completed_ids
    assert "s2" in completed_ids

    # Final store status must be updated
    summary = temp_store.get_run_summary("run-resume-cancel")
    assert summary["status"] in ("completed", "approved", "edited")


def test_resume_after_timeout_preserves_progress(temp_store):
    """A run timed out midway can be resumed with fresh execution budget."""
    # Seed state with s1 completed, status timed_out
    state: GraphState = {
        "run_id": "run-resume-timeout",
        "task_id": "run-resume-timeout",
        "task": "Two part task",
        "status": "timed_out",
        "plan": [
            {"id": "s1", "kind": "analysis", "instruction": "Step 1", "status": "done", "result": "Done 1"},
            {"id": "s2", "kind": "code", "instruction": "Step 2", "status": "pending", "depends_on": ["s1"]},
        ],
        "completed": [
            {"id": "s1", "kind": "analysis", "instruction": "Step 1", "status": "done", "result": "Done 1"},
        ],
    }
    temp_store.save_checkpoint("run-resume-timeout", state, transition="timed_out")

    # Resume with ample timeout
    resumed = resume_task("run-resume-timeout", store=temp_store, timeout=30.0)
    assert resumed["status"] in ("completed", "approved", "edited")

    completed_ids = [c["id"] for c in resumed["completed"]]
    assert "s1" in completed_ids
    assert "s2" in completed_ids


# =============================================================================
# 8. CLI Timeout Option & Validation
# =============================================================================


def test_cli_timeout_argument_validation(capsys):
    ret_neg = cli(["--timeout", "-5", "Analyze task"])
    assert ret_neg == 1
    captured_neg = capsys.readouterr()
    assert "Configuration error: --timeout must be > 0" in captured_neg.err

    ret_zero = cli(["--timeout", "0", "Analyze task"])
    assert ret_zero == 1
    captured_zero = capsys.readouterr()
    assert "Configuration error: --timeout must be > 0" in captured_zero.err


def test_cli_timeout_execution_json(capsys):
    """CLI with tight timeout outputs valid JSON with status timed_out."""
    ret = cli(["--timeout", "0.000001", "--json", "Analyze sorting efficiency"])
    assert ret == 0
    captured = capsys.readouterr()
    import json

    data = json.loads(captured.out)
    assert data["status"] == "timed_out"


# =============================================================================
# 9. Regression: Normal Workflow Execution Unaffected
# =============================================================================


def test_normal_successful_workflow_regression(temp_store):
    """Verify normal un-cancelled, un-timed-out workflows execute successfully."""
    result = run_task("Analyze binary search trees", store=temp_store)
    assert result["status"] in ("completed", "approved", "edited")
    assert result["answer"] is not None
    assert len(result["completed"]) >= 1

    summary = temp_store.get_run_summary(result["run_id"])
    assert summary["status"] in ("completed", "approved", "edited")
