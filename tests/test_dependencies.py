"""Tests for dependency-aware specialist execution, failure propagation, and recovery."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from orchestrator.graph.builder import _run_fallback, check_dependencies, run_task
from orchestrator.graph.state import GraphState
from orchestrator.graph.supervisor import parse_plan_payload
from orchestrator.hitl.queue import Resolution, apply_resolution
from orchestrator.llm import Completion
from orchestrator.memory.long_term import LongTermMemory
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import ShortTermMemory
from orchestrator.memory.short_term import reset_local_store as reset_stm


@pytest.fixture(autouse=True)
def clean_memory():
    """Reset in-process memory stores before and after each test."""
    reset_stm()
    reset_ltm()
    yield
    reset_stm()
    reset_ltm()


def test_subtask_blocked_when_linear_prerequisite_fails():
    """Verify that a downstream subtask is blocked and not executed when its prerequisite fails."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Gather sources"},
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Write article based on sources",
            "depends_on": ["s1"],
        },
    ]

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "research" in system.lower():
            raise RuntimeError("Network timeout while querying search API")
        return Completion(
            "This is a detailed mock output for writing that would normally succeed.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Research and write an article", plan=plan)

        # s1 failed
        failed_tasks = [s for s in result.get("completed", []) if s.get("status") == "failed"]
        assert len(failed_tasks) == 1
        assert failed_tasks[0]["id"] == "s1"
        assert "Network timeout" in failed_tasks[0]["error"]

        # s2 blocked
        blocked_tasks = [s for s in result.get("completed", []) if s.get("status") == "blocked"]
        assert len(blocked_tasks) == 1
        assert blocked_tasks[0]["id"] == "s2"
        assert "s1" in blocked_tasks[0]["error"]
        assert blocked_tasks[0]["result"] == ""

        # writing specialist must not have run or completed successfully
        done_tasks = [s for s in result.get("completed", []) if s.get("status") == "done"]
        assert len(done_tasks) == 0

        # final status and blocked list (pending if escalated, failed if unescalated)
        assert result["status"] in ("failed", "pending")
        assert len(result.get("blocked", [])) == 1
        assert result["blocked"][0]["id"] == "s2"

        # trace contains blocked event
        dispatch_blocked_traces = [
            t for t in result.get("trace", []) if t.get("event") == "blocked" and t.get("subtask") == "s2"
        ]
        assert len(dispatch_blocked_traces) == 1


def test_independent_subtasks_execute_when_another_branch_fails():
    """Verify that independent branches continue executing even if a parallel branch fails."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Fetch data A", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Report on A", "depends_on": ["s1"]},
        {"id": "s3", "kind": "analysis", "instruction": "Analyze dataset B", "depends_on": []},
        {"id": "s4", "kind": "code", "instruction": "Implement algorithm for B", "depends_on": ["s3"]},
    ]

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "research" in system.lower():
            raise RuntimeError("Database connection unreachable")
        if "analysis" in system.lower():
            return Completion("Analysis findings: statistical distribution verified.", "mock", "mock-1")
        if "code" in system.lower():
            return Completion("def solve_b(): return 42 # verified", "mock", "mock-1")
        return Completion("Writing draft for A", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Process parallel datasets", plan=plan)

        # Branch A: s1 failed, s2 blocked
        assert any(s["id"] == "s1" and s["status"] == "failed" for s in result["completed"])
        assert any(s["id"] == "s2" and s["status"] == "blocked" for s in result["completed"])

        # Branch B: s3 done, s4 done
        assert any(s["id"] == "s3" and s["status"] == "done" for s in result["completed"])
        assert any(s["id"] == "s4" and s["status"] == "done" for s in result["completed"])

        # Output includes successful specialist sections
        assert "### Analysis specialist" in result["answer"]
        assert "### Code specialist" in result["answer"]
        assert "Writing draft" not in result["answer"]

        # Status: pending if escalated for human review, partial if evaluated without pending review
        if not result.get("escalated"):
            assert result["status"] == "partial"
        else:
            assert result["status"] == "pending"


def test_cascading_dependencies_blocked_transitively():
    """Verify transitive blocking across a multi-step chain (s1 -> s2 -> s3)."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Step 1: Raw data collection"},
        {"id": "s2", "kind": "analysis", "instruction": "Step 2: Data cleaning", "depends_on": ["s1"]},
        {"id": "s3", "kind": "writing", "instruction": "Step 3: Executive summary", "depends_on": ["s2"]},
    ]

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Data collection failed"),
    ):
        result = run_task("Run 3-step pipeline", plan=plan)

        assert any(s["id"] == "s1" and s["status"] == "failed" for s in result["completed"])
        assert any(s["id"] == "s2" and s["status"] == "blocked" for s in result["completed"])
        assert any(s["id"] == "s3" and s["status"] == "blocked" for s in result["completed"])

        blocked_ids = [s["id"] for s in result.get("blocked", [])]
        assert "s2" in blocked_ids
        assert "s3" in blocked_ids
        assert result["status"] in ("failed", "pending")


def test_diamond_dependencies_execute_in_correct_order():
    """Verify diamond dependency graph: root s1 -> (s2, s3) -> join s4."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Root: specs"},
        {"id": "s2", "kind": "analysis", "instruction": "Branch 1: security analysis", "depends_on": ["s1"]},
        {"id": "s3", "kind": "code", "instruction": "Branch 2: prototype", "depends_on": ["s1"]},
        {"id": "s4", "kind": "writing", "instruction": "Join: final signoff", "depends_on": ["s2", "s3"]},
    ]

    execution_order = []

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "research" in system.lower():
            execution_order.append("s1")
            return Completion("Specs document generated with 10 requirements.", "mock", "mock-1")
        if "analysis" in system.lower():
            execution_order.append("s2")
            return Completion("Security analysis completed: 0 vulnerabilities.", "mock", "mock-1")
        if "code" in system.lower():
            execution_order.append("s3")
            return Completion("Prototype implementation passed 12 unit tests.", "mock", "mock-1")
        if "writing" in system.lower():
            execution_order.append("s4")
            # Verify that prerequisite outputs were passed into prompt
            assert "s2" in prompt
            assert "s3" in prompt
            return Completion("Final report: All security and code checks passed.", "mock", "mock-1")
        return Completion("Default output", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Diamond workflow test", plan=plan)

        assert execution_order[0] == "s1"
        assert set(execution_order[1:3]) == {"s2", "s3"}
        assert execution_order[3] == "s4"
        assert result["status"] == "completed"
        assert len(result["completed"]) == 4
        assert all(s["status"] == "done" for s in result["completed"])


def test_prerequisite_recovery_on_retry_unblocks_downstream():
    """Verify that if an upstream prerequisite succeeds on retry, its dependent subtasks execute."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Query data"},
        {"id": "s2", "kind": "writing", "instruction": "Write report on data", "depends_on": ["s1"]},
    ]

    attempt = 0

    def mock_complete(system="", prompt="", *args, **kwargs):
        nonlocal attempt
        if "research" in system.lower():
            attempt += 1
            if attempt == 1:
                # First attempt fails with transient error
                raise RuntimeError("Transient 503 Service Unavailable")
            # Retry succeeds
            return Completion(
                "Successfully fetched research data on retry with full details.", "mock", "mock-1"
            )
        return Completion("Comprehensive writing report based on recovered data.", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Retry recovery test", plan=plan)

        # Downstream subtask s2 must NOT be blocked
        assert len(result.get("blocked", [])) == 0
        done_ids = [s["id"] for s in result.get("completed", []) if s.get("status") == "done"]
        assert "s1" in done_ids
        assert "s2" in done_ids
        assert result["status"] == "completed"


def test_prerequisite_recovery_via_hitl_resolution():
    """Verify that human resolution (edit/approve) unblocks downstream dependent subtasks."""
    # State paused at s1 with pending s2
    state: GraphState = {
        "task_id": "task-hitl-dep",
        "task": "Perform audited migration",
        "plan": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Evaluate schema migration",
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "writing",
                "instruction": "Document migration signoff",
                "status": "pending",
                "depends_on": ["s1"],
            },
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Evaluate schema migration",
                "status": "done",
                "result": "Initial unapproved migration proposal",
            }
        ],
        "escalated": True,
        "escalated_subtask_id": "s1",
        "approval": "approve_action",
        "escalations": [{"subtask_id": "s1", "reason": "sensitive_operation"}],
    }

    # Human resolves s1 with edit
    res = Resolution(
        task_id="task-hitl-dep",
        decision="edit",
        approver="lead_architect",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="Initial unapproved migration proposal",
        final_input="Approved safe migration proposal",
        final_result="Approved safe migration proposal",
        subtask_id="s1",
    )
    resolved_state = apply_resolution(state, res)
    assert resolved_state["status"] == "edited"
    assert resolved_state["completed"][0]["status"] == "done"

    # Now continue execution of resolved_state: s2 should run and not be blocked
    final_state = _run_fallback(resolved_state)
    assert any(s["id"] == "s2" and s["status"] == "done" for s in final_state["completed"])
    assert len(final_state.get("blocked", [])) == 0


def test_prerequisite_rejection_via_hitl_blocks_downstream():
    """Verify that human rejection of an upstream subtask blocks dependent downstream subtasks."""
    state: GraphState = {
        "task_id": "task-hitl-rej",
        "task": "Dangerous operation",
        "plan": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Check dangerous action",
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "writing",
                "instruction": "Execute dangerous action writeup",
                "status": "pending",
                "depends_on": ["s1"],
            },
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Check dangerous action",
                "status": "done",
                "result": "Dangerous action details",
            }
        ],
        "escalated": True,
        "escalated_subtask_id": "s1",
        "approval": "approve_action",
        "escalations": [{"subtask_id": "s1", "reason": "sensitive_operation"}],
    }

    res = Resolution(
        task_id="task-hitl-rej",
        decision="reject",
        approver="security_officer",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="Dangerous action details",
        final_input="",
        final_result=None,
        subtask_id="s1",
    )
    resolved_state = apply_resolution(state, res)
    assert resolved_state["status"] == "rejected"
    assert resolved_state["completed"][0]["status"] == "rejected"

    # Dependency check transitions s2 to blocked
    blocked_update = check_dependencies(resolved_state)
    assert len(blocked_update.get("blocked", [])) == 1
    assert blocked_update["blocked"][0]["id"] == "s2"


def test_missing_dependency_in_plan_is_blocked_safely():
    """Verify that a subtask declaring a dependency not present in the plan is blocked safely."""
    plan = [
        {"id": "s1", "kind": "code", "instruction": "Implement module", "depends_on": ["missing_subtask_99"]},
    ]
    result = run_task("Test missing dependency", plan=plan)

    assert result["status"] == "blocked"
    assert len(result.get("blocked", [])) == 1
    assert result["blocked"][0]["id"] == "s1"
    assert "not found in plan" in result["blocked"][0]["error"].lower()
    assert "(subtasks blocked" in result["answer"].lower()


def test_circular_dependency_deadlock_detected_safely():
    """Verify that circular dependencies (s1 <-> s2) are detected and blocked without infinite loops."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Needs s2", "depends_on": ["s2"]},
        {"id": "s2", "kind": "analysis", "instruction": "Needs s1", "depends_on": ["s1"]},
    ]
    result = run_task("Test circular dependency", plan=plan)

    assert result["status"] == "blocked"
    blocked_ids = [s["id"] for s in result.get("blocked", [])]
    assert "s1" in blocked_ids
    assert "s2" in blocked_ids
    assert any("circular" in s.get("error", "").lower() for s in result["blocked"])


def test_parse_plan_payload_handles_dependency_aliases():
    """Verify parse_plan_payload parses depends_on, dependencies, and prerequisites."""
    raw_plan = [
        {"id": "step_a", "kind": "research", "instruction": "Research A"},
        {"id": "step_b", "kind": "analysis", "instruction": "Analyze B", "dependencies": ["step_a"]},
        {"id": "step_c", "kind": "writing", "instruction": "Write C", "prerequisites": "step_b"},
        {"kind": "code", "instruction": "Code D", "depends_on": ["step_b", "step_c"]},
    ]
    parsed = parse_plan_payload(raw_plan)
    assert parsed is not None
    assert len(parsed) == 4
    assert parsed[0]["id"] == "step_a"
    assert parsed[0]["depends_on"] == []
    assert parsed[1]["id"] == "step_b"
    assert parsed[1]["depends_on"] == ["step_a"]
    assert parsed[2]["id"] == "step_c"
    assert parsed[2]["depends_on"] == ["step_b"]
    assert parsed[3]["id"] == "s4"
    assert parsed[3]["depends_on"] == ["step_b", "step_c"]


def test_short_term_and_long_term_memory_isolation_with_blocked():
    """Verify ShortTermMemory records blocked subtasks and LongTermMemory omits blocked tasks."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Gather sources"},
        {"id": "s2", "kind": "code", "instruction": "Write code from sources", "depends_on": ["s1"]},
    ]

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Provider offline"),
    ):
        result = run_task("Perform blocked memory task", plan=plan)
        task_id = result["task_id"]

        # Short-term memory has recorded blocked task
        stm = ShortTermMemory(task_id)
        assert stm.get("status") in ("failed", "pending")
        assert stm.get("subtask:s2") is not None
        assert stm.get("subtask:s2")["status"] == "blocked"

        # Long-term memory must NOT contain the blocked outcome
        ltm = LongTermMemory()
        recalled = ltm.recall("Perform blocked memory task")
        assert not any("Perform blocked memory task" in r for r in recalled)


def test_reducer_partial_and_blocked_status():
    """Verify supervisor.reduce produces partial status when some tasks are done and some are blocked,

    and produces blocked status when all tasks are blocked.
    """
    from orchestrator.graph.supervisor import reduce

    # Case 1: Partial completion (1 done, 1 blocked)
    state_partial: GraphState = {
        "completed": [
            {"id": "s1", "kind": "research", "status": "done", "result": "Research output"},
            {"id": "s2", "kind": "code", "status": "blocked", "error": "s1 failed"},
        ],
        "blocked": [
            {"id": "s2", "kind": "code", "status": "blocked", "error": "s1 failed"},
        ],
        "escalations": [],
    }
    red_partial = reduce(state_partial)
    assert red_partial["status"] == "partial"
    assert "### Research specialist" in red_partial["answer"]
    assert red_partial["trace"][0]["parts"] == 1
    assert red_partial["trace"][0]["blocked"] == 1

    # Case 2: All tasks blocked (0 done, 0 failed, 2 blocked)
    state_blocked: GraphState = {
        "completed": [
            {"id": "s1", "kind": "research", "status": "blocked", "error": "cycle"},
            {"id": "s2", "kind": "code", "status": "blocked", "error": "cycle"},
        ],
        "blocked": [
            {"id": "s1", "kind": "research", "status": "blocked", "error": "cycle"},
            {"id": "s2", "kind": "code", "status": "blocked", "error": "cycle"},
        ],
        "escalations": [],
    }
    red_blocked = reduce(state_blocked)
    assert red_blocked["status"] == "blocked"
    assert "(subtasks blocked" in red_blocked["answer"].lower()
    assert red_blocked["trace"][0]["blocked"] == 2
