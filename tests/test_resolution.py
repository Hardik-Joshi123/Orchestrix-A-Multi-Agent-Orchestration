"""Tests for HITL loop closure, resolution application, and safety hardening.

Covers all core regression scenarios and Task 03B hardening requirements:
 1. Escalated task requiring take_over does not return unapproved generated answer.
 2. Unresolved review item remains explicitly pending.
 3. Human takeover replaces workflow's final answer with reviewer-provided response.
 4. Human approval accepts proposed action.
 5. Human edit modifies result according to reviewer input.
 6. Human rejection prevents action from being accepted and marks it rejected.
 7. Resolution cannot be applied to wrong task (task ID mismatch handling).
 8. Invalid decisions and unauthorized resolution attempts are rejected.
 9. Audit provenance is preserved across escalation and resolution lifecycle.
10. Non-escalated tasks proceed normally without requiring review.
11. Stable identifier lookup (item_id and task_id) and missing identifier safety.
12. Queue reordering does not misdirect stable resolution.
13. Multi-subtask early escalation targets exact subtask and preserves unaffected subtasks.
14. Multi-subtask rejection targets exact subtask and preserves unaffected subtasks.
15. Duplicate and conflicting resolutions on already-resolved state are rejected.
16. LangGraph routing function _after_escalate halts take_over and continues dispatch for others.
17. Long-term memory write is skipped for unresolved pending tasks.
18. Long-term memory write is preserved for completed non-escalated tasks.
"""

from __future__ import annotations

import pytest

import orchestrator.hitl.queue as queue_mod
import orchestrator.memory.long_term as long_term_mod
from orchestrator.graph import run_task
from orchestrator.graph.builder import _after_escalate, _after_review
from orchestrator.hitl.audit import AuditChain
from orchestrator.hitl.queue import Resolution, ReviewItem, ReviewQueue, apply_resolution
from orchestrator.llm import LLMClient
from orchestrator.memory.long_term import LongTermMemory
from orchestrator.memory.short_term import ShortTermMemory


@pytest.fixture(autouse=True)
def _clean_hitl():
    queue_mod._LOCAL.clear()
    AuditChain().clear()
    long_term_mod._LOCAL.clear()
    yield
    queue_mod._LOCAL.clear()
    AuditChain().clear()
    long_term_mod._LOCAL.clear()


def test_take_over_does_not_expose_unapproved_answer():
    """1. An escalated task requiring take_over does not return an unapproved generated answer."""
    result = run_task("Please DROP TABLE customers;")
    assert result["escalated"] is True
    assert result["approval"] == "take_over"
    assert result["status"] == "pending"
    assert "Human Review Required (take_over)" in result["answer"]
    assert "Execution paused" in result["answer"]
    # The generated raw specialist output is not exposed as an approved answer
    assert "DROP TABLE" not in result["answer"]


def test_unresolved_review_item_remains_pending():
    """2. An unresolved review item remains pending in the review queue."""
    queue = ReviewQueue()
    assert len(queue.pending()) == 0
    result = run_task("Please DROP TABLE sensitive_records;")
    pending = queue.pending()
    assert len(pending) == 1
    item = pending[0]
    assert item.task_id == result["task_id"]
    assert item.level == "take_over"
    assert item.reason == "sensitive_operation"
    assert result["status"] == "pending"
    # Ensure item stays pending until explicitly resolved
    assert len(queue.pending()) == 1


def test_human_takeover_replaces_final_answer():
    """3. Human takeover replaces the workflow's final answer with the reviewer-provided response."""
    queue = ReviewQueue()
    result = run_task("Please DROP TABLE accounts;")
    assert result["status"] == "pending"

    res = queue.resolve(
        identifier=result["task_id"],
        decision="take_over",
        approver="security_officer",
        note="SELECT count(*) FROM accounts; -- Safely inspected without dropping",
    )
    assert res is not None
    assert res.final_result == "SELECT count(*) FROM accounts; -- Safely inspected without dropping"

    resolved = apply_resolution(result, res)
    assert resolved["status"] == "taken_over"
    assert resolved["escalated"] is False
    assert resolved["answer"] == "SELECT count(*) FROM accounts; -- Safely inspected without dropping"
    assert resolved["resolution"]["decision"] == "take_over"
    assert resolved["resolution"]["approver"] == "security_officer"


def test_human_approval_accepts_proposed_action():
    """4. Human approval accepts the proposed action."""
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="t-app-1",
        task="Check cluster status",
        reason="low_confidence",
        level="approve_action",
        proposed_action="kubectl get pods -A",
        confidence=0.4,
        quality=0.5,
    )
    queue.push(item)
    res = queue.resolve(item.item_id, "approve", approver="devops_lead")
    assert res.final_result == "kubectl get pods -A"

    state = {
        "task_id": "t-app-1",
        "task": "Check cluster status",
        "escalated": True,
        "status": "pending",
        "completed": [{"id": "s1", "kind": "analysis", "result": "kubectl get pods -A", "status": "done"}],
    }
    resolved = apply_resolution(state, res)
    assert resolved["status"] == "approved"
    assert resolved["escalated"] is False
    assert "kubectl get pods -A" in resolved["answer"]


def test_human_edit_modifies_result():
    """5. Human edit modifies the result according to reviewer input."""
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="t-edit-1",
        task="Draft announcement",
        reason="low_quality",
        level="approve_action",
        proposed_action="Draft with minor typo",
        confidence=0.7,
        quality=0.4,
        subtask_id="s1",
    )
    queue.push(item)
    res = queue.resolve(
        item.item_id, "edit", approver="editor_bob", note="Polished announcement without typo"
    )
    assert res.final_result == "Polished announcement without typo"

    state = {
        "task_id": "t-edit-1",
        "task": "Draft announcement",
        "escalated": True,
        "status": "pending",
        "completed": [{"id": "s1", "kind": "writing", "result": "Draft with minor typo", "status": "done"}],
    }
    resolved = apply_resolution(state, res)
    assert resolved["status"] == "edited"
    assert resolved["escalated"] is False
    assert "Polished announcement without typo" in resolved["answer"]
    assert resolved["completed"][-1]["result"] == "Polished announcement without typo"


def test_human_rejection_marks_rejected():
    """6. Human rejection prevents the action from being accepted and marks it rejected."""
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="t-rej-1",
        task="Dangerous operation",
        reason="sensitive_operation",
        level="take_over",
        proposed_action="rm -rf /data",
        confidence=0.8,
        quality=0.8,
        subtask_id="s1",
    )
    queue.push(item)
    res = queue.resolve(item.item_id, "reject", approver="admin_alice")
    assert res.final_result is None

    state = {
        "task_id": "t-rej-1",
        "task": "Dangerous operation",
        "escalated": True,
        "status": "pending",
        "completed": [{"id": "s1", "kind": "code", "result": "rm -rf /data", "status": "done"}],
    }
    resolved = apply_resolution(state, res)
    assert resolved["status"] == "rejected"
    assert resolved["escalated"] is False
    assert "(rejected by human reviewer)" in resolved["answer"]
    assert "rm -rf /data" not in resolved["answer"]
    assert resolved["completed"][-1]["status"] == "rejected"


def test_resolution_cannot_be_applied_to_wrong_task():
    """7. A resolution cannot be applied to the wrong task (task ID mismatch handling)."""
    state = {
        "task_id": "task-AAA",
        "task": "Operation A",
        "escalated": True,
        "status": "pending",
    }
    resolution = Resolution(
        task_id="task-BBB",
        decision="approve",
        approver="admin_alice",
        timestamp="2026-10-18T10:00:00Z",
        proposed_action="Action B",
        final_input="",
        final_result="Action B",
    )
    with pytest.raises(ValueError, match="Task ID mismatch"):
        apply_resolution(state, resolution)


def test_invalid_decisions_and_unauthorized_attempts_rejected():
    """8. Invalid decisions and unauthorized resolution attempts are rejected."""
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="t-inv-1",
        task="Some action",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Do something",
        confidence=0.5,
        quality=0.5,
    )
    queue.push(item)

    # Empty approver rejected
    with pytest.raises(ValueError, match="approver identity is required"):
        queue.resolve(item.item_id, "approve", approver="")

    # Invalid decision rejected
    with pytest.raises(ValueError, match="unknown decision"):
        queue.resolve(item.item_id, "bypass_all", approver="alice")

    # Missing note for edit / take_over rejected
    with pytest.raises(ValueError, match="requires reviewer-provided text"):
        queue.resolve(item.item_id, "take_over", approver="alice", note="")

    with pytest.raises(ValueError, match="requires reviewer-provided text"):
        queue.resolve(item.item_id, "edit", approver="alice", note="   ")

    # apply_resolution also enforces valid decision and non-empty approver
    bad_res_no_approver = Resolution(
        task_id="t-inv-1",
        decision="approve",
        approver="",
        timestamp="",
        proposed_action="",
        final_input="",
        final_result="",
    )
    with pytest.raises(ValueError, match="approver identity is required"):
        apply_resolution({"task_id": "t-inv-1"}, bad_res_no_approver)

    bad_res_decision = Resolution(
        task_id="t-inv-1",
        decision="illegal_decision",
        approver="alice",
        timestamp="",
        proposed_action="",
        final_input="",
        final_result="",
    )
    with pytest.raises(ValueError, match="unknown decision"):
        apply_resolution({"task_id": "t-inv-1"}, bad_res_decision)


def test_audit_provenance_preserved():
    """9. Audit provenance is preserved across the escalation and resolution lifecycle."""
    queue = ReviewQueue()
    chain = AuditChain()
    assert chain.verify() == (True, None)

    result = run_task("Please DROP TABLE sensitive_table;")
    task_id = result["task_id"]
    res = queue.resolve(task_id, "take_over", approver="compliance_officer", note="Archived instead")
    assert res is not None
    resolved = apply_resolution(result, res)

    intact, broken_at = chain.verify()
    assert intact is True
    assert broken_at is None

    records = chain.records()
    assert len(records) == 1
    rec = records[0]
    assert rec["task_id"] == task_id
    assert rec["decision"] == "take_over"
    assert rec["approver"] == "compliance_officer"
    assert rec["final_input"] == "Archived instead"
    assert "record_hash" in rec
    assert "previous_hash" in rec

    trace_events = [t for t in resolved.get("trace", []) if t.get("event") == "resolution_applied"]
    assert len(trace_events) == 1
    assert trace_events[0]["approver"] == "compliance_officer"
    assert trace_events[0]["decision"] == "take_over"


def test_non_escalated_tasks_proceed_normally():
    """10. Non-escalated tasks proceed normally without requiring review."""
    queue = ReviewQueue()
    result = run_task("Summarize the quarterly report")
    assert result["escalated"] is False
    assert result["status"] == "completed"
    assert result["answer"]
    assert "Human Review Required" not in result["answer"]
    assert len(queue.pending()) == 0


def test_resolve_by_stable_identifiers():
    """11. Stable identifier resolution by item_id and task_id."""
    queue = ReviewQueue()
    item1 = ReviewItem(
        task_id="task-st-1",
        task="Task 1",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Action 1",
        confidence=0.5,
        quality=0.5,
    )
    item2 = ReviewItem(
        task_id="task-st-2",
        task="Task 2",
        reason="low_quality",
        level="approve_action",
        proposed_action="Action 2",
        confidence=0.5,
        quality=0.5,
    )
    queue.push(item1)
    queue.push(item2)

    # Resolve item 2 by its unique item_id
    res2 = queue.resolve(item2.item_id, "approve", approver="alice")
    assert res2 is not None
    assert res2.task_id == "task-st-2"
    assert res2.item_id == item2.item_id

    # Resolve item 1 by its task_id
    res1 = queue.resolve("task-st-1", "approve", approver="bob")
    assert res1 is not None
    assert res1.task_id == "task-st-1"
    assert len(queue.pending()) == 0


def test_resolve_unknown_or_missing_identifier_fails_safely():
    """12. Missing, empty, or unknown identifiers fail safely without resolving any item."""
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="task-safe-1",
        task="Task 1",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Action 1",
        confidence=0.5,
        quality=0.5,
    )
    queue.push(item)

    assert queue.resolve("nonexistent-id", "approve", approver="alice") is None
    assert queue.resolve("", "approve", approver="alice") is None
    assert queue.resolve("   ", "approve", approver="alice") is None
    # Item remains safely pending in queue
    assert len(queue.pending()) == 1


def test_queue_reorder_does_not_redirect_resolution():
    """13. Queue reorder does not cause a stable resolution to hit the wrong item."""
    queue = ReviewQueue()
    item_a = ReviewItem(
        task_id="task-A",
        task="Task A",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Action A",
        confidence=0.5,
        quality=0.5,
    )
    item_b = ReviewItem(
        task_id="task-B",
        task="Task B",
        reason="low_quality",
        level="approve_action",
        proposed_action="Action B",
        confidence=0.5,
        quality=0.5,
    )
    queue.push(item_a)
    queue.push(item_b)

    # Reorder the queue in the underlying store (item B is now first in memory)
    queue_mod._LOCAL.reverse()

    # Resolving "task-A" should still resolve item A, NOT item B!
    res = queue.resolve("task-A", "approve", approver="alice")
    assert res is not None
    assert res.task_id == "task-A"
    assert res.proposed_action == "Action A"

    # Item B remains in the queue
    pending = queue.pending()
    assert len(pending) == 1
    assert pending[0].task_id == "task-B"


def test_multi_subtask_early_escalation_targets_correct_subtask():
    """14. Resolution targets the exact escalated subtask and preserves unaffected subtasks."""
    state = {
        "task_id": "task-multi-sub",
        "task": "Research, analyze, and write report",
        "status": "pending",
        "escalated": True,
        "completed": [
            {"id": "s1", "kind": "research", "result": "Research on vector databases", "status": "done"},
            {"id": "s2", "kind": "analysis", "result": "DROP TABLE analysis", "status": "done"},
            {"id": "s3", "kind": "writing", "result": "Summary writeup on trade-offs", "status": "done"},
        ],
    }

    # Reviewer edits subtask s2 specifically
    resolution = Resolution(
        task_id="task-multi-sub",
        decision="edit",
        approver="reviewer_alice",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="DROP TABLE analysis",
        final_input="Safe analysis of trade-offs",
        final_result="Safe analysis of trade-offs",
        subtask_id="s2",
    )

    resolved = apply_resolution(state, resolution)

    # Verify s2 was edited
    assert resolved["status"] == "edited"
    assert resolved["completed"][1]["result"] == "Safe analysis of trade-offs"
    assert resolved["completed"][1]["status"] == "done"

    # Verify s1 and s3 were unaffected
    assert resolved["completed"][0]["result"] == "Research on vector databases"
    assert resolved["completed"][0]["status"] == "done"
    assert resolved["completed"][2]["result"] == "Summary writeup on trade-offs"
    assert resolved["completed"][2]["status"] == "done"

    # Verify final answer includes all three sections
    assert "Research on vector databases" in resolved["answer"]
    assert "Safe analysis of trade-offs" in resolved["answer"]
    assert "Summary writeup on trade-offs" in resolved["answer"]


def test_multi_subtask_rejection_targets_correct_subtask():
    """15. Rejection marks the specific escalated subtask as rejected and preserves unaffected subtasks."""
    state = {
        "task_id": "task-multi-rej",
        "task": "Research and execute action",
        "status": "pending",
        "escalated": True,
        "completed": [
            {"id": "s1", "kind": "research", "result": "Research data", "status": "done"},
            {"id": "s2", "kind": "code", "result": "rm -rf /data", "status": "done"},
        ],
    }

    resolution = Resolution(
        task_id="task-multi-rej",
        decision="reject",
        approver="admin_bob",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="rm -rf /data",
        final_input="",
        final_result=None,
        subtask_id="s2",
    )

    resolved = apply_resolution(state, resolution)

    assert resolved["status"] == "rejected"
    # s2 is marked rejected
    assert resolved["completed"][1]["status"] == "rejected"
    assert resolved["completed"][1]["result"] == "(rejected by human reviewer)"

    # s1 is preserved
    assert resolved["completed"][0]["status"] == "done"
    assert resolved["completed"][0]["result"] == "Research data"
    assert "Research data" in resolved["answer"]
    assert "rm -rf" not in resolved["answer"]


def test_duplicate_and_conflicting_resolution_prevented():
    """16. An already-resolved task cannot be resolved again or overwritten with a conflicting decision."""
    state = {
        "task_id": "task-dup-res",
        "task": "Perform maintenance",
        "status": "pending",
        "escalated": True,
        "completed": [{"id": "s1", "kind": "analysis", "result": "Proposed check", "status": "done"}],
    }

    res_approve = Resolution(
        task_id="task-dup-res",
        decision="approve",
        approver="lead_dev",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="Proposed check",
        final_input="",
        final_result="Proposed check",
    )

    resolved = apply_resolution(state, res_approve)
    assert resolved["status"] == "approved"

    # Attempt duplicate resolution
    with pytest.raises(ValueError, match="already resolved"):
        apply_resolution(resolved, res_approve)

    # Attempt conflicting second resolution
    res_reject = Resolution(
        task_id="task-dup-res",
        decision="reject",
        approver="security_officer",
        timestamp="2026-10-19T10:01:00Z",
        proposed_action="Proposed check",
        final_input="",
        final_result=None,
    )
    with pytest.raises(ValueError, match="already resolved"):
        apply_resolution(resolved, res_reject)

    # State status remains approved
    assert resolved["status"] == "approved"


def test_langgraph_routing_decision_logic():
    """17. Verify routing functions ensure take_over routes to reduce while others continue dispatch."""
    # Escalation routing: take_over routes directly to reduce
    assert _after_escalate({"approval": "take_over"}) == "reduce"
    # Non-takeover escalations continue to dispatch
    assert _after_escalate({"approval": "approve_action"}) == "dispatch"
    assert _after_escalate({"approval": "approve_plan"}) == "dispatch"
    assert _after_escalate({"approval": "notify"}) == "dispatch"
    assert _after_escalate({}) == "dispatch"

    # Review routing
    assert _after_review({"escalated": True}) == "escalate"
    assert _after_review({"escalated": False}) == "dispatch"
    assert _after_review({}) == "dispatch"


def test_long_term_memory_not_written_for_pending_unresolved_tasks():
    """18. Unresolved pending tasks are not written into long-term memory."""
    long_term_mod._LOCAL.clear()

    result = run_task("Please DROP TABLE secret_customers;")
    assert result["status"] == "pending"
    assert result["escalated"] is True

    # Check long-term memory store
    memories = long_term_mod._LOCAL
    task_memories = [
        m for m in memories if m.get("meta", {}).get("task") == "Please DROP TABLE secret_customers;"
    ]
    assert len(task_memories) == 0
    assert len(LongTermMemory().recall("secret_customers")) == 0


def test_long_term_memory_preserved_for_completed_tasks():
    """19. Normal completed tasks correctly record outcomes in long-term memory."""
    long_term_mod._LOCAL.clear()

    result = run_task("Summarize the quarterly report")
    assert result["status"] == "completed"
    assert result["escalated"] is False

    memories = long_term_mod._LOCAL
    task_memories = [m for m in memories if m.get("meta", {}).get("task") == "Summarize the quarterly report"]
    assert len(task_memories) == 1
    assert result["answer"] in task_memories[0]["doc"]
    assert len(LongTermMemory().recall("quarterly report")) >= 1


def test_short_term_memory_tracks_subtasks():
    """Short-term memory records subtasks as they complete."""
    result = run_task("Summarize the quarterly report")
    task_id = result["task_id"]
    mem = ShortTermMemory(task_id).all()
    assert "task" in mem
    assert any(k.startswith("subtask:") for k in mem)


def test_llm_client_fallback_resilience():
    """LLM client gracefully falls back to mock when provider fails."""
    client = LLMClient(provider="openai")
    comp = client.complete("system", "prompt")
    assert comp.provider == "mock"
    assert comp.text


def test_subtask_id_propagated_from_review_to_queue_item():
    """20. Subtask identity is propagated from reviewer to escalation queue item."""
    result = run_task("Please DROP TABLE test_prop;")
    assert result["status"] == "pending"

    queue = ReviewQueue()
    pending = queue.pending()
    assert len(pending) == 1
    assert pending[0].subtask_id == "s1"
    assert pending[0].task_id == result["task_id"]

    # Verify trace events recorded subtask_id
    rev_events = [t for t in result.get("trace", []) if t.get("event") == "review"]
    assert len(rev_events) >= 1
    assert rev_events[-1].get("subtask_id") == "s1"

    esc_events = [t for t in result.get("trace", []) if t.get("event") == "escalate"]
    assert len(esc_events) >= 1
    assert esc_events[-1].get("subtask_id") == "s1"


def test_multiple_review_items_same_task_ambiguity_fails_safely():
    """21. Resolving by task_id fails safely when multiple items exist for the same task."""
    queue = ReviewQueue()
    item1 = ReviewItem(
        task_id="task-shared",
        task="Action 1",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Do 1",
        confidence=0.5,
        quality=0.5,
        subtask_id="s1",
    )
    item2 = ReviewItem(
        task_id="task-shared",
        task="Action 2",
        reason="low_quality",
        level="approve_action",
        proposed_action="Do 2",
        confidence=0.5,
        quality=0.5,
        subtask_id="s2",
    )
    queue.push(item1)
    queue.push(item2)

    # Resolving by ambiguous task_id raises ValueError
    with pytest.raises(ValueError, match="Ambiguous resolution"):
        queue.resolve("task-shared", "approve", approver="alice")

    # Resolving by specific item_id succeeds unambiguously
    res1 = queue.resolve(item1.item_id, "approve", approver="alice")
    assert res1 is not None
    assert res1.item_id == item1.item_id
    assert res1.subtask_id == "s1"

    # Now only item2 remains, resolving by item2's item_id succeeds
    res2 = queue.resolve(item2.item_id, "reject", approver="bob")
    assert res2 is not None
    assert res2.item_id == item2.item_id
    assert res2.subtask_id == "s2"
    assert len(queue.pending()) == 0


def test_legacy_index_resolution_blocked_when_multiple_items_pending():
    """22. Integer index resolution is disallowed when multiple items are pending."""
    queue = ReviewQueue()
    item1 = ReviewItem(
        task_id="t1",
        task="Task 1",
        reason="low_confidence",
        level="approve_action",
        proposed_action="P1",
        confidence=0.5,
        quality=0.5,
    )
    item2 = ReviewItem(
        task_id="t2",
        task="Task 2",
        reason="low_quality",
        level="approve_action",
        proposed_action="P2",
        confidence=0.5,
        quality=0.5,
    )
    queue.push(item1)
    queue.push(item2)

    # Multiple items pending -> integer index rejected
    with pytest.raises(ValueError, match="Ambiguous legacy index resolution"):
        queue.resolve(0, "approve", approver="alice")

    # Resolving via stable item_id succeeds
    queue.resolve(item1.item_id, "approve", approver="alice")

    # Now only 1 item remains in queue -> integer index 0 is unambiguous and allowed
    res2 = queue.resolve(0, "approve", approver="alice")
    assert res2 is not None
    assert res2.task_id == "t2"


def test_apply_resolution_rejects_missing_or_invalid_subtask_in_multi_subtask_state():
    """23. In multi-subtask plans, edit/reject fails safely if subtask_id is missing or invalid."""
    state = {
        "task_id": "task-multi-subtask",
        "task": "Two step task",
        "status": "pending",
        "escalated": True,
        "completed": [
            {"id": "s1", "kind": "analysis", "result": "Subtask 1 done", "status": "done"},
            {"id": "s2", "kind": "writing", "result": "Subtask 2 done", "status": "done"},
        ],
    }

    # Missing subtask_id with multiple completed subtasks -> raises ValueError
    res_missing_subtask = Resolution(
        task_id="task-multi-subtask",
        decision="edit",
        approver="alice",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="Something",
        final_input="Edited text",
        final_result="Edited text",
        subtask_id="",
    )
    with pytest.raises(ValueError, match="subtask_id is required"):
        apply_resolution(state, res_missing_subtask)

    # Invalid subtask_id that does not exist -> raises ValueError
    res_invalid_subtask = Resolution(
        task_id="task-multi-subtask",
        decision="reject",
        approver="alice",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="Something",
        final_input="",
        final_result=None,
        subtask_id="s99_nonexistent",
    )
    with pytest.raises(ValueError, match="not found in completed subtasks"):
        apply_resolution(state, res_invalid_subtask)
