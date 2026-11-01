"""Tests for ShortTermMemory, LongTermMemory, context retrieval, and HITL outcome filtering."""

from __future__ import annotations

import pytest

from orchestrator.graph import run_task
from orchestrator.hitl.queue import ReviewItem, ReviewQueue, apply_resolution
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


def test_short_term_memory_crud_local():
    short = ShortTermMemory("task-test-01")
    assert short.get("missing_key") is None
    assert short.all() == {}

    short.set("step1", {"status": "done", "result": "sample output"})
    short.set("step2", 42)

    assert short.get("step1") == {"status": "done", "result": "sample output"}
    assert short.get("step2") == 42
    assert len(short.all()) == 2

    short.clear()
    assert short.all() == {}


def test_short_term_memory_redis_unavailable_graceful(monkeypatch):
    """Confirm ShortTermMemory operates cleanly even if redis module or connection raises."""
    # Force _connect to simulate connection failure
    monkeypatch.setattr(ShortTermMemory, "_connect", lambda self: None)
    short = ShortTermMemory("task-test-fail")
    assert short._client is None

    # Should not raise any exceptions
    short.set("key", "value")
    assert short.get("key") == "value"
    assert short.all() == {"key": "value"}
    short.clear()
    assert short.all() == {}


def test_long_term_memory_remember_and_recall_local():
    ltm = LongTermMemory()
    ltm.remember(
        task="How do I deploy Kubernetes clusters?",
        outcome="Use terraform scripts and Helm charts for Kubernetes deployment.",
        metadata={"category": "devops"},
    )
    ltm.remember(
        task="Recipe for chocolate chip cookies",
        outcome="Mix flour, sugar, butter, and chocolate chips.",
        metadata={"category": "cooking"},
    )

    recalled = ltm.recall("Deploy Kubernetes on cloud", k=2)
    assert len(recalled) >= 1
    assert any("Kubernetes" in doc for doc in recalled)
    assert not any("chocolate chip cookies" in doc for doc in recalled)


def test_long_term_memory_deduplication():
    """Duplicate remember calls for the identical task and outcome should not create duplicate entries."""
    ltm = LongTermMemory()
    task = "Analyze quarterly financial revenue"
    outcome = "Revenue grew 14% year over year."

    # Call remember multiple times
    ltm.remember(task=task, outcome=outcome, metadata={"run": 1})
    ltm.remember(task=task, outcome=outcome, metadata={"run": 2})

    recalled = ltm.recall("quarterly revenue", k=5)
    # Deduplication ensures only 1 copy of the document exists
    assert len(recalled) == 1


def test_long_term_memory_chroma_unavailable_graceful(monkeypatch):
    """Confirm LongTermMemory gracefully falls back to local keyword store when ChromaDB is absent."""
    monkeypatch.setattr(LongTermMemory, "_connect", lambda self: None)
    ltm = LongTermMemory()
    assert ltm._collection is None

    ltm.remember("Analyze vector databases", "Chroma and Milvus offer distinct trade-offs.")
    recalled = ltm.recall("vector databases", k=1)
    assert len(recalled) == 1
    assert "Chroma and Milvus" in recalled[0]


def test_context_retrieval_before_execution_in_run_task():
    """Prior memories should be recalled before execution and stored in state and short-term memory."""
    ltm = LongTermMemory()
    ltm.remember(
        task="Summarize the quarterly financial report",
        outcome="Previous report indicated strong growth in European markets.",
    )

    result = run_task("Summarize the quarterly financial report")
    assert result.get("context") is not None
    assert len(result["context"]) >= 1
    assert any("European markets" in c for c in result["context"])

    # Confirm short-term working memory captured the recalled context
    task_id = result["task_id"]
    short = ShortTermMemory(task_id)
    recalled_in_short = short.get("recalled_context")
    assert recalled_in_short is not None
    assert any("European markets" in c for c in recalled_in_short)


def test_completed_task_outcome_stored_in_long_term_memory():
    """Successful unescalated tasks must be persisted to LongTermMemory and ShortTermMemory."""
    task_desc = "Analyze cloud computing costs"
    result = run_task(task_desc)

    assert result.get("status") == "completed"
    assert not result.get("escalated")

    # Verify short term memory captured terminal status
    short = ShortTermMemory(result["task_id"])
    assert short.get("status") == "completed"
    assert short.get("answer") == result["answer"]

    # Verify long term memory stored the outcome
    ltm = LongTermMemory()
    recalled = ltm.recall("cloud computing costs", k=1)
    assert len(recalled) >= 1
    assert task_desc in recalled[0]


def test_pending_hitl_task_never_stored_in_long_term_memory():
    """Pending/unresolved tasks requiring human review must NEVER be stored in LongTermMemory."""
    # A sensitive database deletion triggers takeover escalation -> status: pending
    task_desc = "Please DROP TABLE customers;"
    result = run_task(task_desc)

    assert result.get("escalated") is True
    assert result.get("status") == "pending"

    # Confirm it is NOT recalled from long term memory
    ltm = LongTermMemory()
    recalled = ltm.recall("DROP TABLE customers", k=5)
    assert recalled == []


def test_rejected_resolution_never_stored_in_long_term_memory():
    """When a human reviewer rejects an escalated task, the outcome must NOT be saved to LongTermMemory."""
    task_desc = "Please DROP TABLE audit_logs;"
    result = run_task(task_desc)
    assert result.get("status") == "pending"

    queue = ReviewQueue()
    pending = queue.pending()
    assert len(pending) >= 1
    item = next(p for p in pending if p.task_id == result["task_id"])

    res = queue.resolve(
        item.item_id,
        decision="reject",
        approver="secops_admin",
        note="Destructive action strictly disallowed by security policy",
    )
    final_state = apply_resolution(result, res)
    assert final_state.get("status") == "rejected"

    # Verify not stored in long-term memory
    ltm = LongTermMemory()
    recalled = ltm.recall("DROP TABLE audit_logs", k=5)
    assert recalled == []


def test_approved_resolution_stored_in_long_term_memory():
    """When an escalated review item is approved, the validated outcome is stored in LongTermMemory."""
    # Create an in-memory escalated state and item
    task_id = "task-hitl-app"
    state = {
        "task_id": task_id,
        "task": "Archive customer records older than seven years",
        "answer": "### Human Review Required (approve_action)\nPending human review",
        "status": "pending",
        "escalated": True,
        "completed": [
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Archive customer records older than seven years",
                "status": "done",
                "result": "Identified 450 records eligible for cold archival storage.",
                "confidence": 0.85,
            }
        ],
    }

    queue = ReviewQueue()
    item = ReviewItem(
        task_id=task_id,
        task=state["task"],
        reason="sensitive_operation",
        level="approve_action",
        proposed_action=state["completed"][0]["result"],
        confidence=0.85,
        quality=0.75,
        subtask_id="s1",
    )
    queue.push(item)

    res = queue.resolve(
        item.item_id,
        decision="approve",
        approver="compliance_lead",
        note="Archival approved under policy 4.2",
    )
    final_state = apply_resolution(state, res)
    assert final_state.get("status") == "approved"
    assert not final_state.get("escalated")

    # Confirm it is now stored in LongTermMemory
    ltm = LongTermMemory()
    recalled = ltm.recall("Archive customer records", k=1)
    assert len(recalled) == 1
    assert "Archive customer records" in recalled[0]

    # Confirm ShortTermMemory recorded the resolution
    short = ShortTermMemory(task_id)
    assert short.get("status") == "approved"
    assert short.get("resolution")["decision"] == "approve"


def test_taken_over_resolution_stored_in_long_term_memory():
    """When a human takes over an escalated task, the human's replacement text is stored in LongTermMemory."""
    task_id = "task-hitl-takeover"
    state = {
        "task_id": task_id,
        "task": "Emergency failover to secondary database",
        "answer": "### Human Review Required (take_over)",
        "status": "pending",
        "escalated": True,
        "completed": [],
    }

    queue = ReviewQueue()
    item = ReviewItem(
        task_id=task_id,
        task=state["task"],
        reason="sensitive_operation",
        level="take_over",
        proposed_action="",
        confidence=0.75,
        quality=0.65,
    )
    queue.push(item)

    res = queue.resolve(
        item.item_id,
        decision="take_over",
        approver="dba_oncall",
        note="Manual failover executed: read replica promoted to primary with zero data loss.",
    )
    final_state = apply_resolution(state, res)
    assert final_state.get("status") == "taken_over"
    assert "Manual failover executed" in final_state.get("answer")

    # Confirm human's answer was stored in LongTermMemory
    ltm = LongTermMemory()
    recalled = ltm.recall("Emergency failover", k=1)
    assert len(recalled) == 1
    assert "Manual failover executed" in recalled[0]


def test_workflow_stages_recorded_in_short_term_memory():
    """Verify that execution lifecycle stages (init, decompose, subtask, reduce) are recorded."""
    result = run_task("Research caching strategies")
    task_id = result["task_id"]

    short = ShortTermMemory(task_id)
    all_data = short.all()

    assert all_data.get("task") == "Research caching strategies"
    assert all_data.get("status") == "completed"
    assert "subtask:s1" in all_data
    assert all_data["subtask:s1"]["status"] == "done"
