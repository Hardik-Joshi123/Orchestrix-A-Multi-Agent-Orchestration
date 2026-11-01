"""Tests for specialist execution reliability, error handling, partial completion, and retry bounds."""

from __future__ import annotations

from unittest.mock import patch

from orchestrator.config import get_settings
from orchestrator.graph.builder import run_task
from orchestrator.hitl.queue import ReviewQueue
from orchestrator.llm import Completion
from orchestrator.memory.long_term import LongTermMemory
from orchestrator.memory.short_term import ShortTermMemory


def test_specialist_exception_handled_cleanly():
    """Verify that an exception inside a specialist does not crash the workflow.

    The failed subtask must receive status='failed' (never 'done') and error metadata.
    """
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Provider connection reset"),
    ):
        result = run_task("Analyze market trends")

        assert result["status"] in ("failed", "pending")
        failed_subtasks = [s for s in result.get("completed", []) if s.get("status") == "failed"]
        assert len(failed_subtasks) >= 1
        failed_task = failed_subtasks[0]
        assert failed_task["status"] == "failed"
        assert "Provider connection reset" in failed_task["error"]
        assert failed_task["result"] == ""
        assert failed_task["confidence"] == 0.0

        # Also verified in result["failed"]
        assert any(f["id"] == failed_task["id"] for f in result.get("failed", []))


def test_specialist_empty_output_handled_as_failure():
    """Verify that empty/whitespace specialist output is treated as failure and not marked done."""
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(text="   ", provider="mock", model="mock-1"),
    ):
        result = run_task("Analyze performance data")

        failed_subtasks = [s for s in result.get("completed", []) if s.get("status") == "failed"]
        assert len(failed_subtasks) >= 1
        failed_task = failed_subtasks[0]
        assert failed_task["status"] == "failed"
        assert "empty or invalid output" in failed_task["error"].lower()


def test_partial_completion_preserves_successful_results():
    """Verify that when one specialist fails in a multi-specialist plan,

    completed results from other specialists are preserved and reduced.
    """
    plan = [
        {"kind": "research", "instruction": "Find database benchmarks"},
        {"kind": "code", "instruction": "Implement benchmark runner"},
        {"kind": "writing", "instruction": "Document benchmark results"},
    ]

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "coding" in system.lower() or "code" in system.lower():
            raise RuntimeError("Code generation compiler failure")
        if "research" in system.lower():
            return Completion("Research benchmarks: Redis is fast", "mock", "mock-1")
        return Completion("Summary of benchmark results", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Benchmark database systems", plan=plan)

        # Successful subtasks
        done_subtasks = [s for s in result.get("completed", []) if s.get("status") == "done"]
        assert len(done_subtasks) == 2
        done_kinds = [s["kind"] for s in done_subtasks]
        assert "research" in done_kinds
        assert "writing" in done_kinds

        # Failed subtask
        failed_subtasks = [s for s in result.get("completed", []) if s.get("status") == "failed"]
        assert len(failed_subtasks) == 1
        assert failed_subtasks[0]["kind"] == "code"
        assert "Code generation compiler failure" in failed_subtasks[0]["error"]

        # Reducer includes successful specialist sections and preserves outputs
        assert "### Research specialist" in result["answer"]
        assert "Redis is fast" in result["answer"]
        assert "### Writing specialist" in result["answer"]
        assert "### Code specialist" not in result["answer"]

        # Partial status
        if not result.get("escalated"):
            assert result["status"] == "partial"


def test_bounded_retry_recovers_transient_failure():
    """Verify that a transient specialist failure retries up to max_retries and can succeed."""
    specialist_calls = {"count": 0}

    def flaky_complete(system="", prompt="", *args, **kwargs):
        specialist_calls["count"] += 1
        if specialist_calls["count"] == 1:
            raise ConnectionResetError("Transient network drop")
        return Completion("Successfully completed on retry. " * 3, "mock", "mock-1")

    plan = [{"kind": "writing", "instruction": "Write uptime report"}]
    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=flaky_complete):
        result = run_task("Write a report on system uptime", plan=plan)

        assert specialist_calls["count"] == 2
        assert result["retries"] == 1
        assert result["status"] == "completed"
        done_subtasks = [s for s in result.get("completed", []) if s.get("status") == "done"]
        assert len(done_subtasks) == 1
        assert "Successfully completed on retry" in done_subtasks[0]["result"]


def test_repeated_failure_exhausts_retries_and_escalates():
    """Verify that persistent failure exhausts retries and escalates to human review."""
    settings = get_settings()

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Persistent outage"),
    ):
        result = run_task("Analyze logs for anomalies")

        assert result["retries"] >= settings.max_retries
        assert result["escalated"] is True
        assert result["escalation_reason"] == "repeated_failure"
        assert result["approval"] == "approve_plan"
        assert result["status"] == "pending"

        # Item enqueued in review queue
        queue = ReviewQueue()
        items = queue.pending()
        matching = [item for item in items if item.task_id == result["task_id"]]
        assert len(matching) == 1
        assert matching[0].reason == "repeated_failure"


def test_sensitive_operation_escalation_not_bypassed():
    """Verify that existing sensitive HITL gates (take_over) are never bypassed."""
    result = run_task("Please DROP TABLE users;")
    assert result["escalated"] is True
    assert result["approval"] == "take_over"
    assert result["status"] == "pending"
    assert "Human Review Required" in result["answer"]


def test_failed_task_excluded_from_long_term_memory():
    """Verify that a failed task is never written to long-term memory as a completed outcome."""
    from orchestrator.memory.long_term import reset_local_store

    reset_local_store()
    ltm = LongTermMemory()

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Fatal model crash"),
    ):
        result = run_task("UniqueTaskNameToVerifyMemoryExclusion12345")
        assert result["status"] in ("failed", "pending")

        # Query long term memory
        recalled = ltm.recall("UniqueTaskNameToVerifyMemoryExclusion12345")
        assert not recalled


def test_short_term_memory_reflects_failure():
    """Verify working memory contains accurate status and subtask failure metadata."""
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Specialist error in test"),
    ):
        result = run_task("Analyze test data")
        task_id = result["task_id"]
        short = ShortTermMemory(task_id)

        assert short.get("status") in ("failed", "pending")
        subtask_data = short.get("subtask:s1")
        assert isinstance(subtask_data, dict)
        assert subtask_data.get("status") == "failed"
        assert "Specialist error in test" in subtask_data.get("error", "")
