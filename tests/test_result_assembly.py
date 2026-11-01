"""Tests for reliable final result assembly, status accuracy, and error sanitization."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from orchestrator.graph.builder import run_task
from orchestrator.graph.state import GraphState
from orchestrator.graph.supervisor import reduce, sanitize_error
from orchestrator.hitl.queue import ReviewQueue, apply_resolution
from orchestrator.llm import Completion
from orchestrator.main import cli
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


# ============================================================================
# 1. Error Sanitization Tests
# ============================================================================


def test_sanitize_error_strips_traceback():
    tb = (
        "Traceback (most recent call last):\n"
        '  File "/app/src/specialists.py", line 42, in execute\n'
        '  File "/app/src/tools.py", line 12, in call\n'
        "ValueError: Invalid calculation parameter: x must be positive"
    )
    sanitized = sanitize_error(tb)
    assert "Traceback" not in sanitized
    assert "/app/src" not in sanitized
    assert sanitized == "ValueError: Invalid calculation parameter: x must be positive"


def test_sanitize_error_redacts_credentials():
    raw = (
        "Connection refused to https://api.openai.com/v1 with key sk-abcdef1234567890xyz "
        "and header Bearer secret_token_value_9876543210. password=SuperSecretPassword!"
    )
    sanitized = sanitize_error(raw)
    assert "sk-abcdef1234567890xyz" not in sanitized
    assert "[REDACTED_KEY]" in sanitized
    assert "secret_token_value_9876543210" not in sanitized
    assert "Bearer [REDACTED_TOKEN]" in sanitized
    assert "SuperSecretPassword!" not in sanitized
    assert "password=[REDACTED]" in sanitized


def test_sanitize_error_bounds_length():
    huge_msg = "Critical error " + ("x" * 500)
    sanitized = sanitize_error(huge_msg)
    assert len(sanitized) <= 200
    assert sanitized.endswith("...")


def test_sanitize_error_handles_none_and_empty():
    assert sanitize_error(None) == "Unknown error"
    assert sanitize_error("") == "Unknown error"
    assert sanitize_error("   ") == "Unknown error"


# ============================================================================
# 2. Result Assembly Status and Content Tests
# ============================================================================


def test_result_assembly_all_succeed():
    """When all planned subtasks succeed, status is 'completed' with no Incomplete Work section."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Research topic A"},
        {"id": "s2", "kind": "writing", "instruction": "Write article on topic A"},
    ]

    result = run_task("Research and write", plan=plan)

    assert result["status"] == "completed"
    assert "### Research specialist" in result["answer"]
    assert "### Writing specialist" in result["answer"]
    assert "### Incomplete Work" not in result["answer"]
    assert len(result.get("completed", [])) == 2
    assert all(c.get("status") == "done" for c in result["completed"])
    assert result.get("failed", []) == []
    assert result.get("blocked", []) == []
    assert isinstance(result["quality"], float) and result["quality"] > 0.0
    assert isinstance(result["confidence"], float)


def test_result_assembly_partial_success_with_reasons_in_reducer():
    """When some subtasks succeed and others fail/block, reducer produces status 'partial'."""
    tb_error = (
        "Traceback (most recent call last):\n"
        "  File 'eval.py', line 10\n"
        "SyntaxError: invalid syntax with sk-secretkey1234567890"
    )
    state: GraphState = {
        "task_id": "test_partial_1",
        "task": "Build project",
        "plan": [
            {"id": "s1", "kind": "research", "instruction": "Research topic A"},
            {"id": "s2", "kind": "code", "instruction": "Write script"},
            {"id": "s3", "kind": "writing", "instruction": "Write summary", "depends_on": ["s2"]},
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "research",
                "status": "done",
                "result": "Comprehensive research findings on distributed systems.",
                "confidence": 0.8,
            },
            {
                "id": "s2",
                "kind": "code",
                "status": "failed",
                "error": tb_error,
            },
            {
                "id": "s3",
                "kind": "writing",
                "status": "blocked",
                "error": "Blocked: prerequisite subtask 's2' failed",
            },
        ],
        "failed": [
            {
                "id": "s2",
                "kind": "code",
                "status": "failed",
                "error": tb_error,
            }
        ],
        "blocked": [
            {
                "id": "s3",
                "kind": "writing",
                "status": "blocked",
                "error": "Blocked: prerequisite subtask 's2' failed",
            }
        ],
        "escalations": [],
    }

    reduced = reduce(state)

    assert reduced["status"] == "partial"
    # Successful specialist section is preserved
    assert "### Research specialist" in reduced["answer"]
    assert "Comprehensive research findings" in reduced["answer"]
    # Failed specialist section header is not rendered as regular output
    assert "### Code specialist\n" not in reduced["answer"]
    assert "### Writing specialist\n" not in reduced["answer"]

    # Incomplete Work summary block is appended
    assert "### Incomplete Work" in reduced["answer"]
    assert "- Subtask s2 (code): Failed" in reduced["answer"]
    assert "SyntaxError: invalid syntax with [REDACTED_KEY]" in reduced["answer"]
    assert "- Subtask s3 (writing): Blocked" in reduced["answer"]
    assert "Blocked: prerequisite subtask 's2' failed" in reduced["answer"]
    assert "Traceback" not in reduced["answer"]
    assert reduced["quality"] == 0.5


def test_result_assembly_partial_success_end_to_end():
    """End-to-end execution with one failure preserves completed work and summarizes incomplete work."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Research topic A"},
        {"id": "s2", "kind": "code", "instruction": "Write script", "depends_on": []},
        {"id": "s3", "kind": "writing", "instruction": "Write summary of code", "depends_on": ["s2"]},
    ]

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "code" in system.lower():
            raise RuntimeError(
                "Traceback (most recent call last):\n"
                '  File "eval.py", line 10\n'
                "SyntaxError: invalid syntax with sk-secretkey1234567890"
            )
        return Completion(
            "Comprehensive specialist research output that passes quality and confidence heuristics.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Build project", plan=plan)

    # Incomplete work must not claim success
    assert result["status"] in ("partial", "pending")
    assert result["status"] != "completed"

    # Successful specialist section is preserved
    assert "### Research specialist" in result["answer"]
    assert "Comprehensive specialist research output" in result["answer"]
    assert "### Code specialist\n" not in result["answer"]

    # Incomplete Work summary block is appended
    assert "### Incomplete Work" in result["answer"]
    assert "- Subtask s2 (code): Failed" in result["answer"]
    assert "SyntaxError: invalid syntax with [REDACTED_KEY]" in result["answer"]
    assert "- Subtask s3 (writing): Blocked" in result["answer"]
    assert "Upstream dependency 's2' failed" in result["answer"]


def test_result_assembly_all_fail():
    """When all planned subtasks fail, status is failed/pending and failure prefix is present."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Research topic"},
        {"id": "s2", "kind": "code", "instruction": "Write code"},
    ]

    def mock_complete(*args, **kwargs):
        raise ConnectionError("Connection refused by provider API")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        result = run_task("Do research and code", plan=plan)

    assert result["status"] in ("failed", "pending")
    assert result["status"] != "completed"
    assert result["answer"].startswith("(specialist execution failed — ")
    assert "Subtask s1 (research):" in result["answer"]
    assert "Subtask s2 (code):" in result["answer"]
    assert "Connection refused by provider API" in result["answer"]


def test_result_assembly_all_blocked():
    """When all planned subtasks are blocked, status is 'blocked' and answer starts with blocked prefix."""
    state: GraphState = {
        "task_id": "test_all_blocked",
        "task": "Test task",
        "plan": [
            {"id": "s1", "kind": "research", "instruction": "Step 1", "status": "blocked"},
            {"id": "s2", "kind": "writing", "instruction": "Step 2", "status": "blocked"},
        ],
        "completed": [
            {
                "id": "s1",
                "kind": "research",
                "status": "blocked",
                "error": "Blocked: upstream dependency missing",
            },
            {
                "id": "s2",
                "kind": "writing",
                "status": "blocked",
                "error": "Blocked: upstream dependency missing",
            },
        ],
        "escalations": [],
    }

    reduced = reduce(state)
    assert reduced["status"] == "blocked"
    assert reduced["answer"].startswith("(subtasks blocked by unmet dependencies — ")
    assert "Subtask s1 (research): Blocked: upstream dependency missing" in reduced["answer"]
    assert reduced["quality"] == 0.0


def test_result_assembly_pending_hitl_unresolved():
    """When a human review is triggered and pending, status remains 'pending' and execution paused."""
    state: GraphState = {
        "task_id": "test_takeover",
        "task": "Sensitive task",
        "approval": "take_over",
        "resolution": None,
        "completed": [
            {"id": "s1", "kind": "research", "status": "done", "result": "Research summary"}
        ],
        "escalations": [{"subtask_id": "s1", "reason": "sensitive_operation", "level": "take_over"}],
    }

    reduced = reduce(state)
    assert reduced["status"] == "pending"
    assert "### Human Review Required (take_over)" in reduced["answer"]
    assert "Execution paused" in reduced["answer"]
    assert reduced["escalated"] is True


def test_result_assembly_human_rejection():
    """When a subtask is rejected by human reviewer, status reflects rejection."""
    result = run_task("Please DROP TABLE customers;")
    assert result["status"] == "pending"
    assert result["escalated"] is True

    # Now apply reject resolution with valid Resolution object from queue
    queue = ReviewQueue()
    resolution = queue.resolve(result["task_id"], decision="reject", approver="security_auditor")
    assert resolution is not None
    final = apply_resolution(result, resolution)
    assert final["status"] == "rejected"
    assert "(rejected by human reviewer)" in final["answer"]


def test_result_assembly_partial_with_human_rejection():
    """When one subtask succeeds and another is rejected, status is 'partial' with rejection noted."""
    state: GraphState = {
        "task_id": "test_partial_reject",
        "task": "Two step task",
        "plan": [
            {"id": "s1", "kind": "research", "instruction": "Find facts"},
            {"id": "s2", "kind": "code", "instruction": "Dangerous code"},
        ],
        "completed": [
            {"id": "s1", "kind": "research", "status": "done", "result": "Fact 1, Fact 2"},
            {"id": "s2", "kind": "code", "status": "rejected", "result": ""},
        ],
        "escalations": [],
    }

    reduced = reduce(state)
    assert reduced["status"] == "partial"
    assert "### Research specialist\nFact 1, Fact 2" in reduced["answer"]
    assert "### Incomplete Work" in reduced["answer"]
    assert "- Subtask s2 (code): Rejected by reviewer" in reduced["answer"]


# ============================================================================
# 3. CLI JSON Mode Output Fidelity
# ============================================================================


def test_result_assembly_cli_json_fidelity_partial_failure(capsys):
    """CLI --json on a partial failure emits strictly valid JSON with status 'partial' or 'pending'."""
    def mock_complete(system="", prompt="", *args, **kwargs):
        if "code" in system.lower():
            raise RuntimeError("Database connection timed out")
        return Completion(
            "Valid research findings that are extensive enough to pass thresholds.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        rc = cli(["Research and code database solution", "--json"])
        assert rc == 0

    captured = capsys.readouterr()
    raw = captured.out

    # Must be valid single JSON document
    decoder = json.JSONDecoder()
    payload, idx = decoder.raw_decode(raw.strip())
    assert raw.strip()[idx:].strip() == ""

    # Status and content checks
    assert payload["status"] in ("partial", "pending")
    assert payload["status"] != "completed"
    assert "### Research specialist" in payload["answer"]
    assert "### Incomplete Work" in payload["answer"]
    assert isinstance(payload["quality"], float)
    assert isinstance(payload["confidence"], float)

    # Diagnostic headers must not appear in stdout
    assert "provider=" not in raw
    assert "=== REVIEW ===" not in raw
    assert "=== ANSWER ===" not in raw


# ============================================================================
# 4. Memory Integration Integrity
# ============================================================================


def test_memory_records_failed_and_blocked_on_reduce():
    """Verify ShortTermMemory captures failed and blocked lists upon reduction."""
    state: GraphState = {
        "task_id": "task_mem_test_1",
        "task": "Test memory tracking",
        "plan": [
            {"id": "s1", "kind": "research", "instruction": "Step 1"},
            {"id": "s2", "kind": "code", "instruction": "Step 2"},
            {"id": "s3", "kind": "writing", "instruction": "Step 3"},
        ],
        "completed": [
            {"id": "s1", "kind": "research", "status": "done", "result": "Research OK"},
            {"id": "s2", "kind": "code", "status": "failed", "error": "Crash"},
            {"id": "s3", "kind": "writing", "status": "blocked", "error": "Blocked"},
        ],
        "failed": [{"id": "s2", "kind": "code", "status": "failed", "error": "Crash"}],
        "blocked": [{"id": "s3", "kind": "writing", "status": "blocked", "error": "Blocked"}],
        "escalations": [],
    }

    reduce(state)

    stm = ShortTermMemory("task_mem_test_1")
    assert stm.get("status") == "partial"
    assert stm.get("failed") == ["s2"]
    assert stm.get("blocked") == ["s3"]


def test_long_term_memory_does_not_save_partial_or_failed():
    """LongTermMemory should never store non-completed workflows."""
    ltm = LongTermMemory()
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        side_effect=RuntimeError("Provider offline"),
    ):
        result = run_task("Test failing workflow for memory check")
        assert result["status"] in ("failed", "pending")
        assert result["status"] != "completed"

    # Semantic memory should not have stored the failed outcome
    memories = ltm.recall("Test failing workflow for memory check")
    assert memories == []
