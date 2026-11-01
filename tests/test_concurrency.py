"""Tests for bounded specialist concurrency, resource management, and execution safety."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from orchestrator.config import Settings
from orchestrator.graph.builder import _run_fallback, run_task
from orchestrator.llm import Completion
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm

_DETAILED_OUTPUT = (
    "This is a high quality specialist completion result that easily exceeds "
    "the quality threshold and provides structured facts and findings."
)


@pytest.fixture(autouse=True)
def clean_memory():
    """Reset in-process memory stores before and after each test."""
    reset_stm()
    reset_ltm()
    yield
    reset_stm()
    reset_ltm()


# ============================================================================
# 1. Concurrency & Bounded Active Execution
# ============================================================================


def test_independent_subtasks_execute_concurrently():
    """Verify that independent subtasks execute in parallel using synchronization barrier."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Research market size", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Draft opening remarks", "depends_on": []},
    ]

    barrier = threading.Barrier(2, timeout=4.0)

    def concurrent_complete(system="", prompt="", *args, **kwargs):
        # Both threads must reach this barrier at the same time to proceed.
        # If execution were strictly sequential, the first thread would timeout waiting for the second.
        barrier.wait()
        return Completion(
            f"Completed research findings and detailed analysis for: {prompt[:30]} with thorough output.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=concurrent_complete):
        result = run_task("Parallel task execution", plan=plan, max_concurrency=2)

        assert result["status"] == "completed"
        done_ids = {s["id"] for s in result["completed"] if s["status"] == "done"}
        assert done_ids == {"s1", "s2"}


def test_maximum_active_concurrency_strictly_bounded():
    """Verify that active executions never exceed the configured concurrency limit."""
    plan = [
        {"id": f"s{i}", "kind": "research", "instruction": f"Subtask {i}", "depends_on": []}
        for i in range(1, 7)
    ]

    concurrency_limit = 2
    active_lock = threading.Lock()
    current_active = 0
    max_observed_active = 0

    def bounded_complete(system="", prompt="", *args, **kwargs):
        nonlocal current_active, max_observed_active
        with active_lock:
            current_active += 1
            if current_active > max_observed_active:
                max_observed_active = current_active
        time.sleep(0.03)
        with active_lock:
            current_active -= 1
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=bounded_complete):
        result = run_task("Bounded concurrency run", plan=plan, max_concurrency=concurrency_limit)

        assert result["status"] == "completed"
        assert len(result["completed"]) == 6
        assert max_observed_active <= concurrency_limit, (
            f"Observed {max_observed_active} active workers, exceeding limit of {concurrency_limit}"
        )


def test_concurrency_delayed_trace_events():
    """Verify that subtasks waiting for concurrency slots emit 'delayed' trace events."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Task 1", "depends_on": []},
        {"id": "s2", "kind": "analysis", "instruction": "Task 2", "depends_on": []},
        {"id": "s3", "kind": "writing", "instruction": "Task 3", "depends_on": []},
    ]

    barrier = threading.Barrier(2, timeout=4.0)

    def slow_complete(system="", prompt="", *args, **kwargs):
        if "Task 1" in prompt or "Task 2" in prompt:
            barrier.wait()
            time.sleep(0.02)
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=slow_complete):
        result = run_task("Delayed task tracing", plan=plan, max_concurrency=2)

        trace = result.get("trace", [])
        # s3 must have been delayed because only 2 slots exist for 3 ready subtasks
        delayed_events = [t for t in trace if t.get("event") == "delayed" and t.get("subtask") == "s3"]
        assert len(delayed_events) >= 1
        assert delayed_events[0]["reason"] == "concurrency_limit"
        assert delayed_events[0]["concurrency_limit"] == 2

        # Verify ready events exist
        ready_events = [t for t in trace if t.get("event") == "ready"]
        ready_ids = {t["subtask"] for t in ready_events}
        assert ready_ids == {"s1", "s2", "s3"}


# ============================================================================
# 2. Dependency Ordering & Failure Propagation
# ============================================================================


def test_dependent_subtasks_wait_for_prerequisites():
    """Verify that a dependent subtask never starts before its prerequisites complete."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Prerequisite data", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Dependent summary", "depends_on": ["s1"]},
    ]

    s1_finished = False

    def sequence_complete(system="", prompt="", *args, **kwargs):
        nonlocal s1_finished
        if "Prerequisite" in prompt:
            time.sleep(0.04)
            s1_finished = True
            return Completion(
                "Prerequisite data output that is sufficiently long and detailed for quality threshold.",
                "mock",
                "mock-1",
            )
        if "Dependent" in prompt:
            assert s1_finished is True, "Dependent task started before prerequisite finished!"
            return Completion(
                "Dependent summary output that is sufficiently long and detailed for quality threshold.",
                "mock",
                "mock-1",
            )
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=sequence_complete):
        result = run_task("Sequential dependency execution", plan=plan, max_concurrency=4)
        assert result["status"] == "completed"
        assert len(result["completed"]) == 2


def test_failed_prerequisites_block_dependent_subtasks():
    """Verify that prerequisite failure blocks downstream subtasks while respecting concurrency."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Fetch dataset", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Analyze dataset", "depends_on": ["s1"]},
    ]

    def failing_complete(system="", prompt="", *args, **kwargs):
        raise ConnectionResetError("Database connection lost")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=failing_complete):
        result = run_task("Prerequisite failure test", plan=plan, max_concurrency=2)

        failed = [s for s in result["completed"] if s["status"] == "failed"]
        blocked = [s for s in result["completed"] if s["status"] == "blocked"]

        assert len(failed) == 1 and failed[0]["id"] == "s1"
        assert len(blocked) == 1 and blocked[0]["id"] == "s2"
        assert "s1" in blocked[0]["error"]


def test_independent_subtask_continues_when_another_fails():
    """Verify that an independent branch continues executing concurrently even if another branch fails."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Flaky service call", "depends_on": []},
        {"id": "s2", "kind": "code", "instruction": "Downstream from flaky", "depends_on": ["s1"]},
        {"id": "s3", "kind": "analysis", "instruction": "Independent solid analysis", "depends_on": []},
    ]

    def selective_complete(system="", prompt="", *args, **kwargs):
        if "Flaky" in prompt:
            raise RuntimeError("Service unavailable")
        return Completion(
            "Solid independent findings and statistics that are comprehensive and meet standards.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=selective_complete):
        result = run_task("Parallel resilience test", plan=plan, max_concurrency=2)

        done = [s for s in result["completed"] if s["status"] == "done"]
        failed = [s for s in result["completed"] if s["status"] == "failed"]
        blocked = [s for s in result["completed"] if s["status"] == "blocked"]

        assert len(done) == 1 and done[0]["id"] == "s3"
        assert len(failed) == 1 and failed[0]["id"] == "s1"
        assert len(blocked) == 1 and blocked[0]["id"] == "s2"

        # Independent output preserved in final answer
        assert "### Analysis specialist" in result["answer"]
        assert "Solid independent findings" in result["answer"]


# ============================================================================
# 3. Retries & HITL Safety
# ============================================================================


def test_retries_respect_concurrency_limits():
    """Verify that subtask retries do not exceed active concurrency limits."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Flaky then succeeds", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Parallel steady worker", "depends_on": []},
    ]

    attempts = {"s1": 0}
    active_lock = threading.Lock()
    current_active = 0
    max_active = 0

    def retry_complete(system="", prompt="", *args, **kwargs):
        nonlocal current_active, max_active
        with active_lock:
            current_active += 1
            if current_active > max_active:
                max_active = current_active

        try:
            if "Flaky" in prompt:
                attempts["s1"] += 1
                if attempts["s1"] == 1:
                    raise TimeoutError("Transient network timeout")
            time.sleep(0.02)
            return Completion(_DETAILED_OUTPUT, "mock", "mock-1")
        finally:
            with active_lock:
                current_active -= 1

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=retry_complete):
        result = run_task("Retry concurrency test", plan=plan, max_concurrency=2)

        assert result["status"] == "completed"
        assert result["retries"] == 1
        assert max_active <= 2
        assert attempts["s1"] == 2


def test_hitl_take_over_halts_execution_without_starvation():
    """Verify that take_over escalation halts subsequent execution without thread deadlock."""
    plan = [
        {"id": "s1", "kind": "code", "instruction": "DROP TABLE users;", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Unreached subsequent work", "depends_on": ["s1"]},
    ]

    result = run_task("Sensitive operation test", plan=plan, max_concurrency=2)

    assert result["escalated"] is True
    assert result["approval"] == "take_over"
    assert result["status"] == "pending"
    assert "Human Review Required" in result["answer"]

    # s2 must not have completed
    completed_ids = {s["id"] for s in result["completed"]}
    assert "s2" not in completed_ids


# ============================================================================
# 4. Deterministic Result Assembly
# ============================================================================


def test_deterministic_result_assembly_varied_completion_order():
    """Verify that final result sections and completed list follow plan order
    even when tasks finish out of order.
    """
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Research section A", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Writing section B", "depends_on": []},
    ]

    # Force s2 to complete before s1
    s2_finished = threading.Event()

    def out_of_order_complete(system="", prompt="", *args, **kwargs):
        if "Research" in prompt:
            # Wait for s2 to complete first
            s2_finished.wait(timeout=3.0)
            return Completion(
                "Research content A that is sufficiently long and detailed for quality threshold.",
                "mock",
                "mock-1",
            )
        if "Writing" in prompt:
            res = Completion(
                "Writing content B that is sufficiently long and detailed for quality threshold.",
                "mock",
                "mock-1",
            )
            s2_finished.set()
            return res
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=out_of_order_complete):
        result = run_task("Deterministic ordering test", plan=plan, max_concurrency=2)

        assert result["status"] == "completed"
        # s1 must appear before s2 in answer despite s2 finishing first
        pos_s1 = result["answer"].find("### Research specialist")
        pos_s2 = result["answer"].find("### Writing specialist")
        assert pos_s1 != -1 and pos_s2 != -1
        assert pos_s1 < pos_s2, "Results were not assembled in deterministic plan order!"

        # completed list also preserves plan order
        assert [s["id"] for s in result["completed"]] == ["s1", "s2"]


# ============================================================================
# 5. Boundary Conditions & Resource Safety
# ============================================================================


def test_concurrency_limit_of_one():
    """Verify that max_concurrency=1 enforces sequential execution without failure."""
    plan = [
        {"id": "s1", "kind": "research", "instruction": "First", "depends_on": []},
        {"id": "s2", "kind": "analysis", "instruction": "Second", "depends_on": []},
        {"id": "s3", "kind": "writing", "instruction": "Third", "depends_on": []},
    ]

    active_lock = threading.Lock()
    current_active = 0
    max_active = 0

    def single_worker_complete(system="", prompt="", *args, **kwargs):
        nonlocal current_active, max_active
        with active_lock:
            current_active += 1
            if current_active > max_active:
                max_active = current_active
        time.sleep(0.01)
        with active_lock:
            current_active -= 1
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=single_worker_complete):
        result = run_task("Sequential concurrency 1", plan=plan, max_concurrency=1)

        assert result["status"] == "completed"
        assert max_active == 1
        assert len(result["completed"]) == 3


def test_empty_plan_and_single_subtask():
    """Verify that empty plans and single-subtask plans complete cleanly without deadlock."""
    # Empty plan executed directly via fallback executor
    res_empty = _run_fallback({"plan": [], "completed": [], "task": "No-op task"})
    assert res_empty["status"] == "completed"
    assert res_empty["completed"] == []

    # Single subtask
    single_plan = [{"id": "s1", "kind": "research", "instruction": "Solo task", "depends_on": []}]
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(_DETAILED_OUTPUT, "mock", "mock-1"),
    ):
        res_single = run_task("Solo execution", plan=single_plan, max_concurrency=2)
        assert res_single["status"] == "completed"
        assert len(res_single["completed"]) == 1
        assert res_single["completed"][0]["id"] == "s1"


def test_execution_exceptions_do_not_leak_or_corrupt_state():
    """Verify that an unexpected exception in worker thread is captured safely without leaking resources."""
    plan = [
        {"id": "s1", "kind": "code", "instruction": "Faulty worker", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Surviving worker", "depends_on": []},
    ]

    def crash_complete(system="", prompt="", *args, **kwargs):
        if "Faulty" in prompt:
            raise MemoryError("Simulated memory allocation failure")
        return Completion(_DETAILED_OUTPUT, "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=crash_complete):
        result = run_task("Exception safety test", plan=plan, max_concurrency=2)

        failed = [s for s in result["completed"] if s["id"] == "s1"]
        done = [s for s in result["completed"] if s["id"] == "s2"]

        assert len(failed) == 1
        assert failed[0]["status"] == "failed"
        assert "MemoryError" in failed[0]["error"]

        assert len(done) == 1
        assert done[0]["status"] == "done"
        assert result["status"] in ("partial", "pending")


def test_run_id_correlated_across_concurrent_threads():
    """Verify that run_id and task_id are consistently propagated across all
    concurrent worker trace events.
    """
    custom_run = "corr-run-9988"
    plan = [
        {"id": "s1", "kind": "research", "instruction": "Investigate X", "depends_on": []},
        {"id": "s2", "kind": "analysis", "instruction": "Analyze Y", "depends_on": []},
    ]

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(_DETAILED_OUTPUT, "mock", "mock-1"),
    ):
        result = run_task("Correlation test", plan=plan, run_id=custom_run, max_concurrency=2)

    assert result["run_id"] == custom_run
    trace = result.get("trace", [])
    assert len(trace) > 0
    for evt in trace:
        assert evt.get("run_id") == custom_run, f"Event {evt} lacked run_id {custom_run}"
        assert evt.get("task_id") == custom_run, f"Event {evt} lacked task_id {custom_run}"


def test_effective_concurrency_settings_configuration():
    """Verify Settings effective_concurrency property and parameter override priority."""
    s = Settings(max_specialist_concurrency=5, max_concurrency=None)
    assert s.effective_concurrency == 5

    s2 = Settings(max_specialist_concurrency=5, max_concurrency=2)
    assert s2.effective_concurrency == 2

    s3 = Settings(max_specialist_concurrency=0, max_concurrency=None)
    assert s3.effective_concurrency == 1
