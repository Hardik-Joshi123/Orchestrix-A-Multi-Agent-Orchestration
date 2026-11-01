"""Tests for workflow observability, trace correlation, and safe instrumentation.

Validates:
1. Stable, unique run IDs propagated to workflow and subtask events.
2. Run ID isolation across independent executions.
3. Explicit custom run ID propagation.
4. Consistent lifecycle transitions across the pipeline (start, decompose, specialist start/complete/failure,
   retry, block, review, escalate, resolution, reduce).
5. Observability resilience (tracing/OTel failures never break workflow execution).
6. Credential and secret sanitization in trace events and span attributes.
7. Diagnostic quality (traceback stripping and bounded error length).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.graph import builder
from orchestrator.graph.builder import run_task
from orchestrator.hitl.queue import Resolution, apply_resolution
from orchestrator.observability.tracing import (
    create_trace_event,
    get_current_run_id,
    reset_tracer,
    sanitize_trace_attribute,
    span,
    trace_run,
)


@pytest.fixture(autouse=True)
def cleanup_tracer():
    reset_tracer()
    yield
    reset_tracer()


# -----------------------------------------------------------------------------
# 1. Run Correlation & Isolation
# -----------------------------------------------------------------------------


def test_run_id_distinct_and_correlated():
    """Each execution generates a unique run_id; all events within a run share it."""
    r1 = run_task("Summarize the quarterly earnings report")
    r2 = run_task("Analyze market trends for semiconductors")

    assert r1.get("run_id"), "r1 must have a run_id"
    assert r2.get("run_id"), "r2 must have a run_id"
    assert r1["run_id"] != r2["run_id"], "Sequential runs must have distinct run IDs"

    # All events in r1 trace must correlate to r1["run_id"]
    for evt in r1.get("trace", []):
        assert evt.get("run_id") == r1["run_id"], f"Event {evt} did not match r1 run_id"
        assert evt.get("task_id") == r1["run_id"], f"Event {evt} did not match r1 task_id"

    # All events in r2 trace must correlate to r2["run_id"]
    for evt in r2.get("trace", []):
        assert evt.get("run_id") == r2["run_id"], f"Event {evt} did not match r2 run_id"
        assert evt.get("task_id") == r2["run_id"], f"Event {evt} did not match r2 task_id"


def test_explicit_run_id_propagated():
    """An explicitly supplied run_id is preserved and propagated across all events."""
    custom_id = "custom-run-exp-99"
    result = run_task("Summarize key findings", run_id=custom_id)

    assert result.get("run_id") == custom_id
    assert result.get("task_id") == custom_id

    trace = result.get("trace", [])
    assert len(trace) > 0
    for evt in trace:
        assert evt.get("run_id") == custom_id
        assert evt.get("task_id") == custom_id


def test_contextvar_isolation():
    """trace_run context manager cleanly isolates current run_id and resets afterward."""
    assert get_current_run_id() == ""

    with trace_run("run-alpha"):
        assert get_current_run_id() == "run-alpha"
        with trace_run("run-beta"):
            assert get_current_run_id() == "run-beta"
        assert get_current_run_id() == "run-alpha"

    assert get_current_run_id() == ""


# -----------------------------------------------------------------------------
# 2. Lifecycle Events
# -----------------------------------------------------------------------------


def test_lifecycle_events_complete_pipeline():
    """A standard run produces all key lifecycle events."""
    result = run_task("Summarize the project milestone status")

    trace = result.get("trace", [])
    event_names = [f"{evt.get('node')}.{evt.get('event')}" for evt in trace]

    assert "workflow.start" in event_names
    assert "supervisor.decompose" in event_names
    assert any("start" in e for e in event_names if e.startswith("specialist."))
    assert any("complete" in e for e in event_names if e.startswith("specialist."))
    assert "reviewer.review" in event_names
    assert "supervisor.reduce" in event_names


def test_lifecycle_events_blocked():
    """A blocked subtask emits a dispatch.blocked event with correlated metadata."""
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
            raise RuntimeError("Database unavailable")
        from orchestrator.llm import Completion

        return Completion("Writing result", "mock", "mock-1")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        res = run_task("Research and write an article", plan=plan, run_id="blocked-run-1")

    trace = res.get("trace", [])
    blocked_events = [e for e in trace if e.get("node") == "dispatch" and e.get("event") == "blocked"]

    assert len(blocked_events) >= 1
    b = blocked_events[0]
    assert b.get("run_id") == "blocked-run-1"
    assert b.get("subtask") == "s2"
    assert b.get("subtask_id") == "s2"
    assert "s1" in str(b.get("reason"))


def test_lifecycle_events_retry():
    """Fallback execution emits workflow.retry event when retrying a failed specialist."""
    attempts = 0

    def mock_writing(state):
        nonlocal attempts
        attempts += 1
        subtask = state["plan"][0]
        if attempts == 1:
            failed_item = {
                "id": subtask["id"],
                "kind": "writing",
                "status": "failed",
                "error": "Transient error",
            }
            return {
                "completed": [failed_item],
                "failed": [failed_item],
                "confidence": 0.4,
                "quality": 0.4,
            }
        return {
            "completed": [{"id": subtask["id"], "kind": "writing", "status": "done", "result": "Recovered!"}],
            "confidence": 0.95,
            "quality": 0.95,
        }

    with patch("orchestrator.graph.builder.build_graph", side_effect=ImportError("No langgraph")):
        with patch.dict(builder._SPECIALIST_FN, {"writing": mock_writing}):
            res = run_task("Write a summary", run_id="retry-run-1")

    trace = res.get("trace", [])
    retry_events = [e for e in trace if e.get("event") == "retry"]
    assert len(retry_events) >= 1
    r = retry_events[0]
    assert r.get("node") == "workflow"
    assert r.get("run_id") == "retry-run-1"
    assert r.get("retry_count") == 1


def test_lifecycle_events_hitl_resolution():
    """Applying an HITL resolution creates a correlated hitl.resolution_applied event."""
    state = {
        "task_id": "hitl-run-42",
        "run_id": "hitl-run-42",
        "task": "Sensitive deploy operation",
        "escalated": True,
        "completed": [{"id": "s1", "kind": "code", "status": "pending_approval", "result": "deploy scripts"}],
        "trace": [create_trace_event("hitl", "escalate", run_id="hitl-run-42")],
    }
    resolution = Resolution(
        task_id="hitl-run-42",
        subtask_id="s1",
        decision="approve",
        approver="alice@company.internal",
        timestamp="2026-10-19T10:00:00Z",
        proposed_action="deploy scripts",
        final_input="",
        final_result="deploy scripts",
    )

    apply_resolution(state, resolution)

    trace = state["trace"]
    res_evts = [e for e in trace if e.get("node") == "hitl" and e.get("event") == "resolution_applied"]
    assert len(res_evts) == 1
    e = res_evts[0]
    assert e.get("run_id") == "hitl-run-42"
    assert e.get("task_id") == "hitl-run-42"
    assert e.get("subtask_id") == "s1"
    assert e.get("decision") == "approve"
    assert e.get("approver") == "alice@company.internal"


# -----------------------------------------------------------------------------
# 3. Safe Instrumentation & Resilience
# -----------------------------------------------------------------------------


def test_observability_failure_does_not_break_workflow():
    """Exceptions raised inside OpenTelemetry tracer/span never crash run_task."""
    # Simulate a failing tracer setup
    with patch("orchestrator.observability.tracing.setup_tracing", side_effect=RuntimeError("OTel exploded")):
        result = run_task("Summarize incident report")
        assert result.get("status") in ("success", "partial_success", "completed")
        assert result.get("answer")

    # Simulate a span context manager where start_as_current_span throws
    faulty_tracer = MagicMock()
    faulty_tracer.start_as_current_span.side_effect = Exception("Span creation failed")
    with patch("orchestrator.observability.tracing.setup_tracing", return_value=faulty_tracer):
        result = run_task("Summarize incident report")
        assert result.get("status") in ("success", "partial_success", "completed")
        assert result.get("answer")


def test_span_safe_attribute_and_exit_handling():
    """span helper catches attribute errors and exporter exit failures gracefully."""
    mock_span = MagicMock()
    mock_span.set_attribute.side_effect = TypeError("Invalid attribute type")

    mock_cm = MagicMock()
    mock_cm.__enter__.return_value = mock_span
    mock_cm.__exit__.side_effect = RuntimeError("OTel exporter timeout")

    mock_tracer = MagicMock()
    mock_tracer.start_as_current_span.return_value = mock_cm

    with patch("orchestrator.observability.tracing.setup_tracing", return_value=mock_tracer):
        # Should not raise even though set_attribute and __exit__ both fail in OTel
        with span("test.span", run_id="r1", key="val") as s:
            assert s is mock_span


# -----------------------------------------------------------------------------
# 4. Sensitive Data Sanitization & Diagnostics
# -----------------------------------------------------------------------------


def test_sensitive_attributes_redacted_in_trace():
    """API keys, bearer tokens, passwords, and sensitive keys are redacted from trace events."""
    evt = create_trace_event(
        node="specialist.code",
        event="complete",
        run_id="run-sec-1",
        api_key="sk-live1234567890abcdef1234567890",
        auth_header="Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
        password="supersecretpassword123",
        instruction="Use api_key=sk-test999 and password=abc to query https://example.com?secret=xyz",
        clean_text="Normal non-sensitive instruction",
    )

    assert evt["api_key"] == "[REDACTED]"
    assert evt["auth_header"] == "[REDACTED]"
    assert evt["password"] == "[REDACTED]"

    # Pattern redactions inside string text
    instruction = evt["instruction"]
    assert "sk-test999" not in instruction
    assert "[REDACTED]" in instruction
    assert "password=abc" not in instruction
    assert "secret=xyz" not in instruction
    assert evt["clean_text"] == "Normal non-sensitive instruction"


def test_trace_attribute_sanitization_diagnostics():
    """Stack traces are stripped and lengthy errors are bounded in trace attributes."""
    long_raw_error = (
        "Traceback (most recent call last):\n"
        "  File 'agent.py', line 42, in execute\n"
        "    raise ConnectionError('Failed to reach provider')\n"
        "ConnectionError: " + ("x" * 500)
    )

    sanitized = sanitize_trace_attribute("error", long_raw_error)
    assert "Traceback" not in sanitized
    assert "File 'agent.py'" not in sanitized
    assert len(sanitized) <= 260
    assert sanitized.endswith("...")


def test_cli_json_mode_remains_pure_with_tracing(capsys):
    """CLI --json emits strictly one valid JSON document without stdout pollution from spans or tracing."""
    from orchestrator.main import cli

    rc = cli(["Analyze systems telemetry", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    raw = captured.out.strip()
    assert raw.startswith("{")
    assert raw.endswith("}")

    decoder = json.JSONDecoder()
    obj, idx = decoder.raw_decode(raw)
    assert raw[idx:].strip() == ""
    assert obj.get("run_id") or obj.get("task_id")

