"""Tests for tool execution safety, schema validation, timeouts, retries, and HITL approval."""

from __future__ import annotations

import time

import pytest

from orchestrator.graph.builder import run_task
from orchestrator.tools.registry import (
    REGISTRY,
    Tool,
    ToolApprovalError,
    ToolNotFoundError,
    ToolRateLimitError,
    ToolTimeoutError,
    ToolValidationError,
)


def test_unknown_tool_raises_tool_not_found_error():
    """Verify that calling an unregistered tool raises ToolNotFoundError and KeyError."""
    with pytest.raises(ToolNotFoundError) as exc_info:
        REGISTRY.call("nonexistent_service_tool")
    assert isinstance(exc_info.value, KeyError)
    assert "unknown tool: nonexistent_service_tool" in str(exc_info.value)


def test_missing_required_arguments_raises_validation_error():
    """Verify that omitting required arguments raises ToolValidationError."""
    with pytest.raises(ToolValidationError) as exc_info:
        REGISTRY.call("web_search")
    assert isinstance(exc_info.value, (ValueError, TypeError))
    assert "Missing required argument 'query'" in str(exc_info.value)

    with pytest.raises(ToolValidationError):
        REGISTRY.call("calculator")


def test_invalid_argument_type_raises_validation_error():
    """Verify that passing incorrect argument types raises ToolValidationError."""
    with pytest.raises(ToolValidationError) as exc_info:
        REGISTRY.call("calculator", expression=12345)
    assert "must be string" in str(exc_info.value)

    with pytest.raises(ToolValidationError):
        REGISTRY.call("web_search", query=["not", "a", "string"])

    with pytest.raises(ToolValidationError):
        REGISTRY.call("web_search", query=None)


def test_additional_properties_rejected_when_specified():
    """Verify that unexpected arguments are rejected when additionalProperties is False."""
    strict_tool = Tool(
        name="strict_test_tool",
        fn=lambda value: f"value={value}",
        schema={
            "type": "object",
            "properties": {"value": {"type": "string"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )
    REGISTRY.register(strict_tool)

    # Valid call
    assert REGISTRY.call("strict_test_tool", value="valid") == "value=valid"

    # Unexpected argument
    with pytest.raises(ToolValidationError) as exc_info:
        REGISTRY.call("strict_test_tool", value="valid", unexpected_param="bad")
    assert "Unexpected argument 'unexpected_param'" in str(exc_info.value)


def test_tool_execution_timeout_guard():
    """Verify that slow or hanging tools trigger a bounded ToolTimeoutError."""
    slow_tool = Tool(
        name="slow_hanging_tool",
        fn=lambda: time.sleep(0.3) or "done",
        schema={"type": "object"},
        timeout_seconds=0.05,
    )
    REGISTRY.register(slow_tool)

    with pytest.raises(ToolTimeoutError) as exc_info:
        REGISTRY.call("slow_hanging_tool")
    assert isinstance(exc_info.value, TimeoutError)
    assert "timed out after 0.05s" in str(exc_info.value)


def test_bounded_retry_for_non_side_effecting_tool():
    """Verify that non-side-effecting tools with max_retries retry and succeed."""
    attempts = {"count": 0}

    def flaky_fn(query: str) -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectionError("Temporary DNS resolution failure")
        return f"result: {query}"

    retry_tool = Tool(
        name="flaky_search_tool",
        fn=flaky_fn,
        schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
        side_effecting=False,
        max_retries=1,
    )
    REGISTRY.register(retry_tool)

    res = REGISTRY.call("flaky_search_tool", query="python")
    assert res == "result: python"
    assert attempts["count"] == 2


def test_side_effecting_tool_never_retried_automatically():
    """Verify that side-effecting tools are never retried automatically even if max_retries > 0."""
    attempts = {"count": 0}

    def side_effecting_fn() -> str:
        attempts["count"] += 1
        raise RuntimeError("Payment gateway timeout after charge initiation")

    payment_tool = Tool(
        name="payment_charge_tool",
        fn=side_effecting_fn,
        schema={"type": "object"},
        side_effecting=True,
        max_retries=3,  # Should be overridden to 0 for side-effecting
    )
    REGISTRY.register(payment_tool)

    with pytest.raises(RuntimeError, match="Payment gateway timeout"):
        REGISTRY.call("payment_charge_tool")

    # Strictly 1 attempt, zero retries
    assert attempts["count"] == 1


def test_unapproved_side_effecting_tool_execution_enforcement():
    """Verify that execute() blocks unapproved side-effecting tools with ToolApprovalError."""
    unapproved_tool = Tool(
        name="delete_database_records",
        fn=lambda: "deleted",
        schema={"type": "object"},
        side_effecting=True,
        approved=False,
    )
    approved_tool = Tool(
        name="read_audit_records",
        fn=lambda: "records",
        schema={"type": "object"},
        side_effecting=True,
        approved=True,
    )
    REGISTRY.register(unapproved_tool)
    REGISTRY.register(approved_tool)

    # Unapproved raises ToolApprovalError and PermissionError
    with pytest.raises(ToolApprovalError) as exc_info:
        REGISTRY.execute("delete_database_records", enforce_approval=True)
    assert isinstance(exc_info.value, PermissionError)
    assert "requires human approval" in str(exc_info.value)

    # Approved succeeds
    assert REGISTRY.execute("read_audit_records", enforce_approval=True) == "records"


def test_rate_limit_exceeded_raises_tool_rate_limit_error():
    """Verify that rate limit exhaustion raises ToolRateLimitError."""
    limited_tool = Tool(
        name="rate_limited_test_tool",
        fn=lambda: "ok",
        schema={"type": "object"},
        rate_per_min=1,
    )
    REGISTRY.register(limited_tool)

    assert REGISTRY.call("rate_limited_test_tool") == "ok"
    with pytest.raises(ToolRateLimitError) as exc_info:
        REGISTRY.call("rate_limited_test_tool")
    assert isinstance(exc_info.value, RuntimeError)
    assert "rate limit exceeded" in str(exc_info.value)


def test_calculator_division_by_zero():
    """Verify calculator handles division by zero safely with ValueError."""
    with pytest.raises(ValueError, match="division by zero"):
        REGISTRY.call("calculator", expression="100 / 0")


def test_tool_failure_in_specialist_records_tools_used_and_does_not_crash():
    """Verify that when a tool fails during specialist execution, tools_used is recorded accurately."""
    from unittest.mock import patch

    def boom(name, **kwargs):
        if name == "web_search":
            raise RuntimeError("Web search provider is completely down")
        return "ok"

    with patch.object(REGISTRY, "call", side_effect=boom):
        result = run_task("Research the latest distributed consensus algorithms")

        # Workflow completes safely without crashing
        assert result["status"] == "completed"
        # Tool was recorded in tools_used
        assert "web_search" in result.get("tools_used", [])
        # Tool was recorded in completed subtask tools_used
        assert "web_search" in result["completed"][0].get("tools_used", [])
