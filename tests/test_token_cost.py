"""Focused deterministic tests for LLM token usage and estimated cost tracking.

Covers Task 14 requirements:
1. Input and output token usage extraction.
2. Missing or malformed usage metadata.
3. Correct cost calculation from configured rates.
4. Unknown pricing produces unknown cost, not a misleading zero.
5. Primary and fallback provider attribution.
6. Multiple LLM calls aggregate into one run summary.
7. Different runs remain isolated.
8. Failed attempts are not counted as successful completions.
9. Usage tracking failures do not break workflow execution.
10. Trace events do not expose secrets.
11. Existing Completion and LLMClient.complete callers remain compatible.
12. CLI JSON output remains valid and includes usage metadata.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from orchestrator.config import Settings
from orchestrator.graph import run_task
from orchestrator.llm import (
    Completion,
    LLMClient,
    LLMTransientError,
    ModelPricing,
    Usage,
    calculate_estimated_cost,
    clear_run_usage,
    get_model_pricing,
    get_run_usage,
    record_llm_usage,
    register_model_pricing,
)
from orchestrator.main import cli
from orchestrator.observability.tracing import trace_run


@pytest.fixture(autouse=True)
def _clean_usage():
    clear_run_usage()
    yield
    clear_run_usage()


def test_token_usage_extraction_and_dataclass():
    """1. Input, output, and total token usage are extracted and exposed accurately."""
    usage = Usage(input_tokens=150, output_tokens=50, total_tokens=200, is_estimated=False)
    comp = Completion(text="Response text", provider="mock", model="mock-1", usage=usage)

    assert comp.usage is not None
    assert comp.usage.input_tokens == 150
    assert comp.usage.output_tokens == 50
    assert comp.usage.total_tokens == 200
    assert comp.usage.is_estimated is False
    assert comp.usage.to_dict() == {
        "input_tokens": 150,
        "output_tokens": 50,
        "total_tokens": 200,
        "is_estimated": False,
    }


def test_missing_or_malformed_usage_metadata_handled_safely():
    """2. Missing or malformed usage metadata does not crash and is handled cleanly."""
    # When a provider response has no usage attribute
    comp = Completion(text="Response text", provider="mock", model="mock-1", usage=None)
    assert comp.usage is None

    # Recording an attempt without usage
    rec = record_llm_usage(
        provider="mock",
        model="mock-1",
        usage=None,
        success=True,
        run_id="run-missing-meta",
    )
    assert rec.input_tokens == 0
    assert rec.output_tokens == 0
    assert rec.total_tokens == 0

    summary = get_run_usage("run-missing-meta")
    assert summary.successful_requests == 1
    assert summary.total_tokens == 0


def test_cost_calculation_from_configured_rates():
    """3. Input and output token costs are calculated separately using configured rates."""
    # Register custom rates: $2.00 / 1M input, $6.00 / 1M output
    register_model_pricing("custom_prov", "model-alpha", input_cost_per_1m=2.00, output_cost_per_1m=6.00)

    pricing = get_model_pricing("custom_prov", "model-alpha")
    assert pricing is not None
    assert isinstance(pricing, ModelPricing)
    assert pricing.input_cost_per_1m == 2.00
    assert pricing.output_cost_per_1m == 6.00

    # 1,000,000 input tokens = $2.00; 500,000 output tokens = $3.00; total = $5.00
    cost, is_known = calculate_estimated_cost("custom_prov", "model-alpha", 1_000_000, 500_000)
    assert is_known is True
    assert cost == pytest.approx(5.00, rel=1e-5)

    # Smaller counts: 1000 input ($0.002) + 500 output ($0.003) = $0.005
    cost2, is_known2 = calculate_estimated_cost("custom_prov", "model-alpha", 1000, 500)
    assert is_known2 is True
    assert cost2 == pytest.approx(0.005, rel=1e-5)


def test_unknown_pricing_produces_unknown_cost_not_zero():
    """4. Unconfigured models produce unknown cost, not a misleading $0.00."""
    cost, is_known = calculate_estimated_cost("some_provider", "unconfigured_model_xyz", 1000, 500)
    assert is_known is False
    assert cost is None

    record_llm_usage(
        provider="some_provider",
        model="unconfigured_model_xyz",
        usage=Usage(input_tokens=1000, output_tokens=500, total_tokens=1500),
        success=True,
        run_id="run-unknown-price",
    )
    summary = get_run_usage("run-unknown-price")
    assert summary.has_unknown_cost is True
    assert summary.unknown_cost_requests == 1
    assert summary.estimated_cost_usd is None  # Not $0.00

    b = summary.breakdown["some_provider:unconfigured_model_xyz"]
    assert b["is_cost_known"] is False
    assert b["estimated_cost_usd"] is None


def test_primary_and_fallback_provider_attribution():
    """5. Primary failure and fallback success are correctly attributed in breakdown."""
    settings = Settings(llm_max_retries=1, fallback_llm_provider="mock")
    client = LLMClient(provider="openai", fallback_provider="mock", settings=settings)

    # OpenAI fails on attempt 1
    def fake_openai(*args, **kwargs):
        raise LLMTransientError("OpenAI 503", provider="openai")

    client._openai = fake_openai

    with trace_run("run-fallback-attr"):
        comp = client.complete("system instruction", "user prompt")

    assert comp.provider == "mock"
    assert comp.model == "mock-1"

    summary = get_run_usage("run-fallback-attr")
    assert summary.successful_requests == 1
    # OpenAI failed attempts are recorded but not counted as successful completions
    assert summary.failed_requests >= 1

    # Breakdown attributes each provider and model accurately
    assert "openai:gpt-4o-mini" in summary.breakdown
    assert "mock:mock-1" in summary.breakdown

    openai_stats = summary.breakdown["openai:gpt-4o-mini"]
    assert openai_stats["successful_requests"] == 0
    assert openai_stats["failed_requests"] >= 1

    mock_stats = summary.breakdown["mock:mock-1"]
    assert mock_stats["successful_requests"] == 1
    assert mock_stats["failed_requests"] == 0
    assert mock_stats["total_tokens"] > 0


def test_multiple_llm_calls_aggregate_into_one_run_summary():
    """6. Multiple LLM calls in the same run aggregate totals accurately."""
    client = LLMClient(provider="mock")

    with trace_run("run-multi-call"):
        comp1 = client.complete("sys1", "first call")
        comp2 = client.complete("sys2", "second call")
        comp3 = client.complete("sys3", "third call")

    assert comp1.usage and comp2.usage and comp3.usage
    expected_input = comp1.usage.input_tokens + comp2.usage.input_tokens + comp3.usage.input_tokens
    expected_output = comp1.usage.output_tokens + comp2.usage.output_tokens + comp3.usage.output_tokens
    expected_total = comp1.usage.total_tokens + comp2.usage.total_tokens + comp3.usage.total_tokens

    summary = get_run_usage("run-multi-call")
    assert summary.successful_requests == 3
    assert summary.total_input_tokens == expected_input
    assert summary.total_output_tokens == expected_output
    assert summary.total_tokens == expected_total
    assert summary.run_id == "run-multi-call"


def test_different_runs_remain_isolated():
    """7. Token usage from sequential or concurrent runs does not leak between runs."""
    client = LLMClient(provider="mock")

    with trace_run("run-A"):
        client.complete("sys", "task for run A")
        client.complete("sys", "second task for run A")

    with trace_run("run-B"):
        client.complete("sys", "task for run B")

    summary_a = get_run_usage("run-A")
    summary_b = get_run_usage("run-B")

    assert summary_a.run_id == "run-A"
    assert summary_b.run_id == "run-B"
    assert summary_a.successful_requests == 2
    assert summary_b.successful_requests == 1
    assert summary_a.total_tokens > summary_b.total_tokens

    # Verify run A records are absent in run B
    clear_run_usage("run-A")
    assert get_run_usage("run-A").successful_requests == 0
    assert get_run_usage("run-B").successful_requests == 1


def test_failed_attempts_not_counted_as_successful_completions():
    """8. Failed attempts are recorded in failed metrics, not successful completions."""
    record_llm_usage(
        provider="openai",
        model="gpt-4o",
        usage=Usage(100, 0, 100),
        success=False,
        run_id="run-failed-separation",
    )
    record_llm_usage(
        provider="mock",
        model="mock-1",
        usage=Usage(50, 20, 70),
        success=True,
        run_id="run-failed-separation",
    )

    summary = get_run_usage("run-failed-separation")
    assert summary.successful_requests == 1
    assert summary.failed_requests == 1

    # Successful totals exclude failed attempt tokens
    assert summary.total_input_tokens == 50
    assert summary.total_output_tokens == 20
    assert summary.total_tokens == 70

    # Failed attempt tokens are preserved separately
    assert summary.failed_attempts_tokens["total"] == 100


def test_usage_tracking_failures_do_not_break_workflow():
    """9. Telemetry or usage recording errors do not crash workflow or LLM completion."""
    client = LLMClient(provider="mock")

    with patch("orchestrator.llm.record_llm_usage", side_effect=RuntimeError("DB disk full")):
        comp = client.complete("sys", "prompt")
        assert comp.provider == "mock"
        assert comp.text

    # Workflow level resilience
    with patch(
        "orchestrator.observability.usage.get_run_usage",
        side_effect=RuntimeError("Collector failed"),
    ):
        result = run_task("Analyze security policy")
        assert result["status"] == "completed"


def test_trace_events_do_not_expose_secrets():
    """10. Usage trace events contain safe metadata and no secrets or raw prompts."""
    secret = "sk-live-secret-key-1234567890abcdef"
    rec = record_llm_usage(
        provider="openai",
        model="gpt-4o",
        usage=Usage(50, 100, 150),
        success=True,
        run_id=f"run-{secret}",
    )
    assert rec.total_tokens == 150

    # Trace events emitted in LLMClient
    client = LLMClient(provider="mock")
    client.complete("Secret system with bearer token", f"User payload containing {secret}")

    for event in client.events:
        for _k, v in event.items():
            assert secret not in str(v)


def test_completion_and_callers_compatibility():
    """11. Completion retains original positional attributes and backward compatibility."""
    comp = Completion("text only", "mock", "mock-1")
    assert comp.text == "text only"
    assert comp.provider == "mock"
    assert comp.model == "mock-1"
    assert comp.usage is None

    # Unpacking / attribute access
    t, p, m = comp.text, comp.provider, comp.model
    assert (t, p, m) == ("text only", "mock", "mock-1")


def test_cli_json_and_workflow_usage_integration(capsys):
    """12. Workflow runs include usage in GraphState and CLI --json output remains valid."""
    result = run_task("Draft an executive summary")
    assert "usage" in result
    usage = result["usage"]
    assert "total_tokens" in usage
    assert "successful_requests" in usage
    assert usage["successful_requests"] >= 1
    assert "breakdown" in usage

    # CLI in --json mode
    rc = cli(["Draft executive summary", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    assert captured.out.strip(), "CLI output must not be empty"

    data = json.loads(captured.out)
    assert isinstance(data, dict)
    assert "usage" in data
    assert "total_tokens" in data["usage"]
    assert "breakdown" in data["usage"]
