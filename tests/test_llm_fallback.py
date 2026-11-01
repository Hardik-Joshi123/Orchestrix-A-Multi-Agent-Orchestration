"""Focused deterministic tests for LLM provider fallback, retry bounding, and error classification.

Covers Task 13 requirements:
1. Primary provider succeeds without fallback.
2. Transient primary failure triggers configured fallback.
3. Permanent authentication/configuration errors do not cause unnecessary retries.
4. All configured attempts fail and produce a controlled error.
5. Empty or malformed responses are handled safely.
6. Retry count remains bounded.
7. Provider diagnostics do not expose secrets.
8. Telemetry failure does not break successful provider execution.
9. Existing planning and specialist reliability behavior remains intact.
10. CLI JSON output remains valid when provider calls fail.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from orchestrator.config import Settings
from orchestrator.graph import run_task
from orchestrator.graph.supervisor import decompose
from orchestrator.llm import (
    Completion,
    LLMAuthenticationError,
    LLMClient,
    LLMError,
    LLMPermanentError,
    LLMResponseError,
    LLMTimeoutError,
    LLMTransientError,
    is_permanent_error,
    is_transient_error,
)
from orchestrator.main import cli


def test_error_classification_helpers():
    """Verify permanent and transient error classification."""
    assert is_permanent_error(LLMAuthenticationError("bad key"))
    assert is_permanent_error(LLMPermanentError("bad model"))
    assert not is_permanent_error(LLMTransientError("timeout"))
    assert is_transient_error(LLMResponseError("empty response"))

    class HttpError(Exception):
        def __init__(self, status_code: int):
            self.status_code = status_code

    assert is_permanent_error(HttpError(401))
    assert is_permanent_error(HttpError(403))
    assert is_transient_error(HttpError(429))
    assert is_transient_error(HttpError(500))
    assert is_transient_error(HttpError(503))


def test_primary_succeeds_without_fallback():
    """1. Primary provider succeeds directly; no fallback is triggered."""
    client = LLMClient(provider="mock", fallback_provider="anthropic")
    comp = client.complete("system instruction", "user task")
    assert comp.provider == "mock"
    assert "[mock" in comp.text

    started = [e for e in client.events if e.get("event") == "provider_attempt_started"]
    succeeded = [e for e in client.events if e.get("event") == "provider_attempt_succeeded"]
    fallback = [e for e in client.events if e.get("event") == "fallback_selected"]

    assert len(started) == 1
    assert len(succeeded) == 1
    assert len(fallback) == 0


def test_transient_primary_failure_triggers_configured_fallback():
    """2. Transient failure on primary triggers bounded retries then configured fallback."""
    settings = Settings(llm_max_retries=1, fallback_llm_provider="mock")
    client = LLMClient(provider="openai", fallback_provider="mock", settings=settings)
    call_count = 0

    def fake_openai(sys_prompt, user_prompt, json_mode):
        nonlocal call_count
        call_count += 1
        raise LLMTimeoutError("Request timed out", provider="openai")

    client._openai = fake_openai
    comp = client.complete("system", "prompt")

    # Fallback to mock succeeded
    assert comp.provider == "mock"
    # Primary attempted: 1 initial + 1 retry = 2 attempts
    assert call_count == 2

    fallback_events = [e for e in client.events if e.get("event") == "fallback_selected"]
    assert len(fallback_events) == 1
    assert fallback_events[0]["primary_provider"] == "openai"
    assert fallback_events[0]["fallback_provider"] == "mock"


def test_permanent_auth_error_does_not_cause_unnecessary_retries_no_fallback():
    """3a. Permanent auth errors do not retry against the same provider (fallback=None)."""
    settings = Settings(llm_max_retries=3)
    client = LLMClient(provider="openai", fallback_provider=None, settings=settings)
    call_count = 0

    def fake_openai(sys_prompt, user_prompt, json_mode):
        nonlocal call_count
        call_count += 1
        raise LLMAuthenticationError("Invalid API key", provider="openai")

    client._openai = fake_openai
    with pytest.raises(LLMAuthenticationError) as exc_info:
        client.complete("system", "prompt")

    # Must be called exactly once; no retries on permanent error
    assert call_count == 1
    assert exc_info.value.attempts == 1


def test_permanent_auth_error_falls_back_when_configured():
    """3b. Permanent auth error triggers configured fallback after single attempt."""
    settings = Settings(llm_max_retries=3, fallback_llm_provider="mock")
    client = LLMClient(provider="openai", fallback_provider="mock", settings=settings)
    call_count = 0

    def fake_openai(sys_prompt, user_prompt, json_mode):
        nonlocal call_count
        call_count += 1
        raise LLMAuthenticationError("401 Unauthorized", provider="openai")

    client._openai = fake_openai
    comp = client.complete("system", "prompt")

    assert comp.provider == "mock"
    assert call_count == 1  # No unnecessary retry on primary

    fallback_events = [e for e in client.events if e.get("event") == "fallback_selected"]
    assert len(fallback_events) == 1
    assert fallback_events[0]["primary_provider"] == "openai"


def test_all_configured_attempts_fail_produces_controlled_error():
    """4. When all configured attempts fail, raise a controlled LLMError."""
    settings = Settings(llm_max_retries=1)
    client = LLMClient(provider="openai", fallback_provider="anthropic", settings=settings)
    client._openai = MagicMock(side_effect=LLMTransientError("OpenAI down", provider="openai"))
    client._anthropic = MagicMock(side_effect=LLMTransientError("Anthropic down", provider="anthropic"))

    with pytest.raises(LLMError) as exc_info:
        client.complete("system", "prompt")

    assert "All configured LLM providers failed" in str(exc_info.value)
    final_events = [e for e in client.events if e.get("event") == "provider_final_failure"]
    assert len(final_events) == 1
    assert final_events[0]["provider"] == "anthropic"


def test_empty_or_whitespace_response_handled_safely():
    """5a. Empty or whitespace responses trigger validation failure and fallback."""
    settings = Settings(llm_max_retries=1)
    client = LLMClient(provider="openai", fallback_provider="mock", settings=settings)
    client._openai = MagicMock(return_value=Completion("   ", "openai", "gpt-4o-mini"))

    comp = client.complete("system", "prompt")
    assert comp.provider == "mock"
    assert comp.text


def test_malformed_json_response_in_json_mode_handled_safely():
    """5b. Malformed JSON in json_mode triggers validation failure and fallback."""
    settings = Settings(llm_max_retries=1)
    client = LLMClient(provider="openai", fallback_provider="mock", settings=settings)
    client._openai = MagicMock(return_value=Completion("Invalid { JSON", "openai", "gpt-4o-mini"))

    comp = client.complete("system", "prompt", json_mode=True)
    assert comp.provider == "mock"
    parsed = json.loads(comp.text)
    assert "summary" in parsed


def test_retry_count_remains_bounded():
    """6. Retry count remains bounded and never retries indefinitely."""
    retries = 2
    settings = Settings(llm_max_retries=retries)
    client = LLMClient(provider="openai", fallback_provider=None, settings=settings)
    call_count = 0

    def failing_call(sys_prompt, user_prompt, json_mode):
        nonlocal call_count
        call_count += 1
        raise LLMTransientError("Temporary 503 error", provider="openai")

    client._openai = failing_call
    with pytest.raises(LLMError):
        client.complete("system", "prompt")

    assert call_count == 1 + retries  # Exactly 3 attempts


def test_provider_diagnostics_do_not_expose_secrets():
    """7. Trace events and diagnostics never expose API keys, bearer tokens, or secrets."""
    client = LLMClient(provider="openai", fallback_provider=None)
    secret_key = "sk-12345678901234567890abcdef"
    secret_msg = f"Failed to authenticate with key {secret_key} and bearer secret-tok-1234567890123456"

    client._openai = MagicMock(side_effect=RuntimeError(secret_msg))
    with pytest.raises(LLMError):
        client.complete("system", "prompt")

    assert len(client.events) > 0
    for event in client.events:
        for _k, v in event.items():
            val_str = str(v)
            assert secret_key not in val_str
            assert "secret-tok" not in val_str


def test_telemetry_failure_does_not_break_successful_provider_execution(monkeypatch):
    """8. Telemetry and tracing errors never crash provider completion."""
    client = LLMClient(provider="mock")

    def broken_trace_event(*args, **kwargs):
        raise RuntimeError("Telemetry failure: broken collector")

    monkeypatch.setattr("orchestrator.llm.create_trace_event", broken_trace_event)

    comp = client.complete("system", "prompt")
    assert comp.provider == "mock"
    assert comp.text


def test_existing_planning_and_specialist_reliability_remain_intact():
    """9. Supervisor LLM planning falls back cleanly on provider failure."""
    with patch.object(LLMClient, "complete", side_effect=LLMError("Provider down", provider="mock")):
        state = {"task": "Analyze user dataset and write report", "task_id": "test-task"}
        out = decompose(state)
        assert "plan" in out
        assert len(out["plan"]) >= 1
        # Decompose fell back to deterministic keyword planning
        trace_events = [t for t in out.get("trace", []) if t.get("event") == "decompose"]
        assert len(trace_events) >= 1
        assert trace_events[-1].get("plan_source") == "keyword"

    # Full task workflow still completes cleanly
    result = run_task("Analyze test data")
    assert result["status"] == "completed"


def test_cli_json_output_remains_valid_when_provider_calls_fail(capsys, monkeypatch):
    """10. CLI --json output remains valid JSON even when provider encounters errors."""
    def failing_complete(*args, **kwargs):
        raise LLMError("LLM completely failed", provider="mock")

    monkeypatch.setattr(LLMClient, "complete", failing_complete)
    _ = cli(["Summarize quarterly report", "--json"])
    captured = capsys.readouterr()

    # stdout must be parseable JSON
    assert captured.out.strip(), "stdout must contain JSON"
    parsed = json.loads(captured.out)
    assert isinstance(parsed, dict)
    assert "status" in parsed
