"""Provider-agnostic LLM layer over OpenAI + Anthropic, with a mock fallback.

The mock provider is deterministic so the full graph, memory, queue, and review
UI run end-to-end without any API keys.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from .config import Settings, get_settings
from .observability.tracing import create_trace_event, sanitize_trace_attribute, span
from .observability.usage import (
    DEFAULT_PRICING,
    LLMUsageRecord,
    ModelPricing,
    RunUsageSummary,
    Usage,
    calculate_estimated_cost,
    clear_run_usage,
    get_model_pricing,
    get_run_usage,
    record_llm_usage,
    register_model_pricing,
)

logger = logging.getLogger(__name__)

_UNSET = object()


# ── Exceptions ───────────────────────────────────────────────────────────────


class LLMError(RuntimeError):
    """Base exception for all LLM client failures."""

    def __init__(
        self,
        message: str,
        *,
        provider: str = "",
        attempts: int = 0,
        last_error: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.attempts = attempts
        self.last_error = last_error


class LLMTransientError(LLMError):
    """Transient provider failure eligible for retry/fallback (timeout, 5xx, 429, network)."""


class LLMPermanentError(LLMError):
    """Permanent error ineligible for retrying the same provider (auth, permissions, bad config)."""


class LLMAuthenticationError(LLMPermanentError):
    """Authentication or authorization failure (401, 403, missing API key)."""


class LLMResponseError(LLMTransientError):
    """Provider returned empty, whitespace, or malformed response."""


class LLMTimeoutError(LLMTransientError):
    """Provider call timed out."""


def is_permanent_error(exc: Exception) -> bool:
    """Return True if the error is unrecoverable on the same provider without config changes."""
    if isinstance(exc, LLMPermanentError):
        return True
    if isinstance(exc, LLMTransientError):
        return False

    status_code = getattr(exc, "status_code", None)
    if status_code in (401, 403, 400, 404):
        return True

    exc_name = type(exc).__name__
    if exc_name in (
        "AuthenticationError",
        "PermissionDeniedError",
        "NotFoundError",
        "BadRequestError",
    ):
        return True

    msg = str(exc).lower()
    if any(
        kw in msg
        for kw in (
            "unauthorized",
            "forbidden",
            "invalid api key",
            "api_key client option must be set",
            "authentication",
            "permission denied",
            "bad request",
            "not configured",
            "missing or empty in configuration",
        )
    ):
        return True

    return False


def is_transient_error(exc: Exception) -> bool:
    """Return True if the error is a temporary condition eligible for retry or fallback."""
    if isinstance(exc, LLMTransientError):
        return True
    if isinstance(exc, LLMPermanentError):
        return False

    status_code = getattr(exc, "status_code", None)
    if status_code in (408, 429, 500, 502, 503, 504):
        return True

    exc_name = type(exc).__name__
    if exc_name in (
        "RateLimitError",
        "APITimeoutError",
        "InternalServerError",
        "APIConnectionError",
        "TimeoutError",
        "ConnectError",
        "ReadTimeout",
        "ConnectTimeout",
    ):
        return True

    msg = str(exc).lower()
    if any(
        kw in msg
        for kw in (
            "rate limit",
            "timeout",
            "timed out",
            "connection",
            "overloaded",
            "service unavailable",
            "gateway timeout",
            "empty response",
        )
    ):
        return True

    return not is_permanent_error(exc)


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class Completion:
    text: str
    provider: str
    model: str
    usage: Usage | None = None


# ── Client ───────────────────────────────────────────────────────────────────


class LLMClient:
    """Provider wrapper supporting bounded retries, fallback resilience, and telemetry."""

    def __init__(
        self,
        provider: str | None = None,
        fallback_provider: str | None | object = _UNSET,
        settings: Settings | None = None,
    ) -> None:
        resolved_settings = settings or get_settings()
        self.settings = resolved_settings
        self.provider = provider or resolved_settings.active_provider

        if fallback_provider is _UNSET:
            self.fallback_provider = resolved_settings.fallback_llm_provider or None
        else:
            self.fallback_provider = fallback_provider or None

        self.events: list[dict[str, Any]] = []
        self.last_usage: Usage | None = None
        self._openai_client = None
        self._anthropic_client = None

    def _record_event(self, event_name: str, **kwargs: Any) -> None:
        """Record structured trace event safely; observability failures must not break execution."""
        try:
            evt = create_trace_event(node="llm", event=event_name, **kwargs)
            self.events.append(evt)
        except Exception as exc:
            logger.debug("Failed to record LLM trace event '%s': %s", event_name, exc)

    def complete(self, system: str, prompt: str, *, json_mode: bool = False) -> Completion:
        """Complete a prompt with bounded retries and configured fallback resilience."""
        primary_provider = self.provider or "mock"
        fallback_provider = self.fallback_provider
        if fallback_provider == primary_provider:
            fallback_provider = None

        max_attempts = 1 + max(0, self.settings.llm_max_retries)

        # 1. Attempt primary provider
        primary_comp, last_error, attempts_made = self._try_provider(
            primary_provider,
            system,
            prompt,
            json_mode=json_mode,
            max_attempts=max_attempts,
        )
        if primary_comp is not None:
            return primary_comp

        # 2. Primary failed. Check if fallback is configured.
        if fallback_provider is not None:
            safe_reason = (
                sanitize_trace_attribute("reason", str(last_error))
                if last_error
                else "primary_failed"
            )
            self._record_event(
                "fallback_selected",
                primary_provider=primary_provider,
                fallback_provider=fallback_provider,
                reason=safe_reason,
            )
            fallback_comp, fallback_error, fallback_attempts = self._try_provider(
                fallback_provider,
                system,
                prompt,
                json_mode=json_mode,
                max_attempts=max_attempts,
            )
            if fallback_comp is not None:
                return fallback_comp

            # Both providers failed
            total_attempts = attempts_made + fallback_attempts
            safe_error = (
                sanitize_trace_attribute("error", str(fallback_error))
                if fallback_error
                else "fallback_failed"
            )
            self._record_event(
                "provider_final_failure",
                provider=fallback_provider,
                attempts=total_attempts,
                error=safe_error,
            )
            msg = (
                f"All configured LLM providers failed "
                f"({primary_provider}, {fallback_provider}): {fallback_error}"
            )
            raise LLMError(
                msg,
                provider=fallback_provider,
                attempts=total_attempts,
                last_error=fallback_error,
            )

        # 3. No fallback configured; fail with primary error
        safe_error = sanitize_trace_attribute("error", str(last_error)) if last_error else "failed"
        self._record_event(
            "provider_final_failure",
            provider=primary_provider,
            attempts=attempts_made,
            error=safe_error,
        )
        if last_error and isinstance(last_error, LLMError):
            if not getattr(last_error, "attempts", None):
                last_error.attempts = attempts_made
            raise last_error
        raise LLMError(
            f"LLM provider '{primary_provider}' failed after {attempts_made} attempt(s): {last_error}",
            provider=primary_provider,
            attempts=attempts_made,
            last_error=last_error,
        )

    def _try_provider(
        self,
        provider: str,
        system: str,
        prompt: str,
        *,
        json_mode: bool,
        max_attempts: int,
    ) -> tuple[Completion | None, Exception | None, int]:
        """Attempt a single provider with bounded retries for transient errors."""
        attempts_made = 0
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            attempts_made = attempt
            self._record_event(
                "provider_attempt_started",
                provider=provider,
                attempt=attempt,
                json_mode=json_mode,
            )
            try:
                # Wrap telemetry span safely so telemetry errors never break execution
                try:
                    span_ctx = span(f"llm.{provider}.attempt", provider=provider, attempt=attempt)
                except Exception:
                    span_ctx = None

                if span_ctx is not None:
                    with span_ctx:
                        comp = self._execute_provider(provider, system, prompt, json_mode)
                else:
                    comp = self._execute_provider(provider, system, prompt, json_mode)

                inp_tokens = comp.usage.input_tokens if comp.usage else 0
                out_tokens = comp.usage.output_tokens if comp.usage else 0
                tot_tokens = comp.usage.total_tokens if comp.usage else (inp_tokens + out_tokens)

                self._record_event(
                    "provider_attempt_succeeded",
                    provider=provider,
                    attempt=attempt,
                    model=comp.model,
                    input_tokens=inp_tokens,
                    output_tokens=out_tokens,
                    total_tokens=tot_tokens,
                )
                self.last_usage = comp.usage

                try:
                    record_llm_usage(
                        provider=provider,
                        model=comp.model,
                        usage=comp.usage,
                        success=True,
                        is_fallback=(provider == self.fallback_provider),
                        attempt=attempt,
                    )
                except Exception as usage_err:
                    logger.debug("Failed to record successful LLM usage: %s", usage_err)

                return comp, None, attempts_made
            except Exception as exc:
                last_error = exc
                if isinstance(exc, LLMError) and not getattr(exc, "attempts", 0):
                    exc.attempts = attempt
                safe_error = sanitize_trace_attribute("error", str(exc))
                self._record_event(
                    "provider_attempt_failed",
                    provider=provider,
                    attempt=attempt,
                    error=safe_error,
                )

                try:
                    if provider in ("openai", "anthropic"):
                        model_name = getattr(self.settings, f"{provider}_model", "unknown")
                    else:
                        model_name = "mock-1"
                    record_llm_usage(
                        provider=provider,
                        model=model_name,
                        usage=None,
                        success=False,
                        is_fallback=(provider == self.fallback_provider),
                        attempt=attempt,
                    )
                except Exception as usage_err:
                    logger.debug("Failed to record failed LLM usage: %s", usage_err)

                if is_permanent_error(exc):
                    logger.warning(
                        "LLM provider '%s' failed on attempt %d with permanent error (%s); not retrying",
                        provider,
                        attempt,
                        exc,
                    )
                    break
                else:
                    logger.warning(
                        "LLM provider '%s' failed on attempt %d (%s); will retry if attempts remain",
                        provider,
                        attempt,
                        exc,
                    )

        return None, last_error, attempts_made

    def _execute_provider(
        self,
        provider: str,
        system: str,
        prompt: str,
        json_mode: bool,
    ) -> Completion:
        """Route to the concrete provider implementation and validate response."""
        if provider == "openai":
            comp = self._openai(system, prompt, json_mode)
        elif provider == "anthropic":
            comp = self._anthropic(system, prompt, json_mode)
        elif provider == "mock":
            comp = self._mock(system, prompt, json_mode)
        else:
            raise LLMPermanentError(f"Unsupported LLM provider: '{provider}'", provider=provider)

        self._validate_response_content(comp.text, json_mode=json_mode, provider=provider)
        return comp

    def _validate_response_content(self, text: str, *, json_mode: bool, provider: str) -> None:
        """Ensure provider responses are non-empty and valid JSON when requested."""
        if not text or not text.strip():
            raise LLMResponseError(
                f"LLM provider '{provider}' returned an empty or whitespace response",
                provider=provider,
            )
        if json_mode:
            try:
                json.loads(text)
            except Exception as exc:
                raise LLMResponseError(
                    f"LLM provider '{provider}' returned invalid JSON in json_mode: {exc}",
                    provider=provider,
                    last_error=exc,
                ) from exc

    def _classify_and_raise(self, exc: Exception, *, provider: str) -> None:
        """Translate raw SDK or network exceptions into typed LLM exceptions."""
        if isinstance(exc, LLMError):
            raise exc

        msg = str(exc)
        status_code = getattr(exc, "status_code", None)

        if is_permanent_error(exc):
            if (
                status_code in (401, 403)
                or "auth" in type(exc).__name__.lower()
                or "api_key" in msg.lower()
                or "unauthorized" in msg.lower()
                or "forbidden" in msg.lower()
            ):
                sanitized_msg = sanitize_trace_attribute("error", msg)
                raise LLMAuthenticationError(
                    f"Authentication failure with provider '{provider}': {sanitized_msg}",
                    provider=provider,
                    last_error=exc,
                ) from exc
            sanitized_msg = sanitize_trace_attribute("error", msg)
            raise LLMPermanentError(
                f"Permanent error with provider '{provider}': {sanitized_msg}",
                provider=provider,
                last_error=exc,
            ) from exc

        if (
            status_code in (408, 504)
            or "timeout" in type(exc).__name__.lower()
            or "timeout" in msg.lower()
            or "timed out" in msg.lower()
        ):
            raise LLMTimeoutError(
                f"Timeout error with provider '{provider}': {sanitize_trace_attribute('error', msg)}",
                provider=provider,
                last_error=exc,
            ) from exc

        raise LLMTransientError(
            f"Transient error with provider '{provider}': {sanitize_trace_attribute('error', msg)}",
            provider=provider,
            last_error=exc,
        ) from exc

    # ── concrete providers ───────────────────────────────────────────────────

    def _openai(self, system: str, prompt: str, json_mode: bool) -> Completion:
        if not self.settings.openai_api_key:
            raise LLMAuthenticationError(
                "OpenAI API key is missing or empty in configuration", provider="openai"
            )

        if self._openai_client is None:
            try:
                from openai import OpenAI
            except Exception as exc:
                raise LLMPermanentError(
                    f"Failed to import OpenAI client SDK: {exc}",
                    provider="openai",
                    last_error=exc,
                ) from exc

            self._openai_client = OpenAI(
                api_key=self.settings.openai_api_key,
                timeout=self.settings.llm_timeout_seconds,
                max_retries=0,  # Explicitly managed by LLMClient
            )

        client = self._openai_client
        kwargs = {"response_format": {"type": "json_object"}} if json_mode else {}
        try:
            resp = client.chat.completions.create(
                model=self.settings.openai_model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                **kwargs,
            )
        except Exception as exc:
            self._classify_and_raise(exc, provider="openai")

        content = resp.choices[0].message.content or ""
        usage = None
        if hasattr(resp, "usage") and resp.usage is not None:
            try:
                inp = getattr(resp.usage, "prompt_tokens", 0) or 0
                out = getattr(resp.usage, "completion_tokens", 0) or 0
                tot = getattr(resp.usage, "total_tokens", 0) or (inp + out)
                usage = Usage(input_tokens=inp, output_tokens=out, total_tokens=tot, is_estimated=False)
            except Exception:
                usage = None

        return Completion(content, "openai", self.settings.openai_model, usage=usage)

    def _anthropic(self, system: str, prompt: str, json_mode: bool) -> Completion:
        if not self.settings.anthropic_api_key:
            raise LLMAuthenticationError(
                "Anthropic API key is missing or empty in configuration", provider="anthropic"
            )

        if self._anthropic_client is None:
            try:
                import anthropic
            except Exception as exc:
                raise LLMPermanentError(
                    f"Failed to import Anthropic client SDK: {exc}",
                    provider="anthropic",
                    last_error=exc,
                ) from exc

            self._anthropic_client = anthropic.Anthropic(
                api_key=self.settings.anthropic_api_key,
                timeout=self.settings.llm_timeout_seconds,
                max_retries=0,  # Explicitly managed by LLMClient
            )

        client = self._anthropic_client
        actual_prompt = prompt
        if json_mode:
            actual_prompt += "\n\nRespond with valid JSON only."

        try:
            resp = client.messages.create(
                model=self.settings.anthropic_model,
                max_tokens=2048,
                system=system,
                messages=[{"role": "user", "content": actual_prompt}],
            )
        except Exception as exc:
            self._classify_and_raise(exc, provider="anthropic")

        text = "".join(block.text for block in resp.content if block.type == "text")
        usage = None
        if hasattr(resp, "usage") and resp.usage is not None:
            try:
                inp = getattr(resp.usage, "input_tokens", 0) or 0
                out = getattr(resp.usage, "output_tokens", 0) or 0
                tot = inp + out
                usage = Usage(input_tokens=inp, output_tokens=out, total_tokens=tot, is_estimated=False)
            except Exception:
                usage = None

        return Completion(text, "anthropic", self.settings.anthropic_model, usage=usage)

    def _mock(self, system: str, prompt: str, json_mode: bool) -> Completion:
        """Deterministic stand-in. Echoes a structured plan/answer."""
        if json_mode:
            payload = {
                "summary": f"[mock] handled: {prompt[:80]}",
                "confidence": 0.72,
                "quality": 0.71,
            }
            text = json.dumps(payload)
        else:
            text = f"[mock:{system[:24]}] {prompt[:200]}"

        in_tokens = max(1, len(prompt) // 4)
        out_tokens = max(1, len(text) // 4)
        usage = Usage(
            input_tokens=in_tokens,
            output_tokens=out_tokens,
            total_tokens=in_tokens + out_tokens,
            is_estimated=False,
        )
        return Completion(text, "mock", "mock-1", usage=usage)


__all__ = [
    "DEFAULT_PRICING",
    "Completion",
    "LLMAuthenticationError",
    "LLMClient",
    "LLMError",
    "LLMPermanentError",
    "LLMResponseError",
    "LLMTimeoutError",
    "LLMTransientError",
    "LLMUsageRecord",
    "ModelPricing",
    "RunUsageSummary",
    "Usage",
    "calculate_estimated_cost",
    "clear_run_usage",
    "get_model_pricing",
    "get_run_usage",
    "is_permanent_error",
    "is_transient_error",
    "record_llm_usage",
    "register_model_pricing",
]
