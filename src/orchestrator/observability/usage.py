"""Token usage accounting and estimated cost tracking per workflow run."""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from typing import Any

from .tracing import create_trace_event, get_current_run_id

logger = logging.getLogger(__name__)


# ── Data structures ──────────────────────────────────────────────────────────


@dataclass
class Usage:
    """Token counts reported by the provider for a single completion."""

    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    is_estimated: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "is_estimated": self.is_estimated,
        }


@dataclass(frozen=True)
class ModelPricing:
    """Documented pricing rates for a model in USD per 1M tokens."""

    input_cost_per_1m: float
    output_cost_per_1m: float

    @property
    def input_cost_per_token(self) -> float:
        return self.input_cost_per_1m / 1_000_000.0

    @property
    def output_cost_per_token(self) -> float:
        return self.output_cost_per_1m / 1_000_000.0

    def calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (input_tokens * self.input_cost_per_token) + (output_tokens * self.output_cost_per_token)


# Documented default model pricing rates ($ per 1M tokens)
# Sources: OpenAI and Anthropic official pricing tables
DEFAULT_PRICING: dict[str, ModelPricing] = {
    # Mock (deterministic local testing)
    "mock:mock-1": ModelPricing(0.0, 0.0),
    "mock:default": ModelPricing(0.0, 0.0),
    # OpenAI
    "openai:gpt-4o-mini": ModelPricing(0.15, 0.60),
    "openai:gpt-4o": ModelPricing(2.50, 10.00),
    "openai:gpt-4-turbo": ModelPricing(10.00, 30.00),
    "openai:gpt-3.5-turbo": ModelPricing(0.50, 1.50),
    # Anthropic
    "anthropic:claude-opus-4-8": ModelPricing(15.00, 75.00),
    "anthropic:claude-3-opus": ModelPricing(15.00, 75.00),
    "anthropic:claude-3-5-sonnet": ModelPricing(3.00, 15.00),
    "anthropic:claude-3-7-sonnet": ModelPricing(3.00, 15.00),
    "anthropic:claude-3-haiku": ModelPricing(0.25, 1.25),
}

_CUSTOM_PRICING: dict[str, ModelPricing] = {}
_PRICING_LOCK = threading.Lock()


def register_model_pricing(
    provider: str,
    model: str,
    input_cost_per_1m: float,
    output_cost_per_1m: float,
) -> None:
    """Register or override documented pricing rates for a provider model."""
    key = f"{provider.strip().lower()}:{model.strip().lower()}"
    pricing = ModelPricing(
        input_cost_per_1m=max(0.0, float(input_cost_per_1m)),
        output_cost_per_1m=max(0.0, float(output_cost_per_1m)),
    )
    with _PRICING_LOCK:
        _CUSTOM_PRICING[key] = pricing


def get_model_pricing(provider: str, model: str) -> ModelPricing | None:
    """Look up pricing configuration; returns None if model rate is unconfigured."""
    provider_clean = (provider or "").strip().lower()
    model_clean = (model or "").strip().lower()
    full_key = f"{provider_clean}:{model_clean}"

    with _PRICING_LOCK:
        if full_key in _CUSTOM_PRICING:
            return _CUSTOM_PRICING[full_key]
        if model_clean in _CUSTOM_PRICING:
            return _CUSTOM_PRICING[model_clean]

    # Check environment/settings override if configured
    try:
        from ..config import get_settings

        custom_json = get_settings().llm_custom_pricing
        if custom_json and custom_json.strip():
            rates = json.loads(custom_json)
            if isinstance(rates, dict):
                rate_dict = rates.get(full_key) or rates.get(model_clean)
                if isinstance(rate_dict, dict):
                    in_rate = rate_dict.get("input_cost_per_1m", rate_dict.get("input", 0.0))
                    out_rate = rate_dict.get("output_cost_per_1m", rate_dict.get("output", 0.0))
                    return ModelPricing(float(in_rate), float(out_rate))
    except Exception:
        pass

    if full_key in DEFAULT_PRICING:
        return DEFAULT_PRICING[full_key]
    if model_clean in DEFAULT_PRICING:
        return DEFAULT_PRICING[model_clean]

    return None


def calculate_estimated_cost(
    provider: str,
    model: str,
    input_tokens: int,
    output_tokens: int,
) -> tuple[float | None, bool]:
    """Calculate estimated cost for token counts.

    Returns:
        (estimated_cost_usd, is_cost_known). If pricing is not configured,
        returns (None, False) rather than a misleading zero.
    """
    pricing = get_model_pricing(provider, model)
    if pricing is None:
        return None, False
    cost = pricing.calculate_cost(input_tokens, output_tokens)
    return cost, True


# ── Per-Run Accounting ───────────────────────────────────────────────────────


@dataclass
class LLMUsageRecord:
    """Record of a single LLM attempt (successful or failed)."""

    run_id: str
    provider: str
    model: str
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    estimated_cost_usd: float | None = None
    is_cost_known: bool = False
    success: bool = True
    is_fallback: bool = False
    attempt: int = 1
    is_estimated: bool = False


@dataclass
class RunUsageSummary:
    """Aggregated usage and cost summary for a workflow run."""

    run_id: str
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tokens: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    estimated_cost_usd: float | None = 0.0
    has_unknown_cost: bool = False
    unknown_cost_requests: int = 0
    breakdown: dict[str, dict[str, Any]] = field(default_factory=dict)
    failed_attempts_tokens: dict[str, int] = field(
        default_factory=lambda: {"input": 0, "output": 0, "total": 0}
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_tokens": self.total_tokens,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "estimated_cost_usd": (
                round(self.estimated_cost_usd, 6)
                if self.estimated_cost_usd is not None
                else None
            ),
            "has_unknown_cost": self.has_unknown_cost,
            "unknown_cost_requests": self.unknown_cost_requests,
            "breakdown": self.breakdown,
        }


_REGISTRY_LOCK = threading.Lock()
_RUN_REGISTRY: dict[str, list[LLMUsageRecord]] = {}


def clear_run_usage(run_id: str | None = None) -> None:
    """Clear usage records for test isolation or memory management."""
    with _REGISTRY_LOCK:
        if run_id:
            _RUN_REGISTRY.pop(run_id, None)
        else:
            _RUN_REGISTRY.clear()


def record_llm_usage(
    *,
    provider: str,
    model: str,
    usage: Usage | None = None,
    success: bool = True,
    is_fallback: bool = False,
    attempt: int = 1,
    run_id: str = "",
) -> LLMUsageRecord:
    """Record an individual LLM usage event safely correlated with the active run ID."""
    active_run_id = (run_id or get_current_run_id() or "").strip()
    inp = usage.input_tokens if usage else 0
    out = usage.output_tokens if usage else 0
    tot = usage.total_tokens if usage else (inp + out)
    is_est = usage.is_estimated if usage else False

    cost, is_known = calculate_estimated_cost(provider, model, inp, out)

    rec = LLMUsageRecord(
        run_id=active_run_id,
        provider=provider,
        model=model,
        input_tokens=inp,
        output_tokens=out,
        total_tokens=tot,
        estimated_cost_usd=cost,
        is_cost_known=is_known,
        success=success,
        is_fallback=is_fallback,
        attempt=attempt,
        is_estimated=is_est,
    )

    if active_run_id:
        with _REGISTRY_LOCK:
            _RUN_REGISTRY.setdefault(active_run_id, []).append(rec)

    # Emit telemetry event safely
    try:
        create_trace_event(
            node="llm",
            event="token_usage",
            run_id=active_run_id,
            provider=provider,
            model=model,
            input_tokens=inp,
            output_tokens=out,
            total_tokens=tot,
            estimated_cost_usd=cost if is_known else None,
            is_cost_known=is_known,
            success=success,
            is_fallback=is_fallback,
            attempt=attempt,
        )
    except Exception:
        pass

    return rec


def get_run_usage(run_id: str) -> RunUsageSummary:
    """Aggregate all LLM usage records for a specific run ID into a compact summary."""
    active_run_id = (run_id or "").strip()
    with _REGISTRY_LOCK:
        records = list(_RUN_REGISTRY.get(active_run_id, []))

    summary = RunUsageSummary(run_id=active_run_id)
    if not records:
        return summary

    known_cost_sum = 0.0
    any_known_cost = False

    for rec in records:
        key = f"{rec.provider}:{rec.model}"
        if key not in summary.breakdown:
            summary.breakdown[key] = {
                "provider": rec.provider,
                "model": rec.model,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "successful_requests": 0,
                "failed_requests": 0,
                "estimated_cost_usd": 0.0,
                "is_cost_known": True,
            }
        b = summary.breakdown[key]

        if rec.success:
            summary.successful_requests += 1
            summary.total_input_tokens += rec.input_tokens
            summary.total_output_tokens += rec.output_tokens
            summary.total_tokens += rec.total_tokens

            b["successful_requests"] += 1
            b["input_tokens"] += rec.input_tokens
            b["output_tokens"] += rec.output_tokens
            b["total_tokens"] += rec.total_tokens

            if rec.is_cost_known and rec.estimated_cost_usd is not None:
                known_cost_sum += rec.estimated_cost_usd
                any_known_cost = True
                if b["estimated_cost_usd"] is not None:
                    b["estimated_cost_usd"] += rec.estimated_cost_usd
            else:
                summary.has_unknown_cost = True
                summary.unknown_cost_requests += 1
                b["is_cost_known"] = False
                b["estimated_cost_usd"] = None
        else:
            summary.failed_requests += 1
            b["failed_requests"] += 1
            summary.failed_attempts_tokens["input"] += rec.input_tokens
            summary.failed_attempts_tokens["output"] += rec.output_tokens
            summary.failed_attempts_tokens["total"] += rec.total_tokens

    # Format breakdown cost
    for b in summary.breakdown.values():
        if b["estimated_cost_usd"] is not None:
            b["estimated_cost_usd"] = round(b["estimated_cost_usd"], 6)

    # Set overall estimated cost
    if any_known_cost:
        summary.estimated_cost_usd = round(known_cost_sum, 6)
    elif summary.has_unknown_cost:
        summary.estimated_cost_usd = None
    else:
        summary.estimated_cost_usd = 0.0

    return summary
