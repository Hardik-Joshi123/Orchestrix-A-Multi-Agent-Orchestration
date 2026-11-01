"""Reviewer node: validates specialist output before returning to supervisor."""

from __future__ import annotations

from ..config import get_settings
from ..hitl.sensitivity import classify
from ..observability.tracing import create_trace_event, span
from ..tools.registry import REGISTRY
from .state import GraphState


def review(state: GraphState) -> GraphState:
    """Score the latest completed work and decide retry / escalate / accept."""
    settings = get_settings()
    completed = state.get("completed", [])
    latest = completed[-1] if completed else None
    run_id = state.get("run_id") or state.get("task_id", "")

    with span("reviewer.review", run_id=run_id):
        if latest is None:
            quality, confidence = 0.0, 0.0
        elif latest.get("status") == "failed":
            quality, confidence = 0.0, 0.0
        else:
            # Toy heuristic quality signal; swap for an LLM judge in production.
            quality = min(1.0, 0.5 + len(latest.get("result", "")) / 400)
            confidence = latest.get("confidence", 0.0)

        # Authoritative gate: side-effecting tools used by this specialist subtask
        latest_tools = latest.get("tools_used") if latest else None
        tools_to_check = latest_tools if latest_tools is not None else state.get("tools_used", [])
        used_side_effecting = [t for t in tools_to_check if REGISTRY.requires_approval(t)]

        text_sensitive, sensitivity_labels = (
            classify(latest["result"])
            if (latest is not None and latest.get("status") != "failed")
            else (False, [])
        )

        reason = ""
        escalate = False
        is_subtask_failed = latest is not None and latest.get("status") == "failed"
        is_low_score = confidence < settings.confidence_threshold or quality < settings.quality_threshold

        # Side-effecting tool use is the authoritative trigger and takes
        # precedence so it can never be masked by a passing quality score.
        if used_side_effecting:
            escalate, reason = True, "side_effecting_tool"
        elif text_sensitive:
            escalate, reason = True, "sensitive_operation"
        elif state.get("retries", 0) < settings.max_retries and (is_subtask_failed or is_low_score):
            # Allow bounded retry loop in builder to retry before escalating
            escalate, reason = False, ""
        elif state.get("retries", 0) >= settings.max_retries and (is_subtask_failed or is_low_score):
            escalate, reason = True, "repeated_failure"
        elif confidence < settings.confidence_threshold:
            escalate, reason = True, "low_confidence"
        elif quality < settings.quality_threshold:
            escalate, reason = True, "low_quality"

    return {
        "quality": quality,
        "confidence": confidence,
        "escalated": escalate,
        "escalation_reason": reason,
        "escalated_subtask_id": latest["id"] if (escalate and latest) else "",
        "trace": [
            create_trace_event(
                node="reviewer",
                event="review",
                run_id=run_id,
                quality=round(quality, 3),
                confidence=round(confidence, 3),
                escalate=escalate,
                reason=reason,
                subtask=latest["id"] if latest else "",
                subtask_id=latest["id"] if latest else "",
                sensitivity=sensitivity_labels,
                side_effecting_tools=used_side_effecting,
            )
        ],
    }
