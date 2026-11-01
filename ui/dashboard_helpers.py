"""Pure helper and presentation functions for the Orchestration Dashboard.

Separated from Streamlit UI code for high determinism, clean testing,
and safe metadata rendering.
"""

from __future__ import annotations

import re
from typing import Any

_SECRET_PATTERN = re.compile(r"(?i)(sk-[a-zA-Z0-9_\-]{8,}|bearer\s+[a-zA-Z0-9_\-\.]+)")


def format_status_badge(status: str | None) -> dict[str, str]:
    """Return a display badge configuration (label, icon, category) for a given status."""
    st = str(status or "unknown").strip().lower()

    if st == "running":
        return {"label": "Running", "icon": "⏳", "category": "in_progress", "css_color": "#1f77b4"}
    elif st in ("completed", "approved"):
        return {"label": "Completed", "icon": "✅", "category": "success", "css_color": "#2ca02c"}
    elif st == "edited":
        return {"label": "Edited & Approved", "icon": "✏️", "category": "success", "css_color": "#2ca02c"}
    elif st == "partial":
        return {"label": "Partial Success", "icon": "⚠️", "category": "partial", "css_color": "#ff7f0e"}
    elif st == "failed":
        return {"label": "Failed", "icon": "❌", "category": "failure", "css_color": "#d62728"}
    elif st == "rejected":
        return {"label": "Rejected", "icon": "⛔", "category": "failure", "css_color": "#d62728"}
    elif st == "blocked":
        return {"label": "Blocked", "icon": "🚫", "category": "blocked", "css_color": "#7f7f7f"}
    elif st in ("escalated", "pending_review"):
        return {
            "label": "Pending Human Review",
            "icon": "🔔",
            "category": "escalated",
            "css_color": "#9467bd",
        }
    elif st == "pending":
        return {"label": "Pending", "icon": "🕒", "category": "pending", "css_color": "#8c564b"}
    else:
        return {"label": st.capitalize(), "icon": "ℹ️", "category": "unknown", "css_color": "#7f7f7f"}


def format_cost_display(usage: dict[str, Any] | None) -> dict[str, Any]:
    """Format token usage and cost metrics distinctly for confirmed vs estimated rates."""
    if not usage or not isinstance(usage, dict):
        return {
            "available": False,
            "total_tokens": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "successful_requests": 0,
            "cost_display": "N/A",
            "is_estimated": False,
            "notice": "Usage telemetry not available for this run.",
        }

    total_tokens = int(usage.get("total_tokens", 0) or 0)
    input_tokens = int(usage.get("total_input_tokens", 0) or 0)
    output_tokens = int(usage.get("total_output_tokens", 0) or 0)
    successful_requests = int(usage.get("successful_requests", 0) or 0)
    cost_val = usage.get("estimated_cost_usd")
    has_unknown_cost = bool(usage.get("has_unknown_cost", False))

    if cost_val is not None:
        try:
            numeric_cost = float(cost_val)
            cost_str = f"${numeric_cost:.6f}"
            if has_unknown_cost:
                return {
                    "available": True,
                    "total_tokens": total_tokens,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "successful_requests": successful_requests,
                    "cost_display": f"{cost_str} (Estimated)",
                    "numeric_cost": numeric_cost,
                    "is_estimated": True,
                    "notice": "Partial/estimated rate: one or more provider call rates were unspecified.",
                }
            else:
                return {
                    "available": True,
                    "total_tokens": total_tokens,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "successful_requests": successful_requests,
                    "cost_display": cost_str,
                    "numeric_cost": numeric_cost,
                    "is_estimated": False,
                    "notice": "Confirmed provider rate.",
                }
        except (ValueError, TypeError):
            pass

    return {
        "available": True,
        "total_tokens": total_tokens,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "successful_requests": successful_requests,
        "cost_display": "Unknown",
        "numeric_cost": None,
        "is_estimated": True,
        "notice": "Provider usage recorded but cost model not defined for active model.",
    }


def classify_subtasks(run_data: dict[str, Any]) -> dict[str, list[str]]:
    """Group subtasks into completed, failed, blocked, and pending lists."""
    completed = [str(x) for x in run_data.get("completed_subtasks", []) if x]
    failed = [str(x) for x in run_data.get("failed_subtasks", []) if x]
    blocked = [str(x) for x in run_data.get("blocked_subtasks", []) if x]

    # Deduplicate while preserving order
    completed_dedup = list(dict.fromkeys(completed))
    failed_dedup = list(dict.fromkeys(failed))
    blocked_dedup = list(dict.fromkeys(blocked))

    return {
        "completed": completed_dedup,
        "failed": failed_dedup,
        "blocked": blocked_dedup,
    }


def check_resume_eligibility(run_data: dict[str, Any]) -> tuple[bool, str]:
    """Check if a workflow state is eligible for resumption, returning (eligible, reason)."""
    status = str(run_data.get("status", "")).strip().lower()

    if status == "running":
        return False, "Run is currently in-flight; wait for completion before attempting resume."

    # Check for completed runs
    failed_count = len(run_data.get("failed_subtasks", []))
    blocked_count = len(run_data.get("blocked_subtasks", []))
    if status in ("completed", "approved", "edited") and failed_count == 0 and blocked_count == 0:
        return False, "Run is already fully completed and cannot be resumed."

    # Check for pending HITL review
    hitl = run_data.get("hitl") or {}
    if isinstance(hitl, dict):
        if hitl.get("pending_review") or (hitl.get("escalated") and not hitl.get("resolution")):
            return (
                False,
                "Run is escalated and awaiting human reviewer resolution before resumption is allowed.",
            )

    return True, "Run is eligible for resumption from its latest checkpoint."


def format_checkpoint_records(checkpoints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Format step checkpoint metadata for table display without exposing raw state dicts."""
    rows = []
    for cp in checkpoints:
        rows.append(
            {
                "Step": cp.get("step"),
                "Transition": cp.get("transition", "unknown"),
                "Schema": f"v{cp.get('schema_version', 1)}",
                "Timestamp (UTC)": cp.get("created_at", "N/A"),
            }
        )
    return rows


def sanitize_ui_error(raw: str | Exception) -> str:
    """Sanitize error messages before displaying in the UI."""
    msg = str(raw) if not isinstance(raw, str) else raw
    if "Traceback (most recent call last)" in msg:
        return "Internal server execution failed. Details logged securely to server diagnostics."
    redacted = _SECRET_PATTERN.sub("[REDACTED]", msg)
    return redacted
