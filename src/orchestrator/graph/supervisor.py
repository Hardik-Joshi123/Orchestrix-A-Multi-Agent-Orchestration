"""Supervisor node: decomposes the task and reduces specialist outputs."""

from __future__ import annotations

import logging
from typing import Any

from ..llm import LLMClient
from ..observability.tracing import create_trace_event, span
from .state import GraphState, Subtask

logger = logging.getLogger(__name__)

_KEYWORD_ROUTES = {
    "research": ("research", "find", "investigate", "compare", "sources"),
    "code": ("code", "implement", "function", "bug", "refactor", "script"),
    "writing": ("write", "draft", "summarize", "report", "email", "pitch"),
    "analysis": ("analyze", "evaluate", "recommend", "assess", "trade-off"),
}


# Specialists run in this canonical pipeline order so a multi-subtask plan reads
# coherently (gather facts -> reason -> build -> write up).
_ORDER: tuple[str, ...] = ("research", "analysis", "code", "writing")


def _matched_kinds(task: str) -> list[str]:
    """Every specialist whose trigger keywords appear in the task, in pipeline order."""
    text = task.lower()
    return [k for k in _ORDER if any(kw in text for kw in _KEYWORD_ROUTES[k])]


def plan_subtasks(task: str) -> list[Subtask]:
    """Decompose a task into one subtask per relevant specialist.

    A task can legitimately need several specialists ("research X, analyze the
    options, and write a recommendation"), so we emit one subtask per matched
    kind rather than collapsing to a single route. When nothing matches we fall
    back to a single analysis subtask so the graph always has work to do.
    """
    kinds = _matched_kinds(task) or ["analysis"]
    return [
        {
            "id": f"s{i + 1}",
            "kind": kind,
            "instruction": task,
            "status": "pending",
            "result": "",
            "confidence": 0.0,
            "depends_on": [],
        }
        for i, kind in enumerate(kinds)
    ]


MAX_PLAN_SIZE: int = 10
"""Maximum number of subtasks permitted in an execution plan to prevent runaway graphs."""

VALID_SPECIALISTS: tuple[str, ...] = ("research", "analysis", "code", "writing")

SPECIALIST_ALIASES: dict[str, str] = {
    "coder": "code",
    "coding": "code",
    "developer": "code",
    "programmer": "code",
    "software": "code",
    "analyst": "analysis",
    "analyze": "analysis",
    "evaluation": "analysis",
    "evaluator": "analysis",
    "researcher": "research",
    "investigate": "research",
    "search": "research",
    "writer": "writing",
    "write": "writing",
    "documentation": "writing",
    "summary": "writing",
}


def is_supported_specialist(kind: Any) -> bool:
    """Check whether a specialist kind or alias is supported by the orchestrator."""
    if not isinstance(kind, str):
        return False
    canonical = kind.strip().lower()
    return canonical in VALID_SPECIALISTS or canonical in SPECIALIST_ALIASES


def normalize_specialist_kind(kind: str) -> str:
    """Validate and normalize a specialist kind name against registered specialists.

    If the specialist is unknown and cannot be mapped via aliases, safely defaults
    to 'analysis' so the execution pipeline never crashes on unregistered nodes.
    """
    if not isinstance(kind, str):
        return "analysis"
    canonical = kind.strip().lower()
    if canonical in VALID_SPECIALISTS:
        return canonical
    return SPECIALIST_ALIASES.get(canonical, "analysis")


def validate_plan(
    plan: list[Subtask] | list[dict] | None,
    strict_specialists: bool = True,
    max_size: int = MAX_PLAN_SIZE,
) -> tuple[bool, str]:
    """Validate an execution plan against structural, semantic, and dependency constraints.

    Checks:
      1. Plan is a non-empty list of subtask dictionaries.
      2. Plan size is within max_size.
      3. Subtask IDs are unique, non-empty, and stable.
      4. Every subtask has a usable, non-empty instruction/description.
      5. Specialist kinds map to registered specialists (or aliases).
      6. Dependencies reference valid subtask IDs and no subtask depends on itself.
      7. Dependency graph is acyclic (no circular dependencies).

    Returns:
        tuple[bool, str]: (is_valid, failure_reason)
    """
    if not isinstance(plan, list) or not plan:
        return False, "Plan must contain at least one subtask"

    if len(plan) > max_size:
        return False, f"Plan exceeds maximum allowed size of {max_size} subtasks (got {len(plan)})"

    seen_ids: set[str] = set()
    plan_ids: set[str] = set()

    for item in plan:
        if not isinstance(item, dict):
            return False, "Plan subtask must be a dictionary"
        sid = str(item.get("id", "")).strip()
        if not sid:
            return False, "Subtask ID must not be empty"
        if sid in seen_ids:
            return False, f"Duplicate subtask ID found: '{sid}'"
        seen_ids.add(sid)
        plan_ids.add(sid)

    for item in plan:
        sid = str(item.get("id", "")).strip()
        instruction = str(item.get("instruction", "")).strip()
        if not instruction:
            return False, f"Subtask '{sid}' has empty instruction"

        raw_kind = str(item.get("kind", "")).strip()
        if strict_specialists:
            if not is_supported_specialist(raw_kind):
                return False, f"Subtask '{sid}' specifies unsupported specialist kind: '{raw_kind}'"

        raw_deps = item.get("depends_on")
        if raw_deps is None:
            deps_list = []
        elif isinstance(raw_deps, str):
            deps_list = [raw_deps.strip()] if raw_deps.strip() else []
        elif isinstance(raw_deps, (list, tuple, set)):
            deps_list = [str(d).strip() for d in raw_deps if str(d).strip()]
        else:
            return False, f"Subtask '{sid}' has invalid depends_on format"

        # Check self-dependency
        if sid in deps_list:
            return False, f"Subtask '{sid}' cannot depend on itself"

        # Check missing dependencies
        for d in deps_list:
            if d not in plan_ids:
                return False, f"Subtask '{sid}' references non-existent dependency '{d}'"

    # Cycle detection via topological sort (Kahn's algorithm)
    adj: dict[str, list[str]] = {str(s["id"]).strip(): [] for s in plan}
    in_degree: dict[str, int] = {str(s["id"]).strip(): 0 for s in plan}

    for item in plan:
        sid = str(item["id"]).strip()
        raw_deps = item.get("depends_on") or []
        unique_deps = {str(d).strip() for d in raw_deps if str(d).strip()}
        for d in unique_deps:
            adj[d].append(sid)
            in_degree[sid] += 1

    queue = [sid for sid, deg in in_degree.items() if deg == 0]
    visited_count = 0
    while queue:
        curr = queue.pop(0)
        visited_count += 1
        for nxt in adj[curr]:
            in_degree[nxt] -= 1
            if in_degree[nxt] == 0:
                queue.append(nxt)

    if visited_count < len(plan):
        return False, "Dependency cycle detected in plan"

    return True, ""


def parse_plan_payload(
    raw: str | dict | list | None,
    default_instruction: str = "",
    strict_specialists: bool = False,
    max_size: int = MAX_PLAN_SIZE,
) -> list[Subtask] | None:
    """Parse and validate a structured plan payload into valid Subtask items."""
    if raw is None:
        return None

    import json
    import re

    data = None
    if isinstance(raw, (dict, list)):
        data = raw
    elif isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        # Extract markdown codeblock if present
        if "```" in text:
            match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
            if match:
                text = match.group(1).strip()
        try:
            data = json.loads(text)
        except Exception:
            return None

    items = None
    if isinstance(data, dict):
        items = data.get("plan") or data.get("subtasks")
    elif isinstance(data, list):
        items = data

    if not isinstance(items, list) or not items:
        return None

    # Bound oversized plans to max_size
    if len(items) > max_size:
        items = items[:max_size]

    subtasks: list[Subtask] = []
    for i, item in enumerate(items):
        if isinstance(item, dict):
            raw_kind = str(item.get("kind") or item.get("specialist") or item.get("role") or "").strip()
            if strict_specialists and not is_supported_specialist(raw_kind):
                return None
            kind = normalize_specialist_kind(raw_kind)

            raw_inst = item.get("instruction")
            if raw_inst is None:
                raw_inst = item.get("task")
            if raw_inst is not None:
                instruction = str(raw_inst).strip()
            else:
                instruction = str(default_instruction).strip()

            raw_deps = (
                item.get("depends_on")
                if item.get("depends_on") is not None
                else item.get("dependencies")
                if item.get("dependencies") is not None
                else item.get("prerequisites")
            )
            if raw_deps is None:
                depends_on = []
            elif isinstance(raw_deps, str):
                depends_on = [raw_deps.strip()] if raw_deps.strip() else []
            elif isinstance(raw_deps, (list, tuple, set)):
                depends_on = [str(d).strip() for d in raw_deps if str(d).strip()]
            else:
                depends_on = []

            # Deduplicate dependencies while preserving order
            depends_on = list(dict.fromkeys(depends_on))

            subtask_id = str(item.get("id") or f"s{i + 1}").strip()
            subtasks.append(
                {
                    "id": subtask_id,
                    "kind": kind,
                    "instruction": instruction or default_instruction,
                    "status": "pending",
                    "result": "",
                    "confidence": 0.0,
                    "depends_on": depends_on,
                }
            )
        elif isinstance(item, str):
            raw_kind = item.strip()
            if strict_specialists and not is_supported_specialist(raw_kind):
                return None
            kind = normalize_specialist_kind(raw_kind)
            subtasks.append(
                {
                    "id": f"s{i + 1}",
                    "kind": kind,
                    "instruction": default_instruction,
                    "status": "pending",
                    "result": "",
                    "confidence": 0.0,
                    "depends_on": [],
                }
            )

    return subtasks if subtasks else None


def decompose(state: GraphState) -> GraphState:
    """Break the task into ordered subtasks across the specialist pool."""
    task = state["task"]
    run_id = state.get("run_id") or state.get("task_id", "")
    with span("supervisor.decompose", run_id=run_id, task=task):
        plan = None
        plan_source = "keyword"

        # Check if plan was already supplied in state
        if state.get("plan"):
            candidate_plan = parse_plan_payload(
                state["plan"],
                default_instruction=task,
                strict_specialists=False,
                max_size=MAX_PLAN_SIZE,
            )
            if candidate_plan:
                plan = candidate_plan
                plan_source = "injected"

        if not plan:
            llm = LLMClient()
            try:
                completion = llm.complete(
                    system=(
                        "You are a supervisor that decomposes tasks for specialist agents.\n"
                        "Registered specialists:\n"
                        "- research: gathering facts, references, documentation, data\n"
                        "- analysis: evaluating options, trade-offs, architecture, logic\n"
                        "- code: implementing functions, scripts, unit tests, code fixes\n"
                        "- writing: drafting summaries, reports, documentation, communication\n\n"
                        "Planning rules:\n"
                        "1. Produce the minimum necessary subtasks (typically 1 to 4) to fulfill the task.\n"
                        "   Do not create redundant, overlapping, or near-duplicate subtasks.\n"
                        "2. Every subtask must have a clear, actionable instruction with a specific\n"
                        "   expected outcome.\n"
                        "3. Set 'depends_on' ONLY when a genuine prerequisite exists.\n"
                        "   Independent subtasks must have empty dependencies ('depends_on': []).\n"
                        "4. Subtask IDs must be unique ('s1', 's2', ...). No self-dependencies or cycles.\n"
                        "5. Assign specialists based strictly on actual requirements.\n\n"
                        'Respond with a JSON object: {"plan": [{"id": "s1", "kind": "<specialist>", '
                        '"instruction": "<details>", "depends_on": []}, ...]}'
                    ),
                    prompt=f"Decompose this task into ordered subtasks: {task}",
                    json_mode=True,
                )
                candidate_plan = parse_plan_payload(
                    completion.text,
                    default_instruction=task,
                    strict_specialists=True,
                    max_size=MAX_PLAN_SIZE,
                )
                if candidate_plan:
                    is_valid, reason = validate_plan(
                        candidate_plan,
                        strict_specialists=True,
                        max_size=MAX_PLAN_SIZE,
                    )
                    if is_valid:
                        plan = candidate_plan
                        plan_source = "llm"
                    else:
                        logger.warning("LLM plan rejected (%s); falling back to keyword planning", reason)
                        plan = None
            except Exception as exc:
                logger.info("LLM planning failed (%s); falling back to keyword planning", exc)
                plan = None

        # Safe fallback if LLM plan generation is unavailable or invalid
        if not plan:
            candidate_fallback = plan_subtasks(task)
            is_valid, fb_reason = validate_plan(
                candidate_fallback,
                strict_specialists=True,
                max_size=MAX_PLAN_SIZE,
            )
            if is_valid:
                plan = candidate_fallback
                plan_source = "keyword"
            else:
                logger.error("Fallback plan invalid (%s)", fb_reason)
                plan = None

        # If a valid plan cannot be produced at all, return a controlled failure
        if not plan:
            failed_subtask: Subtask = {
                "id": "s1",
                "kind": "analysis",
                "instruction": task or "Plan generation failed",
                "status": "failed",
                "result": "",
                "confidence": 0.0,
                "error": "Plan validation failed: unable to produce a valid execution plan",
                "tools_used": [],
                "depends_on": [],
            }
            return {
                "plan": [failed_subtask],
                "completed": [failed_subtask],
                "failed": [failed_subtask],
                "status": "failed",
                "trace": [
                    create_trace_event(
                        node="supervisor",
                        event="decompose",
                        run_id=run_id,
                        subtasks=0,
                        kinds=[],
                        status="failed",
                        error="Plan validation failed: unable to produce a valid execution plan",
                    )
                ],
            }

        task_id = state.get("task_id")
        if task_id:
            try:
                from ..memory.short_term import ShortTermMemory

                ShortTermMemory(task_id).set("stage", "decompose")
                ShortTermMemory(task_id).set("plan", [s["id"] for s in plan])
            except Exception:
                pass

    return {
        "plan": plan,
        "retries": state.get("retries", 0),
        "trace": [
            create_trace_event(
                node="supervisor",
                event="decompose",
                run_id=run_id,
                subtasks=len(plan),
                kinds=[s["kind"] for s in plan],
                plan_source=plan_source,
            )
        ],
    }


def sanitize_error(error: str | None) -> str:
    """Sanitize internal error messages for user-facing output.

    Strips stack traces, redacts API keys and secrets, and bounds length.
    """
    if not error:
        return "Unknown error"
    text = str(error).strip()
    if not text:
        return "Unknown error"

    # If it contains a traceback, extract the final exception line
    if "Traceback (most recent call last):" in text:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if lines:
            text = lines[-1]

    # Redact API keys and authorization tokens
    import re

    text = re.sub(r"sk-[a-zA-Z0-9_-]{16,}", "[REDACTED_KEY]", text)
    text = re.sub(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{16,}", "Bearer [REDACTED_TOKEN]", text)
    text = re.sub(
        r"(?i)(password|api[_-]?key|secret|token|access[_-]?token)\s*[:=]\s*[^\s,;]+",
        r"\1=[REDACTED]",
        text,
    )

    # Bound single line length
    if len(text) > 200:
        text = text[:197] + "..."
    return text


def reduce(state: GraphState) -> GraphState:
    """Combine completed subtasks into a single, coherent final answer.

    Each specialist's output is kept as a labelled section so a multi-agent run
    aggregates into one readable answer instead of a flat concatenation.
    """
    run_id = state.get("run_id") or state.get("task_id", "")
    with span("supervisor.reduce", run_id=run_id):
        completed_all = state.get("completed", [])
        plan = state.get("plan", [])

        plan_order = {s["id"]: i for i, s in enumerate(plan)}
        done = sorted(
            [c for c in completed_all if c.get("status") == "done"],
            key=lambda c: plan_order.get(c.get("id"), 9999),
        )

        raw_failed = [
            c for c in (completed_all + state.get("failed", [])) if c.get("status") == "failed"
        ]
        seen_failed_ids: set[str] = set()
        failed = []
        for f in raw_failed:
            if f["id"] not in seen_failed_ids:
                seen_failed_ids.add(f["id"])
                failed.append(f)
        failed.sort(key=lambda f: plan_order.get(f.get("id"), 9999))

        raw_blocked = [
            c for c in (completed_all + state.get("blocked", [])) if c.get("status") == "blocked"
        ]
        seen_blocked_ids: set[str] = set()
        blocked = []
        for b in raw_blocked:
            if b["id"] not in seen_blocked_ids:
                seen_blocked_ids.add(b["id"])
                blocked.append(b)
        blocked.sort(key=lambda b: plan_order.get(b.get("id"), 9999))

        raw_rejected = [c for c in completed_all if c.get("status") == "rejected"]
        seen_rejected_ids: set[str] = set()
        rejected = []
        for r in raw_rejected:
            if r["id"] not in seen_rejected_ids:
                seen_rejected_ids.add(r["id"])
                rejected.append(r)
        rejected.sort(key=lambda r: plan_order.get(r.get("id"), 9999))

        processed_ids = {c["id"] for c in completed_all}
        pending = [
            s
            for s in plan
            if s["id"] not in processed_ids
            and s["id"] not in seen_failed_ids
            and s["id"] not in seen_blocked_ids
            and s["id"] not in seen_rejected_ids
        ]

        escalations = state.get("escalations", [])
        is_pending = bool(escalations) and not state.get("resolution")

        has_incomplete = bool(failed or blocked or rejected or pending)

        if state.get("status") == "cancelled":
            status = "cancelled"
            reason = state.get("cancellation_reason") or "user requested cancellation"
            if done:
                sections = [f"### {c['kind'].title()} specialist\n{c['result']}" for c in done]
                answer = "\n\n".join(sections)
                answer += f"\n\n### Workflow Cancelled\nWorkflow execution was cancelled ({reason})."
                if has_incomplete:
                    notes = []
                    for f in failed:
                        reason_str = sanitize_error(f.get("error"))
                        k = f.get("kind", "specialist")
                        notes.append(f"- Subtask {f['id']} ({k}): Failed — {reason_str}")
                    for b in blocked:
                        reason_str = sanitize_error(b.get("error"))
                        k = b.get("kind", "specialist")
                        notes.append(f"- Subtask {b['id']} ({k}): Blocked — {reason_str}")
                    for r in rejected:
                        k = r.get("kind", "specialist")
                        notes.append(f"- Subtask {r['id']} ({k}): Rejected by reviewer")
                    for p in pending:
                        k = p.get("kind", "specialist")
                        notes.append(f"- Subtask {p['id']} ({k}): Cancelled before execution")
                    if notes:
                        answer += "\n\n### Incomplete Work\n" + "\n".join(notes)
            else:
                answer = f"(workflow execution cancelled: {reason})"
        elif state.get("status") == "timed_out":
            status = "timed_out"
            if done:
                sections = [f"### {c['kind'].title()} specialist\n{c['result']}" for c in done]
                answer = "\n\n".join(sections)
                answer += (
                    "\n\n### Execution Deadline Exceeded\n"
                    "Workflow execution timed out before all subtasks completed."
                )
                if has_incomplete:
                    notes = []
                    for f in failed:
                        reason_str = sanitize_error(f.get("error"))
                        k = f.get("kind", "specialist")
                        notes.append(f"- Subtask {f['id']} ({k}): Failed — {reason_str}")
                    for b in blocked:
                        reason_str = sanitize_error(b.get("error"))
                        k = b.get("kind", "specialist")
                        notes.append(f"- Subtask {b['id']} ({k}): Blocked — {reason_str}")
                    for r in rejected:
                        k = r.get("kind", "specialist")
                        notes.append(f"- Subtask {r['id']} ({k}): Rejected by reviewer")
                    for p in pending:
                        k = p.get("kind", "specialist")
                        notes.append(f"- Subtask {p['id']} ({k}): Timed out before execution")
                    if notes:
                        answer += "\n\n### Incomplete Work\n" + "\n".join(notes)
            else:
                answer = "(workflow execution timed out: deadline exceeded)"
        elif state.get("approval") == "take_over" and not state.get("resolution"):
            answer = (
                "### Human Review Required (take_over)\n"
                "Execution paused: This operation requires human review "
                "and take-over before output is finalized."
            )
            status = "pending"
        elif done:
            sections = [f"### {c['kind'].title()} specialist\n{c['result']}" for c in done]
            answer = "\n\n".join(sections)
            if has_incomplete:
                notes = []
                for f in failed:
                    reason = sanitize_error(f.get("error"))
                    notes.append(f"- Subtask {f['id']} ({f.get('kind', 'specialist')}): Failed — {reason}")
                for b in blocked:
                    reason = sanitize_error(b.get("error"))
                    notes.append(f"- Subtask {b['id']} ({b.get('kind', 'specialist')}): Blocked — {reason}")
                for r in rejected:
                    notes.append(f"- Subtask {r['id']} ({r.get('kind', 'specialist')}): Rejected by reviewer")
                for p in pending:
                    notes.append(f"- Subtask {p['id']} ({p.get('kind', 'specialist')}): Pending execution")
                if notes:
                    answer += "\n\n### Incomplete Work\n" + "\n".join(notes)

            if is_pending:
                status = "pending"
            elif has_incomplete:
                if state.get("status") in (None, "completed"):
                    status = "partial"
                else:
                    status = state.get("status", "partial")
            elif state.get("status") in ("approved", "edited", "taken_over", "rejected"):
                status = state.get("status")
            else:
                status = "completed"
        elif failed:
            reasons = [
                f"Subtask {f['id']} ({f.get('kind', 'specialist')}): {sanitize_error(f.get('error'))}"
                for f in failed
            ]
            detail = f" — {'; '.join(reasons)}" if reasons else ""
            answer = f"(specialist execution failed{detail})"
            status = "pending" if is_pending else "failed"
        elif blocked:
            reasons = [
                f"Subtask {b['id']} ({b.get('kind', 'specialist')}): {sanitize_error(b.get('error'))}"
                for b in blocked
            ]
            detail = f" — {'; '.join(reasons)}" if reasons else ""
            answer = f"(subtasks blocked by unmet dependencies{detail})"
            status = "pending" if is_pending else "blocked"
        elif rejected:
            answer = "(rejected by human reviewer)"
            status = "rejected"
        elif is_pending:
            status = "pending"
            answer = "(no specialist output)"
        else:
            answer = "(no specialist output)"
            status = state.get("status", "completed")

        task_id = state.get("task_id")
        if task_id:
            try:
                from ..memory.short_term import ShortTermMemory

                short = ShortTermMemory(task_id)
                short.set("stage", "reduce")
                short.set("status", status)
                short.set("answer", answer)
                if blocked:
                    short.set("blocked", [b["id"] for b in blocked])
                if failed:
                    short.set("failed", [f["id"] for f in failed])
                if rejected:
                    short.set("rejected", [r["id"] for r in rejected])
            except Exception:
                pass

        quality = state.get("quality")
        if quality is None:
            quality = 1.0 if (done and not has_incomplete) else (0.5 if done else 0.0)

        confidence = state.get("confidence")
        if confidence is None:
            confidence = (sum(c.get("confidence", 0.0) for c in done) / len(done)) if done else 0.0

    return {
        "answer": answer,
        "status": status,
        "quality": float(quality),
        "confidence": float(confidence),
        # Authoritative final escalation signal: did *any* subtask need review?
        "escalated": bool(escalations) and not bool(state.get("resolution")),
        "escalation_reason": (
            escalations[-1]["reason"] if escalations else state.get("escalation_reason", "")
        ),
        "trace": [
            create_trace_event(
                node="supervisor",
                event="reduce",
                run_id=run_id,
                status=status,
                parts=len(done),
                failed=len(failed),
                blocked=len(blocked),
                rejected=len(rejected),
                escalations=len(escalations),
            )
        ],
    }

