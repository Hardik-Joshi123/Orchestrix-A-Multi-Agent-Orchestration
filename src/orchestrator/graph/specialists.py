"""Specialist nodes: research, analysis, writing, code.

Each specialist is tool-using: it pulls relevant long-term memories, may call a
registered tool, and produces a result with a self-reported confidence.
"""

from __future__ import annotations

import logging

from ..llm import LLMClient
from ..memory.long_term import LongTermMemory
from ..memory.short_term import ShortTermMemory
from ..observability.tracing import create_trace_event, span
from ..tools.registry import REGISTRY, ToolExecutionContext
from .state import GraphState, Subtask

logger = logging.getLogger(__name__)

_SYSTEM = {
    "research": "You are a research specialist. Gather and cite relevant facts.",
    "analysis": "You are an analysis specialist. Evaluate options and recommend.",
    "writing": "You are a writing specialist. Produce clear, structured prose.",
    "code": "You are a coding specialist. Write correct, tested code.",
}


def _run_specialist(
    state: GraphState,
    kind: str,
    target_subtask: Subtask | None = None,
) -> GraphState:
    if target_subtask is not None:
        subtask = target_subtask
    else:
        plan = state.get("plan", [])
        # A subtask is "pending" until its id shows up in the accumulated `completed`
        processed_ids = {c["id"] for c in state.get("completed", [])}
        done_ids = {c["id"] for c in state.get("completed", []) if c.get("status") == "done"}
        subtask = next(
            (
                s
                for s in plan
                if s["kind"] == kind
                and s["id"] not in processed_ids
                and all(d in done_ids for d in (s.get("depends_on") or []))
            ),
            None,
        )
    if subtask is None:
        return {}

    task_id = state.get("task_id")
    run_id = state.get("run_id") or task_id or ""
    store = state.get("_store")
    cancellation_token = state.get("_cancellation_token") or state.get("cancellation_token")
    fence_token = getattr(store, "fence_token", 1) if store else state.get("fence_token", 1)
    owner_id = getattr(store, "owner_id", None) if store else state.get("owner_id")

    tool_context = (
        ToolExecutionContext(
            run_id=run_id,
            subtask_id=subtask["id"],
            fence_token=fence_token,
            cancellation_token=cancellation_token,
            store=store,
            owner_id=owner_id,
            agent_id=kind,
        )
        if run_id
        else ToolExecutionContext(
            run_id="",
            subtask_id=subtask["id"],
            agent_id=kind,
        )
    )

    tools_used: list[str] = []
    error_msg = ""
    failed = False
    out = None

    with span(f"specialist.{kind}", run_id=run_id, subtask=subtask["id"], subtask_id=subtask["id"]):
        try:
            memories: list[str] = []
            try:
                memories = LongTermMemory().recall(subtask["instruction"], k=3)
            except Exception as mem_err:
                logger.warning("Memory recall failed for subtask %s: %s", subtask["id"], mem_err)
            if not memories and state.get("context"):
                memories = state.get("context", [])
            memo_hint = f" (recalled {len(memories)} memories)" if memories else ""

            # Specialists may use registered tools; demo invokes a safe one if present.
            tool_note = ""
            if kind == "research" and "web_search" in REGISTRY:
                tools_used.append("web_search")
                try:
                    tool_note = REGISTRY.call(
                        "web_search", context=tool_context, query=subtask["instruction"]
                    )
                except Exception as tool_err:
                    logger.warning("Tool call web_search failed for subtask %s: %s", subtask["id"], tool_err)
                    tool_note = f"[Tool Error: web_search failed: {tool_err}]"
            elif kind in ("analysis", "code") and "calculator" in REGISTRY:
                import re

                math_match = re.search(
                    r"calc(?:ulate)?:\s*([0-9\s\+\-\*\/\%\(\)]+)",
                    subtask["instruction"],
                    re.IGNORECASE,
                )
                if math_match:
                    expr = math_match.group(1).strip()
                    tools_used.append("calculator")
                    try:
                        calc_res = REGISTRY.call(
                            "calculator", context=tool_context, expression=expr
                        )
                        tool_note = f"[Calculator output: {calc_res}]"
                    except Exception as tool_err:
                        logger.warning(
                            "Tool call calculator failed for subtask %s: %s", subtask["id"], tool_err
                        )
                        tool_note = f"[Tool Error: calculator failed: {tool_err}]"

            prompt_parts = []
            if memories:
                memo_bullets = "\n".join(f"- {m}" for m in memories)
                prompt_parts.append(f"Relevant context from past tasks:\n{memo_bullets}")

            dep_results = []
            for dep_id in subtask.get("depends_on") or []:
                dep = next(
                    (
                        c
                        for c in state.get("completed", [])
                        if c["id"] == dep_id and c.get("status") == "done"
                    ),
                    None,
                )
                if dep and dep.get("result"):
                    dep_results.append(
                        f"Prerequisite {dep_id} ({dep.get('kind', 'specialist')} output):\n{dep['result']}"
                    )
            if dep_results:
                prompt_parts.append("\n\n".join(dep_results))

            prompt_parts.append(subtask["instruction"])
            if tool_note:
                prompt_parts.append(tool_note)

            llm = LLMClient()
            out = llm.complete(
                system=_SYSTEM[kind],
                prompt="\n\n".join(prompt_parts).strip(),
            )

            # Validate LLM output structure and content
            if out is None or not getattr(out, "text", None) or not str(out.text).strip():
                failed = True
                error_msg = "Specialist produced empty or invalid output"
                logger.warning(
                    "Specialist %s on subtask %s produced empty/invalid output", kind, subtask["id"]
                )
        except Exception as exc:
            failed = True
            error_msg = f"Specialist execution failed: {type(exc).__name__}: {exc}"
            logger.warning("Specialist %s failed on subtask %s: %s", kind, subtask["id"], error_msg)

        if failed:
            failed_subtask: Subtask = {
                **subtask,
                "kind": kind,
                "status": "failed",
                "result": "",
                "confidence": 0.0,
                "error": error_msg,
                "tools_used": tools_used,
            }
            start_event = create_trace_event(
                node=f"specialist.{kind}",
                event="start",
                run_id=run_id,
                subtask=subtask["id"],
                subtask_id=subtask["id"],
                kind=kind,
            )
            fail_event = create_trace_event(
                node=f"specialist.{kind}",
                event="failure",
                run_id=run_id,
                subtask=subtask["id"],
                subtask_id=subtask["id"],
                kind=kind,
                error=error_msg,
                tools_used=tools_used,
            )
            if task_id:
                try:
                    ShortTermMemory(task_id).set(f"subtask:{subtask['id']}", failed_subtask)
                except Exception:
                    pass
            return {
                "completed": [failed_subtask],
                "failed": [failed_subtask],
                "confidence": 0.0,
                "quality": 0.0,
                "tools_used": tools_used,
                "trace": [start_event, fail_event],
            }

        done: Subtask = {
            **subtask,
            "kind": kind,
            "status": "done",
            "result": f"{out.text}{memo_hint}",
            "confidence": 0.75,
            "tools_used": tools_used,
        }
        if task_id:
            try:
                ShortTermMemory(task_id).set(f"subtask:{subtask['id']}", done)
            except Exception:
                pass

        start_event = create_trace_event(
            node=f"specialist.{kind}",
            event="start",
            run_id=run_id,
            subtask=subtask["id"],
            subtask_id=subtask["id"],
            kind=kind,
        )
        done_event = create_trace_event(
            node=f"specialist.{kind}",
            event="complete",
            run_id=run_id,
            subtask=subtask["id"],
            subtask_id=subtask["id"],
            kind=kind,
            provider=out.provider if out else "mock",
            tools_used=tools_used,
        )
    return {
        "completed": [done],
        "confidence": done["confidence"],
        "tools_used": tools_used,
        "trace": [start_event, done_event],
    }


def execute_subtask(state: GraphState, subtask: Subtask) -> GraphState:
    """Execute an explicitly assigned specialist subtask with normalized routing."""
    from .supervisor import normalize_specialist_kind

    kind = normalize_specialist_kind(subtask.get("kind", "analysis"))
    return _run_specialist(state, kind, target_subtask=subtask)


def research(state: GraphState) -> GraphState:
    return _run_specialist(state, "research")


def analysis(state: GraphState) -> GraphState:
    return _run_specialist(state, "analysis")


def writing(state: GraphState) -> GraphState:
    return _run_specialist(state, "writing")


def code(state: GraphState) -> GraphState:
    return _run_specialist(state, "code")
