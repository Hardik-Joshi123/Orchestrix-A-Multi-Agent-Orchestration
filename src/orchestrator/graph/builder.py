"""Assemble the LangGraph: supervisor -> specialists -> reviewer -> (escalate|done).

Falls back to a plain in-process executor when LangGraph isn't installed, so the
demo and tests run anywhere.
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
import uuid
from typing import Any

from ..hitl.escalation import escalate as do_escalate
from ..memory.long_term import LongTermMemory
from ..memory.short_term import ShortTermMemory
from ..observability.tracing import create_trace_event, span, trace_run
from ..persistence.store import AdmissionFencingError
from . import reviewer, specialists, supervisor
from .cancellation import CancellationToken, get_cancellation_registry
from .state import GraphState, Subtask

logger = logging.getLogger(__name__)

_SPECIALIST_FN = {
    "research": specialists.research,
    "analysis": specialists.analysis,
    "writing": specialists.writing,
    "code": specialists.code,
}

_ACTIVE_ENGINE_RUNS: set[str] = set()
_ENGINE_RUNS_LOCK = threading.Lock()


def check_dependencies(state: GraphState) -> GraphState:
    """Evaluate pending subtasks against upstream completion, failure, and blocking.

    Any pending subtask whose declared dependency has failed or is blocked cannot
    execute and is transitioned to 'blocked'.

    Any pending subtask with a missing dependency (not present in the plan) or
    part of an unresolvable cycle is also transitioned to 'blocked'.
    """
    plan = state.get("plan", [])
    if not plan:
        return {}

    plan_ids = {s.get("id") or f"s{i+1}" for i, s in enumerate(plan)}
    completed = state.get("completed", [])
    failed = state.get("failed", [])
    blocked = state.get("blocked", [])

    processed_ids = {c.get("id", "") for c in completed}
    done_ids = {c.get("id", "") for c in completed if c.get("status") == "done"}
    failed_ids = {c.get("id", "") for c in (completed + failed) if c.get("status") == "failed"}
    blocked_ids = {c.get("id", "") for c in (completed + blocked) if c.get("status") == "blocked"}

    current_unusable = set(failed_ids) | set(blocked_ids)
    newly_blocked: list[Subtask] = []
    newly_blocked_ids: set[str] = set()

    changed = True
    while changed:
        changed = False
        for i, s in enumerate(plan):
            sid = s.get("id") or f"s{i+1}"
            if sid in processed_ids or sid in newly_blocked_ids:
                continue
            deps = s.get("depends_on") or []
            if not deps:
                continue

            # Check 1: Missing dependency in plan
            missing = [d for d in deps if d not in plan_ids]
            if missing:
                reason = f"Dependency '{missing[0]}' not found in plan"
                blocked_subtask: Subtask = {
                    **s,
                    "id": sid,
                    "status": "blocked",
                    "result": "",
                    "confidence": 0.0,
                    "error": reason,
                    "tools_used": [],
                }
                newly_blocked.append(blocked_subtask)
                newly_blocked_ids.add(sid)
                current_unusable.add(sid)
                changed = True
                continue

            # Check 2: Upstream dependency failed or blocked
            unusable_dep = [d for d in deps if d in current_unusable]
            if unusable_dep:
                reason = f"Upstream dependency '{unusable_dep[0]}' failed or was blocked"
                blocked_subtask: Subtask = {
                    **s,
                    "id": sid,
                    "status": "blocked",
                    "result": "",
                    "confidence": 0.0,
                    "error": reason,
                    "tools_used": [],
                }
                newly_blocked.append(blocked_subtask)
                newly_blocked_ids.add(sid)
                current_unusable.add(sid)
                changed = True
                continue

    # Cycle / deadlock detection:
    remaining_pending = [
        s
        for i, s in enumerate(plan)
        if (
            (s.get("id") or f"s{i+1}") not in processed_ids
            and (s.get("id") or f"s{i+1}") not in newly_blocked_ids
        )
    ]
    if remaining_pending:
        can_any_start = any(
            all(d in done_ids for d in (s.get("depends_on") or [])) for s in remaining_pending
        )
        if not can_any_start:
            for i, s in enumerate(remaining_pending):
                sid = s.get("id") or f"s{i+1}"
                reason = f"Circular or unresolvable dependency in subtask '{sid}'"
                blocked_subtask: Subtask = {
                    **s,
                    "id": sid,
                    "status": "blocked",
                    "result": "",
                    "confidence": 0.0,
                    "error": reason,
                    "tools_used": [],
                }
                newly_blocked.append(blocked_subtask)
                newly_blocked_ids.add(sid)

    if not newly_blocked:
        return {}

    task_id = state.get("task_id")
    if task_id:
        try:
            from ..memory.short_term import ShortTermMemory

            stm = ShortTermMemory(task_id)
            for b in newly_blocked:
                stm.set(f"subtask:{b['id']}", b)
        except Exception:
            pass

    run_id = state.get("run_id") or state.get("task_id", "")
    trace_events = [
        create_trace_event(
            node="dispatch",
            event="blocked",
            run_id=run_id,
            subtask=b["id"],
            subtask_id=b["id"],
            reason=b.get("error", "dependency failed"),
        )
        for b in newly_blocked
    ]
    return {
        "completed": newly_blocked,
        "blocked": newly_blocked,
        "trace": trace_events,
    }


def _next_specialist(state: GraphState) -> str:
    """Route to the next ready subtask's specialist, or 'reduce' when done.

    A subtask is ready when all of its declared dependencies have completed
    successfully (status == 'done'). If no pending subtask is ready or all
    subtasks have been processed/blocked, routes to 'reduce'.
    """
    processed_ids = {c["id"] for c in state.get("completed", [])}
    done_ids = {c["id"] for c in state.get("completed", []) if c.get("status") == "done"}
    pending = [s for s in state.get("plan", []) if s["id"] not in processed_ids]
    if not pending:
        return "reduce"

    ready_subtask = None
    for s in pending:
        deps = s.get("depends_on") or []
        if all(d in done_ids for d in deps):
            ready_subtask = s
            break

    if ready_subtask is None:
        return "reduce"

    from .supervisor import normalize_specialist_kind

    kind = normalize_specialist_kind(ready_subtask.get("kind", "analysis"))
    if kind not in _SPECIALIST_FN:
        return "analysis"
    return kind


def _after_review(state: GraphState) -> str:
    return "escalate" if state.get("escalated") else "dispatch"


def _after_escalate(state: GraphState) -> str:
    """Route after escalation: take_over halts dispatch and reduces immediately."""
    return "reduce" if state.get("approval") == "take_over" else "dispatch"


def _dispatch(state: GraphState) -> GraphState:
    """Evaluate subtask dependencies and block any subtasks with failed/missing dependencies."""
    return check_dependencies(state)


def build_graph():
    """Build a compiled LangGraph StateGraph (when available).

    Topology: supervisor -> dispatch -> (specialist -> reviewer -> [escalate] ->
    dispatch)* -> reduce. The dispatch loop runs every planned subtask, so a
    multi-subtask plan exercises several specialists in one run.
    """
    from langgraph.graph import END, START, StateGraph

    g = StateGraph(GraphState)
    g.add_node("supervisor", supervisor.decompose)
    g.add_node("dispatch", _dispatch)
    for kind, fn in _SPECIALIST_FN.items():
        g.add_node(kind, fn)
    g.add_node("reviewer", reviewer.review)
    g.add_node("escalate", do_escalate)
    g.add_node("reduce", supervisor.reduce)

    g.add_edge(START, "supervisor")
    g.add_edge("supervisor", "dispatch")
    # Path map: each router return value -> destination node. Specialist kinds map
    # to their like-named nodes; "reduce" ends the dispatch loop.
    dispatch_routes = {kind: kind for kind in _SPECIALIST_FN}
    dispatch_routes["reduce"] = "reduce"
    g.add_conditional_edges("dispatch", _next_specialist, dispatch_routes)
    for kind in _SPECIALIST_FN:
        g.add_edge(kind, "reviewer")
    g.add_conditional_edges("reviewer", _after_review, {"escalate": "escalate", "dispatch": "dispatch"})
    g.add_conditional_edges("escalate", _after_escalate, {"reduce": "reduce", "dispatch": "dispatch"})
    g.add_edge("reduce", END)
    return g.compile()


# List fields that LangGraph would accumulate via operator.add reducers; the
# fallback executor mirrors that so both paths produce identical state.
_ACCUMULATING = ("trace", "completed", "failed", "blocked", "tools_used", "escalations")


def _merge(state: GraphState, update: GraphState) -> None:
    """Apply a node's output, accumulating list fields like LangGraph reducers."""
    for key, value in update.items():
        if key in _ACCUMULATING and isinstance(value, list):
            state[key] = state.get(key, []) + value
        else:
            state[key] = value


def _invoke_specialist(kind: str, state: GraphState, subtask: Subtask) -> GraphState:
    """Invoke specialist node respecting _SPECIALIST_FN mock patches or targeted execution."""
    from .supervisor import normalize_specialist_kind

    normalized = normalize_specialist_kind(kind)
    fn = _SPECIALIST_FN.get(normalized, _SPECIALIST_FN.get("analysis", specialists.analysis))
    if fn in (specialists.research, specialists.analysis, specialists.writing, specialists.code):
        return specialists.execute_subtask(state, subtask)
    return fn(state)


def _execute_subtask_with_review(
    subtask: Subtask,
    state_snapshot: GraphState,
    settings: Any,
    run_id: str,
    deadline: float | None = None,
    cancellation_token: CancellationToken | None = None,
) -> dict[str, Any]:
    """Execute a single specialist subtask with localized retries, review, and HITL handling."""

    accumulated_tools: list[str] = []
    accumulated_trace: list[dict[str, Any]] = []
    accumulated_escalations: list[dict[str, Any]] = []
    retry_count = 0

    with trace_run(run_id):
        try:
            # Check cooperative cancellation before starting specialist
            if cancellation_token and cancellation_token.is_cancelled:
                return {
                    "subtask": {
                        **subtask,
                        "status": "failed",
                        "result": "",
                        "confidence": 0.0,
                        "error": f"Execution cancelled: {cancellation_token.reason}",
                        "tools_used": [],
                    },
                    "quality": 0.0,
                    "confidence": 0.0,
                    "tools_used": [],
                    "trace": [
                        create_trace_event(
                            node="scheduler",
                            event="cancelled",
                            run_id=run_id,
                            subtask=subtask.get("id", ""),
                            subtask_id=subtask.get("id", ""),
                            reason=cancellation_token.reason,
                        )
                    ],
                    "escalated": False,
                    "approval": "",
                    "escalations": [],
                    "retries": 0,
                }

            # Check deadline before starting specialist
            if deadline is not None and time.monotonic() >= deadline:
                return {
                    "subtask": {
                        **subtask,
                        "status": "failed",
                        "result": "",
                        "confidence": 0.0,
                        "error": "Execution deadline exceeded before subtask dispatch",
                        "tools_used": [],
                    },
                    "quality": 0.0,
                    "confidence": 0.0,
                    "tools_used": [],
                    "trace": [
                        create_trace_event(
                            node="scheduler",
                            event="timed_out",
                            run_id=run_id,
                            subtask=subtask.get("id", ""),
                            subtask_id=subtask.get("id", ""),
                            reason="deadline_exceeded",
                        )
                    ],
                    "escalated": False,
                    "approval": "",
                    "escalations": [],
                    "retries": 0,
                }

            # 1. Specialist execution attempt
            kind = subtask.get("kind", "analysis")
            subtask_update = _invoke_specialist(kind, state_snapshot, subtask)
            curr_subtask = subtask_update["completed"][0]
            if subtask_update.get("tools_used"):
                accumulated_tools.extend(subtask_update["tools_used"])
            if subtask_update.get("trace"):
                accumulated_trace.extend(subtask_update["trace"])

            # 2. Review
            review_input: GraphState = {
                **state_snapshot,
                "completed": [curr_subtask],
                "tools_used": list(set(accumulated_tools)),
                "retries": retry_count,
            }
            review_res = reviewer.review(review_input)
            if review_res.get("trace"):
                accumulated_trace.extend(review_res["trace"])

            quality = review_res.get("quality", 0.0)
            confidence = review_res.get("confidence", 0.0)
            is_subtask_failed = curr_subtask.get("status") == "failed"
            is_low_score = (
                confidence < settings.confidence_threshold or quality < settings.quality_threshold
            )

            # 3. Bounded retry loop (if low score/failed, not escalated, retries remaining)
            while (
                not review_res.get("escalated")
                and (is_subtask_failed or is_low_score)
                and retry_count < settings.max_retries
            ):
                if cancellation_token and cancellation_token.is_cancelled:
                    break
                if deadline is not None and time.monotonic() >= deadline:
                    break

                retry_count += 1
                accumulated_trace.append(
                    create_trace_event(
                        node="workflow",
                        event="retry",
                        run_id=run_id,
                        kind=subtask.get("kind", "specialist"),
                        subtask=subtask["id"],
                        subtask_id=subtask["id"],
                        retry_count=retry_count,
                    )
                )

                # Re-execute specialist
                subtask_update = _invoke_specialist(kind, state_snapshot, subtask)
                curr_subtask = subtask_update["completed"][0]
                if subtask_update.get("tools_used"):
                    accumulated_tools.extend(subtask_update["tools_used"])
                if subtask_update.get("trace"):
                    accumulated_trace.extend(subtask_update["trace"])

                # Review again with updated retry count
                review_input = {
                    **state_snapshot,
                    "completed": [curr_subtask],
                    "tools_used": list(set(accumulated_tools)),
                    "retries": retry_count,
                }
                review_res = reviewer.review(review_input)
                if review_res.get("trace"):
                    accumulated_trace.extend(review_res["trace"])

                quality = review_res.get("quality", 0.0)
                confidence = review_res.get("confidence", 0.0)
                is_subtask_failed = curr_subtask.get("status") == "failed"
                is_low_score = (
                    confidence < settings.confidence_threshold or quality < settings.quality_threshold
                )

            # 4. Check if escalation is triggered (skip if execution was cancelled or deadline exceeded)
            is_active_control = not (
                (cancellation_token and cancellation_token.is_cancelled)
                or (deadline is not None and time.monotonic() >= deadline)
            )
            escalated = bool(review_res.get("escalated")) and is_active_control
            approval = ""
            if escalated:
                escalate_input: GraphState = {
                    **state_snapshot,
                    "completed": [curr_subtask],
                    "escalated": True,
                    "escalation_reason": review_res.get("escalation_reason", "low_confidence"),
                    "escalated_subtask_id": subtask["id"],
                    "quality": quality,
                    "confidence": confidence,
                }
                esc_res = do_escalate(escalate_input)
                approval = esc_res.get("approval", "")
                if esc_res.get("escalations"):
                    accumulated_escalations.extend(esc_res["escalations"])
                if esc_res.get("trace"):
                    accumulated_trace.extend(esc_res["trace"])

            return {
                "subtask": curr_subtask,
                "quality": quality,
                "confidence": confidence,
                "tools_used": list(set(accumulated_tools)),
                "trace": accumulated_trace,
                "escalated": escalated,
                "escalation_reason": review_res.get("escalation_reason", ""),
                "approval": approval,
                "escalations": accumulated_escalations,
                "retries": retry_count,
            }

        except Exception as exc:
            logger.exception("Unexpected exception executing subtask %s: %s", subtask.get("id"), exc)
            failed_subtask: Subtask = {
                **subtask,
                "status": "failed",
                "result": "",
                "confidence": 0.0,
                "error": f"Execution failed: {type(exc).__name__}: {exc}",
                "tools_used": list(set(accumulated_tools)),
            }
            fail_event = create_trace_event(
                node="scheduler",
                event="failure",
                run_id=run_id,
                subtask=subtask.get("id", ""),
                subtask_id=subtask.get("id", ""),
                error=str(exc),
            )
            accumulated_trace.append(fail_event)
            return {
                "subtask": failed_subtask,
                "quality": 0.0,
                "confidence": 0.0,
                "tools_used": list(set(accumulated_tools)),
                "trace": accumulated_trace,
                "escalated": False,
                "approval": "",
                "escalations": [],
                "retries": retry_count,
            }


def _run_fallback(
    state: GraphState,
    max_concurrency: int | None = None,
    store: Any | None = None,
    timeout: float | None = None,
    cancellation_token: CancellationToken | None = None,
) -> GraphState:
    """Mirror the graph topology with bounded concurrency, cooperative cancellation, and execution deadlines.

    Executes independent ready subtasks concurrently up to max_concurrency,
    respecting subtask dependencies, localized retries, HITL safety, and execution budgets.
    """
    from ..config import get_settings

    settings = get_settings()
    if max_concurrency is not None:
        concurrency_limit = max(1, int(max_concurrency))
    else:
        concurrency_limit = settings.effective_concurrency

    if store is None and settings.checkpoint_enabled:
        try:
            from ..persistence.store import CheckpointStore

            store = CheckpointStore()
        except Exception as exc:
            logger.warning("Failed to initialize CheckpointStore: %s", exc)
            store = None

    run_id = state.get("run_id") or state.get("task_id", "")
    start_time = time.monotonic()
    if cancellation_token and cancellation_token.deadline is not None:
        deadline = cancellation_token.deadline
    else:
        deadline = (start_time + timeout) if (timeout is not None and timeout > 0) else None

    # Pre-execution check: cooperative cancellation before dispatch
    if cancellation_token and cancellation_token.is_cancelled:
        state["status"] = "cancelled"
        reason = cancellation_token.reason
        state["cancellation_reason"] = reason
        state.setdefault("trace", []).append(
            create_trace_event(
                node="workflow",
                event="cancelled",
                run_id=run_id,
                reason=reason,
            )
        )
        _merge(state, supervisor.reduce(state))
        if store and run_id:
            try:
                store.save_checkpoint(run_id, state, transition="cancelled")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during cancelled checkpoint: %s", fence_err)
                raise
            except Exception as exc:
                logger.warning("Failed to save cancelled checkpoint: %s", exc)
        return state

    # Pre-execution check: execution deadline already expired
    is_expired = (deadline is not None and time.monotonic() >= deadline) or (
        cancellation_token is not None and cancellation_token.is_timed_out
    )
    if is_expired:
        state["status"] = "timed_out"
        state.setdefault("trace", []).append(
            create_trace_event(
                node="workflow",
                event="timed_out",
                run_id=run_id,
                timeout_seconds=timeout,
            )
        )
        _merge(state, supervisor.reduce(state))
        if store and run_id:
            try:
                store.save_checkpoint(run_id, state, transition="timed_out")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during timed_out checkpoint: %s", fence_err)
                raise
            except Exception as exc:
                logger.warning("Failed to save timed_out checkpoint: %s", exc)
        return state

    # When active execution begins and is not pre-cancelled or pre-expired, ensure status is active
    if state.get("status") in ("cancelled", "timed_out", "failed", "partial", "blocked", "interrupted"):
        state["status"] = "running"
        state.pop("cancellation_reason", None)
        state.pop("interrupted_reason", None)

    # Initial decomposition and dependency check
    if "plan" not in state or state["plan"] is None:
        _merge(state, supervisor.decompose(state))
    elif state.get("plan"):
        from .supervisor import normalize_specialist_kind

        normalized_plan = []
        for i, item in enumerate(state["plan"]):
            p = dict(item)
            if not p.get("id"):
                p["id"] = f"s{i+1}"
            if "status" not in p:
                p["status"] = "pending"
            if "kind" in p:
                p["kind"] = normalize_specialist_kind(p["kind"])
            normalized_plan.append(p)
        state["plan"] = normalized_plan

    if store and run_id:
        try:
            store.save_checkpoint(run_id, state, transition="plan")
        except AdmissionFencingError as fence_err:
            logger.error("Admission fencing error during plan checkpoint: %s", fence_err)
            if cancellation_token and not cancellation_token.is_cancelled:
                cancellation_token.request_cancellation(reason=f"Fencing conflict: {fence_err}")
            raise
        except Exception as exc:
            logger.warning("Failed to save plan checkpoint: %s", exc)

    dep_init = _dispatch(state)
    if dep_init:
        _merge(state, dep_init)
        if store and run_id and dep_init.get("blocked"):
            try:
                store.save_checkpoint(run_id, state, transition="blocked")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during blocked checkpoint: %s", fence_err)
                if cancellation_token and not cancellation_token.is_cancelled:
                    cancellation_token.request_cancellation(reason=f"Fencing conflict: {fence_err}")
                raise
            except Exception as exc:
                logger.warning("Failed to save blocked checkpoint: %s", exc)

    plan = state.get("plan", [])
    if not plan:
        _merge(state, supervisor.reduce(state))
        if store and run_id:
            try:
                store.save_checkpoint(run_id, state, transition="reduce")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during reduce checkpoint: %s", fence_err)
                if cancellation_token and not cancellation_token.is_cancelled:
                    cancellation_token.request_cancellation(reason=f"Fencing conflict: {fence_err}")
                raise
            except Exception as exc:
                logger.warning("Failed to save reduce checkpoint: %s", exc)
        return state

    plan_order = {s["id"]: i for i, s in enumerate(plan)}

    in_flight: dict[concurrent.futures.Future, Subtask] = {}
    emitted_ready: set[str] = set()
    emitted_delayed: set[str] = set()
    is_cancelled = False
    is_timed_out = False

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_limit) as executor:
        try:
            max_rounds = len(plan) * 2 + 10
            round_count = 0

            while round_count < max_rounds:
                # Cooperative cancellation check
                if cancellation_token and cancellation_token.is_cancelled:
                    if not is_timed_out:
                        is_cancelled = True
                        for fut in list(in_flight.keys()):
                            fut.cancel()
                        break

                # Execution deadline check
                if deadline is not None and time.monotonic() >= deadline:
                    if not is_cancelled:
                        is_timed_out = True
                        for fut in list(in_flight.keys()):
                            fut.cancel()
                        break

                # 1. Evaluate dependencies to block any subtasks with failed/missing dependencies
                dep_update = _dispatch(state)
                if dep_update:
                    _merge(state, dep_update)
                    if store and run_id and dep_update.get("blocked"):
                        try:
                            store.save_checkpoint(run_id, state, transition="blocked")
                        except Exception as exc:
                            logger.warning("Failed to save blocked checkpoint: %s", exc)

                # 2. If HITL take_over halted execution, cancel non-started and drain
                if state.get("approval") == "take_over":
                    for fut in list(in_flight.keys()):
                        fut.cancel()
                    break

                processed_ids = {c["id"] for c in state.get("completed", [])}
                in_flight_ids = {s["id"] for s in in_flight.values()}
                done_ids = {c["id"] for c in state.get("completed", []) if c.get("status") == "done"}

                # Check if all planned subtasks are processed and nothing is in-flight
                if all(s["id"] in processed_ids for s in plan) and not in_flight:
                    break

                # 3. Identify ready subtasks
                ready_subtasks: list[Subtask] = []
                for s in plan:
                    sid = s["id"]
                    if sid in processed_ids or sid in in_flight_ids:
                        continue
                    deps = s.get("depends_on") or []
                    if all(d in done_ids for d in deps):
                        ready_subtasks.append(s)

                # Emit ready lifecycle events
                for s in ready_subtasks:
                    if s["id"] not in emitted_ready:
                        emitted_ready.add(s["id"])
                        state.setdefault("trace", []).append(
                            create_trace_event(
                                node="scheduler",
                                event="ready",
                                run_id=run_id,
                                subtask=s["id"],
                                subtask_id=s["id"],
                            )
                        )

                # 4. Check available slots
                available_slots = max(0, concurrency_limit - len(in_flight))
                to_launch = ready_subtasks[:available_slots]
                delayed = ready_subtasks[available_slots:]

                # Emit delayed lifecycle events
                for s in delayed:
                    if s["id"] not in emitted_delayed:
                        emitted_delayed.add(s["id"])
                        state.setdefault("trace", []).append(
                            create_trace_event(
                                node="scheduler",
                                event="delayed",
                                run_id=run_id,
                                subtask=s["id"],
                                subtask_id=s["id"],
                                reason="concurrency_limit",
                                active_count=len(in_flight),
                                concurrency_limit=concurrency_limit,
                            )
                        )

                # 5. Launch ready subtasks up to available slots
                for s in to_launch:
                    if s["id"] in emitted_delayed:
                        emitted_delayed.discard(s["id"])

                    state_snapshot = dict(state)
                    state_snapshot["completed"] = list(state.get("completed", []))
                    state_snapshot["_store"] = store
                    state_snapshot["_cancellation_token"] = cancellation_token

                    fut = executor.submit(
                        _execute_subtask_with_review,
                        s,
                        state_snapshot,
                        settings,
                        run_id,
                        deadline,
                        cancellation_token,
                    )
                    in_flight[fut] = s

                # 6. If no active executions:
                if not in_flight:
                    _merge(state, _dispatch(state))
                    break

                # 7. Wait for at least one future to complete, bounded by remaining deadline or 0.2s poll
                remaining = (deadline - time.monotonic()) if deadline is not None else None
                if remaining is not None and remaining <= 0:
                    if not is_cancelled:
                        is_timed_out = True
                        for fut in list(in_flight.keys()):
                            fut.cancel()
                        break

                wait_timeout = min(0.2, max(0.01, remaining)) if remaining is not None else 0.2
                done_futures, _ = concurrent.futures.wait(
                    in_flight.keys(),
                    timeout=wait_timeout,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )

                if not done_futures:
                    # Wait slice timeout without finished future; re-check cancellation and deadline
                    continue

                round_count += 1

                for fut in done_futures:
                    subtask = in_flight.pop(fut)
                    try:
                        res = fut.result()
                    except Exception as fut_err:
                        logger.exception("Worker future failed for %s: %s", subtask.get("id"), fut_err)
                        failed_subtask: Subtask = {
                            **subtask,
                            "status": "failed",
                            "result": "",
                            "confidence": 0.0,
                            "error": f"Worker failure: {type(fut_err).__name__}: {fut_err}",
                            "tools_used": [],
                        }
                        res = {
                            "subtask": failed_subtask,
                            "tools_used": [],
                            "trace": [
                                create_trace_event(
                                    node="scheduler",
                                    event="failure",
                                    run_id=run_id,
                                    subtask=subtask["id"],
                                    subtask_id=subtask["id"],
                                    error=str(fut_err),
                                )
                            ],
                            "escalated": False,
                            "retries": 0,
                        }

                    # Merge result into main thread state
                    st = res["subtask"]
                    state.setdefault("completed", []).append(st)
                    if st.get("status") == "failed":
                        state.setdefault("failed", []).append(st)
                    if res.get("tools_used"):
                        state.setdefault("tools_used", []).extend(res["tools_used"])
                    if res.get("trace"):
                        state.setdefault("trace", []).extend(res["trace"])
                    if res.get("escalations"):
                        state.setdefault("escalations", []).extend(res["escalations"])
                    if res.get("escalated"):
                        state["escalated"] = True
                        if res.get("escalation_reason"):
                            state["escalation_reason"] = res["escalation_reason"]
                        if res.get("approval"):
                            state["approval"] = res["approval"]
                    state["retries"] = state.get("retries", 0) + res.get("retries", 0)

                    if store and run_id:
                        transition = (
                            "subtask_failed" if st.get("status") == "failed" else "subtask_complete"
                        )
                        try:
                            store.save_checkpoint(run_id, state, transition=transition)
                        except AdmissionFencingError as fence_err:
                            logger.error("Admission fencing error during subtask checkpoint: %s", fence_err)
                            if cancellation_token and not cancellation_token.is_cancelled:
                                cancellation_token.request_cancellation(
                                    reason=f"Fencing conflict: {fence_err}"
                                )
                            raise
                        except Exception as exc:
                            logger.warning("Failed to save subtask checkpoint: %s", exc)
                        if res.get("escalated"):
                            try:
                                store.save_checkpoint(run_id, state, transition="escalate")
                            except AdmissionFencingError as fence_err:
                                logger.error(
                                    "Admission fencing error during escalate checkpoint: %s",
                                    fence_err,
                                )
                                if cancellation_token and not cancellation_token.is_cancelled:
                                    cancellation_token.request_cancellation(
                                        reason=f"Fencing conflict: {fence_err}"
                                    )
                                raise
                            except Exception as exc:
                                logger.warning("Failed to save escalate checkpoint: %s", exc)

        finally:
            # Safely handle in-flight workers: unstarted futures are cancelled.
            # When cancelled or timed out, do not block the thread indefinitely.
            wait_for_workers = not (is_cancelled or is_timed_out)
            executor.shutdown(wait=wait_for_workers, cancel_futures=True)

    # Post-execution terminal status assignment
    if is_cancelled:
        state["status"] = "cancelled"
        reason = cancellation_token.reason if cancellation_token else "User requested cancellation"
        state["cancellation_reason"] = reason
        state.setdefault("trace", []).append(
            create_trace_event(
                node="workflow",
                event="cancelled",
                run_id=run_id,
                reason=reason,
            )
        )
    elif is_timed_out:
        state["status"] = "timed_out"
        state.setdefault("trace", []).append(
            create_trace_event(
                node="workflow",
                event="timed_out",
                run_id=run_id,
                timeout_seconds=timeout,
                elapsed_seconds=round(time.monotonic() - start_time, 3),
            )
        )

    # Sort completed by plan order to ensure deterministic result assembly
    if state.get("completed"):
        state["completed"] = sorted(
            state["completed"],
            key=lambda c: plan_order.get(c.get("id"), 9999),
        )

    _merge(state, _dispatch(state))
    _merge(state, supervisor.reduce(state))
    if store and run_id:
        transition = "cancelled" if is_cancelled else ("timed_out" if is_timed_out else "reduce")
        try:
            store.save_checkpoint(run_id, state, transition=transition)
        except AdmissionFencingError as fence_err:
            logger.error("Admission fencing error during final checkpoint: %s", fence_err)
            raise
        except Exception as exc:
            logger.warning("Failed to save %s checkpoint: %s", transition, exc)
    return state


def run_task(
    task: str,
    plan: list[dict | Subtask] | None = None,
    run_id: str | None = None,
    max_concurrency: int | None = None,
    store: Any | None = None,
    timeout: float | None = None,
    cancellation_token: CancellationToken | None = None,
) -> GraphState:
    """Entry point: run one task through the orchestration graph with deadline and cancellation."""
    from ..config import get_settings

    settings = get_settings()
    task = (task or "").strip()
    if not task:
        raise ValueError("task must not be empty")
    limit = settings.max_task_chars
    if len(task) > limit:
        raise ValueError(f"task exceeds maximum length of {limit} characters")

    # Validate execution deadline / timeout
    if timeout is not None:
        if timeout <= 0:
            raise ValueError("Workflow timeout must be greater than 0 seconds.")
    else:
        timeout = settings.workflow_timeout_seconds

    task_id = (run_id or "").strip() or uuid.uuid4().hex[:12]

    with _ENGINE_RUNS_LOCK:
        if task_id in _ACTIVE_ENGINE_RUNS:
            raise ValueError(f"Run '{task_id}' is already executing concurrently in-process.")
        _ACTIVE_ENGINE_RUNS.add(task_id)

    # Register in cancellation registry with fresh token
    reg = get_cancellation_registry()
    if cancellation_token is None:
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        cancellation_token = reg.create_fresh(task_id, deadline=deadline, timeout_seconds=timeout)
    else:
        reg.register(task_id, cancellation_token)

    try:
        short = ShortTermMemory(task_id)
        short.set("task", task)
        short.set("stage", "init")

        # Context retrieval before execution
        ltm = LongTermMemory()
        recalled_context = ltm.recall(task, k=3)
        if recalled_context:
            short.set("recalled_context", recalled_context)

        start_event = create_trace_event(
            node="workflow",
            event="start",
            run_id=task_id,
            task=task,
        )

        state: GraphState = {
            "task_id": task_id,
            "run_id": task_id,
            "task": task,
            "context": recalled_context,
            "retries": 0,
            "completed": [],
            "failed": [],
            "blocked": [],
            "tools_used": [],
            "escalations": [],
            "trace": [start_event],
        }
        if timeout is not None:
            state["timeout_seconds"] = timeout

        if plan is not None:
            from .supervisor import normalize_specialist_kind

            normalized_plan = []
            for i, item in enumerate(plan):
                p = dict(item)
                if not p.get("id"):
                    p["id"] = f"s{i+1}"
                if "status" not in p:
                    p["status"] = "pending"
                if "kind" in p:
                    p["kind"] = normalize_specialist_kind(p["kind"])
                normalized_plan.append(p)
            state["plan"] = normalized_plan

        if store is None and settings.checkpoint_enabled:
            try:
                from ..persistence.store import CheckpointStore

                store = CheckpointStore()
            except Exception as exc:
                logger.warning("Failed to initialize CheckpointStore: %s", exc)
                store = None

        if store and getattr(store, "enabled", False):
            try:
                store.save_checkpoint(task_id, state, transition="init")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during init checkpoint: %s", fence_err)
                raise
            except Exception as exc:
                logger.warning("Failed to save initial checkpoint: %s", exc)

        with trace_run(task_id):
            with span("workflow.run", run_id=task_id, task=task):
                try:
                    graph = build_graph()
                except ImportError as exc:
                    logger.info("LangGraph unavailable (%s); using in-process fallback executor", exc)
                    result = _run_fallback(
                        state,
                        max_concurrency=max_concurrency,
                        store=store,
                        timeout=timeout,
                        cancellation_token=cancellation_token,
                    )
                else:
                    if timeout is not None or (cancellation_token and cancellation_token.is_cancelled):
                        result = _run_fallback(
                            state,
                            max_concurrency=max_concurrency,
                            store=store,
                            timeout=timeout,
                            cancellation_token=cancellation_token,
                        )
                    else:
                        result = graph.invoke(state)
                        if store:
                            try:
                                store.save_checkpoint(task_id, result, transition="reduce")
                            except AdmissionFencingError as fence_err:
                                logger.error(
                                    "Admission fencing error during reduce checkpoint: %s",
                                    fence_err,
                                )
                                raise
                            except Exception as exc:
                                logger.warning("Failed to save reduce checkpoint: %s", exc)

        result["task_id"] = task_id
        result["run_id"] = task_id

        # Attach aggregated token usage and cost tracking
        try:
            from ..observability.usage import get_run_usage

            usage_summary = get_run_usage(task_id)
            if usage_summary and (
                usage_summary.successful_requests > 0
                or usage_summary.failed_requests > 0
                or usage_summary.total_tokens > 0
            ):
                result["usage"] = usage_summary.to_dict()
        except Exception as exc:
            logger.debug("Failed to attach run usage summary: %s", exc)

        # Update working memory with terminal status
        short.set("status", result.get("status"))
        short.set("answer", result.get("answer"))

        # Persist what worked into long-term semantic memory
        # (skip for unresolved/pending/rejected/failed/blocked/cancelled/timed_out tasks).
        if not result.get("escalated") and result.get("status") not in (
            "pending",
            "rejected",
            "failed",
            "blocked",
            "cancelled",
            "timed_out",
        ):
            ltm.remember(
                task=task,
                outcome=result.get("answer", ""),
                metadata={
                    "task_id": task_id,
                    "run_id": task_id,
                    "quality": result.get("quality", 0.0),
                    "status": result.get("status", ""),
                },
            )
        return result
    finally:
        reg.remove(task_id)
        with _ENGINE_RUNS_LOCK:
            _ACTIVE_ENGINE_RUNS.discard(task_id)


def resume_task(
    run_id: str,
    max_concurrency: int | None = None,
    store: Any | None = None,
    timeout: float | None = None,
    cancellation_token: CancellationToken | None = None,
) -> GraphState:
    """Resume execution of an interrupted or incomplete task from its latest checkpoint.

    Bypasses already completed subtasks (status == 'done'), re-evaluates dependencies,
    and resumes scheduling with configured concurrency, cancellation, and deadline limits.

    Note on tool execution:
        External tool execution cannot guarantee exactly-once side effects across
        process restarts if an external side-effecting call was interrupted.
    """
    clean_run_id = (run_id or "").strip()
    if not clean_run_id:
        raise ValueError("run_id must not be empty")

    with _ENGINE_RUNS_LOCK:
        if clean_run_id in _ACTIVE_ENGINE_RUNS:
            raise ValueError(f"Run '{clean_run_id}' is already executing concurrently in-process.")
        _ACTIVE_ENGINE_RUNS.add(clean_run_id)

    from ..config import get_settings

    settings = get_settings()

    if timeout is not None:
        if timeout <= 0:
            raise ValueError("Workflow timeout must be greater than 0 seconds.")
    else:
        timeout = settings.workflow_timeout_seconds

    reg = get_cancellation_registry()
    if cancellation_token is None:
        deadline = (time.monotonic() + timeout) if timeout is not None else None
        cancellation_token = reg.create_fresh(clean_run_id, deadline=deadline, timeout_seconds=timeout)
    else:
        reg.register(clean_run_id, cancellation_token)

    try:
        from ..persistence.store import CheckpointStore

        if store is None:
            store = CheckpointStore()

        record = store.load_latest_checkpoint(clean_run_id)
        state: GraphState = dict(record.state)

        # Ensure required state fields are present
        state.setdefault("run_id", clean_run_id)
        state.setdefault("task_id", clean_run_id)
        state.setdefault("completed", [])
        state.setdefault("failed", [])
        state.setdefault("blocked", [])
        state.setdefault("tools_used", [])
        state.setdefault("escalations", [])
        state.setdefault("trace", [])
        state.setdefault("retries", 0)
        if timeout is not None:
            state["timeout_seconds"] = timeout

        plan = state.get("plan", [])
        if not plan:
            raise ValueError(
                f"Run '{clean_run_id}' cannot be safely resumed: checkpoint contains no execution plan."
            )

        done_ids = {c["id"] for c in state.get("completed", []) if c.get("status") == "done"}
        all_done = bool(plan) and all(s["id"] in done_ids for s in plan)

        # If the workflow is already terminal and all planned subtasks are done, return state
        if all_done and state.get("status") in ("completed", "approved", "edited"):
            logger.info("Task %s is already completed. Returning cached state.", clean_run_id)
            return state

        # Append workflow.resume trace event
        resume_event = create_trace_event(
            node="workflow",
            event="resume",
            run_id=clean_run_id,
            completed_count=len(done_ids),
            total_subtasks=len(plan),
        )
        state["trace"].append(resume_event)

        # Reset any previously blocked subtasks so dependency re-evaluation can check
        # whether their upstream requirements have now been satisfied
        state["completed"] = [c for c in state["completed"] if c.get("status") != "blocked"]
        state["blocked"] = []

        # Reset terminal status from previous pause/cancellation/timeout so resumed execution can progress
        if state.get("status") in ("cancelled", "timed_out", "failed", "partial", "blocked", "interrupted"):
            state["status"] = "running"
            state.pop("cancellation_reason", None)
            state.pop("interrupted_reason", None)

        # Checkpoint the resume boundary
        if store.enabled:
            try:
                store.save_checkpoint(clean_run_id, state, transition="resume")
            except AdmissionFencingError as fence_err:
                logger.error("Admission fencing error during resume checkpoint: %s", fence_err)
                raise
            except Exception as exc:
                logger.warning("Failed to save resume checkpoint: %s", exc)

        with trace_run(clean_run_id):
            with span("workflow.resume", run_id=clean_run_id):
                result = _run_fallback(
                    state,
                    max_concurrency=max_concurrency,
                    store=store,
                    timeout=timeout,
                    cancellation_token=cancellation_token,
                )

        result["task_id"] = clean_run_id
        result["run_id"] = clean_run_id

        # Attach aggregated token usage and cost tracking
        try:
            from ..observability.usage import get_run_usage

            usage_summary = get_run_usage(clean_run_id)
            if usage_summary and (
                usage_summary.successful_requests > 0
                or usage_summary.failed_requests > 0
                or usage_summary.total_tokens > 0
            ):
                result["usage"] = usage_summary.to_dict()
        except Exception as exc:
            logger.debug("Failed to attach run usage summary: %s", exc)

        return result
    finally:
        reg.remove(clean_run_id)
        with _ENGINE_RUNS_LOCK:
            _ACTIVE_ENGINE_RUNS.discard(clean_run_id)
