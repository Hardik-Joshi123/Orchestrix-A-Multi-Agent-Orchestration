"""Review queue backed by Redis lists. Degrades to an in-process list.

Resolving an item records a real, attributable decision and (for ``edit`` /
``take_over``) the human's revised text, then writes a tamper-evident audit
record via the hash chain in :mod:`orchestrator.hitl.audit`.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field

from ..config import get_settings
from ..observability.tracing import create_trace_event
from .audit import AuditChain

_QUEUE_KEY = "hitl:review_queue"
_LOCAL: list[str] = []

# The four decisions a reviewer can record.
VALID_DECISIONS = ("approve", "reject", "edit", "take_over")
# Decisions that require the human to supply replacement text.
_TEXT_REQUIRED = ("edit", "take_over")


@dataclass
class ReviewItem:
    task_id: str
    task: str
    reason: str
    level: str
    proposed_action: str
    confidence: float
    quality: float
    memories: list[str] = field(default_factory=list)
    item_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    subtask_id: str = ""

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(raw: str) -> ReviewItem:
        data = json.loads(raw)
        return ReviewItem(
            task_id=data["task_id"],
            task=data["task"],
            reason=data["reason"],
            level=data["level"],
            proposed_action=data["proposed_action"],
            confidence=data["confidence"],
            quality=data["quality"],
            memories=data.get("memories", []),
            item_id=data.get("item_id") or uuid.uuid4().hex[:12],
            subtask_id=data.get("subtask_id", ""),
        )


@dataclass
class Resolution:
    """The recorded outcome of a human review decision.

    ``final_result`` is what the workflow should treat as the authoritative
    answer for the task:
      - approve    -> the agent's proposed action stands
      - reject     -> None (the proposed action is discarded)
      - edit       -> the reviewer's revised text
      - take_over  -> the reviewer's own answer becomes the result
    """

    task_id: str
    decision: str
    approver: str
    timestamp: str
    proposed_action: str
    final_input: str
    final_result: str | None
    item_id: str = ""
    subtask_id: str = ""


class ReviewQueue:
    def __init__(self) -> None:
        self._client = self._connect()

    def _connect(self):
        try:
            import redis

            client = redis.from_url(get_settings().redis_url, decode_responses=True)
            client.ping()
            return client
        except Exception:
            return None

    def push(self, item: ReviewItem) -> None:
        if self._client:
            self._client.rpush(_QUEUE_KEY, item.to_json())
        else:
            _LOCAL.append(item.to_json())

        try:
            from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

            record_audit_event(
                event_type=AuditEventType.HITL_ESCALATED,
                outcome=AuditOutcome.ESCALATED,
                run_id=item.task_id,
                actor="system",
                metadata={
                    "item_id": item.item_id,
                    "subtask_id": item.subtask_id,
                    "reason": item.reason,
                    "level": item.level,
                    "quality": item.quality,
                    "confidence": item.confidence,
                },
            )
        except Exception:
            pass

    def pending(self) -> list[ReviewItem]:
        raws = self._client.lrange(_QUEUE_KEY, 0, -1) if self._client else list(_LOCAL)
        return [ReviewItem.from_json(r) for r in raws]

    def resolve(
        self,
        identifier: str | int,
        decision: str,
        approver: str,
        note: str | None = None,
    ) -> Resolution | None:
        """Apply an authenticated human's decision to a queued item.

        `identifier` can be a stable string (`item_id` or `task_id`) or an integer index
        (retained for backward compatibility). Lookups by stable identifier prevent race
        conditions and queue-reorder misdirection.

        `approver` is the authenticated reviewer identity and is required so
        every resolution is attributable (VA-01). The decision and any human
        input are committed to the tamper-evident audit chain (VA-09).

        Returns the :class:`Resolution` (including the workflow's final result),
        or ``None`` if no matching item is found.
        """
        if not approver:
            raise ValueError("approver identity is required to resolve a review item")
        if decision not in VALID_DECISIONS:
            raise ValueError(f"unknown decision: {decision!r} (expected one of {VALID_DECISIONS})")
        note = (note or "").strip()
        if decision in _TEXT_REQUIRED and not note:
            raise ValueError(f"decision {decision!r} requires reviewer-provided text")

        raws = self._client.lrange(_QUEUE_KEY, 0, -1) if self._client else list(_LOCAL)
        if not raws:
            return None

        target_raw = None
        target_item = None

        if isinstance(identifier, int):
            # Guard against legacy index ambiguity: index resolution is unsafe if multiple items exist.
            if len(raws) > 1:
                raise ValueError(
                    f"Ambiguous legacy index resolution: queue has {len(raws)} items; integer index "
                    f"is disallowed when multiple items are pending. Use stable item_id."
                )
            if identifier == 0 and len(raws) == 1:
                target_raw = raws[0]
                target_item = ReviewItem.from_json(target_raw)
            else:
                return None
        elif isinstance(identifier, str):
            identifier = identifier.strip()
            if not identifier:
                return None

            # First priority: unambiguous match on unique item_id
            for raw in raws:
                item = ReviewItem.from_json(raw)
                if item.item_id == identifier:
                    target_raw = raw
                    target_item = item
                    break

            # Second priority: match on task_id if item_id was not matched
            if target_item is None:
                matching_task = []
                for raw in raws:
                    item = ReviewItem.from_json(raw)
                    if item.task_id == identifier:
                        matching_task.append((raw, item))

                if len(matching_task) == 1:
                    target_raw, target_item = matching_task[0]
                elif len(matching_task) > 1:
                    raise ValueError(
                        f"Ambiguous resolution: task_id {identifier!r} matches {len(matching_task)} "
                        f"pending review items. Specify unique item_id."
                    )
                else:
                    return None
        else:
            return None

        if self._client:
            self._client.lrem(_QUEUE_KEY, 1, target_raw)
        else:
            _LOCAL.remove(target_raw)

        # The decision determines what the workflow should adopt as final.
        if decision == "approve":
            final_result: str | None = target_item.proposed_action
        elif decision == "reject":
            final_result = None
        else:  # edit | take_over both substitute the reviewer's text
            final_result = note

        record = AuditChain().append(
            task_id=target_item.task_id,
            action=target_item.level,
            decision=decision,
            approver=approver,
            proposed_action=target_item.proposed_action,
            final_input=note,
        )

        try:
            from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

            if decision == "approve":
                evt_type = AuditEventType.HITL_APPROVED
                evt_outcome = AuditOutcome.SUCCESS
            elif decision == "reject":
                evt_type = AuditEventType.HITL_REJECTED
                evt_outcome = AuditOutcome.SUCCESS
            else:
                evt_type = AuditEventType.HITL_EDITED
                evt_outcome = AuditOutcome.SUCCESS

            record_audit_event(
                event_type=evt_type,
                outcome=evt_outcome,
                run_id=target_item.task_id,
                actor=approver,
                metadata={
                    "item_id": target_item.item_id,
                    "subtask_id": target_item.subtask_id,
                    "decision": decision,
                    "note_chars": len(note) if note else 0,
                },
            )
        except Exception:
            pass
        return Resolution(
            task_id=target_item.task_id,
            decision=decision,
            approver=approver,
            timestamp=record["timestamp"],
            proposed_action=target_item.proposed_action,
            final_input=note,
            final_result=final_result,
            item_id=target_item.item_id,
            subtask_id=target_item.subtask_id,
        )

    def audit_log(self) -> list[dict]:
        """Return the tamper-evident, append-only record of who resolved what."""
        return AuditChain().records()


def apply_resolution(state: dict, resolution: Resolution) -> dict:
    """Apply a human reviewer's resolution to an orchestrated task state.

    Updates the state's answer, completion status, and audit trace so human
    decisions (approve, reject, edit, take_over) authoritatively govern the result.
    Enforces that:
      - approver identity is required
      - decision is one of VALID_DECISIONS
      - task_id matches between resolution and state
      - task is not already resolved
      - subtask targeting accurately targets the escalated subtask
    """
    if not resolution.approver or not resolution.approver.strip():
        raise ValueError("approver identity is required to apply resolution")
    if resolution.decision not in VALID_DECISIONS:
        raise ValueError(f"unknown decision: {resolution.decision!r} (expected one of {VALID_DECISIONS})")

    state_task_id = state.get("task_id")
    if state_task_id and resolution.task_id and state_task_id != resolution.task_id:
        raise ValueError(
            f"Task ID mismatch: state task_id {state_task_id!r} "
            f"does not match resolution task_id {resolution.task_id!r}"
        )

    # Prevent repeated or conflicting resolutions on an already-resolved state
    if state.get("resolution") or state.get("status") in ("taken_over", "edited", "rejected", "approved"):
        prev_decision = state.get("resolution", {}).get("decision", state.get("status"))
        raise ValueError(
            f"Task {state_task_id or resolution.task_id!r} is already resolved "
            f"with decision {prev_decision!r} and cannot be re-resolved"
        )

    decision = resolution.decision
    completed = list(state.get("completed", []))

    # Identify the exact subtask to target
    target_idx = None
    target_subtask_id = resolution.subtask_id
    if target_subtask_id:
        for idx, sub in enumerate(completed):
            if sub.get("id") == target_subtask_id:
                target_idx = idx
                break
        if target_idx is None and decision in ("edit", "reject"):
            raise ValueError(
                f"Cannot apply {decision!r}: subtask {target_subtask_id!r} not found in completed subtasks"
            )
    elif decision in ("edit", "reject"):
        if len(completed) == 1:
            target_idx = 0
            target_subtask_id = completed[0].get("id", "")
        elif len(completed) > 1:
            raise ValueError(
                f"Cannot apply {decision!r}: subtask_id is required to target a specific subtask "
                f"when multiple completed subtasks exist"
            )
        else:
            raise ValueError(f"Cannot apply {decision!r}: no completed subtasks exist in state")

    if decision == "take_over":
        state["answer"] = resolution.final_result or ""
        state["escalated"] = False
        state["status"] = "taken_over"
        state["resolution"] = asdict(resolution)
    elif decision == "edit":
        if target_idx is not None:
            completed[target_idx] = {
                **completed[target_idx],
                "result": resolution.final_result or "",
                "status": "done",
            }
            state["completed"] = completed
        sections = [
            f"### {c['kind'].title()} specialist\n{c['result']}"
            for c in completed
            if c.get("status") == "done"
        ]
        state["answer"] = "\n\n".join(sections) if sections else (resolution.final_result or "")
        state["escalated"] = False
        state["status"] = "edited"
        state["resolution"] = asdict(resolution)
    elif decision == "reject":
        if target_idx is not None:
            completed[target_idx] = {
                **completed[target_idx],
                "status": "rejected",
                "result": "(rejected by human reviewer)",
            }
            state["completed"] = completed
        sections = [
            f"### {c['kind'].title()} specialist\n{c['result']}"
            for c in completed
            if c.get("status") == "done"
        ]
        if sections:
            notes = [f"- Subtask {target_subtask_id or 'unknown'}: Rejected by human reviewer"]
            state["answer"] = "\n\n".join(sections) + "\n\n### Incomplete Work\n" + "\n".join(notes)
            state["status"] = "rejected"
        else:
            state["answer"] = "(rejected by human reviewer)"
            state["status"] = "rejected"
        state["escalated"] = False
        state["resolution"] = asdict(resolution)
    elif decision == "approve":
        if target_idx is not None:
            completed[target_idx] = {
                **completed[target_idx],
                "status": "done",
            }
            state["completed"] = completed
        elif completed and not target_subtask_id and len(completed) == 1:
            completed[0] = {
                **completed[0],
                "status": "done",
            }
            state["completed"] = completed
        state["escalated"] = False
        state["status"] = "approved"
        state["resolution"] = asdict(resolution)
        sections = [
            f"### {c['kind'].title()} specialist\n{c['result']}"
            for c in completed
            if c.get("status") == "done"
        ]
        state["answer"] = "\n\n".join(sections) if sections else (resolution.proposed_action or "")

    trace = list(state.get("trace", []))
    run_id = state.get("run_id") or resolution.task_id or state.get("task_id", "")
    trace.append(
        create_trace_event(
            node="hitl",
            event="resolution_applied",
            run_id=run_id,
            decision=decision,
            approver=resolution.approver,
            task_id=resolution.task_id or state.get("task_id", ""),
            subtask=target_subtask_id,
            subtask_id=target_subtask_id,
        )
    )
    state["trace"] = trace

    # Remember completed, validated outcome in long-term memory (never for reject)
    if decision in ("approve", "edit", "take_over"):
        task_text = state.get("task", "")
        answer_text = state.get("answer", "")
        if task_text and answer_text:
            try:
                from ..memory.long_term import LongTermMemory

                LongTermMemory().remember(
                    task=task_text,
                    outcome=answer_text,
                    metadata={
                        "task_id": resolution.task_id or state.get("task_id", ""),
                        "decision": decision,
                        "approver": resolution.approver,
                        "status": state.get("status", ""),
                    },
                )
            except Exception:
                pass

    # Update working short-term memory with resolution details
    resolved_task_id = resolution.task_id or state.get("task_id", "")
    if resolved_task_id:
        try:
            from ..memory.short_term import ShortTermMemory

            short = ShortTermMemory(resolved_task_id)
            short.set("resolution", asdict(resolution))
            short.set("status", state.get("status", ""))
            short.set("answer", state.get("answer", ""))
        except Exception:
            pass

    # Checkpoint resolution state for durable resume
    try:
        from ..persistence.store import CheckpointStore

        store = CheckpointStore()
        run_id = state.get("run_id") or resolution.task_id or state.get("task_id", "")
        if run_id and store.enabled:
            store.save_checkpoint(run_id, state, transition="resolution", privileged=True)
    except Exception:
        pass

    return state
