"""Shared state passed between graph nodes."""

from __future__ import annotations

import operator
from typing import Annotated, Any, Literal, TypedDict

SpecialistKind = Literal["research", "analysis", "writing", "code"]


class Subtask(TypedDict, total=False):
    id: str
    kind: SpecialistKind
    instruction: str
    status: Literal["pending", "done", "rejected", "failed", "blocked", "cancelled"]
    result: str
    confidence: float
    error: str
    tools_used: list[str]
    depends_on: list[str]


class GraphState(TypedDict, total=False):
    """The object that flows through every LangGraph node.

    `operator.add` reducers let parallel specialist nodes append to shared
    lists without clobbering each other.
    """

    run_id: str
    task_id: str
    task: str
    context: list[str]

    plan: list[Subtask]
    completed: Annotated[list[Subtask], operator.add]
    failed: Annotated[list[Subtask], operator.add]
    blocked: Annotated[list[Subtask], operator.add]
    tools_used: Annotated[list[str], operator.add]

    retries: int
    quality: float
    confidence: float

    # Human-in-the-loop
    escalated: bool
    escalation_reason: str
    escalated_subtask_id: str
    approval: str  # notify | approve_action | approve_plan | take_over
    # Every escalation across all subtasks, accumulated so the final answer can
    # report whether *any* subtask required review (not just the last one).
    escalations: Annotated[list[dict], operator.add]
    resolution: dict

    # Final: completed | pending | approved | edited | taken_over | rejected
    #        partial | failed | blocked | cancelled | timed_out | interrupted
    status: str
    answer: str
    usage: dict[str, Any]

    # Cancellation, Deadline & Interruption
    cancelled: bool
    cancellation_reason: str
    timeout_seconds: float
    interrupted: bool
    interrupted_reason: str

    trace: Annotated[list[dict], operator.add]
