"""Celery worker integration: exports tasks and application entrypoints."""

from __future__ import annotations

from typing import Any

from ..celery_app import (
    CeleryWorkerHeartbeatTracker,
    app,
    celery_app,
    execute_workflow_run,
    get_celery_app,
    get_worker_heartbeat_tracker,
    resume_workflow_run,
)


def run_task_async(task: str) -> dict[str, Any]:
    """Legacy helper: execute task synchronously via graph and format basic summary."""
    from ..graph import run_task

    result = run_task(task)
    return {
        "task_id": result.get("task_id"),
        "answer": result.get("answer"),
        "escalated": result.get("escalated", False),
        "quality": result.get("quality"),
    }


__all__ = [
    "CeleryWorkerHeartbeatTracker",
    "app",
    "celery_app",
    "execute_workflow_run",
    "get_celery_app",
    "get_worker_heartbeat_tracker",
    "resume_workflow_run",
    "run_task_async",
]
