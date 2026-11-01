"""Celery application and background task definitions for distributed workflow execution."""

from __future__ import annotations

import datetime
import logging
import os
import threading
import uuid
from typing import Any

from .config import Settings, get_settings, sanitize_broker_url
from .persistence.store import CheckpointStore

logger = logging.getLogger(__name__)

_CELERY_AVAILABLE = True
try:
    from celery import Celery
    from celery.signals import worker_ready, worker_shutdown
except ImportError:
    _CELERY_AVAILABLE = False
    Celery = None  # type: ignore[assignment,misc]


def _register_tasks(app: Any) -> None:
    """Register orchestrator tasks with the Celery application."""
    if getattr(app, "_orchestrator_tasks_registered", False):
        return

    @app.task(
        name="orchestrator.tasks.execute_workflow_run",
        bind=True,
        acks_late=True,
        max_retries=0,
    )
    def execute_workflow_run(
        self: Any,
        run_id: str,
        task: str,
        plan: list[dict[str, Any]] | None = None,
        max_concurrency: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Execute a dispatched workflow run inside an asynchronous Celery worker."""
        return _execute_workflow_run_impl(
            run_id=run_id,
            task=task,
            plan=plan,
            max_concurrency=max_concurrency,
            timeout_seconds=timeout_seconds,
        )

    @app.task(
        name="orchestrator.tasks.resume_workflow_run",
        bind=True,
        acks_late=True,
        max_retries=0,
    )
    def resume_workflow_run(
        self: Any,
        run_id: str,
        max_concurrency: int | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Resume a waiting or eligible workflow run inside an asynchronous Celery worker."""
        return _resume_workflow_run_impl(
            run_id=run_id,
            max_concurrency=max_concurrency,
            timeout_seconds=timeout_seconds,
        )

    # Expose tasks on module for direct imports
    globals()["execute_workflow_run"] = execute_workflow_run
    globals()["resume_workflow_run"] = resume_workflow_run
    app._orchestrator_tasks_registered = True


_celery_app_instance: Any = None


def get_celery_app(settings: Settings | None = None) -> Any:
    """Factory creating and configuring the Celery application."""
    global _celery_app_instance
    if not _CELERY_AVAILABLE:
        raise RuntimeError("Celery is not installed. Install with 'pip install celery>=5.4.0'.")

    s = settings or get_settings()
    if _celery_app_instance is None:
        broker = s.celery_broker_url or s.redis_url or "redis://localhost:6379/0"
        backend = s.celery_result_backend
        _celery_app_instance = Celery("orchestrator", broker=broker, backend=backend)
        _register_tasks(_celery_app_instance)

    conf: dict[str, Any] = {
        "task_default_queue": s.celery_task_default_queue,
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
        "worker_prefetch_multiplier": 1,
        "worker_concurrency": s.celery_worker_concurrency,
        "task_track_started": True,
        "result_expires": 3600,
        "broker_connection_retry_on_startup": False,
        "task_always_eager": bool(s.celery_always_eager),
        "task_eager_propagates": bool(s.celery_always_eager),
    }
    if s.celery_broker_url:
        conf["broker_url"] = s.celery_broker_url
    if s.celery_task_time_limit is not None:
        conf["task_time_limit"] = s.celery_task_time_limit
    if s.celery_task_soft_time_limit is not None:
        conf["task_soft_time_limit"] = s.celery_task_soft_time_limit

    _celery_app_instance.conf.update(conf)
    return _celery_app_instance


def get_celery_diagnostics(settings: Settings | None = None) -> dict[str, Any]:
    """Return a diagnostic snapshot of Celery worker and broker configuration."""
    s = settings or get_settings()
    is_eager = bool(s.celery_always_eager)
    enabled = bool(s.celery_enabled or s.workflow_execution_backend == "celery")

    return {
        "celery_available": _CELERY_AVAILABLE,
        "enabled": enabled,
        "execution_backend": s.workflow_execution_backend,
        "broker_url": sanitize_broker_url(s.celery_broker_url),
        "result_backend": sanitize_broker_url(s.celery_result_backend),
        "default_queue": s.celery_task_default_queue,
        "worker_concurrency": s.celery_worker_concurrency,
        "task_time_limit": s.celery_task_time_limit,
        "task_soft_time_limit": s.celery_task_soft_time_limit,
        "task_always_eager": is_eager,
        "tasks": [
            "orchestrator.tasks.execute_workflow_run",
            "orchestrator.tasks.resume_workflow_run",
        ],
    }


class CeleryWorkerHeartbeatTracker:
    """Manages periodic heartbeat pulses and task state tracking for a Celery worker."""

    def __init__(
        self,
        worker_id: str,
        store: CheckpointStore | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.worker_id = worker_id
        self._store = store or CheckpointStore()
        self._settings = settings or get_settings()
        self._interval = max(1.0, float(self._settings.celery_worker_heartbeat_interval_seconds))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active_tasks = 0
        self._lock = threading.Lock()
        self._queues = str(self._settings.celery_task_default_queue or "orchestrator_workflows")

    def start(self) -> None:
        """Start background heartbeat pulse thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self.pulse(status="active")
        self._thread = threading.Thread(
            target=self._run_loop,
            name=f"celery-hb-{self.worker_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """Stop background heartbeat thread and mark worker offline."""
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self.pulse(status="offline", active_tasks=0)

    def pulse(
        self,
        status: str = "active",
        active_tasks: int | None = None,
        last_completed_at: str | None = None,
        last_failure_at: str | None = None,
        last_failure_reason: str | None = None,
    ) -> dict[str, Any]:
        """Record an explicit heartbeat pulse."""
        with self._lock:
            tasks = self._active_tasks if active_tasks is None else max(0, int(active_tasks))
        return self._store.record_worker_heartbeat(
            worker_id=self.worker_id,
            status=status,
            active_tasks=tasks,
            queues=self._queues,
            last_completed_at=last_completed_at,
            last_failure_at=last_failure_at,
            last_failure_reason=last_failure_reason,
        )

    def on_task_start(self) -> None:
        """Record task initiation."""
        with self._lock:
            self._active_tasks += 1
        self.pulse(status="active")

    def on_task_success(self) -> None:
        """Record task successful completion."""
        with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.pulse(status="active", last_completed_at=now)

    def on_task_failure(self, error: str) -> None:
        """Record task failure with sanitized failure reason."""
        with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.pulse(status="active", last_failure_at=now, last_failure_reason=error)

    def _run_loop(self) -> None:
        """Daemon worker loop pulsing heartbeat at regular interval."""
        while not self._stop_event.wait(self._interval):
            try:
                self.pulse(status="active")
            except Exception as exc:
                logger.warning("Error pulsing worker heartbeat for %s: %s", self.worker_id, exc)


_worker_trackers: dict[str, CeleryWorkerHeartbeatTracker] = {}
_worker_trackers_lock = threading.Lock()


def get_worker_heartbeat_tracker(
    worker_id: str | None = None,
    store: CheckpointStore | None = None,
    settings: Settings | None = None,
) -> CeleryWorkerHeartbeatTracker:
    """Retrieve or instantiate worker heartbeat tracker singleton."""
    s = settings or get_settings()
    wid = str(worker_id or "").strip() or f"celery-worker-{os.getpid()}"
    with _worker_trackers_lock:
        if wid not in _worker_trackers:
            _worker_trackers[wid] = CeleryWorkerHeartbeatTracker(
                worker_id=wid,
                store=store,
                settings=s,
            )
        return _worker_trackers[wid]


def _execute_workflow_run_impl(
    run_id: str,
    task: str,
    plan: list[dict[str, Any]] | None = None,
    max_concurrency: int | None = None,
    timeout_seconds: float | None = None,
    store: CheckpointStore | None = None,
) -> dict[str, Any]:
    """Internal implementation for executing a workflow run in a Celery worker."""
    effective_store = store or CheckpointStore()
    clean_run_id = str(run_id or "").strip()
    if not clean_run_id:
        raise ValueError("run_id must not be empty.")

    tracker = get_worker_heartbeat_tracker(store=effective_store)
    tracker.on_task_start()

    try:
        # 1. Duplicate task delivery & terminal status verification
        if effective_store.enabled:
            summary = effective_store.get_run_summary(clean_run_id)
            if summary:
                db_status = summary.get("status")
                terminal_statuses = (
                    "completed",
                    "cancelled",
                    "timed_out",
                    "failed",
                    "approved",
                    "edited",
                    "rejected",
                )
                if db_status in terminal_statuses:
                    logger.warning(
                        "Duplicate Celery delivery for run '%s' ignored: terminal status '%s'.",
                        clean_run_id,
                        db_status,
                    )
                    tracker.on_task_success()
                    return summary
                if db_status == "waiting_approval":
                    logger.info(
                        "Celery delivery for run '%s' ignored: paused waiting for tool approval.",
                        clean_run_id,
                    )
                    tracker.on_task_success()
                    return summary

        # 2. Check cooperative cancellation before starting worker
        from .graph.cancellation import get_cancellation_registry

        reg = get_cancellation_registry()
        tok = reg.get(clean_run_id)
        if tok and tok.is_cancelled:
            logger.info(
                "Workflow run '%s' was cancelled before worker execution started.",
                clean_run_id,
            )
            if effective_store.enabled:
                try:
                    rec = effective_store.cancel_run_administratively(
                        clean_run_id,
                        reason=tok.reason or "Cancelled before worker execution",
                    )
                    tracker.on_task_success()
                    return rec.state
                except Exception:
                    pass
            tracker.on_task_success()
            return {
                "run_id": clean_run_id,
                "status": "cancelled",
                "cancellation_reason": tok.reason,
            }

        # 3. Worker execution through RunManager with admission and fencing
        from .api.manager import RunManager

        worker_owner = f"celery-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        manager = RunManager(owner_id=worker_owner, store=effective_store)

        res = manager.execute_dispatched_run(
            run_id=clean_run_id,
            task=task,
            plan=plan,
            max_concurrency=max_concurrency,
            timeout_seconds=timeout_seconds,
            store=effective_store,
        )
        tracker.on_task_success()
        return res
    except Exception as exc:
        tracker.on_task_failure(str(exc))
        raise


def _resume_workflow_run_impl(
    run_id: str,
    max_concurrency: int | None = None,
    timeout_seconds: float | None = None,
    store: CheckpointStore | None = None,
) -> dict[str, Any]:
    """Internal implementation for resuming a workflow run in a Celery worker."""
    effective_store = store or CheckpointStore()
    clean_run_id = str(run_id or "").strip()
    if not clean_run_id:
        raise ValueError("run_id must not be empty.")

    tracker = get_worker_heartbeat_tracker(store=effective_store)
    tracker.on_task_start()

    try:
        from .api.manager import RunManager

        worker_owner = f"celery-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        manager = RunManager(owner_id=worker_owner, store=effective_store)

        resp = manager.resume_run(
            run_id=clean_run_id,
            max_concurrency=max_concurrency,
            background=False,
            store=effective_store,
            timeout_seconds=timeout_seconds,
        )
        tracker.on_task_success()
        return resp.model_dump()
    except Exception as exc:
        tracker.on_task_failure(str(exc))
        raise


# Module-level default application for CLI worker discovery (e.g. celery -A orchestrator.celery_app worker)
if _CELERY_AVAILABLE:
    try:
        celery_app = get_celery_app()
        app = celery_app

        @worker_ready.connect
        def _on_worker_ready(sender: Any = None, **kwargs: Any) -> None:
            logger.info("Celery orchestrator worker ready for background task processing.")
            worker_name = str(
                getattr(sender, "hostname", "")
                or getattr(sender, "nodename", "")
                or f"celery-worker-{os.getpid()}"
            ).strip()
            w_tracker = get_worker_heartbeat_tracker(worker_id=worker_name)
            w_tracker.start()

        @worker_shutdown.connect
        def _on_worker_shutdown(sender: Any = None, **kwargs: Any) -> None:
            logger.info("Celery orchestrator worker shutting down gracefully.")
            worker_name = str(
                getattr(sender, "hostname", "")
                or getattr(sender, "nodename", "")
                or f"celery-worker-{os.getpid()}"
            ).strip()
            w_tracker = get_worker_heartbeat_tracker(worker_id=worker_name)
            w_tracker.stop()

    except Exception as _app_exc:
        logger.debug("Deferred Celery app initialization: %s", _app_exc)
        celery_app = None
        app = None
else:
    celery_app = None
    app = None
