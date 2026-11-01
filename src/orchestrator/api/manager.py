"""In-process execution manager and concurrency coordinator for the orchestration API."""

from __future__ import annotations

import concurrent.futures
import datetime
import logging
import os
import threading
import time
import uuid
from typing import Any

from ..config import get_settings
from ..graph.builder import resume_task, run_task
from ..graph.cancellation import get_cancellation_registry
from ..graph.supervisor import sanitize_error
from ..hitl.queue import ReviewQueue
from ..persistence.store import (
    AdmissionFencingError,
    CheckpointConflictError,
    CheckpointError,
    CheckpointNotFoundError,
    CheckpointStore,
)
from .models import (
    HitlStatusSummary,
    RunResponse,
    RunReviewSummary,
)

logger = logging.getLogger(__name__)


class RunConflictError(Exception):
    """Raised when an operation conflicts with active or terminal run state."""


class ShutdownError(Exception):
    """Raised when an operation is attempted while the manager is shutting down."""


class CapacityExhaustedError(Exception):
    """Raised when an operation is rejected because active workflow capacity is exhausted."""

    def __init__(
        self,
        message: str = "Workflow admission rejected: server capacity limit reached.",
        max_active_runs: int = 10,
        active_runs: int = 10,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.max_active_runs = max_active_runs
        self.active_runs = active_runs


class BrokerUnavailableError(Exception):
    """Raised when the Celery message broker is unavailable or rejects task submission."""


def _emit_audit(
    event_type: str,
    outcome: str,
    run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    store: CheckpointStore | None = None,
    dedup: bool = False,
    actor: str | None = None,
) -> None:
    try:
        from ..observability.audit import record_audit_event

        record_audit_event(
            event_type=event_type,
            outcome=outcome,
            run_id=run_id,
            metadata=metadata or {},
            store=store,
            dedup=dedup,
            actor=actor or "system",
        )
    except Exception:
        pass


class RunManager:
    """Coordinates execution, concurrency bounds, active run registry, and background jobs."""

    def __init__(
        self,
        max_background_workers: int = 8,
        max_active_runs: int | None = None,
        owner_id: str | None = None,
        store: CheckpointStore | None = None,
        lease_ttl_seconds: float | None = None,
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self._is_shutting_down = False
        self._started_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._last_recovery_summary: dict[str, Any] | None = None
        self._run_locks: dict[str, threading.RLock] = {}
        self._active_runs: dict[str, dict[str, Any]] = {}
        self._results_cache: dict[str, dict[str, Any]] = {}
        self._owner_id = str(owner_id).strip() if owner_id else f"worker-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._store = store

        settings = get_settings()
        if max_active_runs is not None:
            self._max_active_runs = max(1, int(max_active_runs))
        else:
            self._max_active_runs = getattr(settings, "max_active_runs", 10)

        if lease_ttl_seconds is not None:
            self._lease_ttl = max(0.001, float(lease_ttl_seconds))
        else:
            self._lease_ttl = float(getattr(settings, "admission_lease_ttl_seconds", 30.0))

        if heartbeat_interval_seconds is not None:
            self._heartbeat_interval = max(0.001, float(heartbeat_interval_seconds))
        else:
            self._heartbeat_interval = float(getattr(settings, "admission_heartbeat_interval_seconds", 5.0))

        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, max_background_workers),
            thread_name_prefix=f"api-run-worker-{self._owner_id[-6:]}",
        )

        # Background heartbeat loop for renewing cross-process admission leases
        self._heartbeat_stop_event = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"admission-heartbeat-{self._owner_id[-6:]}",
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _heartbeat_loop_pulse(self, store: CheckpointStore | None = None) -> None:
        """Execute a single heartbeat renewal pass across all currently active runs."""
        with self._lock:
            active_items = {
                r_id: {
                    "fence_token": info.get("fence_token", 1),
                    "last_heartbeat_at": info.get("last_heartbeat_at", time.monotonic()),
                }
                for r_id, info in self._active_runs.items()
            }
        if not active_items:
            return

        active_store = store or self._store or CheckpointStore()
        if not active_store or not active_store.enabled:
            return

        now_mono = time.monotonic()
        try:
            active_tokens = {r_id: d["fence_token"] for r_id, d in active_items.items()}
            _cnt, renewed_ids, failed_ids = active_store.heartbeat_active_runs(
                self._owner_id,
                active_tokens,
                lease_ttl_seconds=self._lease_ttl,
            )
            with self._lock:
                for r_id in renewed_ids:
                    if r_id in self._active_runs:
                        self._active_runs[r_id]["last_heartbeat_at"] = now_mono

            if failed_ids:
                cancel_reg = get_cancellation_registry()
                with self._lock:
                    for r_id in failed_ids:
                        if r_id in self._active_runs:
                            self._active_runs[r_id]["lease_lost"] = True
                for r_id in failed_ids:
                    tok = cancel_reg.get(r_id)
                    if tok and not tok.is_cancelled:
                        tok.request_cancellation(
                            reason="Admission lease expired or ownership lost to another worker"
                        )
        except Exception as exc:
            logger.warning(
                "Heartbeat renewal query failed for owner %s: %s",
                self._owner_id,
                exc,
            )
            cancel_reg = get_cancellation_registry()
            expired_ids = []
            with self._lock:
                for r_id, d in active_items.items():
                    elapsed = now_mono - d["last_heartbeat_at"]
                    if elapsed >= self._lease_ttl:
                        if r_id in self._active_runs:
                            self._active_runs[r_id]["lease_lost"] = True
                        expired_ids.append(r_id)
            for r_id in expired_ids:
                tok = cancel_reg.get(r_id)
                if tok and not tok.is_cancelled:
                    tok.request_cancellation(
                        reason=(
                            f"Admission lease unconfirmed for {self._lease_ttl:.1f}s "
                            f"due to database unavailability"
                        )
                    )

    def _heartbeat_loop(self) -> None:
        """Periodically renew cross-process admission leases for all active runs with fencing tracking."""
        while not self._heartbeat_stop_event.is_set():
            self._heartbeat_stop_event.wait(timeout=self._heartbeat_interval)
            if self._heartbeat_stop_event.is_set():
                break
            self._heartbeat_loop_pulse()

    @property
    def owner_id(self) -> str:
        """Return the unique worker instance identifier."""
        return self._owner_id

    @property
    def lease_ttl(self) -> float:
        """Return cross-process admission lease TTL in seconds."""
        return self._lease_ttl

    @property
    def max_active_runs(self) -> int:
        """Return configured maximum concurrent active workflow runs."""
        with self._lock:
            return self._max_active_runs

    @max_active_runs.setter
    def max_active_runs(self, value: int) -> None:
        """Update maximum concurrent active workflow runs limit."""
        val = int(value)
        if val < 1:
            raise ValueError("max_active_runs must be an integer >= 1.")
        with self._lock:
            self._max_active_runs = val

    @property
    def available_capacity(self) -> int:
        """Return remaining workflow admission slots available before backpressure rejection."""
        active_store = self._store
        if active_store and active_store.enabled:
            cnt = active_store.count_active_admissions(self._lease_ttl)
            with self._lock:
                return max(0, self._max_active_runs - cnt)
        with self._lock:
            return max(0, self._max_active_runs - len(self._active_runs))

    def get_available_capacity(self, store: CheckpointStore | None = None) -> int:
        """Return remaining admission slots, querying global persistence if store is provided."""
        active_store = store or self._store
        if active_store and active_store.enabled:
            cnt = active_store.count_active_admissions(self._lease_ttl)
            with self._lock:
                return max(0, self._max_active_runs - cnt)
        with self._lock:
            return max(0, self._max_active_runs - len(self._active_runs))

    @property
    def started_at(self) -> str:
        """Return ISO UTC timestamp when the manager was initialized."""
        return self._started_at

    def get_recovery_summary(self) -> dict[str, Any] | None:
        """Return the most recent startup recovery summary, if available."""
        with self._lock:
            return dict(self._last_recovery_summary) if self._last_recovery_summary is not None else None

    @property
    def is_shutting_down(self) -> bool:
        """Check whether the manager has initiated graceful shutdown."""
        with self._lock:
            return self._is_shutting_down

    def _get_run_lock(self, run_id: str) -> threading.RLock:
        """Obtain a dedicated, reentrant per-run synchronization lock."""
        clean = str(run_id or "").strip()
        with self._lock:
            if clean not in self._run_locks:
                self._run_locks[clean] = threading.RLock()
            return self._run_locks[clean]

    def active_runs_count(self, store: CheckpointStore | None = None) -> int:
        """Return the number of currently executing runs.
        When store is provided (or configured) and enabled, returns the global cross-process active count,
        ensuring local in-flight runs are also accounted for.
        """
        with self._lock:
            local_cnt = len(self._active_runs)

        active_store = store or self._store
        if active_store and active_store.enabled:
            global_cnt = active_store.count_active_admissions(self._lease_ttl)
            return max(local_cnt, global_cnt)
        return local_cnt

    def local_active_runs_count(self) -> int:
        """Return the number of currently executing runs in this specific process."""
        with self._lock:
            return len(self._active_runs)

    def is_active(self, run_id: str) -> bool:
        """Check if a run is currently in-flight."""
        with self._lock:
            return run_id in self._active_runs

    def _register_active(self, run_id: str, task: str) -> None:
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._lock:
            if run_id in self._active_runs:
                raise RunConflictError(f"Run '{run_id}' is already executing in-process.")
            self._active_runs[run_id] = {
                "run_id": run_id,
                "task": task,
                "status": "running",
                "started_at": now,
            }

    def _unregister_active(self, run_id: str) -> None:
        with self._lock:
            self._active_runs.pop(run_id, None)

    def _record_result(self, run_id: str, state: dict[str, Any]) -> None:
        with self._lock:
            self._results_cache[run_id] = state

    def start_run(
        self,
        task: str,
        plan: list[dict[str, Any]] | None = None,
        max_concurrency: int | None = None,
        run_id: str | None = None,
        background: bool = False,
        store: CheckpointStore | None = None,
        timeout_seconds: float | None = None,
        execution_backend: str | None = None,
    ) -> RunResponse:
        """Start a new workflow run, either synchronously or in the background."""
        clean_task = str(task or "").strip()
        settings = get_settings()

        if not clean_task:
            raise ValueError("Task instruction must not be empty.")
        if len(clean_task) > settings.max_task_chars:
            raise ValueError(
                f"Task exceeds maximum allowed length of {settings.max_task_chars} characters."
            )
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number greater than 0.")

        clean_run_id = (run_id or "").strip() or f"run-{uuid.uuid4().hex[:12]}"
        run_lock = self._get_run_lock(clean_run_id)
        effective_store = store or self._store or CheckpointStore()

        eff_backend = (execution_backend or "").strip().lower()
        if not eff_backend:
            if getattr(settings, "celery_enabled", False) or getattr(
                settings, "workflow_execution_backend", "direct"
            ) == "celery":
                eff_backend = "celery" if background else "direct"
            elif background:
                eff_backend = "thread"
            else:
                eff_backend = "direct"
        elif eff_backend in ("sync", "direct"):
            eff_backend = "direct"

        if eff_backend == "celery":
            return self._start_celery_run(
                clean_run_id=clean_run_id,
                clean_task=clean_task,
                plan=plan,
                max_concurrency=max_concurrency,
                store=effective_store,
                timeout_seconds=timeout_seconds,
            )

        with run_lock:
            with self._lock:
                if self._is_shutting_down:
                    raise ShutdownError("Application is shutting down; new workflow runs cannot be accepted.")
                if clean_run_id in self._active_runs:
                    raise RunConflictError(f"Run '{clean_run_id}' is already executing in-process.")
                if len(self._active_runs) >= self._max_active_runs:
                    raise CapacityExhaustedError(
                        f"Workflow admission rejected: server capacity limit of {self._max_active_runs} "
                        f"active runs reached. Please retry shortly.",
                        max_active_runs=self._max_active_runs,
                        active_runs=len(self._active_runs),
                    )
                # Atomically reserve slot under lock to prevent check-then-act races
                # across concurrent run_locks
                self._active_runs[clean_run_id] = {
                    "run_id": clean_run_id,
                    "task": clean_task,
                    "status": "running",
                    "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }

            fence_token = 1
            # Atomic database-level admission check across processes
            if effective_store.enabled:
                admitted = effective_store.acquire_workflow_admission(
                    run_id=clean_run_id,
                    owner_id=self._owner_id,
                    max_active_runs=self._max_active_runs,
                    lease_ttl_seconds=self._lease_ttl,
                )
                if not admitted:
                    self._unregister_active(clean_run_id)
                    global_cnt = effective_store.count_active_admissions(self._lease_ttl)
                    raise CapacityExhaustedError(
                        f"Workflow admission rejected: server capacity limit of {self._max_active_runs} "
                        f"active runs reached across worker processes. Please retry shortly.",
                        max_active_runs=self._max_active_runs,
                        active_runs=global_cnt,
                    )

                adm = effective_store.get_workflow_admission(clean_run_id)
                if adm:
                    fence_token = int(adm.get("fence_token", 1))

                summary = effective_store.get_run_summary(clean_run_id)
                if summary and summary.get("status") in (
                    "completed",
                    "approved",
                    "edited",
                    "cancelled",
                    "timed_out",
                    "failed",
                ):
                    self._unregister_active(clean_run_id)
                    effective_store.release_workflow_admission(
                        clean_run_id, self._owner_id, fence_token=fence_token
                    )
                    raise RunConflictError(
                        f"Run '{clean_run_id}' already exists with terminal status '{summary['status']}'. "
                        f"Specify a new run ID or resume if eligible."
                    )

            with self._lock:
                if clean_run_id in self._active_runs:
                    self._active_runs[clean_run_id]["fence_token"] = fence_token
                    self._active_runs[clean_run_id]["last_heartbeat_at"] = time.monotonic()
                    self._active_runs[clean_run_id]["lease_lost"] = False

            fenced_store = (
                effective_store.with_fencing(self._owner_id, fence_token)
                if effective_store and effective_store.enabled
                else effective_store
            )

            _emit_audit(
                "workflow.created",
                "success",
                run_id=clean_run_id,
                metadata={"task": clean_task, "background": background},
                store=fenced_store,
            )

            if background:
                try:
                    self._executor.submit(
                        self._run_worker,
                        clean_run_id,
                        clean_task,
                        plan,
                        max_concurrency,
                        fenced_store,
                        timeout_seconds,
                    )
                    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    return RunResponse(
                        run_id=clean_run_id,
                        task=clean_task,
                        status="running",
                        created_at=now,
                        updated_at=now,
                    )
                except Exception:
                    self._unregister_active(clean_run_id)
                    if effective_store.enabled:
                        effective_store.release_workflow_admission(
                            clean_run_id, self._owner_id, fence_token=fence_token
                        )
                    raise

        # Synchronous execution outside start_run's lock so worker manages lock
        try:
            state = self._run_worker(
                clean_run_id,
                clean_task,
                plan,
                max_concurrency,
                fenced_store,
                timeout_seconds,
            )
            return self.format_run_response(state)
        except Exception:
            self._unregister_active(clean_run_id)
            if effective_store.enabled:
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
            raise

    def _run_worker(
        self,
        run_id: str,
        task: str,
        plan: list[dict[str, Any]] | None,
        max_concurrency: int | None,
        store: CheckpointStore | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        run_lock = self._get_run_lock(run_id)
        _emit_audit(
            "workflow.started",
            "success",
            run_id=run_id,
            metadata={"task": task},
            store=store,
        )
        try:
            state = run_task(
                task=task,
                plan=plan,
                run_id=run_id,
                max_concurrency=max_concurrency,
                store=store,
                timeout=timeout_seconds,
            )
            with run_lock:
                with self._lock:
                    active_info = self._active_runs.get(run_id)
                    is_already_cancelled = active_info and active_info.get("status") == "cancelled"
                    default_reason = "User requested cancellation"
                    reason = active_info.get("cancellation_reason", default_reason) if active_info else ""

                # If cancel_run won concurrently, preserve cancelled terminal status
                if is_already_cancelled and state.get("status") not in ("cancelled", "timed_out"):
                    state["status"] = "cancelled"
                    state["cancellation_reason"] = reason

                self._record_result(run_id, state)

                final_status = state.get("status")
                if final_status == "completed":
                    _emit_audit(
                        "workflow.completed",
                        "success",
                        run_id=run_id,
                        metadata={"quality": state.get("quality"), "confidence": state.get("confidence")},
                        store=store,
                    )
                elif final_status == "timed_out":
                    _emit_audit(
                        "workflow.timed_out",
                        "timeout",
                        run_id=run_id,
                        metadata={"reason": state.get("cancellation_reason")},
                        store=store,
                    )
                elif final_status == "cancelled":
                    _emit_audit(
                        "workflow.cancelled",
                        "success",
                        run_id=run_id,
                        metadata={"reason": state.get("cancellation_reason")},
                        store=store,
                    )
                elif final_status == "failed":
                    _emit_audit(
                        "workflow.failed",
                        "failure",
                        run_id=run_id,
                        metadata={"error": state.get("error")},
                        store=store,
                    )

                return state
        except Exception as exc:
            is_fencing_err = isinstance(exc, AdmissionFencingError)
            with self._lock:
                active_info = self._active_runs.get(run_id)
                lease_lost = bool(active_info and active_info.get("lease_lost"))

            if is_fencing_err or lease_lost:
                logger.warning(
                    "Workflow run %s lost admission ownership or encountered fencing conflict: %s. "
                    "Skipping persistent state update to protect database integrity.",
                    run_id,
                    exc,
                )
                failed_state: dict[str, Any] = {
                    "run_id": run_id,
                    "task_id": run_id,
                    "task": task,
                    "status": "failed",
                    "answer": f"Admission lease lost: {sanitize_error(str(exc))}",
                    "completed": [],
                    "failed": [],
                    "blocked": [],
                    "error": sanitize_error(str(exc)),
                }
                with self._lock:
                    self._results_cache.pop(run_id, None)
                return failed_state

            logger.exception("Workflow run %s failed with unhandled exception: %s", run_id, exc)
            failed_state = {
                "run_id": run_id,
                "task_id": run_id,
                "task": task,
                "status": "failed",
                "answer": f"Workflow failed: {sanitize_error(str(exc))}",
                "completed": [],
                "failed": [],
                "blocked": [],
                "error": sanitize_error(str(exc)),
            }
            with run_lock:
                with self._lock:
                    active_info = self._active_runs.get(run_id)
                    if active_info and active_info.get("status") == "cancelled":
                        failed_state["status"] = "cancelled"
                        failed_state["cancellation_reason"] = active_info.get("cancellation_reason", "")

                if store and store.enabled:
                    try:
                        transition = "cancelled" if failed_state["status"] == "cancelled" else "error"
                        store.save_checkpoint(run_id, failed_state, transition=transition)
                    except Exception:
                        pass
                self._record_result(run_id, failed_state)

                if failed_state.get("status") == "cancelled":
                    _emit_audit(
                        "workflow.cancelled",
                        "success",
                        run_id=run_id,
                        metadata={"reason": failed_state.get("cancellation_reason")},
                        store=store,
                    )
                else:
                    _emit_audit(
                        "workflow.failed",
                        "failure",
                        run_id=run_id,
                        metadata={"error": failed_state.get("error")},
                        store=store,
                    )

                return failed_state
        finally:
            with self._lock:
                active_info = self._active_runs.get(run_id)
                fence_token = active_info.get("fence_token", 1) if active_info else 1
            self._unregister_active(run_id)
            if store and store.enabled:
                store.release_workflow_admission(run_id, self._owner_id, fence_token=fence_token)

    def _start_celery_run(
        self,
        clean_run_id: str,
        clean_task: str,
        plan: list[dict[str, Any]] | None,
        max_concurrency: int | None,
        store: CheckpointStore | None,
        timeout_seconds: float | None,
    ) -> RunResponse:
        """Enqueue workflow execution to the Celery distributed task queue."""
        settings = get_settings()
        effective_store = store or self._store or CheckpointStore()

        run_lock = self._get_run_lock(clean_run_id)
        with run_lock:
            with self._lock:
                if self._is_shutting_down:
                    raise ShutdownError(
                        "Application is shutting down; new workflow runs cannot be accepted."
                    )
                if clean_run_id in self._active_runs:
                    raise RunConflictError(f"Run '{clean_run_id}' is already executing in-process.")

            if effective_store.enabled:
                summary = effective_store.get_run_summary(clean_run_id)
                if summary:
                    db_status = summary.get("status")
                    if db_status in (
                        "completed",
                        "approved",
                        "edited",
                        "cancelled",
                        "timed_out",
                        "failed",
                    ):
                        raise RunConflictError(
                            f"Run '{clean_run_id}' already exists with terminal status '{db_status}'."
                        )
                    if db_status in ("running", "queued"):
                        raise RunConflictError(f"Run '{clean_run_id}' is already in progress.")

                # Save initial queued checkpoint record
                state: dict[str, Any] = {
                    "run_id": clean_run_id,
                    "task": clean_task,
                    "status": "queued",
                    "plan": plan or [],
                }
                effective_store.save_checkpoint(clean_run_id, state, transition="enqueue")

            _emit_audit(
                "workflow.created",
                "success",
                run_id=clean_run_id,
                metadata={"task": clean_task, "background": True, "backend": "celery"},
                store=effective_store,
            )

            try:
                from ..celery_app import execute_workflow_run, get_celery_app

                app = get_celery_app(settings)
                # Verify broker connectivity unless in local eager mode
                if not getattr(app.conf, "task_always_eager", False):
                    try:
                        with app.connection_for_write() as conn:
                            conn.ensure_connection(max_retries=1)
                    except Exception as conn_err:
                        raise BrokerUnavailableError(
                            f"Celery message broker connection failed: {conn_err}"
                        ) from conn_err

                execute_workflow_run.apply_async(
                    kwargs={
                        "run_id": clean_run_id,
                        "task": clean_task,
                        "plan": plan,
                        "max_concurrency": max_concurrency,
                        "timeout_seconds": timeout_seconds,
                    },
                    queue=settings.celery_task_default_queue,
                )
            except Exception as exc:
                if effective_store.enabled:
                    try:
                        with effective_store._get_db_lock(effective_store.db_path):
                            conn = effective_store._get_connection()
                            conn.execute(
                                "DELETE FROM checkpoints WHERE run_id = ?",
                                (clean_run_id,),
                            )
                            conn.execute("DELETE FROM runs WHERE run_id = ?", (clean_run_id,))
                            conn.commit()
                    except Exception:
                        pass
                if isinstance(exc, BrokerUnavailableError):
                    raise
                raise BrokerUnavailableError(
                    f"Failed to submit workflow run to Celery: {exc}"
                ) from exc

            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            current_status = "queued"
            if effective_store.enabled:
                sum_now = effective_store.get_run_summary(clean_run_id)
                if sum_now:
                    current_status = sum_now.get("status", "queued")

            return RunResponse(
                run_id=clean_run_id,
                task=clean_task,
                status=current_status,
                created_at=now,
                updated_at=now,
            )

    def execute_dispatched_run(
        self,
        run_id: str,
        task: str,
        plan: list[dict[str, Any]] | None = None,
        max_concurrency: int | None = None,
        timeout_seconds: float | None = None,
        store: CheckpointStore | None = None,
    ) -> dict[str, Any]:
        """Execute a workflow run dispatched by an asynchronous worker (e.g. Celery)."""
        clean_run_id = str(run_id or "").strip()
        clean_task = str(task or "").strip()
        effective_store = store or self._store or CheckpointStore()

        run_lock = self._get_run_lock(clean_run_id)
        with run_lock:
            with self._lock:
                if self._is_shutting_down:
                    raise ShutdownError("Worker is shutting down; run cannot be processed.")
                if clean_run_id in self._active_runs:
                    raise RunConflictError(f"Run '{clean_run_id}' is already executing in-process.")
                self._active_runs[clean_run_id] = {
                    "run_id": clean_run_id,
                    "task": clean_task,
                    "status": "running",
                    "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }

            fence_token = 1
            if effective_store.enabled:
                admitted = effective_store.acquire_workflow_admission(
                    run_id=clean_run_id,
                    owner_id=self._owner_id,
                    max_active_runs=self._max_active_runs,
                    lease_ttl_seconds=self._lease_ttl,
                )
                if not admitted:
                    self._unregister_active(clean_run_id)
                    global_cnt = effective_store.count_active_admissions(self._lease_ttl)
                    raise CapacityExhaustedError(
                        f"Workflow admission rejected: server capacity limit of {self._max_active_runs} "
                        f"active runs reached across worker processes.",
                        max_active_runs=self._max_active_runs,
                        active_runs=global_cnt,
                    )

                adm = effective_store.get_workflow_admission(clean_run_id)
                if adm:
                    fence_token = int(adm.get("fence_token", 1))

                summary = effective_store.get_run_summary(clean_run_id)
                if summary and summary.get("status") in (
                    "completed",
                    "approved",
                    "edited",
                    "cancelled",
                    "timed_out",
                    "failed",
                ):
                    self._unregister_active(clean_run_id)
                    effective_store.release_workflow_admission(
                        clean_run_id, self._owner_id, fence_token=fence_token
                    )
                    return summary

            with self._lock:
                if clean_run_id in self._active_runs:
                    self._active_runs[clean_run_id]["fence_token"] = fence_token
                    self._active_runs[clean_run_id]["last_heartbeat_at"] = time.monotonic()
                    self._active_runs[clean_run_id]["lease_lost"] = False

            fenced_store = (
                effective_store.with_fencing(self._owner_id, fence_token)
                if effective_store and effective_store.enabled
                else effective_store
            )

        try:
            return self._run_worker(
                clean_run_id,
                clean_task,
                plan,
                max_concurrency,
                fenced_store,
                timeout_seconds,
            )
        except Exception:
            self._unregister_active(clean_run_id)
            if effective_store.enabled:
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
            raise

    def resume_run(
        self,
        run_id: str,
        max_concurrency: int | None = None,
        background: bool = False,
        store: CheckpointStore | None = None,
        timeout_seconds: float | None = None,
    ) -> RunResponse:
        """Resume an existing, eligible workflow execution."""
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise ValueError("run_id must not be empty.")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a positive number greater than 0.")

        run_lock = self._get_run_lock(clean_run_id)
        with run_lock:
            # Ensure manager is not shutting down, run is not already in-flight, and capacity is available
            with self._lock:
                if self._is_shutting_down:
                    raise ShutdownError("Application is shutting down; workflows cannot be resumed.")
                if clean_run_id in self._active_runs:
                    raise RunConflictError(f"Run '{clean_run_id}' is already executing in-process.")
                if len(self._active_runs) >= self._max_active_runs:
                    raise CapacityExhaustedError(
                        f"Workflow admission rejected: server capacity limit of {self._max_active_runs} "
                        f"active runs reached. Please retry shortly.",
                        max_active_runs=self._max_active_runs,
                        active_runs=len(self._active_runs),
                    )
                # Atomically reserve slot under lock to prevent check-then-act races
                # across concurrent run_locks
                self._active_runs[clean_run_id] = {
                    "run_id": clean_run_id,
                    "task": "",
                    "status": "running",
                    "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }

            effective_store = store or self._store or CheckpointStore()
            if not effective_store.enabled:
                self._unregister_active(clean_run_id)
                raise CheckpointError("CheckpointStore is disabled; resumption is not supported.")

            admitted = effective_store.acquire_workflow_admission(
                run_id=clean_run_id,
                owner_id=self._owner_id,
                max_active_runs=self._max_active_runs,
                lease_ttl_seconds=self._lease_ttl,
            )
            if not admitted:
                self._unregister_active(clean_run_id)
                global_cnt = effective_store.count_active_admissions(self._lease_ttl)
                raise CapacityExhaustedError(
                    f"Workflow admission rejected: server capacity limit of {self._max_active_runs} "
                    f"active runs reached across worker processes. Please retry shortly.",
                    max_active_runs=self._max_active_runs,
                    active_runs=global_cnt,
                )

            adm = effective_store.get_workflow_admission(clean_run_id)
            fence_token = int(adm.get("fence_token", 1)) if adm else 1

            # Load latest checkpoint to assess eligibility under run_lock
            try:
                rec = effective_store.load_latest_checkpoint(clean_run_id)
            except Exception:
                self._unregister_active(clean_run_id)
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
                raise

            latest_state = rec.state or {}
            curr_status = latest_state.get("status")

            plan = latest_state.get("plan", [])
            if not plan:
                self._unregister_active(clean_run_id)
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
                raise ValueError(
                    f"Run '{clean_run_id}' cannot be safely resumed: checkpoint contains no execution plan."
                )

            done_ids = {c["id"] for c in latest_state.get("completed", []) if c.get("status") == "done"}
            all_done = bool(plan) and all(s["id"] in done_ids for s in plan)

            if all_done and curr_status in ("completed", "approved", "edited"):
                self._unregister_active(clean_run_id)
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
                raise RunConflictError(
                    f"Run '{clean_run_id}' is already fully completed and cannot be resumed."
                )

            # Check if unresolved HITL escalation is pending
            if latest_state.get("escalated") and not latest_state.get("resolution"):
                self._unregister_active(clean_run_id)
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
                raise RunConflictError(
                    f"Run '{clean_run_id}' is escalated and awaiting human reviewer resolution "
                    f"in the review queue before resumption."
                )

            task_text = latest_state.get("task", "")
            with self._lock:
                if clean_run_id in self._active_runs:
                    self._active_runs[clean_run_id]["task"] = task_text
                    self._active_runs[clean_run_id]["fence_token"] = fence_token
                    self._active_runs[clean_run_id]["last_heartbeat_at"] = time.monotonic()
                    self._active_runs[clean_run_id]["lease_lost"] = False

            fenced_store = (
                effective_store.with_fencing(self._owner_id, fence_token)
                if effective_store and effective_store.enabled
                else effective_store
            )

            _emit_audit(
                "workflow.resumed",
                "success",
                run_id=clean_run_id,
                metadata={"background": background},
                store=fenced_store,
            )

            if background:
                try:
                    self._executor.submit(
                        self._resume_worker,
                        clean_run_id,
                        max_concurrency,
                        fenced_store,
                        timeout_seconds,
                    )
                    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    return RunResponse(
                        run_id=clean_run_id,
                        task=task_text,
                        status="running",
                        latest_step=rec.step,
                        created_at=rec.created_at,
                        updated_at=now,
                    )
                except Exception:
                    self._unregister_active(clean_run_id)
                    effective_store.release_workflow_admission(
                        clean_run_id, self._owner_id, fence_token=fence_token
                    )
                    raise

        # Synchronous resume
        try:
            state = self._resume_worker(clean_run_id, max_concurrency, fenced_store, timeout_seconds)
            return self.format_run_response(state)
        except Exception:
            self._unregister_active(clean_run_id)
            if effective_store.enabled:
                effective_store.release_workflow_admission(
                    clean_run_id, self._owner_id, fence_token=fence_token
                )
            raise

    def _resume_worker(
        self,
        run_id: str,
        max_concurrency: int | None,
        store: CheckpointStore,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        run_lock = self._get_run_lock(run_id)
        _emit_audit(
            "workflow.started",
            "success",
            run_id=run_id,
            metadata={"resumed": True},
            store=store,
        )
        try:
            state = resume_task(
                run_id=run_id,
                max_concurrency=max_concurrency,
                store=store,
                timeout=timeout_seconds,
            )
            with run_lock:
                with self._lock:
                    active_info = self._active_runs.get(run_id)
                    is_already_cancelled = active_info and active_info.get("status") == "cancelled"
                    default_reason = "User requested cancellation"
                    reason = active_info.get("cancellation_reason", default_reason) if active_info else ""

                if is_already_cancelled and state.get("status") not in ("cancelled", "timed_out"):
                    state["status"] = "cancelled"
                    state["cancellation_reason"] = reason

                self._record_result(run_id, state)

                final_status = state.get("status")
                if final_status == "completed":
                    _emit_audit(
                        "workflow.completed",
                        "success",
                        run_id=run_id,
                        metadata={"quality": state.get("quality"), "confidence": state.get("confidence")},
                        store=store,
                    )
                elif final_status == "timed_out":
                    _emit_audit(
                        "workflow.timed_out",
                        "timeout",
                        run_id=run_id,
                        metadata={"reason": state.get("cancellation_reason")},
                        store=store,
                    )
                elif final_status == "cancelled":
                    _emit_audit(
                        "workflow.cancelled",
                        "success",
                        run_id=run_id,
                        metadata={"reason": state.get("cancellation_reason")},
                        store=store,
                    )
                elif final_status == "failed":
                    _emit_audit(
                        "workflow.failed",
                        "failure",
                        run_id=run_id,
                        metadata={"error": state.get("error")},
                        store=store,
                    )

                return state
        except Exception as exc:
            is_fencing_err = isinstance(exc, AdmissionFencingError)
            with self._lock:
                active_info = self._active_runs.get(run_id)
                lease_lost = bool(active_info and active_info.get("lease_lost"))

            if is_fencing_err or lease_lost:
                logger.warning(
                    "Workflow resumption for run %s lost ownership or hit fencing conflict: %s. "
                    "Skipping persistent state update to protect database integrity.",
                    run_id,
                    exc,
                )
                failed_state: dict[str, Any] = {
                    "run_id": run_id,
                    "task_id": run_id,
                    "status": "failed",
                    "answer": f"Admission lease lost: {sanitize_error(str(exc))}",
                    "error": sanitize_error(str(exc)),
                }
                with self._lock:
                    self._results_cache.pop(run_id, None)
                return failed_state

            logger.exception("Resume for run %s failed with exception: %s", run_id, exc)
            failed_state = {
                "run_id": run_id,
                "task_id": run_id,
                "status": "failed",
                "answer": f"Workflow resumption failed: {sanitize_error(str(exc))}",
                "error": sanitize_error(str(exc)),
            }
            with run_lock:
                with self._lock:
                    active_info = self._active_runs.get(run_id)
                    if active_info and active_info.get("status") == "cancelled":
                        failed_state["status"] = "cancelled"
                        failed_state["cancellation_reason"] = active_info.get("cancellation_reason", "")

                try:
                    transition = "cancelled" if failed_state["status"] == "cancelled" else "error"
                    store.save_checkpoint(run_id, failed_state, transition=transition)
                except Exception:
                    pass
                self._record_result(run_id, failed_state)

                if failed_state.get("status") == "cancelled":
                    _emit_audit(
                        "workflow.cancelled",
                        "success",
                        run_id=run_id,
                        metadata={"reason": failed_state.get("cancellation_reason")},
                        store=store,
                    )
                else:
                    _emit_audit(
                        "workflow.failed",
                        "failure",
                        run_id=run_id,
                        metadata={"error": failed_state.get("error")},
                        store=store,
                    )

                return failed_state
        finally:
            with self._lock:
                active_info = self._active_runs.get(run_id)
                fence_token = active_info.get("fence_token", 1) if active_info else 1
            self._unregister_active(run_id)
            if store and store.enabled:
                store.release_workflow_admission(run_id, self._owner_id, fence_token=fence_token)

    def cancel_run(
        self,
        run_id: str,
        reason: str = "User requested cancellation",
        store: CheckpointStore | None = None,
    ) -> RunResponse:
        """Cooperatively cancel an active run or return terminal status."""
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise ValueError("run_id must not be empty.")

        store = store or CheckpointStore()
        run_lock = self._get_run_lock(clean_run_id)

        with run_lock:
            # 1. Check results cache under run_lock
            with self._lock:
                cached = self._results_cache.get(clean_run_id)
            if cached:
                curr_status = cached.get("status")
                if curr_status in ("completed", "failed", "timed_out", "approved", "edited", "rejected"):
                    raise RunConflictError(
                        f"Cannot cancel run '{clean_run_id}': "
                        f"run has already reached terminal status '{curr_status}'."
                    )
                if curr_status == "cancelled":
                    return self.format_run_response(cached)

            # 2. Check active in-process
            with self._lock:
                is_active = clean_run_id in self._active_runs
                active_info = dict(self._active_runs[clean_run_id]) if is_active else None

            if is_active:
                if store.enabled:
                    summary = store.get_run_summary(clean_run_id)
                    if summary:
                        db_status = summary.get("status")
                        term_statuses = (
                            "completed",
                            "failed",
                            "timed_out",
                            "approved",
                            "edited",
                            "rejected",
                        )
                        if db_status in term_statuses:
                            raise RunConflictError(
                                f"Cannot cancel run '{clean_run_id}': "
                                f"run has already reached terminal status '{db_status}'."
                            )
                active_status = active_info.get("status") if active_info else None
                if active_status in ("completed", "failed", "timed_out", "approved", "edited", "rejected"):
                    raise RunConflictError(
                        f"Cannot cancel run '{clean_run_id}': "
                        f"run has already reached terminal status '{active_status}'."
                    )
                if active_status == "cancelled":
                    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    active_reason = active_info.get("cancellation_reason", reason)
                    return RunResponse(
                        run_id=clean_run_id,
                        task=active_info.get("task", ""),
                        status="cancelled",
                        answer=f"(workflow execution cancelled: {active_reason})",
                        created_at=active_info.get("started_at", now),
                        updated_at=now,
                    )

                # Signal cooperative cancellation
                from ..graph.cancellation import get_cancellation_registry

                reg = get_cancellation_registry()
                reg.cancel(clean_run_id, reason=reason)

                with self._lock:
                    if clean_run_id in self._active_runs:
                        self._active_runs[clean_run_id]["status"] = "cancelled"
                        self._active_runs[clean_run_id]["cancellation_reason"] = reason

                now = datetime.datetime.now(datetime.timezone.utc).isoformat()
                task_text = active_info.get("task", "") if active_info else ""
                started_at = active_info.get("started_at", now) if active_info else now
                _emit_audit(
                    "workflow.cancelled",
                    "success",
                    run_id=clean_run_id,
                    metadata={"reason": reason},
                    store=store,
                )
                return RunResponse(
                    run_id=clean_run_id,
                    task=task_text,
                    status="cancelled",
                    answer=f"(workflow execution cancelled: {reason})",
                    created_at=started_at,
                    updated_at=now,
                )

            # 3. Check persistence store if not active
            if store.enabled:
                try:
                    rec = store.cancel_run_administratively(clean_run_id, reason=reason)
                    _emit_audit(
                        "workflow.cancelled",
                        "success",
                        run_id=clean_run_id,
                        metadata={"reason": reason},
                        store=store,
                    )
                    return self.format_run_response(rec.state, step=rec.step, created_at=rec.created_at)
                except CheckpointNotFoundError:
                    pass
                except CheckpointConflictError as c_err:
                    raise RunConflictError(str(c_err)) from c_err

                summary = store.get_run_summary(clean_run_id)
                if summary:
                    curr_status = summary.get("status")
                    if curr_status == "cancelled":
                        return RunResponse(
                            run_id=clean_run_id,
                            task=summary.get("task", ""),
                            status="cancelled",
                            latest_step=summary.get("latest_step"),
                            created_at=summary.get("created_at"),
                            updated_at=summary.get("updated_at"),
                        )
                    if curr_status in ("completed", "failed", "timed_out", "approved", "edited", "rejected"):
                        raise RunConflictError(
                            f"Cannot cancel run '{clean_run_id}': "
                            f"run has already reached terminal status '{curr_status}'."
                        )

            # 4. Check memory cache fallback
            with self._lock:
                cached_state = self._results_cache.get(clean_run_id)
            if cached_state is not None:
                curr_status = cached_state.get("status")
                if curr_status == "cancelled":
                    return self.format_run_response(cached_state)
                if curr_status in ("completed", "failed", "timed_out", "approved", "edited", "rejected"):
                    raise RunConflictError(
                        f"Cannot cancel run '{clean_run_id}': "
                        f"run has already reached terminal status '{curr_status}'."
                    )

            raise CheckpointNotFoundError(f"Run '{clean_run_id}' not found.")

    def get_run(self, run_id: str, store: CheckpointStore | None = None) -> RunResponse:
        """Retrieve the latest status, results, and HITL metadata for a run."""
        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise ValueError("run_id must not be empty.")

        # 1. If currently active in-process
        with self._lock:
            if clean_run_id in self._active_runs:
                active_meta = self._active_runs[clean_run_id]
                return RunResponse(
                    run_id=clean_run_id,
                    task=active_meta.get("task", ""),
                    status="running",
                    created_at=active_meta.get("started_at"),
                    updated_at=active_meta.get("started_at"),
                )

        # 2. Check persistence store
        store = store or CheckpointStore()
        rec = None
        if store.enabled:
            try:
                rec = store.load_latest_checkpoint(clean_run_id)
            except CheckpointNotFoundError:
                rec = None

        if rec is not None:
            return self.format_run_response(rec.state, step=rec.step, created_at=rec.created_at)

        # 3. Check memory cache fallback
        with self._lock:
            cached_state = self._results_cache.get(clean_run_id)
        if cached_state is not None:
            return self.format_run_response(cached_state)

        # 4. Check run index table
        if store.enabled:
            summary = store.get_run_summary(clean_run_id)
            if summary:
                return RunResponse(
                    run_id=clean_run_id,
                    task=summary.get("task", ""),
                    status=summary.get("status", "unknown"),
                    latest_step=summary.get("latest_step"),
                    created_at=summary.get("created_at"),
                    updated_at=summary.get("updated_at"),
                )

        raise CheckpointNotFoundError(f"Run '{clean_run_id}' not found.")

    def format_run_response(
        self,
        state: dict[str, Any],
        step: int | None = None,
        created_at: str | None = None,
    ) -> RunResponse:
        """Safely format a graph state dict into a public RunResponse model."""
        run_id = state.get("run_id") or state.get("task_id", "")
        task_text = state.get("task", "")
        status = state.get("status", "unknown")
        raw_answer = state.get("answer", "")

        # Sanitize answer text from potential credentials
        from ..persistence.store import sanitize_state_for_persistence

        clean_answer = sanitize_state_for_persistence(raw_answer) if raw_answer else None

        completed_ids = [
            c["id"]
            for c in state.get("completed", [])
            if c.get("status") in ("done", "approved", "edited")
        ]
        failed_ids = [
            c["id"]
            for c in (state.get("completed", []) + state.get("failed", []))
            if c.get("status") in ("failed", "rejected")
        ]
        blocked_ids = [
            c["id"]
            for c in (state.get("completed", []) + state.get("blocked", []))
            if c.get("status") == "blocked"
        ]

        # Review summary
        quality = state.get("quality")
        confidence = state.get("confidence")
        escalated = bool(state.get("escalated"))
        esc_reason = state.get("escalation_reason")
        review_summary = RunReviewSummary(
            quality=round(float(quality), 3) if quality is not None else None,
            confidence=round(float(confidence), 3) if confidence is not None else None,
            escalated=escalated,
            escalation_reason=esc_reason,
        )

        # HITL metadata
        hitl_summary = None
        has_escalated = escalated or bool(state.get("escalations")) or bool(state.get("resolution"))
        if has_escalated:
            q = ReviewQueue()
            item = next((i for i in q.pending() if i.task_id == run_id), None)
            hitl_summary = HitlStatusSummary(
                escalated=True,
                escalation_reason=esc_reason or (item.reason if item else None),
                escalated_subtask_id=(
                    state.get("escalated_subtask_id") or (item.subtask_id if item else None)
                ),
                pending_review=bool(item),
                approval_level=item.level if item else None,
                resolution=state.get("resolution"),
            )

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        return RunResponse(
            run_id=run_id,
            task=task_text,
            status=status,
            answer=clean_answer,
            completed_subtasks=list(dict.fromkeys(completed_ids)),
            failed_subtasks=list(dict.fromkeys(failed_ids)),
            blocked_subtasks=list(dict.fromkeys(blocked_ids)),
            latest_step=step,
            review=review_summary,
            hitl=hitl_summary,
            usage=state.get("usage"),
            created_at=created_at or now,
            updated_at=now,
        )

    def shutdown(
        self,
        grace_period: float | None = None,
        store: CheckpointStore | None = None,
    ) -> None:
        """Perform a bounded graceful shutdown of the execution manager.

        1. Set is_shutting_down to reject any incoming start/resume requests.
        2. Cooperatively signal all active runs via their cancellation tokens.
        3. Wait up to `grace_period` seconds for active workers to complete/stop.
        4. Forcefully stop executor without waiting if grace period expires.
        5. Persist any lingering active runs as 'interrupted'.
        """
        import time

        with self._lock:
            self._is_shutting_down = True
            active_run_ids = list(self._active_runs.keys())

        logger.info(
            "RunManager initiating shutdown with %d active runs (grace period: %s seconds).",
            len(active_run_ids),
            grace_period,
        )

        # Signal cooperative cancellation to all in-flight runs
        for run_id in active_run_ids:
            try:
                self.cancel_run(run_id, reason="Application shutdown", store=store)
            except Exception as exc:
                logger.debug("Error requesting cancellation for run %s during shutdown: %s", run_id, exc)

        # Bounded wait for active workers
        if grace_period is None:
            settings = get_settings()
            grace_period = settings.api_shutdown_grace_seconds

        timeout = max(0.0, float(grace_period))
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if self.active_runs_count() == 0:
                break
            time.sleep(0.05)

        # Stop executor without blocking
        self._executor.shutdown(wait=False, cancel_futures=True)

        # Stop heartbeat loop thread
        self._heartbeat_stop_event.set()
        if hasattr(self, "_heartbeat_thread") and self._heartbeat_thread.is_alive():
            try:
                self._heartbeat_thread.join(timeout=0.5)
            except Exception:
                pass

        # If any runs are still tracked as active, persist them as interrupted
        with self._lock:
            lingering = list(self._active_runs.keys())

        if lingering and store and store.enabled:
            for run_id in lingering:
                try:
                    store.recover_run_as_interrupted(
                        run_id,
                        reason="Application shutdown exceeded grace period",
                        force=True,
                        owner_id=self._owner_id,
                    )
                    store.release_workflow_admission(run_id, self._owner_id)
                    _emit_audit(
                        "workflow.interrupted",
                        "interrupted",
                        run_id=run_id,
                        metadata={"reason": "Application shutdown exceeded grace period"},
                        store=store,
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to mark lingering run %s as interrupted during shutdown: %s",
                        run_id,
                        exc,
                    )

        with self._lock:
            self._active_runs.clear()

    def recover_interrupted_runs(
        self,
        store: CheckpointStore | None = None,
        reason: str = "Process terminated while run was in progress",
    ) -> list[str]:
        """Identify and transition any runs left in active/running state by a previous process.

        Guarantees:
        - Never modifies runs with terminal status.
        - Leaves recovered runs in explicit 'interrupted' state without auto-executing.
        - Shields runs actively heartbeated by another live worker process.
        - Strictly idempotent across multiple calls.
        """
        active_store = store or self._store or CheckpointStore()
        if not active_store.enabled:
            return []

        interrupted_ids = active_store.find_interrupted_runs(lease_ttl_seconds=self._lease_ttl)
        recovered: list[str] = []
        for rid in interrupted_ids:
            run_lock = self._get_run_lock(rid)
            with run_lock:
                rec = active_store.recover_run_as_interrupted(rid, reason=reason)
                if rec is not None:
                    recovered.append(rid)
                    self._record_result(rid, rec.state)
                    _emit_audit(
                        "workflow.recovered",
                        "success",
                        run_id=rid,
                        metadata={"reason": reason},
                        store=active_store,
                        dedup=True,
                    )

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._lock:
            self._last_recovery_summary = {
                "status": "completed",
                "recovered_count": len(recovered),
                "recovered_run_ids": list(recovered),
                "timestamp": now,
            }

        return recovered


# Global default manager instance
_DEFAULT_MANAGER: RunManager | None = None
_MANAGER_LOCK = threading.Lock()


def get_run_manager() -> RunManager:
    """Retrieve or initialize the process-wide RunManager instance."""
    global _DEFAULT_MANAGER
    if _DEFAULT_MANAGER is None:
        with _MANAGER_LOCK:
            if _DEFAULT_MANAGER is None:
                _DEFAULT_MANAGER = RunManager()
    return _DEFAULT_MANAGER


def reset_run_manager() -> None:
    """Reset the process-wide RunManager singleton instance (used for test isolation)."""
    global _DEFAULT_MANAGER
    with _MANAGER_LOCK:
        _DEFAULT_MANAGER = None
