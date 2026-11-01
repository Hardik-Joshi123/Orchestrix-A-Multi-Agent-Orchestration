"""Automated database backup scheduling, cross-process leasing, and operational monitoring.

Coordinates periodic online SQLite database backups, integrity verification,
retention lifecycle pruning, and operational observability with fence-token-based
leadership leasing across worker processes.
"""

from __future__ import annotations

import datetime
import logging
import os
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from ..config import Settings, get_settings
from ..observability.audit import (
    AuditEventType,
    AuditOutcome,
    record_audit_event,
    sanitize_audit_metadata,
)
from .backup import (
    BackupManager,
    BackupValidationError,
    RestoreDrillResult,
)
from .store import CheckpointStore

logger = logging.getLogger(__name__)

_GLOBAL_BACKUP_SCHEDULER: DatabaseBackupScheduler | None = None
_GLOBAL_SCHEDULER_LOCK = threading.Lock()


class DatabaseBackupScheduler:
    """Coordinates automated periodic SQLite database backups with cross-process fencing.

    Guarantees:
    - Cross-process leadership fencing via `scheduler_leases` table.
    - Prevents overlapping backup jobs across threads and processes.
    - End-to-end backup cycle: backup creation -> integrity check -> retention pruning.
    - Preserves single latest valid backup unconditionally.
    - Structured audit logging for backup started, completed, and failed events.
    - Bounded retry backoff on transient failures.
    - Safe operational diagnostics exposure without credential leakage.
    - Graceful startup, manual on-demand triggers, and bounded shutdown.
    """

    def __init__(
        self,
        store: CheckpointStore | None = None,
        backup_manager: BackupManager | None = None,
        settings: Settings | None = None,
        owner_id: str | None = None,
        enabled: bool | None = None,
        interval_seconds: float | None = None,
        backup_dir: str | None = None,
        retention_count: int | None = None,
        retention_days: int | None = None,
        max_duration_seconds: float | None = None,
        max_retries: int | None = None,
        retry_backoff_seconds: float | None = None,
        max_backoff_seconds: float | None = None,
        lease_ttl_seconds: float | None = None,
        heartbeat_interval_seconds: float | None = None,
        lease_name: str = "database_backup",
        restore_drill_enabled: bool | None = None,
        restore_drill_interval_seconds: float | None = None,
        restore_drill_max_duration_seconds: float | None = None,
        restore_drill_history_limit: int | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        cfg = settings if settings is not None else get_settings()
        self._settings = cfg
        self._store = store if store is not None else CheckpointStore()
        self._clock = clock if clock is not None else time.time

        if enabled is not None:
            self._enabled = bool(enabled)
        else:
            self._enabled = bool(getattr(cfg, "backup_scheduler_enabled", False))

        if interval_seconds is not None:
            self._interval = max(1.0, float(interval_seconds))
        else:
            self._interval = float(getattr(cfg, "backup_interval_seconds", 86400.0))

        if backup_dir is not None:
            self._backup_dir = str(backup_dir).strip()
        else:
            self._backup_dir = str(getattr(cfg, "backup_dir", "backups")).strip()

        if retention_count is not None:
            self._retention_count = max(0, int(retention_count))
        else:
            self._retention_count = int(getattr(cfg, "backup_retention_count", 10))

        if retention_days is not None:
            self._retention_days = max(0, int(retention_days))
        else:
            self._retention_days = int(getattr(cfg, "backup_retention_days", 30))

        if max_duration_seconds is not None:
            self._max_duration = max(1.0, float(max_duration_seconds))
        else:
            self._max_duration = float(getattr(cfg, "backup_max_duration_seconds", 300.0))

        if max_retries is not None:
            self._max_retries = max(0, int(max_retries))
        else:
            self._max_retries = int(getattr(cfg, "backup_max_retries", 3))

        if retry_backoff_seconds is not None:
            self._retry_backoff = max(0.1, float(retry_backoff_seconds))
        else:
            self._retry_backoff = float(getattr(cfg, "backup_retry_backoff_seconds", 5.0))

        if max_backoff_seconds is not None:
            self._max_backoff = max(self._retry_backoff, float(max_backoff_seconds))
        else:
            self._max_backoff = float(getattr(cfg, "backup_max_backoff_seconds", 60.0))

        if lease_ttl_seconds is not None:
            self._lease_ttl = max(1.0, float(lease_ttl_seconds))
        else:
            self._lease_ttl = float(getattr(cfg, "backup_scheduler_lease_ttl_seconds", 60.0))

        if heartbeat_interval_seconds is not None:
            self._heartbeat_interval = max(0.1, float(heartbeat_interval_seconds))
        else:
            self._heartbeat_interval = float(
                getattr(cfg, "backup_scheduler_heartbeat_interval_seconds", 10.0)
            )

        self._lease_name = (lease_name or "database_backup").strip()

        if restore_drill_enabled is not None:
            self._drill_enabled = bool(restore_drill_enabled)
        else:
            self._drill_enabled = bool(getattr(cfg, "restore_drill_enabled", False))

        if restore_drill_interval_seconds is not None:
            self._drill_interval = max(10.0, float(restore_drill_interval_seconds))
        else:
            self._drill_interval = float(
                getattr(cfg, "restore_drill_interval_seconds", 604800.0)
            )

        if restore_drill_max_duration_seconds is not None:
            self._drill_max_duration = max(1.0, float(restore_drill_max_duration_seconds))
        else:
            self._drill_max_duration = float(
                getattr(cfg, "restore_drill_max_duration_seconds", 300.0)
            )

        if restore_drill_history_limit is not None:
            self._drill_history_limit = max(1, int(restore_drill_history_limit))
        else:
            self._drill_history_limit = int(
                getattr(cfg, "restore_drill_history_limit", 50)
            )

        self._drill_exec_lock = threading.Lock()

        # Operational state and metrics for restore drills
        self._last_drill_at: str | None = None
        self._last_drill_success_at: str | None = None
        self._last_drill_failed_at: str | None = None
        self._last_drill_status: str | None = None
        self._last_drill_duration_ms: float = 0.0
        self._last_drill_backup_path: str | None = None
        self._last_drill_backup_age_seconds: float = 0.0
        self._last_drill_tables_verified: int = 0
        self._last_drill_records_verified: int = 0
        self._total_drills_count: int = 0
        self._drill_failure_count: int = 0
        self._last_drill_error: str | None = None
        self._next_drill_due_time: float = 0.0

        if owner_id and owner_id.strip():
            self._owner_id = owner_id.strip()
        else:
            self._owner_id = f"backup-scheduler-{os.getpid()}-{uuid.uuid4().hex[:8]}"

        self._backup_manager = (
            backup_manager
            if backup_manager is not None
            else BackupManager(
                db_path=self._store.db_path,
                backup_dir=self._backup_dir,
                settings=cfg,
            )
        )

        self._lock = threading.RLock()
        self._exec_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._is_running = False

        # Operational state
        self._is_leader = False
        self._fence_token: int | None = None
        self._leadership_state = "standby" if self._enabled else "disabled"
        self._next_due_time: float = 0.0

        # Metrics
        self._last_run_at: str | None = None
        self._last_success_at: str | None = None
        self._last_failed_at: str | None = None
        self._last_duration_ms: float = 0.0
        self._last_backup_path: str | None = None
        self._last_size_bytes: int = 0
        self._last_integrity: str | None = None
        self._last_tables_count: int = 0
        self._total_backups_count: int = 0
        self._failure_count: int = 0
        self._consecutive_failures: int = 0
        self._last_error: str | None = None

        self._initialize_from_existing_backups()

    def _initialize_from_existing_backups(self) -> None:
        """Inspect storage to prime metrics and schedule next run based on last backup."""
        try:
            backups = self._backup_manager.list_backups(dest_dir=self._backup_dir)
            if backups:
                latest = backups[0]
                self._last_backup_path = latest.backup_path
                self._last_size_bytes = latest.size_bytes
                self._last_integrity = latest.integrity
                self._last_tables_count = len(latest.table_counts)
                self._last_success_at = latest.timestamp
                self._total_backups_count = len(backups)
                try:
                    latest_dt = datetime.datetime.fromisoformat(latest.timestamp)
                    latest_ts = latest_dt.timestamp()
                    now_ts = self._clock()
                    if (now_ts - latest_ts) < self._interval:
                        self._next_due_time = latest_ts + self._interval
                    else:
                        self._next_due_time = now_ts
                except Exception:
                    self._next_due_time = self._clock()
            else:
                self._next_due_time = self._clock()
        except Exception as exc:
            logger.debug("Failed inspecting existing backups for scheduler startup: %s", exc)
            self._next_due_time = self._clock()

        try:
            latest_drill = self._store.get_latest_restore_drill()
            if latest_drill:
                self._last_drill_at = latest_drill.get("started_at")
                self._last_drill_status = latest_drill.get("status")
                self._last_drill_duration_ms = latest_drill.get("duration_ms", 0.0)
                self._last_drill_backup_path = latest_drill.get("backup_path")
                self._last_drill_backup_age_seconds = latest_drill.get(
                    "backup_age_seconds", 0.0
                )
                self._last_drill_tables_verified = latest_drill.get("tables_verified", 0)
                self._last_drill_records_verified = latest_drill.get("records_verified", 0)
                if latest_drill.get("status") == "success":
                    self._last_drill_success_at = (
                        latest_drill.get("completed_at") or latest_drill.get("started_at")
                    )
                else:
                    self._last_drill_failed_at = (
                        latest_drill.get("completed_at") or latest_drill.get("started_at")
                    )
                    self._last_drill_error = latest_drill.get("error_text")
                all_drills = self._store.list_restore_drills(limit=500)
                self._total_drills_count = len(all_drills)
                self._drill_failure_count = sum(
                    1 for d in all_drills if d.get("status") == "failed"
                )

                if self._last_drill_at:
                    try:
                        drill_dt = datetime.datetime.fromisoformat(self._last_drill_at)
                        drill_ts = drill_dt.timestamp()
                        now_ts = self._clock()
                        if (now_ts - drill_ts) < self._drill_interval:
                            self._next_drill_due_time = drill_ts + self._drill_interval
                        else:
                            self._next_drill_due_time = now_ts
                    except Exception:
                        self._next_drill_due_time = self._clock()
                else:
                    self._next_drill_due_time = self._clock()
            else:
                self._next_drill_due_time = self._clock()
        except Exception as exc:
            logger.debug("Failed initializing restore drill state: %s", exc)
            self._next_drill_due_time = self._clock()

    @property
    def enabled(self) -> bool:
        """Whether automated periodic database backup scheduling is enabled."""
        return self._enabled

    @property
    def is_running(self) -> bool:
        """Whether the background scheduler worker thread is actively running."""
        with self._lock:
            return self._is_running and self._thread is not None and self._thread.is_alive()

    @property
    def is_leader(self) -> bool:
        """Whether this scheduler currently holds the cross-process leadership lease."""
        with self._lock:
            return self._is_leader

    @property
    def owner_id(self) -> str:
        """Unique identifier of this scheduler worker instance."""
        return self._owner_id

    @property
    def fence_token(self) -> int | None:
        """Active generation token of the leadership lease, if leader."""
        with self._lock:
            return self._fence_token

    @property
    def leadership_state(self) -> str:
        """Current leadership state: 'leader', 'standby', 'lost', 'stopped', 'disabled'."""
        with self._lock:
            return self._leadership_state

    @property
    def interval_seconds(self) -> float:
        """Configured interval in seconds between automated backup executions."""
        return self._interval

    @property
    def backup_dir(self) -> str:
        """Configured directory path where backups are stored."""
        return self._backup_dir

    @property
    def restore_drill_enabled(self) -> bool:
        """Whether automated database restore drill verification is enabled."""
        return self._drill_enabled

    @property
    def restore_drill_interval_seconds(self) -> float:
        """Configured interval in seconds between automated restore drill runs."""
        return self._drill_interval

    def start(self) -> None:
        """Start the background scheduler thread if enabled and not already running."""
        if not self._enabled:
            logger.info("Database backup scheduler is disabled; worker thread not started.")
            return

        with self._lock:
            if self._is_running and self._thread and self._thread.is_alive():
                logger.debug("Database backup scheduler background thread is already running.")
                return

            self._stop_event.clear()
            self._wake_event.clear()
            self._is_running = True

            thread_name = f"BackupScheduler-{self._owner_id[:16]}"
            self._thread = threading.Thread(
                target=self._run_loop,
                name=thread_name,
                daemon=True,
            )
            self._thread.start()
            logger.info("Started database backup scheduler thread '%s'.", thread_name)

    def stop(self, timeout: float = 5.0) -> None:
        """Gracefully stop background worker thread and release cross-process lease."""
        with self._lock:
            was_running = self._is_running
            self._is_running = False

        if was_running:
            self._stop_event.set()
            self._wake_event.set()
            thread = self._thread
            if thread and thread.is_alive():
                thread.join(timeout=max(0.1, float(timeout)))

        with self._lock:
            self._thread = None
            fence = self._fence_token
            was_leader = self._is_leader
            self._is_leader = False
            self._leadership_state = "stopped" if self._enabled else "disabled"
            self._fence_token = None

        if was_leader and fence is not None and self._store.enabled:
            try:
                now_dt = datetime.datetime.fromtimestamp(
                    self._clock(), tz=datetime.timezone.utc
                )
                self._store.release_scheduler_lease(
                    lease_name=self._lease_name,
                    owner_id=self._owner_id,
                    fence_token=fence,
                    now_dt=now_dt,
                )
                logger.info("Released database backup scheduler lease '%s'.", self._lease_name)
            except Exception as exc:
                logger.debug("Error releasing backup scheduler lease on shutdown: %s", exc)

    def wake(self) -> None:
        """Signal background thread to wake up immediately and evaluate lease/work."""
        self._wake_event.set()

    def _run_loop(self) -> None:
        """Background loop coordinating periodic leasing, heartbeats, and backups."""
        while not self._stop_event.is_set():
            now_ts = self._clock()
            now_dt = datetime.datetime.fromtimestamp(now_ts, tz=datetime.timezone.utc)

            # 1. Manage leadership lease
            if not self._is_leader:
                try:
                    acquired, fence, _rec = self._store.acquire_scheduler_lease(
                        lease_name=self._lease_name,
                        owner_id=self._owner_id,
                        lease_ttl_seconds=self._lease_ttl,
                        now_dt=now_dt,
                    )
                    with self._lock:
                        if acquired:
                            self._is_leader = True
                            self._fence_token = fence
                            self._leadership_state = "leader"
                            logger.info(
                                "Database backup scheduler acquired lease '%s' (fence=%d).",
                                self._lease_name,
                                fence,
                            )
                        else:
                            self._is_leader = False
                            self._leadership_state = "standby"
                except Exception as exc:
                    logger.debug("Error attempting to acquire backup scheduler lease: %s", exc)
                    with self._lock:
                        self._is_leader = False
                        self._leadership_state = "standby"
            else:
                try:
                    renewed = self._store.renew_scheduler_lease(
                        lease_name=self._lease_name,
                        owner_id=self._owner_id,
                        fence_token=self._fence_token or 1,
                        lease_ttl_seconds=self._lease_ttl,
                        now_dt=now_dt,
                    )
                    with self._lock:
                        if renewed:
                            self._leadership_state = "leader"
                        else:
                            self._is_leader = False
                            self._fence_token = None
                            self._leadership_state = "lost"
                            logger.warning(
                                "Lost database backup scheduler lease '%s'.",
                                self._lease_name,
                            )
                except Exception as exc:
                    logger.debug("Error renewing backup scheduler lease: %s", exc)
                    with self._lock:
                        self._is_leader = False
                        self._leadership_state = "lost"

            # 2. If leader, check if scheduled backup is due
            if self._is_leader:
                if self._next_due_time <= 0 or now_ts >= self._next_due_time:
                    try:
                        self.run_backup_cycle(actor="scheduler")
                    except Exception as exc:
                        logger.error("Unhandled error in scheduled backup cycle: %s", exc)

            # 2b. If leader and restore drills enabled, check if drill is due
            if self._is_leader and self._drill_enabled:
                if self._next_drill_due_time <= 0 or now_ts >= self._next_drill_due_time:
                    try:
                        self.run_restore_drill(actor="scheduler")
                    except Exception as exc:
                        logger.error("Unhandled error in scheduled restore drill: %s", exc)

            # 3. Sleep until next heartbeat interval or wake signal
            self._wake_event.clear()
            self._stop_event.wait(timeout=self._heartbeat_interval)

    def run_backup_cycle(
        self,
        now: float | None = None,
        actor: str = "scheduler",
        operator: str | None = None,
        reason: str | None = None,
        tag: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Execute a complete database backup, validation, and retention cycle.

        Enforces non-overlapping execution and cross-process lease ownership.
        """
        # 1. Prevent concurrent overlapping backup jobs within the same process
        acquired = self._exec_lock.acquire(blocking=False)
        if not acquired:
            logger.warning("Backup cycle skipped: previous backup execution is still in progress.")
            return {
                "status": "skipped",
                "message": "Backup execution already in progress.",
                "is_leader": self._is_leader,
                "fence_token": self._fence_token,
            }

        try:
            return self._execute_backup_cycle(
                now=now,
                actor=actor,
                operator=operator,
                reason=reason,
                tag=tag,
                force=force,
            )
        finally:
            self._exec_lock.release()

    def _execute_backup_cycle(
        self,
        now: float | None = None,
        actor: str = "scheduler",
        operator: str | None = None,
        reason: str | None = None,
        tag: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        # 2. Verify leadership lease unless explicitly forced (e.g. manual CLI)
        if not force and not self._is_leader:
            logger.debug("Backup cycle skipped: instance is not the scheduler lease leader.")
            return {
                "status": "skipped",
                "message": "Instance is in standby mode; not the active scheduler leader.",
                "is_leader": False,
                "fence_token": self._fence_token,
            }

        current_ts = now if now is not None else self._clock()
        now_dt = datetime.datetime.fromtimestamp(current_ts, tz=datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        clean_actor = operator or actor
        fence = self._fence_token

        # 3. Record backup started audit event
        try:
            record_audit_event(
                event_type=AuditEventType.BACKUP_STARTED,
                outcome=AuditOutcome.SUCCESS,
                actor=clean_actor,
                metadata={
                    "trigger": actor,
                    "destination_dir": self._backup_dir,
                    "fence_token": fence,
                    "reason": reason or "scheduled periodic database backup",
                },
                store=self._store,
            )
        except Exception as exc:
            logger.debug("Failed recording backup started audit event: %s", exc)

        # 4. Execute backup with bounded retries and integrity validation
        attempts = 0
        max_attempts = self._max_retries + 1 if actor == "scheduler" else 1
        last_exc: Exception | None = None
        start_mono = time.monotonic()

        while attempts < max_attempts:
            attempts += 1
            try:
                backup_tag = tag or f"sched-{int(current_ts)}"
                meta = self._backup_manager.create_backup(
                    dest=self._backup_dir,
                    tag=backup_tag,
                )

                # Validate backup archive integrity
                val = self._backup_manager.validate_backup(
                    backup_path=meta.backup_path,
                    deep=True,
                )
                if not val.is_valid:
                    err_msg = "; ".join(val.errors) if val.errors else "Integrity check failed"
                    raise BackupValidationError(f"Backup validation failed: {err_msg}")

                # Enforce retention policy (protects latest backup unconditionally)
                ret = self._backup_manager.clean_backups(
                    dest_dir=self._backup_dir,
                    keep_count=self._retention_count,
                    keep_days=self._retention_days,
                )

                duration_ms = round((time.monotonic() - start_mono) * 1000.0, 2)

                # Update operational metrics
                with self._lock:
                    self._last_run_at = now_iso
                    self._last_success_at = now_iso
                    self._last_duration_ms = duration_ms
                    self._last_backup_path = meta.backup_path
                    self._last_size_bytes = meta.size_bytes
                    self._last_integrity = val.integrity_check
                    self._last_tables_count = len(meta.table_counts)
                    self._total_backups_count += 1
                    self._consecutive_failures = 0
                    self._last_error = None
                    self._next_due_time = self._clock() + self._interval

                # Record backup completed audit event
                try:
                    record_audit_event(
                        event_type=AuditEventType.BACKUP_COMPLETED,
                        outcome=AuditOutcome.SUCCESS,
                        actor=clean_actor,
                        metadata={
                            "backup_path": meta.backup_path,
                            "size_bytes": meta.size_bytes,
                            "integrity": val.integrity_check,
                            "duration_ms": duration_ms,
                            "tables_count": len(meta.table_counts),
                            "retained_count": ret.total_retained,
                            "deleted_count": ret.total_deleted,
                            "fence_token": fence,
                        },
                        store=self._store,
                    )
                except Exception as exc:
                    logger.debug("Failed recording backup completed audit event: %s", exc)

                return {
                    "status": "success",
                    "message": "Database backup completed and verified successfully.",
                    "metadata": meta.to_dict(),
                    "validation": val.to_dict(),
                    "retention": ret.to_dict(),
                    "duration_ms": duration_ms,
                    "fence_token": fence,
                }

            except Exception as exc:
                last_exc = exc
                sanitized_err = sanitize_audit_metadata(str(exc))
                logger.warning(
                    "Database backup attempt %d/%d failed: %s",
                    attempts,
                    max_attempts,
                    sanitized_err,
                )
                if attempts < max_attempts and not self._stop_event.is_set():
                    backoff = min(
                        self._max_backoff,
                        self._retry_backoff * (2 ** (attempts - 1)),
                    )
                    self._stop_event.wait(timeout=backoff)

        # 5. Handle failure after retry exhaustion
        duration_ms = round((time.monotonic() - start_mono) * 1000.0, 2)
        sanitized_err = sanitize_audit_metadata(str(last_exc)) if last_exc else "Unknown error"

        with self._lock:
            self._last_run_at = now_iso
            self._last_failed_at = now_iso
            self._last_duration_ms = duration_ms
            self._failure_count += 1
            self._consecutive_failures += 1
            self._last_error = str(sanitized_err)
            self._next_due_time = self._clock() + self._retry_backoff

        try:
            record_audit_event(
                event_type=AuditEventType.BACKUP_FAILED,
                outcome=AuditOutcome.FAILURE,
                actor=clean_actor,
                metadata={
                    "error": sanitized_err,
                    "attempts": attempts,
                    "duration_ms": duration_ms,
                    "fence_token": fence,
                },
                store=self._store,
            )
        except Exception as exc:
            logger.debug("Failed recording backup failed audit event: %s", exc)

        return {
            "status": "failed",
            "message": f"Database backup failed after {attempts} attempts: {sanitized_err}",
            "error": str(sanitized_err),
            "duration_ms": duration_ms,
            "fence_token": fence,
        }

    def run_restore_drill(
        self,
        backup_path: str | None = None,
        actor: str = "scheduler",
        operator: str | None = None,
        reason: str | None = None,
        target_temp_dir: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Execute an isolated non-destructive database restore drill.

        Guarantees non-overlapping execution and records results in SQLite history and audit logs.
        """
        acquired = self._drill_exec_lock.acquire(blocking=False)
        if not acquired:
            logger.warning(
                "Restore drill skipped: previous drill execution is still in progress."
            )
            return {
                "status": "skipped",
                "message": "Restore drill execution already in progress.",
                "is_leader": self._is_leader,
                "fence_token": self._fence_token,
            }

        try:
            return self._execute_restore_drill(
                backup_path=backup_path,
                actor=actor,
                operator=operator,
                reason=reason,
                target_temp_dir=target_temp_dir,
                force=force,
            )
        finally:
            self._drill_exec_lock.release()

    def _execute_restore_drill(
        self,
        backup_path: str | None = None,
        actor: str = "scheduler",
        operator: str | None = None,
        reason: str | None = None,
        target_temp_dir: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        if not force and not self._is_leader:
            logger.debug(
                "Restore drill skipped: instance is not the scheduler lease leader."
            )
            return {
                "status": "skipped",
                "message": "Instance is in standby mode; not the active scheduler leader.",
                "is_leader": False,
                "fence_token": self._fence_token,
            }

        current_ts = self._clock()
        now_dt = datetime.datetime.fromtimestamp(current_ts, tz=datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        clean_actor = operator or actor
        fence = self._fence_token

        # Audit event: RESTORE_DRILL_STARTED
        try:
            record_audit_event(
                event_type=AuditEventType.RESTORE_DRILL_STARTED,
                outcome=AuditOutcome.SUCCESS,
                actor=clean_actor,
                metadata={
                    "trigger": actor,
                    "target_backup": backup_path or "latest",
                    "fence_token": fence,
                    "reason": reason or "scheduled periodic restore drill",
                },
                store=self._store,
            )
        except Exception as exc:
            logger.debug("Failed recording restore drill started audit event: %s", exc)

        start_mono = time.monotonic()
        try:
            drill_res = self._backup_manager.run_restore_drill(
                backup_path=backup_path,
                target_temp_dir=target_temp_dir,
                sample_records=True,
            )
        except Exception as exc:
            duration_ms = round((time.monotonic() - start_mono) * 1000.0, 2)
            sanitized_err = sanitize_audit_metadata(str(exc))
            drill_res = RestoreDrillResult(
                drill_id=f"drill_{uuid.uuid4().hex[:12]}",
                success=False,
                backup_path=str(backup_path or ""),
                duration_ms=duration_ms,
                errors=[str(sanitized_err)],
                cleaned_up=True,
                timestamp=now_iso,
            )

        duration_ms = drill_res.duration_ms
        completed_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        status_val = "success" if drill_res.success else "failed"
        err_msg = "; ".join(drill_res.errors) if drill_res.errors else None

        # Update operational metrics
        with self._lock:
            self._last_drill_at = now_iso
            self._last_drill_status = status_val
            self._last_drill_duration_ms = duration_ms
            self._last_drill_backup_path = drill_res.backup_path
            self._last_drill_backup_age_seconds = drill_res.backup_age_seconds
            self._last_drill_tables_verified = len(drill_res.tables_verified)
            self._last_drill_records_verified = drill_res.records_verified
            self._total_drills_count += 1

            if drill_res.success:
                self._last_drill_success_at = completed_iso
                self._last_drill_error = None
                self._next_drill_due_time = self._clock() + self._drill_interval
            else:
                self._last_drill_failed_at = completed_iso
                self._drill_failure_count += 1
                self._last_drill_error = err_msg
                self._next_drill_due_time = self._clock() + self._retry_backoff

        # Persist restore drill record in SQLite
        if self._store.enabled:
            try:
                self._store.record_restore_drill(
                    drill_id=drill_res.drill_id,
                    started_at=drill_res.timestamp,
                    completed_at=completed_iso,
                    status=status_val,
                    backup_path=drill_res.backup_path,
                    backup_timestamp=drill_res.backup_timestamp,
                    backup_age_seconds=drill_res.backup_age_seconds,
                    duration_ms=duration_ms,
                    tables_verified=len(drill_res.tables_verified),
                    records_verified=drill_res.records_verified,
                    queries_verified=drill_res.queries_verified,
                    integrity_check=drill_res.integrity_check,
                    quick_check=drill_res.quick_check,
                    foreign_key_check=drill_res.foreign_key_check,
                    error_text=err_msg,
                    operator=clean_actor,
                    reason=reason or (
                        "scheduled drill" if actor == "scheduler" else "manual drill"
                    ),
                    fence_token=fence,
                    retention_limit=self._drill_history_limit,
                )
            except Exception as exc:
                logger.warning("Failed persisting restore drill record to store: %s", exc)

        # Audit event: RESTORE_DRILL_COMPLETED or RESTORE_DRILL_FAILED
        if drill_res.success:
            try:
                record_audit_event(
                    event_type=AuditEventType.RESTORE_DRILL_COMPLETED,
                    outcome=AuditOutcome.SUCCESS,
                    actor=clean_actor,
                    metadata={
                        "drill_id": drill_res.drill_id,
                        "backup_path": drill_res.backup_path,
                        "backup_age_seconds": drill_res.backup_age_seconds,
                        "duration_ms": duration_ms,
                        "tables_verified": len(drill_res.tables_verified),
                        "records_verified": drill_res.records_verified,
                        "fence_token": fence,
                    },
                    store=self._store,
                )
            except Exception as exc:
                logger.debug("Failed recording restore drill completed audit event: %s", exc)

            return {
                "status": "success",
                "message": "Database backup restore drill completed and verified successfully.",
                "drill": drill_res.to_dict(),
                "duration_ms": duration_ms,
                "fence_token": fence,
            }
        else:
            try:
                record_audit_event(
                    event_type=AuditEventType.RESTORE_DRILL_FAILED,
                    outcome=AuditOutcome.FAILURE,
                    actor=clean_actor,
                    metadata={
                        "drill_id": drill_res.drill_id,
                        "backup_path": drill_res.backup_path,
                        "error": err_msg,
                        "duration_ms": duration_ms,
                        "fence_token": fence,
                    },
                    store=self._store,
                )
            except Exception as exc:
                logger.debug("Failed recording restore drill failed audit event: %s", exc)

            return {
                "status": "failed",
                "message": f"Database backup restore drill failed: {err_msg}",
                "drill": drill_res.to_dict(),
                "error": err_msg,
                "duration_ms": duration_ms,
                "fence_token": fence,
            }

    def trigger_restore_drill(
        self,
        backup_path: str | None = None,
        actor: str = "manual",
        operator: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Trigger an immediate on-demand database restore drill."""
        return self.run_restore_drill(
            backup_path=backup_path,
            actor=actor,
            operator=operator,
            reason=reason or "manual on-demand restore drill trigger",
            force=True,
        )

    def trigger_backup(
        self,
        actor: str = "manual",
        operator: str | None = None,
        reason: str | None = None,
        tag: str | None = None,
    ) -> dict[str, Any]:
        """Trigger an immediate on-demand database backup execution."""
        return self.run_backup_cycle(
            actor=actor,
            operator=operator,
            reason=reason or "manual on-demand backup trigger",
            tag=tag,
            force=True,
        )

    def get_status(self) -> dict[str, Any]:
        """Return operational state, leader lease status, and backup execution metrics."""
        with self._lock:
            next_dt_str: str | None = None
            if self._enabled and self._next_due_time > 0:
                next_dt = datetime.datetime.fromtimestamp(
                    self._next_due_time, tz=datetime.timezone.utc
                )
                next_dt_str = next_dt.isoformat()

            return {
                "enabled": self._enabled,
                "running": self.is_running,
                "interval_seconds": self._interval,
                "backup_dir": self._backup_dir,
                "retention_count": self._retention_count,
                "retention_days": self._retention_days,
                "max_duration_seconds": self._max_duration,
                "max_retries": self._max_retries,
                "retry_backoff_seconds": self._retry_backoff,
                "max_backoff_seconds": self._max_backoff,
                "is_leader": self._is_leader,
                "leadership_state": self._leadership_state,
                "owner_id": self._owner_id,
                "fence_token": self._fence_token,
                "last_run_at": self._last_run_at,
                "last_success_at": self._last_success_at,
                "last_failed_at": self._last_failed_at,
                "next_scheduled_run_at": next_dt_str,
                "last_duration_ms": self._last_duration_ms,
                "last_backup_path": self._last_backup_path,
                "last_size_bytes": self._last_size_bytes,
                "last_integrity": self._last_integrity,
                "last_tables_count": self._last_tables_count,
                "total_backups_count": self._total_backups_count,
                "failure_count": self._failure_count,
                "consecutive_failures": self._consecutive_failures,
                "last_error": self._last_error,
                "restore_drill_enabled": self._drill_enabled,
                "restore_drill_interval_seconds": self._drill_interval,
                "restore_drill_max_duration_seconds": self._drill_max_duration,
                "restore_drill_history_limit": self._drill_history_limit,
                "last_drill_at": self._last_drill_at,
                "last_drill_success_at": self._last_drill_success_at,
                "last_drill_failed_at": self._last_drill_failed_at,
                "last_drill_status": self._last_drill_status,
                "last_drill_duration_ms": self._last_drill_duration_ms,
                "last_drill_backup_path": self._last_drill_backup_path,
                "last_drill_backup_age_seconds": self._last_drill_backup_age_seconds,
                "last_drill_tables_verified": self._last_drill_tables_verified,
                "last_drill_records_verified": self._last_drill_records_verified,
                "total_drills_count": self._total_drills_count,
                "drill_failure_count": self._drill_failure_count,
                "last_drill_error": self._last_drill_error,
            }

    def get_diagnostics(self) -> dict[str, Any]:
        """Alias for get_status providing runtime diagnostics."""
        return self.get_status()


def get_backup_scheduler(
    store: CheckpointStore | None = None,
    settings: Settings | None = None,
) -> DatabaseBackupScheduler:
    """Obtain or initialize the global singleton DatabaseBackupScheduler instance."""
    global _GLOBAL_BACKUP_SCHEDULER
    with _GLOBAL_SCHEDULER_LOCK:
        if _GLOBAL_BACKUP_SCHEDULER is None:
            _GLOBAL_BACKUP_SCHEDULER = DatabaseBackupScheduler(
                store=store,
                settings=settings,
            )
        return _GLOBAL_BACKUP_SCHEDULER


def reset_backup_scheduler() -> None:
    """Reset the global singleton backup scheduler for clean test isolation."""
    global _GLOBAL_BACKUP_SCHEDULER
    with _GLOBAL_SCHEDULER_LOCK:
        if _GLOBAL_BACKUP_SCHEDULER is not None:
            try:
                _GLOBAL_BACKUP_SCHEDULER.stop(timeout=1.0)
            except Exception:
                pass
            _GLOBAL_BACKUP_SCHEDULER = None
