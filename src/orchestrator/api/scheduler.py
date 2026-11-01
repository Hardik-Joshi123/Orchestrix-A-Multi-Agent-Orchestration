"""Background scheduler for automated approval expiration and overdue escalation."""

from __future__ import annotations

import datetime
import logging
import os
import threading
import uuid
from collections.abc import Callable
from typing import Any

from ..config import Settings, get_settings
from ..graph.supervisor import sanitize_error
from ..persistence.store import CheckpointStore, SchedulerFencingError

logger = logging.getLogger(__name__)


class ApprovalLifecycleScheduler:
    """Coordinates periodic background scans for tool approval expiration and overdue escalation.

    Guarantees:
    - Never auto-approves, executes, or redispatches tools.
    - Reuses durable persistence checks from CheckpointStore atomically.
    - Protects against duplicate background execution loops within the same process.
    - Recovers safely from scan errors without terminating future scan cycles.
    - Stops gracefully on shutdown within bounded grace periods without orphaned threads.
    """

    def __init__(
        self,
        store: CheckpointStore | None = None,
        enabled: bool | None = None,
        scan_interval_seconds: float | None = None,
        auto_escalate: bool | None = None,
        settings: Settings | None = None,
        clock: Callable[[], datetime.datetime] | None = None,
        owner_id: str | None = None,
        lease_ttl_seconds: float | None = None,
        heartbeat_interval_seconds: float | None = None,
    ) -> None:
        self._store = store if store is not None else CheckpointStore()
        cfg = settings if settings is not None else get_settings()

        if enabled is not None:
            self._enabled = bool(enabled)
        else:
            self._enabled = bool(getattr(cfg, "tool_approval_scheduler_enabled", True))

        if scan_interval_seconds is not None:
            self._scan_interval = max(1.0, float(scan_interval_seconds))
        else:
            self._scan_interval = float(
                getattr(cfg, "tool_approval_scan_interval_seconds", 60.0)
            )

        if auto_escalate is not None:
            self._auto_escalate = bool(auto_escalate)
        else:
            self._auto_escalate = bool(
                getattr(cfg, "tool_approval_escalation_enabled", True)
            )

        if owner_id is not None:
            self._owner_id = str(owner_id).strip()
        else:
            self._owner_id = f"scheduler-{os.getpid()}-{uuid.uuid4().hex[:8]}"

        self._lease_name = "tool_approval_scheduler"

        if lease_ttl_seconds is not None:
            self._lease_ttl = max(1.0, float(lease_ttl_seconds))
        else:
            self._lease_ttl = float(
                getattr(cfg, "tool_approval_scheduler_lease_ttl_seconds", 30.0)
            )

        if heartbeat_interval_seconds is not None:
            self._heartbeat_interval = max(0.1, float(heartbeat_interval_seconds))
        else:
            self._heartbeat_interval = float(
                getattr(cfg, "tool_approval_scheduler_heartbeat_interval_seconds", 5.0)
            )

        self._clock = clock or (lambda: datetime.datetime.now(datetime.timezone.utc))

        self._lock = threading.Lock()
        self._scan_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None

        self._is_running: bool = False
        self._is_leader: bool = False
        self._leadership_state: str = "standby" if self._enabled else "disabled"
        self._fence_token: int | None = None
        self._last_lease_renewed_at: str | None = None
        self._current_lease_owner: str | None = None
        self._current_lease_expires_at: str | None = None

        self._total_scans: int = 0
        self._total_expired: int = 0
        self._total_escalated: int = 0
        self._consecutive_failures: int = 0
        self._last_scan_at: str | None = None
        self._last_successful_scan_at: str | None = None
        self._next_scheduled_scan_at: str | None = None
        self._last_error: str | None = None
        self._last_scan_result: dict[str, Any] | None = None

    @property
    def enabled(self) -> bool:
        """Whether automated lifecycle scanning is enabled."""
        return self._enabled

    @property
    def is_running(self) -> bool:
        """Whether the background scheduler thread is currently active."""
        with self._lock:
            return self._is_running and bool(self._thread and self._thread.is_alive())

    @property
    def is_leader(self) -> bool:
        """Whether this scheduler instance currently holds the active leadership lease."""
        with self._lock:
            return self._is_leader

    @property
    def leadership_state(self) -> str:
        """Current leadership state: leader, standby, lost, or disabled."""
        with self._lock:
            return self._leadership_state

    @property
    def owner_id(self) -> str:
        """Unique worker instance identifier of this scheduler process."""
        return self._owner_id

    @property
    def fence_token(self) -> int | None:
        """Current monotonic fencing generation token if leader."""
        with self._lock:
            return self._fence_token

    @property
    def lease_ttl(self) -> float:
        """Configured cross-process scheduler lease TTL in seconds."""
        return self._lease_ttl

    @property
    def heartbeat_interval(self) -> float:
        """Configured interval in seconds between leadership lease heartbeats."""
        return self._heartbeat_interval

    @property
    def scan_interval(self) -> float:
        """Configured scan interval in seconds."""
        return self._scan_interval

    @property
    def store(self) -> CheckpointStore:
        """Underlying persistence store."""
        return self._store

    def start(self) -> None:
        """Start the background scheduler thread if enabled and not already running."""
        with self._lock:
            if not self._enabled:
                self._leadership_state = "disabled"
                logger.debug(
                    "Approval lifecycle scheduler is disabled; background loop not started."
                )
                return

            if self._is_running and self._thread and self._thread.is_alive():
                return

            self._stop_event.clear()
            self._wake_event.clear()
            self._is_running = True

            now = self._clock()
            try:
                acquired, token, lease_rec = self._store.acquire_scheduler_lease(
                    lease_name=self._lease_name,
                    owner_id=self._owner_id,
                    lease_ttl_seconds=self._lease_ttl,
                    now_dt=now,
                )
                if acquired:
                    self._is_leader = True
                    self._leadership_state = "leader"
                    self._fence_token = token
                    self._last_lease_renewed_at = now.isoformat()
                    self._next_scheduled_scan_at = (
                        now + datetime.timedelta(seconds=self._scan_interval)
                    ).isoformat()
                    if lease_rec:
                        self._current_lease_owner = lease_rec.get("owner_id")
                        self._current_lease_expires_at = lease_rec.get("expires_at")
                    logger.info(
                        "Approval scheduler leadership acquired for '%s' (token=%d)",
                        self._owner_id,
                        token,
                    )
                else:
                    self._is_leader = False
                    self._leadership_state = "standby"
                    self._fence_token = None
                    self._next_scheduled_scan_at = None
                    if lease_rec:
                        self._current_lease_owner = lease_rec.get("owner_id")
                        self._current_lease_expires_at = lease_rec.get("expires_at")
                    logger.info(
                        "Approval scheduler started in standby mode (held by '%s')",
                        self._current_lease_owner,
                    )
            except Exception as acq_err:
                logger.warning("Initial scheduler lease acquisition failed: %s", acq_err)
                self._is_leader = False
                self._leadership_state = "standby"

            self._thread = threading.Thread(
                target=self._run_loop,
                name=f"approval-scheduler-{self._owner_id[-8:]}",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "Approval lifecycle scheduler started (interval: %.1fs, auto_escalate: %s, leader: %s)",
                self._scan_interval,
                self._auto_escalate,
                self._is_leader,
            )

    def stop(self, timeout: float = 2.0) -> None:
        """Signal the background loop to stop and gracefully release leadership lease."""
        with self._lock:
            if not self._is_running and (self._thread is None or not self._thread.is_alive()):
                self._is_running = False
                return

            was_leader = self._is_leader
            fence = self._fence_token
            self._is_running = False
            self._is_leader = False
            self._leadership_state = "standby"
            self._fence_token = None
            self._stop_event.set()
            self._wake_event.set()
            thread = self._thread
            self._thread = None
            self._next_scheduled_scan_at = None

        if was_leader:
            try:
                self._store.release_scheduler_lease(
                    lease_name=self._lease_name,
                    owner_id=self._owner_id,
                    fence_token=fence,
                    now_dt=self._clock(),
                )
                logger.info(
                    "Approval scheduler leadership lease released by owner '%s'",
                    self._owner_id,
                )
            except Exception as rel_err:
                logger.debug("Failed to release scheduler lease on shutdown: %s", rel_err)

        if thread and thread.is_alive():
            join_timeout = max(0.1, float(timeout))
            thread.join(timeout=join_timeout)
            if thread.is_alive():
                logger.warning(
                    "Approval lifecycle scheduler thread did not terminate within %.1fs timeout",
                    join_timeout,
                )
            else:
                logger.info("Approval lifecycle scheduler stopped gracefully.")

    def acquire_leadership(self) -> bool:
        """Attempt to acquire or re-acquire the scheduler leadership lease immediately."""
        now = self._clock()
        acquired, token, lease_rec = self._store.acquire_scheduler_lease(
            lease_name=self._lease_name,
            owner_id=self._owner_id,
            lease_ttl_seconds=self._lease_ttl,
            now_dt=now,
        )
        with self._lock:
            if acquired:
                self._is_leader = True
                self._leadership_state = "leader"
                self._fence_token = token
                self._last_lease_renewed_at = now.isoformat()
                if lease_rec:
                    self._current_lease_owner = lease_rec.get("owner_id")
                    self._current_lease_expires_at = lease_rec.get("expires_at")
                return True
            else:
                self._is_leader = False
                self._leadership_state = "standby"
                self._fence_token = None
                if lease_rec:
                    self._current_lease_owner = lease_rec.get("owner_id")
                    self._current_lease_expires_at = lease_rec.get("expires_at")
                return False

    def release_leadership(self) -> bool:
        """Release the scheduler leadership lease gracefully."""
        with self._lock:
            fence = self._fence_token
            was_leader = self._is_leader
            self._is_leader = False
            self._leadership_state = "stopped"
            self._fence_token = None
            self._next_scheduled_scan_at = None

        if was_leader and fence is not None:
            return self._store.release_scheduler_lease(
                lease_name=self._lease_name,
                owner_id=self._owner_id,
                fence_token=fence,
                now_dt=self._clock(),
            )
        return False

    def run_scan_cycle(
        self,
        actor: str = "scheduler",
        run_id: str | None = None,
        store: CheckpointStore | None = None,
    ) -> dict[str, Any]:
        """Execute a single scan cycle for approval expiration and overdue escalation.

        Serializes execution via a dedicated scan lock to prevent conflicting concurrent scans.
        Catches errors safely without terminating future cycles.
        """
        now = self._clock()
        now_str = now.isoformat()
        target_store = store if store is not None else self._store

        # If scheduled scan, verify that this process is currently the leader
        expected_owner = None
        expected_fence = None
        if self._is_running and actor == "scheduler":
            with self._lock:
                leader = self._is_leader
                fence = self._fence_token

            if not leader or fence is None:
                logger.debug("Skipping scheduled scan: process is not the leader.")
                return {
                    "success": False,
                    "scanned_at": now_str,
                    "expired_count": 0,
                    "escalated_count": 0,
                    "expired_operation_keys": [],
                    "escalated_operation_keys": [],
                    "failed_count": 0,
                    "failed_operation_keys": [],
                    "reason": "Not scheduler leader",
                }

            # Revalidate lease immediately before work to protect against stale owners
            is_valid = target_store.validate_scheduler_lease_ownership(
                lease_name=self._lease_name,
                owner_id=self._owner_id,
                fence_token=fence,
                now_dt=now,
            )
            if not is_valid:
                with self._lock:
                    self._is_leader = False
                    self._leadership_state = "lost"
                    self._fence_token = None
                    self._next_scheduled_scan_at = None
                logger.warning(
                    "Scheduler lease validation failed before scan; fencing out owner '%s' (token %s)",
                    self._owner_id,
                    fence,
                )
                return {
                    "success": False,
                    "scanned_at": now_str,
                    "expired_count": 0,
                    "escalated_count": 0,
                    "expired_operation_keys": [],
                    "escalated_operation_keys": [],
                    "failed_count": 0,
                    "failed_operation_keys": [],
                    "reason": "Fenced out: scheduler lease lost",
                }

            expected_owner = self._owner_id
            expected_fence = fence

        # Prevent overlapping concurrent scans
        with self._scan_lock:
            try:
                res = target_store.check_and_expire_tool_approvals(
                    run_id=run_id,
                    before_timestamp=now_str,
                    auto_escalate=self._auto_escalate,
                    actor=actor,
                    lease_name=self._lease_name if actor == "scheduler" else None,
                    expected_owner_id=expected_owner,
                    expected_fence_token=expected_fence,
                )
                expired_count = int(res.get("expired_count", 0))
                escalated_keys = list(res.get("escalated_operation_keys", []))
                failed_keys = list(res.get("failed_operation_keys", []))
                partial_failures = list(res.get("partial_failures", []))

                # Identify and escalate pending approvals approaching expiry or waiting overdue
                if self._auto_escalate and hasattr(target_store, "check_overdue_approvals"):
                    try:
                        overdue = target_store.check_overdue_approvals(
                            run_id=run_id,
                            now_timestamp=now_str,
                        )
                        for item in overdue:
                            op_k = item.get("operation_key")
                            if (
                                op_k
                                and op_k not in escalated_keys
                                and op_k not in res.get("expired_operation_keys", [])
                                and op_k not in failed_keys
                            ):
                                try:
                                    target_store.escalate_tool_approval(
                                        run_id=item["run_id"],
                                        operation_key=op_k,
                                        tool_name=item.get("tool_name"),
                                        reason="Automated scan: overdue threshold reached",
                                        actor=actor,
                                        lease_name=self._lease_name if actor == "scheduler" else None,
                                        expected_owner_id=expected_owner,
                                        expected_fence_token=expected_fence,
                                    )
                                    escalated_keys.append(op_k)
                                except SchedulerFencingError:
                                    raise
                                except Exception as esc_err:
                                    logger.debug(
                                        "Could not auto-escalate approval %s: %s",
                                        op_k,
                                        esc_err,
                                    )
                                    if op_k not in failed_keys:
                                        failed_keys.append(op_k)
                                    partial_failures.append({
                                        "operation_key": op_k,
                                        "run_id": item["run_id"],
                                        "error": str(esc_err),
                                    })
                    except SchedulerFencingError:
                        raise
                    except Exception as overdue_err:
                        logger.debug("Could not check overdue approvals: %s", overdue_err)

                unique_failed_keys = list(dict.fromkeys(failed_keys))
                escalated_count = len(escalated_keys)
                res["escalated_count"] = escalated_count
                res["escalated_operation_keys"] = escalated_keys
                res["failed_count"] = len(unique_failed_keys)
                res["failed_operation_keys"] = unique_failed_keys
                res["partial_failures"] = partial_failures
                res["success"] = True

                with self._lock:
                    self._total_scans += 1
                    self._total_expired += expired_count
                    self._total_escalated += escalated_count
                    self._consecutive_failures = 0
                    self._last_scan_at = now_str
                    self._last_successful_scan_at = now_str
                    self._last_error = None
                    if partial_failures:
                        self._last_error = f"Partial scan failure on {len(failed_keys)} approvals"
                    self._last_scan_result = res
                    if self._is_running and self._is_leader:
                        self._next_scheduled_scan_at = (
                            now + datetime.timedelta(seconds=self._scan_interval)
                        ).isoformat()

                if expired_count > 0 or escalated_count > 0 or failed_keys:
                    logger.info(
                        "Approval lifecycle scan completed: expired=%d, escalated=%d, failed=%d",
                        expired_count,
                        escalated_count,
                        len(failed_keys),
                    )
                return res

            except SchedulerFencingError as fence_err:
                clean_err = sanitize_error(str(fence_err))
                with self._lock:
                    self._is_leader = False
                    self._leadership_state = "lost"
                    self._fence_token = None
                    self._next_scheduled_scan_at = None
                    self._last_scan_at = now_str
                    self._last_error = clean_err
                logger.warning(
                    "Scheduler lease lost or superseded during scheduled scan: %s",
                    clean_err,
                )
                return {
                    "success": False,
                    "scanned_at": now_str,
                    "expired_count": 0,
                    "escalated_count": 0,
                    "expired_operation_keys": [],
                    "escalated_operation_keys": [],
                    "failed_count": 0,
                    "failed_operation_keys": [],
                    "reason": f"Fenced out: {clean_err}",
                }

            except Exception as exc:
                clean_err = sanitize_error(str(exc))
                with self._lock:
                    self._total_scans += 1
                    self._consecutive_failures += 1
                    self._last_scan_at = now_str
                    self._last_error = clean_err
                    if self._is_running and self._is_leader:
                        # Back off on error to avoid tight loops
                        backoff = min(5.0, self._scan_interval)
                        self._next_scheduled_scan_at = (
                            now + datetime.timedelta(seconds=backoff)
                        ).isoformat()

                logger.warning(
                    "Approval lifecycle scan encountered an error (consecutive failures: %d): %s",
                    self._consecutive_failures,
                    clean_err,
                )
                return {
                    "success": False,
                    "scanned_at": now_str,
                    "expired_count": 0,
                    "escalated_count": 0,
                    "expired_operation_keys": [],
                    "escalated_operation_keys": [],
                    "error": clean_err,
                }

    def trigger_scan(
        self,
        actor: str = "manual_trigger",
        store: CheckpointStore | None = None,
    ) -> dict[str, Any]:
        """Trigger an immediate scan cycle on demand."""
        return self.run_scan_cycle(actor=actor, store=store)

    def get_status(self) -> dict[str, Any]:
        """Return a safe status snapshot suitable for diagnostics and health monitoring."""
        with self._lock:
            running = self._is_running and bool(self._thread and self._thread.is_alive())
            next_scan = self._next_scheduled_scan_at if (running and self._is_leader) else None
            state = (
                self._leadership_state
                if running
                else ("disabled" if not self._enabled else "stopped")
            )
            owner = self._current_lease_owner
            expires = self._current_lease_expires_at
            token = self._fence_token

        # If not leader or lease details unset, try reading current lease from store
        if owner is None or expires is None or token is None:
            try:
                lease = self._store.get_scheduler_lease(self._lease_name)
                if lease:
                    owner = lease.get("owner_id")
                    expires = lease.get("expires_at")
                    if token is None:
                        token = lease.get("fence_token")
            except Exception:
                pass

        with self._lock:
            last_failed = (
                int(self._last_scan_result.get("failed_count", 0))
                if isinstance(self._last_scan_result, dict)
                else 0
            )
            return {
                "enabled": self._enabled,
                "running": running,
                "scan_interval_seconds": self._scan_interval,
                "last_scan_at": self._last_scan_at,
                "last_successful_scan_at": self._last_successful_scan_at,
                "next_scheduled_scan_at": next_scan,
                "total_scans": self._total_scans,
                "total_expired": self._total_expired,
                "total_escalated": self._total_escalated,
                "consecutive_failures": self._consecutive_failures,
                "last_scan_failed_count": last_failed,
                "last_error": self._last_error,
                "is_leader": self._is_leader,
                "leadership_state": state,
                "owner_id": self._owner_id,
                "lease_owner_id": owner,
                "lease_expires_at": expires,
                "lease_renewed_at": self._last_lease_renewed_at,
                "fence_token": token,
                "lease_ttl_seconds": self._lease_ttl,
                "heartbeat_interval_seconds": self._heartbeat_interval,
            }

    def _run_loop(self) -> None:
        """Internal worker loop managing cross-process lease leadership and scheduled scans."""
        while not self._stop_event.is_set():
            tick_wait = min(self._heartbeat_interval, self._scan_interval)
            with self._lock:
                if self._consecutive_failures > 0:
                    tick_wait = min(2.0, tick_wait)

            # Wait for tick interval or stop/wake signal
            woken = self._wake_event.wait(timeout=tick_wait)
            if self._stop_event.is_set():
                break

            if woken:
                self._wake_event.clear()

            now = self._clock()

            # 1. Leadership maintenance / acquisition
            with self._lock:
                is_currently_leader = self._is_leader
                curr_fence = self._fence_token

            if is_currently_leader and curr_fence is not None:
                # Renew leadership
                renewed = self._store.renew_scheduler_lease(
                    lease_name=self._lease_name,
                    owner_id=self._owner_id,
                    fence_token=curr_fence,
                    lease_ttl_seconds=self._lease_ttl,
                    now_dt=now,
                )
                if renewed:
                    with self._lock:
                        self._last_lease_renewed_at = now.isoformat()
                        self._leadership_state = "leader"
                else:
                    # Lease lost or superseded!
                    with self._lock:
                        self._is_leader = False
                        self._leadership_state = "lost"
                        self._fence_token = None
                        self._next_scheduled_scan_at = None
                    logger.warning(
                        "Scheduler leadership lease lost or superseded for owner '%s' (token %d)",
                        self._owner_id,
                        curr_fence,
                    )
            else:
                # In standby: attempt acquisition / takeover
                acquired, token, lease_rec = self._store.acquire_scheduler_lease(
                    lease_name=self._lease_name,
                    owner_id=self._owner_id,
                    lease_ttl_seconds=self._lease_ttl,
                    now_dt=now,
                )
                with self._lock:
                    if acquired:
                        self._is_leader = True
                        self._leadership_state = "leader"
                        self._fence_token = token
                        self._last_lease_renewed_at = now.isoformat()
                        if self._next_scheduled_scan_at is None:
                            self._next_scheduled_scan_at = (
                                now + datetime.timedelta(seconds=self._scan_interval)
                            ).isoformat()
                        if lease_rec:
                            self._current_lease_owner = lease_rec.get("owner_id")
                            self._current_lease_expires_at = lease_rec.get("expires_at")
                        logger.info(
                            "Scheduler leadership lease acquired for owner '%s' (token %d)",
                            self._owner_id,
                            token,
                        )
                    else:
                        self._is_leader = False
                        self._leadership_state = "standby"
                        self._fence_token = None
                        self._next_scheduled_scan_at = None
                        if lease_rec:
                            self._current_lease_owner = lease_rec.get("owner_id")
                            self._current_lease_expires_at = lease_rec.get("expires_at")

            # 2. If leader, check if it's time to run a scheduled scan cycle
            with self._lock:
                leader = self._is_leader
                next_scan_at_str = self._next_scheduled_scan_at

            if leader:
                should_scan = False
                if next_scan_at_str is None:
                    should_scan = True
                else:
                    try:
                        next_dt = datetime.datetime.fromisoformat(
                            next_scan_at_str.replace("Z", "+00:00")
                        )
                        if now >= next_dt:
                            should_scan = True
                    except Exception:
                        should_scan = True

                if should_scan:
                    try:
                        self.run_scan_cycle(actor="scheduler")
                    except Exception as loop_err:
                        logger.warning(
                            "Unexpected error in approval scheduler scan: %s", loop_err
                        )


# ── Process-Wide Singleton Management ─────────────────────────────────────────

_DEFAULT_SCHEDULER: ApprovalLifecycleScheduler | None = None
_SCHEDULER_LOCK = threading.Lock()


def get_approval_scheduler(
    store: CheckpointStore | None = None,
    enabled: bool | None = None,
    scan_interval_seconds: float | None = None,
    auto_escalate: bool | None = None,
    settings: Settings | None = None,
    clock: Callable[[], datetime.datetime] | None = None,
    owner_id: str | None = None,
    lease_ttl_seconds: float | None = None,
    heartbeat_interval_seconds: float | None = None,
) -> ApprovalLifecycleScheduler:
    """Retrieve or initialize the process-wide ApprovalLifecycleScheduler singleton."""
    global _DEFAULT_SCHEDULER
    if _DEFAULT_SCHEDULER is None:
        with _SCHEDULER_LOCK:
            if _DEFAULT_SCHEDULER is None:
                _DEFAULT_SCHEDULER = ApprovalLifecycleScheduler(
                    store=store,
                    enabled=enabled,
                    scan_interval_seconds=scan_interval_seconds,
                    auto_escalate=auto_escalate,
                    settings=settings,
                    clock=clock,
                    owner_id=owner_id,
                    lease_ttl_seconds=lease_ttl_seconds,
                    heartbeat_interval_seconds=heartbeat_interval_seconds,
                )
    return _DEFAULT_SCHEDULER


def reset_approval_scheduler() -> None:
    """Stop and clear the process-wide scheduler singleton (for test isolation)."""
    global _DEFAULT_SCHEDULER
    with _SCHEDULER_LOCK:
        if _DEFAULT_SCHEDULER is not None:
            try:
                _DEFAULT_SCHEDULER.stop(timeout=1.0)
            except Exception:
                pass
            _DEFAULT_SCHEDULER = None
