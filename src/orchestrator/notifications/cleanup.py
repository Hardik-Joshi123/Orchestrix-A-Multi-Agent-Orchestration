"""Automated maintenance and bounded lifecycle cleanup for durable webhook replay records."""

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
from ..persistence.store import CheckpointStore
from .signing import BaseReplayStore, get_default_replay_store

logger = logging.getLogger(__name__)

_GLOBAL_REPLAY_SCHEDULER: ReplayCleanupScheduler | None = None
_GLOBAL_SCHEDULER_LOCK = threading.Lock()


class ReplayCleanupScheduler:
    """Coordinates periodic background maintenance and pruning of expired replay records.

    Guarantees:
    - Thread-safe lifecycle controls (start, stop, wake) with duplicate scheduler prevention.
    - Safe multi-process coordination using cross-process scheduler leases.
    - Bounded batch deletion of expired records without touching active replay identities.
    - Graceful startup and bounded shutdown with lease release.
    - Synchronous on-demand triggers for testing and administrative maintenance.
    """

    def __init__(
        self,
        store: CheckpointStore | None = None,
        replay_store: BaseReplayStore | None = None,
        settings: Settings | None = None,
        owner_id: str | None = None,
        enabled: bool | None = None,
        interval_seconds: float | None = None,
        batch_size: int | None = None,
        lease_ttl_seconds: float = 30.0,
        lease_name: str = "webhook_replay_cleanup",
        clock: Callable[[], float] | None = None,
    ) -> None:
        cfg = settings if settings is not None else get_settings()
        self._store = store if store is not None else CheckpointStore()
        self._replay_store = (
            replay_store
            if replay_store is not None
            else get_default_replay_store(settings=cfg, store=self._store)
        )
        self._clock = clock if clock is not None else time.time

        if enabled is not None:
            self._enabled = bool(enabled)
        else:
            self._enabled = bool(
                getattr(cfg, "notification_webhook_replay_cleanup_enabled", True)
                and getattr(cfg, "notification_webhook_replay_protection_enabled", True)
            )

        if interval_seconds is not None:
            self._interval = max(1.0, float(interval_seconds))
        else:
            self._interval = float(
                getattr(cfg, "notification_webhook_replay_cleanup_interval_seconds", 300.0)
            )

        if batch_size is not None:
            self._batch_size = max(10, min(int(batch_size), 10000))
        else:
            self._batch_size = int(
                getattr(cfg, "notification_webhook_replay_cleanup_batch_size", 500)
            )

        self._lease_ttl = max(5.0, float(lease_ttl_seconds))
        self._lease_name = lease_name.strip()
        self._heartbeat_interval = max(1.0, min(5.0, self._lease_ttl / 3.0))

        if owner_id and owner_id.strip():
            self._owner_id = owner_id.strip()
        else:
            self._owner_id = f"replay-cleaner-{os.getpid()}-{uuid.uuid4().hex[:8]}"

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._is_running = False

        # Operational state
        self._is_leader = False
        self._fence_token: int | None = None
        self._leadership_state = "standby"
        self._last_lease_renewed_at: str | None = None
        self._last_cleanup_at: str | None = None
        self._last_deleted_count = 0
        self._total_deleted = 0
        self._cycle_count = 0
        self._consecutive_failures = 0

    @property
    def enabled(self) -> bool:
        """Whether automated replay cleanup scheduling is enabled."""
        return self._enabled

    @property
    def is_running(self) -> bool:
        """Whether the background worker thread is actively running."""
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
    def interval_seconds(self) -> float:
        """Configured interval in seconds between automated cleanup cycles."""
        return self._interval

    @property
    def batch_size(self) -> int:
        """Configured maximum number of expired records pruned per cycle."""
        return self._batch_size

    def start(self) -> None:
        """Start the background cleanup worker thread if enabled and not already running."""
        if not self._enabled:
            logger.info("Replay cleanup scheduler is disabled; worker thread not started.")
            return

        with self._lock:
            if self._is_running and self._thread and self._thread.is_alive():
                logger.debug("Replay cleanup scheduler background thread is already running.")
                return

            self._stop_event.clear()
            self._wake_event.clear()
            self._is_running = True

            thread_name = f"ReplayCleanup-{self._owner_id[:16]}"
            self._thread = threading.Thread(
                target=self._run_loop,
                name=thread_name,
                daemon=True,
            )
            self._thread.start()
            logger.info("Started replay cleanup scheduler thread '%s'.", thread_name)

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
            self._leadership_state = "stopped"
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
            except Exception as exc:
                logger.debug("Error releasing replay cleanup lease on shutdown: %s", exc)

    def wake(self) -> None:
        """Signal background thread to wake up immediately and evaluate work."""
        self._wake_event.set()

    def run_cleanup_cycle(
        self,
        now: float | None = None,
        limit: int | None = None,
        actor: str = "scheduler",
        operator: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Execute a single bounded replay cleanup cycle.

        Enforces leadership lease ownership across worker processes. Standby workers
        skip record deletion to avoid duplicate database work.
        """
        current_ts = now if now is not None else self._clock()
        now_dt = datetime.datetime.fromtimestamp(current_ts, tz=datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        batch_limit = limit if limit is not None else self._batch_size

        if not self._store.enabled or self._replay_store is None:
            return {
                "scanned_at": now_iso,
                "leadership_state": "disabled",
                "is_leader": False,
                "deleted_count": 0,
                "limit": batch_limit,
                "owner_id": self._owner_id,
            }

        # Coordinate leadership lease across processes
        is_leader = False
        with self._lock:
            curr_fence = self._fence_token
            already_leader = self._is_leader

        if already_leader and curr_fence is not None:
            renewed = self._store.renew_scheduler_lease(
                lease_name=self._lease_name,
                owner_id=self._owner_id,
                fence_token=curr_fence,
                lease_ttl_seconds=self._lease_ttl,
                now_dt=now_dt,
            )
            if renewed:
                is_leader = True
            else:
                with self._lock:
                    self._is_leader = False
                    self._leadership_state = "lost"
                    self._fence_token = None
        else:
            acquired, token, _ = self._store.acquire_scheduler_lease(
                lease_name=self._lease_name,
                owner_id=self._owner_id,
                lease_ttl_seconds=self._lease_ttl,
                now_dt=now_dt,
            )
            if acquired:
                with self._lock:
                    self._is_leader = True
                    self._fence_token = token
                    self._leadership_state = "leader"
                    self._last_lease_renewed_at = now_iso
                is_leader = True
            else:
                with self._lock:
                    self._is_leader = False
                    self._leadership_state = "standby"
                    self._fence_token = token

        if not is_leader:
            return {
                "scanned_at": now_iso,
                "leadership_state": "standby",
                "is_leader": False,
                "deleted_count": 0,
                "limit": batch_limit,
                "owner_id": self._owner_id,
            }

        # Perform bounded prune
        start_mono = time.monotonic()
        try:
            deleted = self._replay_store.cleanup_expired(
                now=current_ts,
                limit=batch_limit,
                trigger_type=actor,
                operator=operator,
                reason=reason,
            )
            duration_ms = round((time.monotonic() - start_mono) * 1000.0, 3)

            with self._lock:
                self._last_cleanup_at = now_iso
                self._last_deleted_count = deleted
                self._total_deleted += deleted
                self._cycle_count += 1
                self._consecutive_failures = 0

            return {
                "scanned_at": now_iso,
                "leadership_state": "leader",
                "is_leader": True,
                "deleted_count": deleted,
                "limit": batch_limit,
                "duration_ms": duration_ms,
                "owner_id": self._owner_id,
            }
        except Exception as exc:
            with self._lock:
                self._consecutive_failures += 1
            logger.error(
                "Error during replay cleanup cycle (attempt %d): %s",
                self._consecutive_failures,
                exc,
            )
            raise

    def trigger_cleanup(
        self,
        now: float | None = None,
        limit: int | None = None,
        operator: str | None = None,
        reason: str | None = None,
    ) -> dict[str, Any]:
        """Manually trigger a bounded cleanup cycle, bypassing the scheduled interval."""
        return self.run_cleanup_cycle(
            now=now,
            limit=limit,
            actor="manual",
            operator=operator or "operator",
            reason=reason or "manual operational cleanup",
        )

    def get_status(self) -> dict[str, Any]:
        """Return safe operational status snapshot for health checks and diagnostics."""
        with self._lock:
            return {
                "enabled": self._enabled,
                "is_running": self._is_running and self._thread is not None and self._thread.is_alive(),
                "leadership_state": self._leadership_state,
                "is_leader": self._is_leader,
                "fence_token": self._fence_token,
                "owner_id": self._owner_id,
                "interval_seconds": self._interval,
                "batch_size": self._batch_size,
                "last_lease_renewed_at": self._last_lease_renewed_at,
                "last_cleanup_at": self._last_cleanup_at,
                "last_deleted_count": self._last_deleted_count,
                "total_deleted": self._total_deleted,
                "cycle_count": self._cycle_count,
                "consecutive_failures": self._consecutive_failures,
            }

    def _run_loop(self) -> None:
        """Internal background loop running scheduled cleanup and heartbeat renewal."""
        last_run_mono = 0.0

        while not self._stop_event.is_set():
            tick_wait = min(self._heartbeat_interval, self._interval)
            woken = self._wake_event.wait(timeout=tick_wait)
            if self._stop_event.is_set():
                break

            if woken:
                self._wake_event.clear()

            now_mono = time.monotonic()
            elapsed_since_run = now_mono - last_run_mono

            if elapsed_since_run >= self._interval or woken:
                try:
                    self.run_cleanup_cycle()
                    last_run_mono = time.monotonic()
                except Exception as exc:
                    logger.warning("Replay cleanup cycle failed: %s", exc)
            else:
                # Renew heartbeat while waiting for interval
                with self._lock:
                    is_leader = self._is_leader
                    fence = self._fence_token

                if is_leader and fence is not None and self._store.enabled:
                    now_dt = datetime.datetime.now(datetime.timezone.utc)
                    renewed = self._store.renew_scheduler_lease(
                        lease_name=self._lease_name,
                        owner_id=self._owner_id,
                        fence_token=fence,
                        lease_ttl_seconds=self._lease_ttl,
                        now_dt=now_dt,
                    )
                    if renewed:
                        with self._lock:
                            self._last_lease_renewed_at = now_dt.isoformat()
                    else:
                        with self._lock:
                            self._is_leader = False
                            self._leadership_state = "lost"
                            self._fence_token = None


def get_replay_cleanup_scheduler(
    store: CheckpointStore | None = None,
    replay_store: BaseReplayStore | None = None,
    settings: Settings | None = None,
    owner_id: str | None = None,
    force_new: bool = False,
) -> ReplayCleanupScheduler:
    """Get or instantiate the global singleton ReplayCleanupScheduler."""
    global _GLOBAL_REPLAY_SCHEDULER
    with _GLOBAL_SCHEDULER_LOCK:
        if _GLOBAL_REPLAY_SCHEDULER is None or force_new:
            _GLOBAL_REPLAY_SCHEDULER = ReplayCleanupScheduler(
                store=store,
                replay_store=replay_store,
                settings=settings,
                owner_id=owner_id,
            )
        return _GLOBAL_REPLAY_SCHEDULER


def reset_replay_cleanup_scheduler() -> None:
    """Stop and reset the global ReplayCleanupScheduler singleton (for test isolation)."""
    global _GLOBAL_REPLAY_SCHEDULER
    with _GLOBAL_SCHEDULER_LOCK:
        if _GLOBAL_REPLAY_SCHEDULER is not None:
            try:
                _GLOBAL_REPLAY_SCHEDULER.stop(timeout=2.0)
            except Exception:
                pass
            _GLOBAL_REPLAY_SCHEDULER = None
