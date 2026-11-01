"""Bounded cross-process transactional outbox dispatcher."""

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
from ..persistence.store import CheckpointStore, StaleDispatcherError
from .handler import NotificationDeliveryResult, NotificationHandler
from .registry import get_notification_handler

logger = logging.getLogger(__name__)


class OutboxDispatcher:
    """Coordinates reliable delivery of outbox notification events to external handlers.

    Guarantees:
    - Atomically claims pending or expired events across processes using durable leases and
      monotonic fencing tokens.
    - Prevents stale dispatchers from modifying event delivery state after losing ownership.
    - Applies configurable exponential backoff and bounded retry limits.
    - Moves permanently failed events to durable dead_letter status without data loss.
    - Supports graceful background thread shutdown and synchronous deterministic dispatch cycles.
    """

    def __init__(
        self,
        store: CheckpointStore | None = None,
        handler: NotificationHandler | None = None,
        settings: Settings | None = None,
        owner_id: str | None = None,
        enabled: bool | None = None,
        poll_interval_seconds: float | None = None,
        batch_size: int | None = None,
        max_attempts: int | None = None,
        backoff_base_seconds: float | None = None,
        backoff_max_seconds: float | None = None,
        claim_ttl_seconds: float | None = None,
        clock: Callable[[], datetime.datetime] | None = None,
    ) -> None:
        cfg = settings if settings is not None else get_settings()
        self._store = store if store is not None else CheckpointStore()
        self._handler = (
            handler
            if handler is not None
            else get_notification_handler(settings=cfg)
        )

        if enabled is not None:
            self._enabled = bool(enabled)
        else:
            self._enabled = bool(
                getattr(cfg, "outbox_dispatcher_enabled", True)
                and getattr(cfg, "outbox_enabled", True)
            )

        if poll_interval_seconds is not None:
            self._poll_interval = max(0.1, float(poll_interval_seconds))
        else:
            self._poll_interval = float(getattr(cfg, "outbox_poll_interval_seconds", 2.0))

        if batch_size is not None:
            self._batch_size = max(1, min(int(batch_size), 1000))
        else:
            self._batch_size = int(getattr(cfg, "outbox_batch_size", 50))

        if max_attempts is not None:
            self._max_attempts = max(1, min(int(max_attempts), 100))
        else:
            self._max_attempts = int(getattr(cfg, "outbox_max_attempts", 5))

        if backoff_base_seconds is not None:
            self._backoff_base = max(0.1, float(backoff_base_seconds))
        else:
            self._backoff_base = float(getattr(cfg, "outbox_retry_backoff_base_seconds", 2.0))

        if backoff_max_seconds is not None:
            self._backoff_max = max(1.0, float(backoff_max_seconds))
        else:
            self._backoff_max = float(getattr(cfg, "outbox_retry_backoff_max_seconds", 300.0))

        if claim_ttl_seconds is not None:
            self._claim_ttl = max(1.0, float(claim_ttl_seconds))
        else:
            self._claim_ttl = float(getattr(cfg, "outbox_claim_ttl_seconds", 30.0))

        if owner_id is not None:
            self._owner_id = str(owner_id).strip()
        else:
            self._owner_id = f"dispatcher-{os.getpid()}-{uuid.uuid4().hex[:8]}"

        self._clock = clock or (lambda: datetime.datetime.now(datetime.timezone.utc))

        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._is_running: bool = False

        self._total_claimed: int = 0
        self._total_delivered: int = 0
        self._total_failed: int = 0
        self._total_dead_letter: int = 0
        self._total_stale: int = 0
        self._last_cycle_at: str | None = None
        self._last_error: str | None = None

    @property
    def enabled(self) -> bool:
        """Whether outbox dispatching is enabled."""
        return self._enabled

    @property
    def is_running(self) -> bool:
        """Whether the background dispatcher thread is active."""
        with self._lock:
            return self._is_running and bool(self._thread and self._thread.is_alive())

    @property
    def owner_id(self) -> str:
        """Worker identifier of this dispatcher."""
        return self._owner_id

    @property
    def handler(self) -> NotificationHandler:
        """The registered external notification handler."""
        return self._handler

    def set_handler(self, handler: NotificationHandler) -> None:
        """Replace the notification handler."""
        self._handler = handler

    def start(self) -> None:
        """Start the background dispatcher thread if enabled."""
        if not self._enabled:
            logger.info("Outbox dispatcher is disabled; background thread not started.")
            return

        with self._lock:
            if self._is_running and self._thread and self._thread.is_alive():
                logger.debug("Outbox dispatcher background thread is already running.")
                return

            self._stop_event.clear()
            self._wake_event.clear()
            self._is_running = True
            self._thread = threading.Thread(
                target=self._run_loop,
                name=f"OutboxDispatcher-{self._owner_id}",
                daemon=True,
            )
            self._thread.start()
            logger.info("Started outbox dispatcher thread '%s'.", self._thread.name)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the background dispatcher thread gracefully within bounded timeout."""
        with self._lock:
            if not self._is_running:
                return
            self._is_running = False

        self._stop_event.set()
        self._wake_event.set()

        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=max(0.1, float(timeout)))
            if thread.is_alive():
                logger.warning(
                    "Outbox dispatcher thread did not terminate within %.1fs timeout.",
                    timeout,
                )
            else:
                logger.info("Outbox dispatcher thread stopped successfully.")

    def _run_loop(self) -> None:
        """Internal background dispatch loop."""
        logger.debug("Entering outbox dispatcher loop for owner '%s'.", self._owner_id)
        while not self._stop_event.is_set():
            try:
                self.run_dispatch_cycle()
            except Exception as exc:
                sanitized = sanitize_error(str(exc))
                logger.error("Outbox dispatch loop encountered an error: %s", sanitized)
                with self._lock:
                    self._last_error = sanitized

            # Wait poll interval or wake event
            self._wake_event.wait(timeout=self._poll_interval)
            self._wake_event.clear()

        logger.debug("Exited outbox dispatcher loop for owner '%s'.", self._owner_id)

    def run_dispatch_cycle(self, batch_size: int | None = None) -> dict[str, Any]:
        """Execute a single bounded outbox dispatch cycle.

        Claims pending/expired events and delivers each to the notification handler.
        Updates durable database state under monotonic fencing tokens.
        """
        now_dt = self._clock()
        now_str = now_dt.isoformat()
        clean_limit = batch_size if batch_size is not None else self._batch_size

        with self._lock:
            self._last_cycle_at = now_str

        claimed = self._store.claim_outbox_events(
            owner_id=self._owner_id,
            batch_size=clean_limit,
            claim_ttl_seconds=self._claim_ttl,
            now=now_str,
        )

        cycle_summary = {
            "scanned_at": now_str,
            "claimed_count": len(claimed),
            "delivered_count": 0,
            "failed_count": 0,
            "dead_letter_count": 0,
            "stale_count": 0,
            "events": [],
        }

        if not claimed:
            return cycle_summary

        with self._lock:
            self._total_claimed += len(claimed)

        for event in claimed:
            ev_id = event["event_id"]
            fence = event["fence_token"]

            try:
                result = self._handler.send_notification(event)
            except Exception as handler_exc:
                result = NotificationDeliveryResult(
                    success=False,
                    error=f"Handler exception: {handler_exc}",
                    retryable=True,
                )

            try:
                updated = self._store.update_outbox_event_delivery(
                    event_id=ev_id,
                    claimed_by=self._owner_id,
                    fence_token=fence,
                    success=result.success,
                    error=result.error,
                    retryable=result.retryable,
                    max_attempts=self._max_attempts,
                    backoff_base=self._backoff_base,
                    backoff_max=self._backoff_max,
                    now=now_str,
                )
                status = updated.get("status", "unknown")
                if status == "delivered":
                    cycle_summary["delivered_count"] += 1
                    with self._lock:
                        self._total_delivered += 1
                elif status == "dead_letter":
                    cycle_summary["dead_letter_count"] += 1
                    with self._lock:
                        self._total_dead_letter += 1
                else:
                    cycle_summary["failed_count"] += 1
                    with self._lock:
                        self._total_failed += 1

                cycle_summary["events"].append(
                    {"event_id": ev_id, "status": status, "attempts": updated.get("attempt_count")}
                )

            except StaleDispatcherError as stale_exc:
                logger.warning(
                    "Stale dispatcher prevented from updating outbox event %s: %s",
                    ev_id,
                    stale_exc,
                )
                cycle_summary["stale_count"] += 1
                with self._lock:
                    self._total_stale += 1
                cycle_summary["events"].append(
                    {"event_id": ev_id, "status": "stale_fenced", "error": str(stale_exc)}
                )

            except Exception as db_exc:
                sanitized_db = sanitize_error(str(db_exc))
                logger.error(
                    "Failed to update outbox delivery status for event %s: %s",
                    ev_id,
                    sanitized_db,
                )
                cycle_summary["failed_count"] += 1
                with self._lock:
                    self._total_failed += 1

        return cycle_summary

    def get_status(self) -> dict[str, Any]:
        """Return operational diagnostics for this dispatcher instance."""
        with self._lock:
            return {
                "enabled": self._enabled,
                "is_running": self.is_running,
                "owner_id": self._owner_id,
                "poll_interval_seconds": self._poll_interval,
                "batch_size": self._batch_size,
                "max_attempts": self._max_attempts,
                "claim_ttl_seconds": self._claim_ttl,
                "total_claimed": self._total_claimed,
                "total_delivered": self._total_delivered,
                "total_failed": self._total_failed,
                "total_dead_letter": self._total_dead_letter,
                "total_stale": self._total_stale,
                "last_cycle_at": self._last_cycle_at,
                "last_error": self._last_error,
            }


_GLOBAL_OUTBOX_DISPATCHER: OutboxDispatcher | None = None
_GLOBAL_DISPATCHER_LOCK = threading.Lock()


def get_outbox_dispatcher(
    store: CheckpointStore | None = None,
    handler: NotificationHandler | None = None,
    settings: Settings | None = None,
    owner_id: str | None = None,
    force_new: bool = False,
) -> OutboxDispatcher:
    """Get or instantiate the global singleton OutboxDispatcher."""
    global _GLOBAL_OUTBOX_DISPATCHER
    with _GLOBAL_DISPATCHER_LOCK:
        if _GLOBAL_OUTBOX_DISPATCHER is None or force_new:
            _GLOBAL_OUTBOX_DISPATCHER = OutboxDispatcher(
                store=store,
                handler=handler,
                settings=settings,
                owner_id=owner_id,
            )
        return _GLOBAL_OUTBOX_DISPATCHER
