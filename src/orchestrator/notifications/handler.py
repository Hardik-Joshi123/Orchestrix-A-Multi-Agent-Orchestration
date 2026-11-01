"""Notification handler interface and test implementations for transactional outbox."""

from __future__ import annotations

import datetime
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class NotificationDeliveryResult:
    """Result of attempting to deliver an outbox notification event."""

    success: bool
    error: str | None = None
    retryable: bool = True
    delivered_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class NotificationHandler(ABC):
    """Abstract interface for external notification delivery destinations.

    Guarantees & Constraints:
    - Dispatchers deliver events with stable idempotency keys and event IDs.
    - Exactly-once delivery to external systems cannot be guaranteed without receiver-side
      deduplication using the event's stable idempotency_key or event_id.
    - Handlers must be idempotent to protect against repeated deliveries caused by process crashes
      or network timeouts occurring after external transmission but before database status commit.
    """

    @abstractmethod
    def send_notification(self, event: dict[str, Any]) -> NotificationDeliveryResult:
        """Deliver an outbox notification event to an external destination.

        Parameters
        ----------
        event:
            Dictionary containing outbox event fields:
            - event_id: str (unique persistent UUID)
            - event_type: str (e.g. tool.approval_requested, tool.approval_granted, etc.)
            - payload: dict[str, Any] (parsed event details)
            - idempotency_key: str | None (stable deduplication key)
            - attempt_count: int (current attempt count)
            - created_at: str (ISO8601 creation timestamp)

        Returns
        -------
        NotificationDeliveryResult:
            Delivery outcome indicating success or retryable/permanent failure.
        """
        raise NotImplementedError


class TestNotificationHandler(NotificationHandler):
    """Deterministic, thread-safe in-memory notification handler for testing and simulation.

    Never makes real network requests or sends real messages.
    Supports simulating:
    - Transient delivery failures with retryable=True
    - Terminal delivery failures with retryable=False (dead-letter)
    - Receiver-side deduplication using idempotency_key
    - Call counting, delivered event inspection, and artificial delays
    """

    __test__ = False

    def __init__(
        self,
        fail_all: bool = False,
        fail_events: dict[str, int] | None = None,
        non_retryable_event_ids: set[str] | None = None,
        record_duplicates: bool = True,
    ) -> None:
        self.fail_all = fail_all
        # Map event_id or event_type to remaining failure count
        self.fail_events: dict[str, int] = dict(fail_events or {})
        self.non_retryable_event_ids: set[str] = set(non_retryable_event_ids or set())
        self.record_duplicates = record_duplicates

        self.delivered_events: list[dict[str, Any]] = []
        self.attempted_events: list[dict[str, Any]] = []
        self.seen_idempotency_keys: dict[str, int] = {}
        self.delivery_attempts: dict[str, int] = {}

    def send_notification(self, event: dict[str, Any]) -> NotificationDeliveryResult:
        event_id = str(event.get("event_id", ""))
        event_type = str(event.get("event_type", ""))
        idemp_key = event.get("idempotency_key")

        self.attempted_events.append(dict(event))
        self.delivery_attempts[event_id] = self.delivery_attempts.get(event_id, 0) + 1

        if idemp_key:
            self.seen_idempotency_keys[str(idemp_key)] = (
                self.seen_idempotency_keys.get(str(idemp_key), 0) + 1
            )

        # Check unconditional failure
        if self.fail_all:
            is_retryable = event_id not in self.non_retryable_event_ids
            return NotificationDeliveryResult(
                success=False,
                error="Simulated global notification delivery failure",
                retryable=is_retryable,
            )

        # Check counted failure per event_id or event_type
        remaining_fails = self.fail_events.get(event_id, self.fail_events.get(event_type, 0))
        if remaining_fails > 0:
            if event_id in self.fail_events:
                self.fail_events[event_id] -= 1
            elif event_type in self.fail_events:
                self.fail_events[event_type] -= 1

            is_retryable = event_id not in self.non_retryable_event_ids
            return NotificationDeliveryResult(
                success=False,
                error=f"Simulated transient notification failure (remaining: {remaining_fails - 1})",
                retryable=is_retryable,
            )

        # Check non-retryable terminal failure
        if event_id in self.non_retryable_event_ids:
            return NotificationDeliveryResult(
                success=False,
                error=f"Terminal unretryable error for event {event_id}",
                retryable=False,
            )

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self.delivered_events.append(dict(event))

        return NotificationDeliveryResult(
            success=True,
            delivered_at=now,
            metadata={"attempt": self.delivery_attempts[event_id]},
        )

    def reset(self) -> None:
        """Reset internal delivery history and tracking state."""
        self.delivered_events.clear()
        self.attempted_events.clear()
        self.seen_idempotency_keys.clear()
        self.delivery_attempts.clear()
        self.fail_events.clear()
        self.non_retryable_event_ids.clear()
