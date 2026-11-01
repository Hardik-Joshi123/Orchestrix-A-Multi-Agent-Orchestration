"""Cooperative cancellation tokens and process-wide registry for workflow execution."""

from __future__ import annotations

import datetime
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)


class WorkflowCancelledError(Exception):
    """Raised when a workflow or subtask execution is interrupted by cancellation."""

    def __init__(self, message: str = "Workflow execution was cancelled.", reason: str = "") -> None:
        super().__init__(message)
        self.reason = reason or message


class WorkflowTimeoutError(Exception):
    """Raised when a workflow execution budget or deadline expires."""

    def __init__(
        self,
        message: str = "Workflow execution deadline exceeded.",
        timeout_seconds: float = 0.0,
    ) -> None:
        super().__init__(message)
        self.timeout_seconds = timeout_seconds


class CancellationToken:
    """Thread-safe cooperative cancellation token for orchestrator workflows.

    Workflows and specialist workers cooperatively check `is_cancelled` or call
    `check_cancelled()` at execution boundaries. Python threads are NOT forcibly
    terminated; in-flight work completes or drains while new dispatches are halted.
    """

    def __init__(
        self,
        run_id: str = "",
        deadline: float | None = None,
        timeout_seconds: float | None = None,
    ) -> None:
        self.run_id = run_id
        self.deadline = deadline
        self.timeout_seconds = timeout_seconds
        self._event = threading.Event()
        self._reason: str = ""
        self._cancelled_at: str | None = None
        self._lock = threading.Lock()

    def cancel(self, reason: str = "User requested cancellation") -> bool:
        """Signal cancellation cooperatively.

        Returns True if this call transitioned the token from uncancelled to cancelled,
        or False if it was already cancelled.
        """
        with self._lock:
            if self._event.is_set():
                return False
            self._reason = str(reason or "User requested cancellation").strip()
            self._cancelled_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self._event.set()
            logger.info("Cancellation requested for run '%s': %s", self.run_id, self._reason)
            return True

    def request_cancellation(self, reason: str = "User requested cancellation") -> bool:
        """Alias for cancel() to support semantic cancellation requests."""
        return self.cancel(reason)

    @property
    def is_cancelled(self) -> bool:
        """Return True if cancellation has been signaled."""
        return self._event.is_set()

    @property
    def is_timed_out(self) -> bool:
        """Return True if execution deadline has been reached or exceeded."""
        if self.deadline is not None:
            import time

            return time.monotonic() >= self.deadline
        return False

    @property
    def reason(self) -> str:
        """Return the cancellation reason provided."""
        return self._reason or "User requested cancellation"

    @property
    def cancelled_at(self) -> str | None:
        """Return ISO timestamp of when cancellation was requested, if set."""
        return self._cancelled_at

    def check_cancelled(self) -> None:
        """Raise WorkflowCancelledError if cancellation has been signaled."""
        if self.is_cancelled:
            raise WorkflowCancelledError(
                f"Workflow execution cancelled: {self.reason}",
                reason=self.reason,
            )
        if self.is_timed_out:
            raise WorkflowTimeoutError(
                "Workflow execution deadline exceeded.",
                timeout_seconds=self.timeout_seconds or 0.0,
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "is_cancelled": self.is_cancelled,
            "is_timed_out": self.is_timed_out,
            "reason": self.reason if self.is_cancelled else None,
            "cancelled_at": self._cancelled_at,
            "timeout_seconds": self.timeout_seconds,
        }


class CancellationRegistry:
    """Thread-safe registry managing active cancellation tokens across in-process runs."""

    def __init__(self) -> None:
        self._tokens: dict[str, CancellationToken] = {}
        self._lock = threading.Lock()

    def create_fresh(
        self,
        run_id: str,
        deadline: float | None = None,
        timeout_seconds: float | None = None,
    ) -> CancellationToken:
        """Create and register a brand new, uncancelled token for run_id, replacing any prior token."""
        clean_id = str(run_id or "").strip()
        token = CancellationToken(run_id=clean_id, deadline=deadline, timeout_seconds=timeout_seconds)
        with self._lock:
            self._tokens[clean_id] = token
            return token

    def get_or_create(self, run_id: str) -> CancellationToken:
        """Retrieve existing token for run_id or create a new active token."""
        clean_id = str(run_id or "").strip()
        with self._lock:
            if clean_id not in self._tokens:
                self._tokens[clean_id] = CancellationToken(run_id=clean_id)
            return self._tokens[clean_id]

    def get(self, run_id: str) -> CancellationToken | None:
        """Retrieve token for run_id if currently registered."""
        clean_id = str(run_id or "").strip()
        with self._lock:
            return self._tokens.get(clean_id)

    def register(self, run_id: str, token: CancellationToken) -> None:
        """Register a specific token instance for a run ID."""
        clean_id = str(run_id or "").strip()
        with self._lock:
            self._tokens[clean_id] = token

    def cancel(self, run_id: str, reason: str = "User requested cancellation") -> bool:
        """Signal cancellation on registered token for run_id.

        Returns True if token existed and was cancelled; False otherwise.
        """
        clean_id = str(run_id or "").strip()
        with self._lock:
            token = self._tokens.get(clean_id)
        if token is not None:
            return token.cancel(reason=reason)
        return False

    def remove(self, run_id: str) -> CancellationToken | None:
        """Unregister and return token for run_id upon workflow completion."""
        clean_id = str(run_id or "").strip()
        with self._lock:
            return self._tokens.pop(clean_id, None)

    def clear(self) -> None:
        """Clear all registered tokens (useful in test teardown)."""
        with self._lock:
            self._tokens.clear()


# Global cancellation registry
_REGISTRY: CancellationRegistry | None = None
_REGISTRY_LOCK = threading.Lock()


def get_cancellation_registry() -> CancellationRegistry:
    """Access the process-wide cancellation registry singleton."""
    global _REGISTRY
    if _REGISTRY is None:
        with _REGISTRY_LOCK:
            if _REGISTRY is None:
                _REGISTRY = CancellationRegistry()
    return _REGISTRY
