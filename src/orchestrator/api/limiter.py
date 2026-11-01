"""In-process thread-safe API rate limiter with trusted-proxy client identification."""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from ..config import get_settings

logger = logging.getLogger(__name__)


def get_client_ip(request: Request, trusted_proxies: set[str] | list[str] | None = None) -> str:
    """Extract client IP, inspecting X-Forwarded-For only when the direct peer is a trusted proxy."""
    direct_host = (request.client.host if request.client else "127.0.0.1").strip().lower()
    if trusted_proxies is not None:
        trusted = {p.strip().lower() for p in trusted_proxies if p.strip()}
    else:
        env_proxies = os.environ.get("ORCHESTRATOR_TRUSTED_PROXIES")
        if env_proxies is not None:
            trusted = {p.strip().lower() for p in env_proxies.split(",") if p.strip()}
        else:
            trusted = {p.strip().lower() for p in get_settings().effective_trusted_proxies if p.strip()}

    if direct_host in trusted:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            parts = [p.strip() for p in xff.split(",") if p.strip()]
            if parts:
                return parts[0]

    return direct_host


class RateLimiter:
    """Sliding-window in-memory rate limiter with thread-safe locking."""

    def __init__(
        self,
        max_requests: int | None = None,
        window_seconds: float | None = None,
        enabled: bool | None = None,
        max_keys: int = 10_000,
        requests_per_window: int | None = None,
    ) -> None:
        self._max_requests_override = max_requests if max_requests is not None else requests_per_window
        self._window_seconds_override = window_seconds
        self._enabled_override = enabled
        self.max_keys = max_keys
        self._records: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        if self._enabled_override is not None:
            return self._enabled_override
        env_val = os.environ.get("ORCHESTRATOR_RATE_LIMIT_ENABLED")
        if env_val is not None:
            return env_val.strip().lower() in ("true", "1", "yes", "on")
        return get_settings().api_rate_limit_enabled

    @property
    def max_requests(self) -> int:
        if self._max_requests_override is not None:
            return self._max_requests_override
        env_val = os.environ.get("ORCHESTRATOR_RATE_LIMIT_REQUESTS")
        if env_val:
            try:
                return int(env_val)
            except ValueError:
                pass
        return get_settings().api_rate_limit_requests

    @property
    def window_seconds(self) -> float:
        if self._window_seconds_override is not None:
            return self._window_seconds_override
        env_val = os.environ.get("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS")
        if env_val:
            try:
                return float(env_val)
            except ValueError:
                pass
        return get_settings().api_rate_limit_window_seconds

    def check(self, key: str) -> tuple[bool, int]:
        """Check if request for key is allowed under the rate limit.

        Returns (allowed: bool, retry_after: int).
        """
        if not self.enabled:
            return True, 0

        now = time.monotonic()
        limit = self.max_requests
        window = self.window_seconds
        cutoff = now - window

        with self._lock:
            # Memory safety: prune empty keys if record store grows large
            if len(self._records) > self.max_keys:
                self._prune_expired(cutoff)

            q = self._records.setdefault(key, deque())

            # Purge timestamps outside the active sliding window
            while q and q[0] <= cutoff:
                q.popleft()

            if len(q) >= limit:
                oldest = q[0]
                retry_after = max(1, int(oldest + window - now + 0.999))
                logger.warning(
                    "Rate limit exceeded for key=%s (count=%d/%d, retry_after=%ds)",
                    key,
                    len(q),
                    limit,
                    retry_after,
                )
                return False, retry_after

            q.append(now)
            return True, 0

    def acquire(self, key: str) -> bool:
        """Attempt to acquire a request token. Return True if allowed, False if throttled."""
        allowed, _ = self.check(key)
        return allowed

    def acquire_info(self, key: str) -> tuple[bool, int]:
        """Alias for check(key) returning (allowed: bool, retry_after: int)."""
        return self.check(key)

    def _prune_expired(self, cutoff: float) -> None:
        """Internal helper to remove dead keys and bound memory usage."""
        dead_keys = [k for k, q in self._records.items() if not q or q[-1] <= cutoff]
        for k in dead_keys:
            self._records.pop(k, None)

    def reset(self) -> None:
        """Clear all stored rate limit history."""
        with self._lock:
            self._records.clear()


_DEFAULT_LIMITER: RateLimiter | None = None
_LIMITER_LOCK = threading.Lock()


def get_rate_limiter() -> RateLimiter:
    """Retrieve or initialize the global in-process RateLimiter."""
    global _DEFAULT_LIMITER
    if _DEFAULT_LIMITER is None:
        with _LIMITER_LOCK:
            if _DEFAULT_LIMITER is None:
                _DEFAULT_LIMITER = RateLimiter()
    return _DEFAULT_LIMITER


def reset_rate_limiter() -> None:
    """Reset the global in-process RateLimiter state."""
    global _DEFAULT_LIMITER
    with _LIMITER_LOCK:
        if _DEFAULT_LIMITER is not None:
            _DEFAULT_LIMITER.reset()
        _DEFAULT_LIMITER = None


def check_rate_limit(
    request: Request,
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> None:
    """FastAPI route dependency enforcing rate limits on expensive operations."""
    if not limiter.enabled:
        return

    settings = get_settings()
    client_ip = get_client_ip(request, settings.effective_trusted_proxies)
    allowed, retry_after = limiter.check(client_ip)

    if not allowed:
        try:
            from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

            record_audit_event(
                event_type=AuditEventType.SECURITY_RATE_LIMIT_EXCEEDED,
                outcome=AuditOutcome.DENIED,
                actor=client_ip,
                metadata={
                    "path": request.url.path,
                    "retry_after": retry_after,
                    "max_requests": limiter.max_requests,
                    "window_seconds": limiter.window_seconds,
                },
            )
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"Rate limit exceeded. Maximum of {limiter.max_requests} requests "
                f"per {int(limiter.window_seconds)}s."
            ),
            headers={"Retry-After": str(retry_after)},
        )
