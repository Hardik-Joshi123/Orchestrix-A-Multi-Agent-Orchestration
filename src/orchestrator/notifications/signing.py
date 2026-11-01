"""HMAC payload signing, signature verification, and replay protection for webhooks."""

from __future__ import annotations

import datetime
import hashlib
import hmac
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import Settings
    from ..persistence.store import CheckpointStore

logger = logging.getLogger(__name__)


class WebhookSignatureError(ValueError):
    """Raised when webhook signature generation, parsing, or verification fails."""


class BaseReplayStore(ABC):
    """Abstract base class for webhook replay protection stores."""

    @abstractmethod
    def check_and_record(
        self,
        event_id: str,
        timestamp: int,
        tolerance_seconds: float = 300.0,
        now: float | None = None,
    ) -> bool:
        """Verify that an event ID has not been seen within the active tolerance window.

        Atomically records the event identity if fresh.

        Returns
        -------
        bool:
            True if event is fresh and recorded; False if duplicate or active replay.
        """

    @abstractmethod
    def cleanup_expired(
        self,
        now: float | None = None,
        limit: int | None = None,
        trigger_type: str = "scheduled",
        operator: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Prune expired replay records.

        Returns
        -------
        int:
            Number of deleted expired records.
        """

    @abstractmethod
    def get_diagnostics(self, now: float | None = None) -> dict[str, Any]:
        """Return aggregate metrics and maintenance diagnostics."""

    @abstractmethod
    def clear(self) -> None:
        """Reset internal replay tracking state."""


class InMemoryReplayCache(BaseReplayStore):
    """Thread-safe in-memory cache tracking recent event IDs to prevent replay attacks."""

    def __init__(self, max_entries: int = 10000) -> None:
        self._max_entries = max(100, min(int(max_entries), 1000000))
        self._entries: dict[str, float] = {}  # event_id -> expiry_timestamp
        self._last_cleanup_metadata: dict[str, Any] | None = None
        self._lock = threading.Lock()

    def check_and_record(
        self,
        event_id: str,
        timestamp: int,
        tolerance_seconds: float = 300.0,
        now: float | None = None,
    ) -> bool:
        """Verify that an event ID has not been seen within the active window."""
        if not event_id:
            return True

        current_time = now if now is not None else time.time()
        expiry = max(float(timestamp), current_time) + max(1.0, float(tolerance_seconds))

        with self._lock:
            # Prune expired entries
            expired = [k for k, exp in self._entries.items() if exp < current_time]
            for k in expired:
                del self._entries[k]

            # Replay check
            if event_id in self._entries:
                logger.warning("Replay attack detected for outbox event %s", event_id)
                return False

            # Bounded capacity eviction (drop oldest if exceeded)
            if len(self._entries) >= self._max_entries:
                oldest_key = min(self._entries, key=self._entries.get)  # type: ignore[arg-type]
                del self._entries[oldest_key]

            self._entries[event_id] = expiry
            return True

    def cleanup_expired(
        self,
        now: float | None = None,
        limit: int | None = None,
        trigger_type: str = "scheduled",
        operator: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Prune expired entries from memory."""
        current_time = now if now is not None else time.time()
        start_mono = time.monotonic()
        with self._lock:
            expired = [k for k, exp in self._entries.items() if exp < current_time]
            if limit is not None and limit > 0:
                expired = expired[:limit]
            for k in expired:
                del self._entries[k]
            duration_ms = round((time.monotonic() - start_mono) * 1000.0, 3)
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self._last_cleanup_metadata = {
                "cleaned_at": now_iso,
                "deleted_count": len(expired),
                "duration_ms": duration_ms,
                "trigger_type": str(trigger_type or "scheduled").strip(),
                "operator": str(operator or "system").strip(),
                "reason": str(reason or "").strip(),
            }
            return len(expired)

    def get_diagnostics(self, now: float | None = None) -> dict[str, Any]:
        """Return bounded diagnostics for in-memory cache."""
        current_time = now if now is not None else time.time()
        with self._lock:
            total = len(self._entries)
            active = sum(1 for exp in self._entries.values() if exp >= current_time)
            expired = total - active
            return {
                "enabled": True,
                "storage_backend": "memory",
                "retention_seconds": None,
                "cleanup_batch_size": None,
                "total_records": total,
                "active_records": active,
                "expired_records": expired,
                "oldest_timestamp": None,
                "newest_timestamp": None,
                "oldest_recorded_at": None,
                "newest_recorded_at": None,
                "last_cleanup": (
                    dict(self._last_cleanup_metadata)
                    if self._last_cleanup_metadata
                    else None
                ),
            }

    def clear(self) -> None:
        """Reset internal replay tracking state."""
        with self._lock:
            self._entries.clear()
            self._last_cleanup_metadata = None


# Backward-compatible alias for in-memory cache
ReplayProtectionCache = InMemoryReplayCache


class SqliteReplayStore(BaseReplayStore):
    """Durable multi-process replay store backed by SQLite."""

    def __init__(
        self,
        store: CheckpointStore | None = None,
        db_path: str | None = None,
        retention_seconds: float = 86400.0,
        cleanup_batch_size: int = 500,
    ) -> None:
        if store is not None:
            self._store = store
        else:
            from ..persistence.store import CheckpointStore

            self._store = CheckpointStore(db_path=db_path or ":memory:")
        self._retention_seconds = max(60.0, float(retention_seconds))
        self._cleanup_batch_size = max(10, int(cleanup_batch_size))

    @property
    def store(self) -> CheckpointStore:
        """Return the underlying CheckpointStore instance."""
        return self._store

    @property
    def retention_seconds(self) -> float:
        """Configured record retention TTL in seconds."""
        return self._retention_seconds

    @property
    def cleanup_batch_size(self) -> int:
        """Configured batch size for expired record pruning."""
        return self._cleanup_batch_size

    def check_and_record(
        self,
        event_id: str,
        timestamp: int,
        tolerance_seconds: float = 300.0,
        now: float | None = None,
    ) -> bool:
        """Atomically check and record event ID in SQLite table."""
        if not event_id:
            return True

        current_time = now if now is not None else time.time()
        window = max(self._retention_seconds, float(tolerance_seconds))
        expires_at = max(float(timestamp), current_time) + window

        return self._store.record_webhook_replay(
            event_id=event_id,
            timestamp=timestamp,
            expires_at=expires_at,
            now=current_time,
        )

    def cleanup_expired(
        self,
        now: float | None = None,
        limit: int | None = None,
        trigger_type: str = "scheduled",
        operator: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Prune expired replay records from SQLite."""
        batch_limit = limit if limit is not None else self._cleanup_batch_size
        return self._store.cleanup_expired_webhook_replays(
            now=now,
            limit=batch_limit,
            trigger_type=trigger_type,
            operator=operator,
            reason=reason,
        )

    def get_count(
        self,
        active_only: bool = True,
        now: float | None = None,
    ) -> int:
        """Return count of replay records stored."""
        return self._store.get_webhook_replay_count(
            active_only=active_only,
            now=now,
        )

    def get_diagnostics(self, now: float | None = None) -> dict[str, Any]:
        """Derive bounded diagnostics for replay store."""
        diag = self._store.get_webhook_replay_diagnostics(now=now)
        diag["retention_seconds"] = self._retention_seconds
        diag["cleanup_batch_size"] = self._cleanup_batch_size
        return diag

    def get_cleanup_history(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return maintenance history of replay cleanup operations."""
        return self._store.get_webhook_replay_cleanup_history(limit=limit)

    def clear(self) -> None:
        """Clear all replay records and maintenance logs from database (primarily for tests)."""
        with self._store._get_db_lock(self._store.db_path):
            conn = self._store._get_connection()
            try:
                with conn:
                    conn.execute("DELETE FROM webhook_replay_records;")
                    conn.execute("DELETE FROM webhook_replay_cleanup_log;")
            finally:
                if not self._store._is_memory:
                    conn.close()


def get_default_replay_store(
    settings: Settings | None = None,
    store: CheckpointStore | None = None,
    db_path: str | None = None,
) -> BaseReplayStore | None:
    """Factory creating configured replay store based on Settings."""
    from ..config import get_settings

    cfg = settings or get_settings()

    if not cfg.notification_webhook_replay_protection_enabled:
        return None

    storage_type = (cfg.notification_webhook_replay_storage or "sqlite").lower().strip()
    if storage_type == "memory":
        return InMemoryReplayCache()

    return SqliteReplayStore(
        store=store,
        db_path=db_path or cfg.checkpoint_db_path,
        retention_seconds=cfg.notification_webhook_replay_retention_seconds,
        cleanup_batch_size=cfg.notification_webhook_replay_cleanup_batch_size,
    )


def generate_webhook_signature(
    payload_bytes: bytes,
    secret: str,
    timestamp: int | None = None,
    event_id: str | None = None,
) -> tuple[int, str]:
    """Generate an HMAC-SHA256 signature for a webhook payload.

    The signed context combines timestamp, event ID, and payload bytes:
        signature = HMAC-SHA256(secret, "{timestamp}.{event_id}." + payload_bytes)

    Parameters
    ----------
    payload_bytes:
        Exact serialized request body bytes.
    secret:
        HMAC signing secret key.
    timestamp:
        Unix epoch timestamp in seconds. If None, current system time is used.
    event_id:
        Persistent outbox event UUID (included in context to bind signature to event).

    Returns
    -------
    tuple[int, str]:
        (timestamp_integer, hex_encoded_hmac_signature)
    """
    if not secret:
        raise WebhookSignatureError("Signing secret must be a non-empty string.")

    ts = int(timestamp) if timestamp is not None else int(time.time())
    ev_context = (event_id or "").strip()

    # Canonical signed content format: "<timestamp>.<event_id>.<payload_bytes>"
    prefix = f"{ts}.{ev_context}.".encode()
    content = prefix + payload_bytes

    sig = hmac.new(secret.encode(), content, hashlib.sha256).hexdigest()
    return ts, sig


def format_signature_header(
    timestamp: int,
    signatures: list[str] | str,
    version: str = "v1",
) -> str:
    """Format the versioned webhook signature header.

    Format:
        t={timestamp},v1={sig_curr}[,v1={sig_prev}]

    Parameters
    ----------
    timestamp:
        Unix epoch integer timestamp.
    signatures:
        One or more hex-encoded signature strings.
    version:
        Signature scheme version identifier (default: 'v1').

    Returns
    -------
    str:
        Formatted header string.
    """
    sigs = [signatures] if isinstance(signatures, str) else signatures
    parts = [f"t={int(timestamp)}"]
    for s in sigs:
        if s:
            parts.append(f"{version}={s.strip()}")
    return ",".join(parts)


def parse_signature_header(header_value: str) -> tuple[int | None, list[tuple[str, str]]]:
    """Parse a versioned webhook signature header into timestamp and signature hashes.

    Parameters
    ----------
    header_value:
        Raw header string (e.g. 't=1730376000,v1=9f86d...').

    Returns
    -------
    tuple[int | None, list[tuple[str, str]]]:
        (timestamp or None, [(version, hash), ...])
    """
    if not header_value or not isinstance(header_value, str):
        return None, []

    timestamp: int | None = None
    signatures: list[tuple[str, str]] = []

    for item in header_value.split(","):
        item = item.strip()
        if not item or "=" not in item:
            continue
        key, val = item.split("=", 1)
        key = key.strip()
        val = val.strip()

        if key == "t":
            try:
                timestamp = int(val)
            except ValueError:
                timestamp = None
        elif key.startswith("v"):
            signatures.append((key, val))

    return timestamp, signatures


class WebhookSignatureVerifier:
    """Cryptographic verifier for inbound webhook payloads with replay protection."""

    def __init__(
        self,
        secrets: str | list[str] | tuple[str, ...],
        tolerance_seconds: float = 300.0,
        replay_store: BaseReplayStore | None = None,
        replay_cache: Any | None = None,
        replay_enabled: bool = True,
        clock: Callable[[], float] | None = None,
    ) -> None:
        raw_secrets = [secrets] if isinstance(secrets, str) else list(secrets)
        clean_secrets = [s.strip() for s in raw_secrets if s and s.strip()]
        if not clean_secrets:
            raise WebhookSignatureError(
                "At least one non-empty secret must be configured for verification."
            )

        self._secrets = clean_secrets
        self._tolerance = max(1.0, float(tolerance_seconds))
        self._replay_store: BaseReplayStore | None = replay_store or replay_cache
        self._replay_enabled = replay_enabled
        self._clock = clock or time.time

    @property
    def replay_store(self) -> BaseReplayStore | None:
        """Return the active replay protection store."""
        return self._replay_store

    @property
    def replay_cache(self) -> BaseReplayStore | None:
        """Backward-compatible alias for replay_store."""
        return self._replay_store

    def verify(
        self,
        body: bytes | str,
        header_value: str,
        event_id: str | None = None,
        now: float | None = None,
        raise_on_error: bool = True,
    ) -> bool:
        """Verify webhook signature against candidate secrets with constant-time equality.

        Signature validation occurs strictly BEFORE replay registration, ensuring
        that invalid signatures never consume or record an event identity.

        Parameters
        ----------
        body:
            Raw request body (bytes or string).
        header_value:
            Value of the signature header (e.g. 't=1730376000,v1=...').
        event_id:
            Event ID header or parsed ID to bind context.
        now:
            Current timestamp override for testing.
        raise_on_error:
            Whether to raise WebhookSignatureError on verification failure.

        Returns
        -------
        bool:
            True if signature is valid; False otherwise (if raise_on_error=False).
        """
        payload_bytes = body.encode() if isinstance(body, str) else body

        ts, signatures = parse_signature_header(header_value)
        if ts is None:
            if raise_on_error:
                raise WebhookSignatureError("Missing or invalid timestamp 't' in signature header.")
            return False

        if not signatures:
            if raise_on_error:
                raise WebhookSignatureError("No valid signature hashes found in signature header.")
            return False

        current_time = now if now is not None else self._clock()
        drift = current_time - ts

        # Timestamp tolerance check
        if drift > self._tolerance:
            if raise_on_error:
                raise WebhookSignatureError(
                    f"Signature timestamp expired (drift: {drift:.1f}s exceeds "
                    f"tolerance: {self._tolerance:.1f}s)."
                )
            return False

        if drift < -self._tolerance:
            if raise_on_error:
                raise WebhookSignatureError(
                    f"Signature timestamp is too far in future (drift: {drift:.1f}s exceeds "
                    f"-tolerance: -{self._tolerance:.1f}s)."
                )
            return False

        # STEP 1: Verify HMAC signature matching any candidate secret FIRST!
        # Invalid signatures MUST NOT consume or record an event identity in replay store.
        valid_signature = False
        for secret in self._secrets:
            _, expected_sig = generate_webhook_signature(
                payload_bytes=payload_bytes,
                secret=secret,
                timestamp=ts,
                event_id=event_id,
            )
            for version, sig_hash in signatures:
                if version == "v1" and hmac.compare_digest(expected_sig, sig_hash):
                    valid_signature = True
                    break
            if valid_signature:
                break

        if not valid_signature:
            if raise_on_error:
                raise WebhookSignatureError(
                    "Webhook signature mismatch: payload signature does not match any configured secret."
                )
            return False

        # STEP 2: Only after signature is cryptographically verified, perform replay check!
        if self._replay_enabled and self._replay_store is not None and event_id:
            try:
                is_fresh = self._replay_store.check_and_record(
                    event_id=event_id,
                    timestamp=ts,
                    tolerance_seconds=self._tolerance,
                    now=current_time,
                )
            except Exception as exc:
                logger.error(
                    "Replay protection storage failure for event %s: %s (failing closed)",
                    event_id,
                    exc,
                )
                if raise_on_error:
                    raise WebhookSignatureError(
                        f"Replay protection storage error: {exc}"
                    ) from exc
                return False

            if not is_fresh:
                if raise_on_error:
                    raise WebhookSignatureError(
                        f"Replay detected: event '{event_id}' has already been verified."
                    )
                return False

        return True


def verify_webhook_signature(
    body: bytes | str,
    header_value: str,
    secrets: str | list[str] | tuple[str, ...],
    event_id: str | None = None,
    tolerance_seconds: float = 300.0,
    now: float | None = None,
    replay_store: BaseReplayStore | None = None,
    replay_cache: Any | None = None,
    replay_enabled: bool = True,
    raise_on_error: bool = False,
) -> bool:
    """Convenience helper to verify an inbound webhook signature.

    Parameters
    ----------
    body:
        Raw request body bytes or string.
    header_value:
        Signature header string.
    secrets:
        One or more allowed signing secrets (e.g. current and previous secret).
    event_id:
        Outbox event UUID if present.
    tolerance_seconds:
        Timestamp drift tolerance window in seconds.
    now:
        Optional reference timestamp.
    replay_store:
        Optional BaseReplayStore instance (or SQLite / in-memory store).
    replay_cache:
        Backward-compatible parameter for replay_store.
    replay_enabled:
        Whether replay verification is active.
    raise_on_error:
        If True, raises WebhookSignatureError instead of returning False.

    Returns
    -------
    bool:
        True if valid; False otherwise.
    """
    verifier = WebhookSignatureVerifier(
        secrets=secrets,
        tolerance_seconds=tolerance_seconds,
        replay_store=replay_store,
        replay_cache=replay_cache,
        replay_enabled=replay_enabled,
    )
    return verifier.verify(
        body=body,
        header_value=header_value,
        event_id=event_id,
        now=now,
        raise_on_error=raise_on_error,
    )
