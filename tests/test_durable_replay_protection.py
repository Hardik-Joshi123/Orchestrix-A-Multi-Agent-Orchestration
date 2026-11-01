"""Tests for durable webhook replay protection and multi-process verification."""

from __future__ import annotations

import concurrent.futures
import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from orchestrator.config import Settings
from orchestrator.notifications.signing import (
    BaseReplayStore,
    InMemoryReplayCache,
    SqliteReplayStore,
    WebhookSignatureError,
    WebhookSignatureVerifier,
    format_signature_header,
    generate_webhook_signature,
    get_default_replay_store,
    verify_webhook_signature,
)
from orchestrator.persistence.store import CheckpointStore


def test_durable_replay_basic_duplicate_rejection(tmp_path: Path) -> None:
    """Verify durable SQLite replay store rejects duplicate verified requests."""
    db_file = str(tmp_path / "replay_basic.db")
    store = SqliteReplayStore(db_path=db_file, retention_seconds=3600.0)
    payload = b'{"event_id": "evt-durable-1", "action": "approved"}'
    secret = "whsec_test_durable_secret_1"
    ts = 1730380000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-durable-1")
    header = format_signature_header(ts, sig)

    verifier = WebhookSignatureVerifier(
        secrets=secret,
        tolerance_seconds=300.0,
        replay_store=store,
    )

    # First attempt: valid and fresh
    assert verifier.verify(payload, header, event_id="evt-durable-1", now=float(ts + 5)) is True
    assert store.get_count(active_only=True, now=float(ts + 5)) == 1

    # Second attempt: same payload, same event ID -> replay detected
    with pytest.raises(WebhookSignatureError, match="Replay detected"):
        verifier.verify(payload, header, event_id="evt-durable-1", now=float(ts + 6))

    # A different event ID within tolerance passes
    _, sig2 = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-durable-2")
    header2 = format_signature_header(ts, sig2)
    assert verifier.verify(payload, header2, event_id="evt-durable-2", now=float(ts + 7)) is True
    assert store.get_count(active_only=True, now=float(ts + 7)) == 2


def test_durable_replay_restart_persistence(tmp_path: Path) -> None:
    """Verify replay protection survives process restarts and instance recreation."""
    db_file = str(tmp_path / "replay_restart.db")
    payload = b'{"event_id": "evt-persist-1", "status": "resolved"}'
    secret = "whsec_restart_secret"
    ts = 1730381000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-persist-1")
    header = format_signature_header(ts, sig)

    # Instance 1: verifies and records event
    store1 = SqliteReplayStore(db_path=db_file, retention_seconds=7200.0)
    verifier1 = WebhookSignatureVerifier(secrets=secret, replay_store=store1)
    assert verifier1.verify(payload, header, event_id="evt-persist-1", now=float(ts + 10)) is True

    # Simulate process restart: completely new store and verifier connected to same DB
    store2 = SqliteReplayStore(db_path=db_file, retention_seconds=7200.0)
    verifier2 = WebhookSignatureVerifier(secrets=secret, replay_store=store2)

    # Event 1 must remain blocked across restart
    with pytest.raises(WebhookSignatureError, match="Replay detected"):
        verifier2.verify(payload, header, event_id="evt-persist-1", now=float(ts + 20))

    # Fresh event 2 passes
    _, sig2 = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-persist-2")
    header2 = format_signature_header(ts, sig2)
    assert verifier2.verify(payload, header2, event_id="evt-persist-2", now=float(ts + 25)) is True


def test_durable_replay_concurrent_requests(tmp_path: Path) -> None:
    """Verify atomic DB operations prevent race conditions across concurrent workers."""
    db_file = str(tmp_path / "replay_concurrent.db")
    checkpoint_store = CheckpointStore(db_path=db_file)
    store = SqliteReplayStore(store=checkpoint_store, retention_seconds=3600.0)

    payload = b'{"event_id": "evt-race-1", "action": "execute"}'
    secret = "whsec_race_secret"
    ts = 1730382000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-race-1")
    header = format_signature_header(ts, sig)

    num_threads = 10
    success_count = 0
    replay_count = 0

    def verify_attempt() -> bool:
        verifier = WebhookSignatureVerifier(
            secrets=secret,
            tolerance_seconds=300.0,
            replay_store=store,
        )
        return verifier.verify(
            payload,
            header,
            event_id="evt-race-1",
            now=float(ts + 2),
            raise_on_error=False,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(verify_attempt) for _ in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            if f.result() is True:
                success_count += 1
            else:
                replay_count += 1

    # Exactly 1 request must succeed; all 9 concurrent duplicates must be rejected!
    assert success_count == 1
    assert replay_count == num_threads - 1
    assert store.get_count(active_only=True, now=float(ts + 2)) == 1


def test_durable_replay_expiry_and_cleanup(tmp_path: Path) -> None:
    """Verify expired replay records are pruned and do not permanently block reuse."""
    db_file = str(tmp_path / "replay_expiry.db")
    # Retention 60 seconds
    store = SqliteReplayStore(db_path=db_file, retention_seconds=60.0, cleanup_batch_size=100)
    ts = 1000

    # Insert record at t=1000 with tolerance 30 -> expires at max(1000, 1000) + max(60, 30) = 1060
    assert store.check_and_record("evt-exp-1", timestamp=ts, tolerance_seconds=30.0, now=1000.0) is True

    # Active count at t=1050 is 1; duplicate rejected
    assert store.get_count(active_only=True, now=1050.0) == 1
    assert store.check_and_record("evt-exp-1", timestamp=ts, tolerance_seconds=30.0, now=1050.0) is False

    # At t=1065, record has expired
    assert store.get_count(active_only=True, now=1065.0) == 0
    assert store.get_count(active_only=False, now=1065.0) == 1  # Still in table before cleanup

    # Run cleanup at t=1065 -> record pruned
    pruned = store.cleanup_expired(now=1065.0)
    assert pruned == 1
    assert store.get_count(active_only=False, now=1065.0) == 0

    # If the same event ID arrives again after expiry, it is accepted (UPSERT replacement)
    assert store.check_and_record("evt-exp-1", timestamp=1070, tolerance_seconds=30.0, now=1070.0) is True
    assert store.get_count(active_only=True, now=1070.0) == 1


def test_invalid_signature_does_not_consume_replay_identity(tmp_path: Path) -> None:
    """Verify invalid signatures fail before replay check, protecting event identities."""
    db_file = str(tmp_path / "replay_spoof.db")
    store = SqliteReplayStore(db_path=db_file, retention_seconds=3600.0)
    secret_legit = "whsec_legit_secret_123"
    secret_attacker = "whsec_attacker_secret_999"
    payload = b'{"event_id": "evt-target-1", "command": "approve"}'
    ts = 1730383000

    # Attacker generates signature with wrong secret
    _, bad_sig = generate_webhook_signature(
        payload,
        secret_attacker,
        timestamp=ts,
        event_id="evt-target-1",
    )
    bad_header = format_signature_header(ts, bad_sig)

    verifier = WebhookSignatureVerifier(
        secrets=secret_legit,
        tolerance_seconds=300.0,
        replay_store=store,
    )

    # Attacker's request is rejected for invalid signature
    with pytest.raises(WebhookSignatureError, match="signature mismatch"):
        verifier.verify(payload, bad_header, event_id="evt-target-1", now=float(ts + 5))

    # Verify event identity was NOT consumed in the replay store
    assert store.get_count(active_only=True, now=float(ts + 5)) == 0

    # Legitimate request arrives with valid signature for same event ID -> succeeds!
    _, good_sig = generate_webhook_signature(
        payload,
        secret_legit,
        timestamp=ts,
        event_id="evt-target-1",
    )
    good_header = format_signature_header(ts, good_sig)
    assert verifier.verify(payload, good_header, event_id="evt-target-1", now=float(ts + 6)) is True
    assert store.get_count(active_only=True, now=float(ts + 6)) == 1


def test_replay_database_failure_fails_closed(tmp_path: Path) -> None:
    """Verify verification fails closed when replay persistence throws an operational error."""
    mock_store = MagicMock(spec=BaseReplayStore)
    mock_store.check_and_record.side_effect = sqlite3.OperationalError("database disk image is malformed")

    secret = "whsec_failclosed_secret"
    payload = b'{"event_id": "evt-failclose-1"}'
    ts = 1730384000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-failclose-1")
    header = format_signature_header(ts, sig)

    verifier = WebhookSignatureVerifier(
        secrets=secret,
        tolerance_seconds=300.0,
        replay_store=mock_store,
    )

    # Must fail closed with WebhookSignatureError when raise_on_error=True
    with pytest.raises(WebhookSignatureError, match="Replay protection storage error"):
        verifier.verify(payload, header, event_id="evt-failclose-1", now=float(ts + 5))

    # Must return False when raise_on_error=False
    assert (
        verifier.verify(
            payload,
            header,
            event_id="evt-failclose-1",
            now=float(ts + 5),
            raise_on_error=False,
        )
        is False
    )


def test_secret_rotation_with_durable_replay(tmp_path: Path) -> None:
    """Verify secret rotation works seamlessly with durable SQLite replay store."""
    db_file = str(tmp_path / "replay_rotation.db")
    store = SqliteReplayStore(db_path=db_file, retention_seconds=3600.0)

    secret_current = "whsec_current_secret_key"
    secret_previous = "whsec_previous_secret_key"
    payload = b'{"event_id": "evt-rot-1", "action": "rotate"}'
    ts = 1730385000

    # Sender signs using previous secret during rotation transition
    _, sig_prev = generate_webhook_signature(
        payload,
        secret_previous,
        timestamp=ts,
        event_id="evt-rot-1",
    )
    header_prev = format_signature_header(ts, sig_prev)

    verifier = WebhookSignatureVerifier(
        secrets=[secret_current, secret_previous],
        tolerance_seconds=300.0,
        replay_store=store,
    )

    # Verification with previous secret succeeds and records replay
    assert verifier.verify(payload, header_prev, event_id="evt-rot-1", now=float(ts + 10)) is True

    # Immediate replay attempt using current secret is still caught by event ID replay check!
    _, sig_curr = generate_webhook_signature(
        payload,
        secret_current,
        timestamp=ts,
        event_id="evt-rot-1",
    )
    header_curr = format_signature_header(ts, sig_curr)

    with pytest.raises(WebhookSignatureError, match="Replay detected"):
        verifier.verify(payload, header_curr, event_id="evt-rot-1", now=float(ts + 12))


def test_replay_disabled_configuration(tmp_path: Path) -> None:
    """Verify that when replay protection is disabled, duplicate events are permitted."""
    secret = "whsec_disabled_replay"
    payload = b'{"event_id": "evt-dupe-ok"}'
    ts = 1730386000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-dupe-ok")
    header = format_signature_header(ts, sig)

    verifier = WebhookSignatureVerifier(
        secrets=secret,
        tolerance_seconds=300.0,
        replay_store=None,
        replay_enabled=False,
    )

    # Both requests succeed because replay protection is disabled
    assert verifier.verify(payload, header, event_id="evt-dupe-ok", now=float(ts + 5)) is True
    assert verifier.verify(payload, header, event_id="evt-dupe-ok", now=float(ts + 6)) is True


def test_factory_get_default_replay_store(tmp_path: Path) -> None:
    """Verify get_default_replay_store instantiates the appropriate backend from settings."""
    db_file = str(tmp_path / "factory.db")

    # SQLite default
    s_sqlite = Settings(
        checkpoint_db_path=db_file,
        notification_webhook_replay_protection_enabled=True,
        notification_webhook_replay_storage="sqlite",
        notification_webhook_replay_retention_seconds=3600.0,
        notification_webhook_replay_cleanup_batch_size=250,
    )
    store_sqlite = get_default_replay_store(settings=s_sqlite)
    assert isinstance(store_sqlite, SqliteReplayStore)

    # Memory backend
    s_mem = Settings(
        notification_webhook_replay_protection_enabled=True,
        notification_webhook_replay_storage="memory",
    )
    store_mem = get_default_replay_store(settings=s_mem)
    assert isinstance(store_mem, InMemoryReplayCache)

    # Disabled
    s_off = Settings(notification_webhook_replay_protection_enabled=False)
    store_off = get_default_replay_store(settings=s_off)
    assert store_off is None


def test_config_validation_replay_settings() -> None:
    """Verify configuration validation for replay protection settings."""
    # Invalid storage backend
    with pytest.raises(ValidationError, match="notification_webhook_replay_storage"):
        Settings(notification_webhook_replay_storage="redis")

    # Retention below 60 seconds
    with pytest.raises(ValidationError, match="at least 60.0 seconds"):
        Settings(notification_webhook_replay_retention_seconds=30.0)

    # Retention above 30 days
    with pytest.raises(ValidationError, match="cannot exceed 2592000.0 seconds"):
        Settings(notification_webhook_replay_retention_seconds=3000000.0)

    # Cleanup batch size below 10
    with pytest.raises(ValidationError, match="at least 10"):
        Settings(notification_webhook_replay_cleanup_batch_size=5)

    # Cleanup batch size above 10000
    with pytest.raises(ValidationError, match="cannot exceed 10000"):
        Settings(notification_webhook_replay_cleanup_batch_size=20000)


def test_convenience_verify_webhook_signature_with_replay(tmp_path: Path) -> None:
    """Verify the top-level convenience verify_webhook_signature function."""
    db_file = str(tmp_path / "convenience.db")
    store = SqliteReplayStore(db_path=db_file)
    secret = "whsec_conv_secret"
    payload = b'{"msg": "test"}'
    ts = 1730387000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-conv-1")
    header = format_signature_header(ts, sig)

    # First call succeeds
    assert (
        verify_webhook_signature(
            body=payload,
            header_value=header,
            secrets=secret,
            event_id="evt-conv-1",
            now=float(ts + 5),
            replay_store=store,
        )
        is True
    )

    # Duplicate call returns False (raise_on_error=False default)
    assert (
        verify_webhook_signature(
            body=payload,
            header_value=header,
            secrets=secret,
            event_id="evt-conv-1",
            now=float(ts + 6),
            replay_store=store,
        )
        is False
    )
