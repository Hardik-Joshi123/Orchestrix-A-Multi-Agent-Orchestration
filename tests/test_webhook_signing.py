"""Deterministic tests for webhook payload signing, replay protection, and secret rotation."""

from __future__ import annotations

import concurrent.futures
import http.server
import json
import threading
from typing import Any

import pytest

from orchestrator.config import Settings
from orchestrator.notifications.dispatcher import OutboxDispatcher
from orchestrator.notifications.signing import (
    ReplayProtectionCache,
    WebhookSignatureError,
    WebhookSignatureVerifier,
    format_signature_header,
    generate_webhook_signature,
    parse_signature_header,
    verify_webhook_signature,
)
from orchestrator.notifications.webhook import HttpWebhookNotificationHandler
from orchestrator.persistence.store import CheckpointStore


class MockWebhookHandler(http.server.BaseHTTPRequestHandler):
    """Deterministic HTTP request handler for webhook testing."""

    def do_POST(self) -> None:
        server: MockServer = self.server  # type: ignore
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length)

        record = {
            "path": self.path,
            "headers": {k: v for k, v in self.headers.items()},
            "body": body_bytes,
        }
        server.recorded_requests.append(record)

        if server.response_status == 200:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_response(server.response_status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(server.response_body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stderr HTTP server logging."""


class MockServer(http.server.HTTPServer):
    """Lightweight test HTTP server with recorded requests."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        super().__init__((host, port), MockWebhookHandler)
        self.recorded_requests: list[dict[str, Any]] = []
        self.response_status: int = 200
        self.response_body: bytes = b'{"status": "ok"}'


@pytest.fixture
def mock_server():
    """Start and tear down a deterministic local mock HTTP server."""
    server = MockServer()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


@pytest.fixture
def temp_store(tmp_path: Any) -> CheckpointStore:
    """Return a CheckpointStore instance bound to an isolated temporary database."""
    db_file = str(tmp_path / "test_signing_outbox.db")
    return CheckpointStore(db_path=db_file)


# ---------------------------------------------------------------------------
# Unit Tests: Signing, Parsing, and Formatting
# ---------------------------------------------------------------------------


def test_signature_generation_and_header_formatting() -> None:
    """Verify HMAC-SHA256 signature generation and versioned header format."""
    payload = b'{"event_id": "evt-123", "action": "test"}'
    secret = "whsec_primary_secret_key_12345"
    fixed_ts = 1730376000

    ts, sig = generate_webhook_signature(
        payload_bytes=payload,
        secret=secret,
        timestamp=fixed_ts,
        event_id="evt-123",
    )
    assert ts == fixed_ts
    assert len(sig) == 64  # SHA-256 hex digest length

    # Same inputs yield identical deterministic signature
    _, sig_repeat = generate_webhook_signature(
        payload_bytes=payload,
        secret=secret,
        timestamp=fixed_ts,
        event_id="evt-123",
    )
    assert sig == sig_repeat

    header_str = format_signature_header(ts, sig)
    assert header_str == f"t={fixed_ts},v1={sig}"

    parsed_ts, parsed_sigs = parse_signature_header(header_str)
    assert parsed_ts == fixed_ts
    assert len(parsed_sigs) == 1
    assert parsed_sigs[0] == ("v1", sig)


def test_dual_signature_header_formatting_for_rotation() -> None:
    """Verify dual signature header formatting for active rotation windows."""
    sig1 = "a" * 64
    sig2 = "b" * 64
    header_str = format_signature_header(1730376000, [sig1, sig2])

    assert header_str == f"t=1730376000,v1={sig1},v1={sig2}"
    parsed_ts, parsed_sigs = parse_signature_header(header_str)
    assert parsed_ts == 1730376000
    assert len(parsed_sigs) == 2
    assert parsed_sigs[0] == ("v1", sig1)
    assert parsed_sigs[1] == ("v1", sig2)


# ---------------------------------------------------------------------------
# Verification & Tampering Protection Tests
# ---------------------------------------------------------------------------


def test_signature_verification_success() -> None:
    """Verify successful verification of unaltered webhook payload and headers."""
    payload = b'{"event_id": "evt-001", "event_type": "tool.approval_requested"}'
    secret = "whsec_test_secret_abc"
    fixed_ts = 1730376000

    _, sig = generate_webhook_signature(payload, secret, timestamp=fixed_ts, event_id="evt-001")
    header = format_signature_header(fixed_ts, sig)

    # Verification succeeds when referencing the same timestamp
    is_valid = verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=secret,
        event_id="evt-001",
        tolerance_seconds=300.0,
        now=float(fixed_ts + 10),
    )
    assert is_valid is True


def test_payload_tampering_rejected() -> None:
    """Verify any modification to body bytes causes signature verification to fail."""
    payload = b'{"event_id": "evt-001", "amount": 100}'
    tampered_payload = b'{"event_id": "evt-001", "amount": 999}'
    secret = "whsec_test_secret_abc"
    fixed_ts = 1730376000

    _, sig = generate_webhook_signature(payload, secret, timestamp=fixed_ts, event_id="evt-001")
    header = format_signature_header(fixed_ts, sig)

    # Tampered payload fails verification
    assert not verify_webhook_signature(
        body=tampered_payload,
        header_value=header,
        secrets=secret,
        event_id="evt-001",
        now=float(fixed_ts + 5),
    )

    with pytest.raises(WebhookSignatureError, match="signature mismatch"):
        verify_webhook_signature(
            body=tampered_payload,
            header_value=header,
            secrets=secret,
            event_id="evt-001",
            now=float(fixed_ts + 5),
            raise_on_error=True,
        )


def test_timestamp_tampering_rejected() -> None:
    """Verify altering timestamp in header invalidates signature."""
    payload = b'{"event_id": "evt-001"}'
    secret = "whsec_test_secret_abc"
    fixed_ts = 1730376000

    _, sig = generate_webhook_signature(payload, secret, timestamp=fixed_ts, event_id="evt-001")
    # Attacker alters timestamp in header
    tampered_header = format_signature_header(fixed_ts + 50, sig)

    assert not verify_webhook_signature(
        body=payload,
        header_value=tampered_header,
        secrets=secret,
        event_id="evt-001",
        now=float(fixed_ts + 60),
    )


def test_event_id_tampering_rejected() -> None:
    """Verify mismatched event ID between header/context and signature fails verification."""
    payload = b'{"message": "hello"}'
    secret = "whsec_test_secret_abc"
    fixed_ts = 1730376000

    _, sig = generate_webhook_signature(payload, secret, timestamp=fixed_ts, event_id="evt-original")
    header = format_signature_header(fixed_ts, sig)

    # Receiver verifies with different event_id -> fails
    assert not verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=secret,
        event_id="evt-tampered",
        now=float(fixed_ts + 5),
    )


# ---------------------------------------------------------------------------
# Timestamp Tolerance & Expiry Tests
# ---------------------------------------------------------------------------


def test_expired_timestamp_rejected() -> None:
    """Verify request with timestamp older than tolerance window is rejected."""
    payload = b'{"test": "expired"}'
    secret = "whsec_test_secret"
    ts = 1730376000
    tolerance = 300.0  # 5 minutes

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-exp")
    header = format_signature_header(ts, sig)

    # 301 seconds later -> expired
    now_time = float(ts + 301)
    with pytest.raises(WebhookSignatureError, match="timestamp expired"):
        verify_webhook_signature(
            body=payload,
            header_value=header,
            secrets=secret,
            event_id="evt-exp",
            tolerance_seconds=tolerance,
            now=now_time,
            raise_on_error=True,
        )

    # 299 seconds later -> valid
    assert verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=secret,
        event_id="evt-exp",
        tolerance_seconds=tolerance,
        now=float(ts + 299),
    )


def test_future_dated_timestamp_rejected() -> None:
    """Verify request with excessively future-dated timestamp is rejected."""
    payload = b'{"test": "future"}'
    secret = "whsec_test_secret"
    ts = 1730376000
    tolerance = 300.0

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-fut")
    header = format_signature_header(ts, sig)

    # Current time is 305 seconds BEFORE the header timestamp
    now_time = float(ts - 305)
    with pytest.raises(WebhookSignatureError, match="too far in future"):
        verify_webhook_signature(
            body=payload,
            header_value=header,
            secrets=secret,
            event_id="evt-fut",
            tolerance_seconds=tolerance,
            now=now_time,
            raise_on_error=True,
        )


# ---------------------------------------------------------------------------
# Replay Protection Tests
# ---------------------------------------------------------------------------


def test_replay_protection_detection() -> None:
    """Verify replay cache detects and rejects duplicate deliveries of same event ID."""
    cache = ReplayProtectionCache(max_entries=100)
    payload = b'{"event_id": "evt-replay-1", "action": "approve"}'
    secret = "whsec_secret_123"
    ts = 1730376000

    _, sig = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-replay-1")
    header = format_signature_header(ts, sig)

    verifier = WebhookSignatureVerifier(
        secrets=secret,
        tolerance_seconds=300.0,
        replay_cache=cache,
    )

    # First verification passes
    assert verifier.verify(payload, header, event_id="evt-replay-1", now=float(ts + 5)) is True

    # Immediate second verification of same event ID is caught as a replay attack
    with pytest.raises(WebhookSignatureError, match="Replay detected"):
        verifier.verify(payload, header, event_id="evt-replay-1", now=float(ts + 6))

    # A different event ID within the same window passes
    _, sig2 = generate_webhook_signature(payload, secret, timestamp=ts, event_id="evt-replay-2")
    header2 = format_signature_header(ts, sig2)
    assert verifier.verify(payload, header2, event_id="evt-replay-2", now=float(ts + 7)) is True


# ---------------------------------------------------------------------------
# Secret Rotation Tests
# ---------------------------------------------------------------------------


def test_secret_rotation_verification_against_either_secret() -> None:
    """Verify dual-signed request verifies with either current or previous secret."""
    payload = b'{"event_id": "evt-rotate", "status": "ok"}'
    secret_current = "whsec_new_active_secret_2026"
    secret_previous = "whsec_old_retiring_secret_2025"
    ts = 1730376000

    _, sig_curr = generate_webhook_signature(
        payload, secret_current, timestamp=ts, event_id="evt-rotate"
    )
    _, sig_prev = generate_webhook_signature(
        payload, secret_previous, timestamp=ts, event_id="evt-rotate"
    )

    # Dual signature header sent during rotation window
    header = format_signature_header(ts, [sig_curr, sig_prev])

    # Receiver configured with only CURRENT secret verifies
    assert verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=secret_current,
        event_id="evt-rotate",
        now=float(ts + 5),
    )

    # Receiver configured with only PREVIOUS secret verifies
    assert verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=secret_previous,
        event_id="evt-rotate",
        now=float(ts + 5),
    )

    # Receiver configured with BOTH secrets verifies
    assert verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets=[secret_current, secret_previous],
        event_id="evt-rotate",
        now=float(ts + 5),
    )

    # Receiver configured with an unrelated third secret fails
    assert not verify_webhook_signature(
        body=payload,
        header_value=header,
        secrets="whsec_unrelated_foreign_secret",
        event_id="evt-rotate",
        now=float(ts + 5),
    )


# ---------------------------------------------------------------------------
# Secret Redaction Tests
# ---------------------------------------------------------------------------


def test_signing_secrets_redacted_in_errors(mock_server: MockServer) -> None:
    """Verify current and previous signing secrets are redacted from error logs."""
    secret_curr = "whsec_super_secret_current_xyz999"
    secret_prev = "whsec_super_secret_previous_abc888"

    mock_server.response_status = 500
    mock_server.response_body = (
        f"Server failed. Dump: curr={secret_curr}, prev={secret_prev}".encode()
    )

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        signing_secret=secret_curr,
        signing_secret_previous=secret_prev,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-redact-signing", "event_type": "tool.approval_requested"}
    result = handler.send_notification(event)

    assert result.success is False
    assert secret_curr not in (result.error or "")
    assert secret_prev not in (result.error or "")
    assert "[REDACTED_SECRET]" in (result.error or "")
    handler.close()


# ---------------------------------------------------------------------------
# End-to-End Integration Tests with Mock HTTP Server
# ---------------------------------------------------------------------------


def test_e2e_webhook_delivery_with_valid_signature(mock_server: MockServer) -> None:
    """Verify outbound HTTP webhook delivery emits valid verifiable signature header."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/webhook-signed"
    secret = "whsec_e2e_delivery_secret"

    fixed_time = 1730376000.0

    handler = HttpWebhookNotificationHandler(
        url=url,
        signing_secret=secret,
        signature_header="X-Webhook-Signature",
        allow_private_ips=True,
        allow_insecure_http=True,
        clock=lambda: fixed_time,
    )

    event = {
        "event_id": "evt-e2e-sig-1",
        "event_type": "tool.approval_granted",
        "payload": {"approval_id": "appr-sig-1", "command": "run_analysis"},
        "idempotency_key": "idemp-sig-1",
        "attempt_count": 1,
        "created_at": "2026-10-31T12:00:00Z",
    }

    result = handler.send_notification(event)
    assert result.success is True
    assert result.error is None
    assert result.delivered_at is not None

    # Inspect request received by mock server
    assert len(mock_server.recorded_requests) == 1
    req = mock_server.recorded_requests[0]
    headers = req["headers"]
    body_bytes = req["body"]

    assert "X-Webhook-Signature" in headers
    sig_header_val = headers["X-Webhook-Signature"]

    # Verify signature on receiver side using verify_webhook_signature
    verified = verify_webhook_signature(
        body=body_bytes,
        header_value=sig_header_val,
        secrets=secret,
        event_id="evt-e2e-sig-1",
        now=fixed_time + 5,
        raise_on_error=True,
    )
    assert verified is True
    handler.close()


def test_outbox_dispatcher_integration_with_signed_webhooks(
    mock_server: MockServer, temp_store: CheckpointStore
) -> None:
    """Verify OutboxDispatcher claims events and delivers them with valid HMAC signatures."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/outbox-signed"
    secret = "whsec_dispatcher_secret_999"

    event_row = temp_store.insert_outbox_event(
        event_type="tool.approval_requested",
        payload={"approval_id": "appr-disp-sig", "tool": "file_reader"},
        idempotency_key="idemp-disp-sig",
    )
    event_id = event_row["event_id"]

    handler = HttpWebhookNotificationHandler(
        url=url,
        signing_secret=secret,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    dispatcher = OutboxDispatcher(
        store=temp_store,
        handler=handler,
        owner_id="dispatcher-signing-worker",
    )

    summary = dispatcher.run_dispatch_cycle()
    assert summary["claimed_count"] == 1
    assert summary["delivered_count"] == 1

    # Verify mock server received and can cryptographically verify the signature
    assert len(mock_server.recorded_requests) == 1
    req = mock_server.recorded_requests[0]
    sig_header = req["headers"]["X-Webhook-Signature"]

    is_valid = verify_webhook_signature(
        body=req["body"],
        header_value=sig_header,
        secrets=secret,
        event_id=event_id,
        tolerance_seconds=300.0,
    )
    assert is_valid is True

    # Check store status
    delivered_events = temp_store.get_outbox_events(status="delivered")
    assert len(delivered_events) == 1
    assert delivered_events[0]["event_id"] == event_id
    handler.close()


# ---------------------------------------------------------------------------
# Concurrency & Configuration Tests
# ---------------------------------------------------------------------------


def test_concurrent_webhook_signing_delivery(mock_server: MockServer) -> None:
    """Verify concurrent webhook deliveries generate unique, valid signatures without race conditions."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/concurrent"
    secret = "whsec_concurrent_test_secret"

    handler = HttpWebhookNotificationHandler(
        url=url,
        signing_secret=secret,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    num_threads = 10
    events = [
        {
            "event_id": f"evt-conc-{i}",
            "event_type": "tool.approval_granted",
            "payload": {"index": i},
            "idempotency_key": f"idemp-conc-{i}",
        }
        for i in range(num_threads)
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(handler.send_notification, ev) for ev in events]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    assert all(r.success for r in results)
    assert len(mock_server.recorded_requests) == num_threads

    # Verify every recorded request has a valid signature matching its respective event ID
    for req in mock_server.recorded_requests:
        body_obj = json.loads(req["body"].decode("utf-8"))
        ev_id = body_obj["event_id"]
        sig_header = req["headers"]["X-Webhook-Signature"]

        valid = verify_webhook_signature(
            body=req["body"],
            header_value=sig_header,
            secrets=secret,
            event_id=ev_id,
            tolerance_seconds=300.0,
        )
        assert valid is True

    handler.close()


def test_configuration_validation_and_diagnostics_for_signing() -> None:
    """Verify settings validation rules and secret-safe diagnostics for signing."""
    # Tolerance bounds
    with pytest.raises(ValueError, match="at least 5.0 seconds"):
        Settings(notification_webhook_signature_tolerance_seconds=2.0)

    with pytest.raises(ValueError, match="cannot exceed 86400.0 seconds"):
        Settings(notification_webhook_signature_tolerance_seconds=100000.0)

    # Diagnostics do not expose secrets
    secret_val = "super-secret-key-do-not-leak"
    cfg = Settings(
        notification_provider="webhook",
        notification_webhook_url="https://example.com/webhook",
        notification_webhook_signing_secret=secret_val,
        notification_webhook_signing_secret_previous="prev-secret-do-not-leak",
    )
    from orchestrator.config import get_startup_diagnostics

    diag = get_startup_diagnostics(cfg)
    notif_diag = diag["notifications"]

    assert notif_diag["signing_enabled"] is True
    assert notif_diag["has_previous_signing_secret"] is True
    assert notif_diag["signature_header"] == "X-Webhook-Signature"

    diag_str = json.dumps(diag)
    assert secret_val not in diag_str
    assert "prev-secret-do-not-leak" not in diag_str
