"""Deterministic unit and integration tests for production webhook notifications."""

from __future__ import annotations

import http.server
import json
import threading
import time
from typing import Any

import pytest

from orchestrator.config import Settings
from orchestrator.notifications.dispatcher import OutboxDispatcher
from orchestrator.notifications.handler import TestNotificationHandler
from orchestrator.notifications.registry import (
    get_notification_handler,
    register_notification_provider,
)
from orchestrator.notifications.security import (
    SsrfProtectionError,
    is_forbidden_ip,
    validate_webhook_url,
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

        if server.response_delay > 0:
            time.sleep(server.response_delay)

        if server.custom_responder:
            server.custom_responder(self, record)
            return

        self.send_response(server.response_status)
        for h_key, h_val in server.response_headers.items():
            self.send_header(h_key, h_val)
        self.end_headers()
        self.wfile.write(server.response_body)

    def log_message(self, format: str, *args: Any) -> None:
        """Suppress default stderr HTTP server logging."""


class MockServer(http.server.HTTPServer):
    """Lightweight test HTTP server with recorded requests and configurable responses."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        super().__init__((host, port), MockWebhookHandler)
        self.recorded_requests: list[dict[str, Any]] = []
        self.response_status: int = 200
        self.response_headers: dict[str, str] = {"Content-Type": "application/json"}
        self.response_body: bytes = b'{"status": "ok"}'
        self.response_delay: float = 0.0
        self.custom_responder: Any = None


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
    db_file = str(tmp_path / "test_webhook_outbox.db")
    return CheckpointStore(db_path=db_file)


def test_successful_webhook_delivery(mock_server: MockServer) -> None:
    """Verify successful 200 OK webhook delivery with stable headers and JSON body."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/notifications/webhook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        auth_token="test-secret-token-12345",
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {
        "event_id": "evt-123",
        "event_type": "tool.approval_requested",
        "payload": {"approval_id": "appr-001", "tool_name": "bash"},
        "idempotency_key": "idemp-evt-123",
        "attempt_count": 1,
        "created_at": "2026-10-31T12:00:00Z",
    }

    result = handler.send_notification(event)
    assert result.success is True
    assert result.error is None
    assert result.delivered_at is not None
    assert result.metadata["status_code"] == 200
    assert result.metadata["attempt"] == 1

    # Verify server received expected request
    assert len(mock_server.recorded_requests) == 1
    req = mock_server.recorded_requests[0]
    assert req["path"] == "/notifications/webhook"
    headers = req["headers"]
    assert headers.get("X-Event-ID") == "evt-123"
    assert headers.get("X-Event-Type") == "tool.approval_requested"
    assert headers.get("X-Idempotency-Key") == "idemp-evt-123"
    assert headers.get("Authorization") == "Bearer test-secret-token-12345"
    assert headers.get("Content-Type") == "application/json"

    body = json.loads(req["body"].decode("utf-8"))
    assert body["event_id"] == "evt-123"
    assert body["event_type"] == "tool.approval_requested"
    assert body["payload"]["tool_name"] == "bash"
    handler.close()


def test_custom_auth_header(mock_server: MockServer) -> None:
    """Verify custom auth header without 'Bearer ' prefix."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        auth_token="custom-api-key-999",
        auth_header="X-API-Key",
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-auth-1", "event_type": "tool.approval_granted"}
    result = handler.send_notification(event)
    assert result.success is True

    req = mock_server.recorded_requests[0]
    assert req["headers"].get("X-API-Key") == "custom-api-key-999"
    handler.close()


@pytest.mark.parametrize("status_code", [429, 500, 502, 503, 504])
def test_retryable_status_codes(mock_server: MockServer, status_code: int) -> None:
    """Verify HTTP 429 and 5xx statuses are classified as retryable failures."""
    mock_server.response_status = status_code
    mock_server.response_body = f'{{"error": "status {status_code}"}}'.encode()

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": f"evt-{status_code}", "event_type": "tool.approval_rejected"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is True
    assert result.metadata["status_code"] == status_code
    assert str(status_code) in (result.error or "")
    handler.close()


@pytest.mark.parametrize("status_code", [400, 401, 403, 404, 422])
def test_non_retryable_client_error_codes(mock_server: MockServer, status_code: int) -> None:
    """Verify HTTP 4xx statuses (except 429) are classified as non-retryable failures."""
    mock_server.response_status = status_code
    mock_server.response_body = f'{{"error": "client error {status_code}"}}'.encode()

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": f"evt-{status_code}", "event_type": "tool.approval_expired"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is False
    assert result.metadata["status_code"] == status_code
    assert str(status_code) in (result.error or "")
    handler.close()


def test_redirect_rejection(mock_server: MockServer) -> None:
    """Verify HTTP redirects (301, 302) are rejected and not followed for SSRF safety."""
    mock_server.response_status = 302
    mock_server.response_headers = {"Location": "http://127.0.0.1:9999/malicious"}
    mock_server.response_body = b"Found"

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-redirect", "event_type": "tool.approval_escalated"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is False
    assert result.metadata["status_code"] == 302
    assert "redirect" in (result.error or "").lower()
    handler.close()


def test_request_timeout(mock_server: MockServer) -> None:
    """Verify request timeouts are classified as retryable delivery failures."""
    mock_server.response_delay = 0.5

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        timeout_seconds=0.1,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-timeout", "event_type": "tool.approval_requested"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is True
    assert "timed out" in (result.error or "").lower()
    handler.close()


def test_connection_failure() -> None:
    """Verify connection errors to an unreachable port are retryable."""
    # Connect to a port where nothing is listening
    url = "http://127.0.0.1:59998/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        timeout_seconds=0.5,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-conn-fail", "event_type": "tool.approval_granted"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is True
    err_lower = (result.error or "").lower()
    assert any(term in err_lower for term in ("network error", "timed out", "connection"))
    handler.close()


@pytest.mark.parametrize(
    "restricted_url",
    [
        "https://127.0.0.1/webhook",
        "https://localhost/webhook",
        "https://10.0.0.1/webhook",
        "https://192.168.1.1/webhook",
        "https://172.16.0.1/webhook",
        "https://169.254.169.254/latest/meta-data",
        "https://[::1]/webhook",
        "https://[::ffff:127.0.0.1]/webhook",
    ],
)
def test_ssrf_blocking_private_and_reserved_ips(restricted_url: str) -> None:
    """Verify SSRF protection rejects private, loopback, link-local, and cloud metadata IPs."""
    # When allow_private_ips=False (the production default)
    handler = HttpWebhookNotificationHandler(
        url=restricted_url,
        allow_private_ips=False,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-ssrf", "event_type": "tool.approval_requested"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is False
    assert "ssrf" in (result.error or "").lower()
    handler.close()


def test_ssrf_scheme_and_credential_validation() -> None:
    """Verify scheme restrictions and embedded credentials reject at initialization."""
    with pytest.raises(SsrfProtectionError, match="Unsupported URL scheme"):
        validate_webhook_url("ftp://example.com/webhook")

    with pytest.raises(SsrfProtectionError, match="Insecure HTTP scheme"):
        validate_webhook_url("http://example.com/webhook", allow_insecure_http=False)

    with pytest.raises(SsrfProtectionError, match="URL credentials"):
        validate_webhook_url("https://user:password@example.com/webhook")

    with pytest.raises(SsrfProtectionError, match="valid hostname"):
        validate_webhook_url("https:///path")


def test_forbidden_ip_checker() -> None:
    """Test is_forbidden_ip helper across IP ranges."""
    assert is_forbidden_ip("127.0.0.1") is True
    assert is_forbidden_ip("10.0.0.5") is True
    assert is_forbidden_ip("192.168.0.1") is True
    assert is_forbidden_ip("172.16.0.1") is True
    assert is_forbidden_ip("169.254.169.254") is True
    assert is_forbidden_ip("100.64.0.1") is True  # CGNAT
    assert is_forbidden_ip("198.18.0.1") is True  # Benchmark
    assert is_forbidden_ip("::1") is True
    assert is_forbidden_ip("::ffff:127.0.0.1") is True
    assert is_forbidden_ip("8.8.8.8") is False
    assert is_forbidden_ip("1.1.1.1") is False


def test_response_body_truncation(mock_server: MockServer) -> None:
    """Verify error responses exceeding max_response_bytes are truncated with indicator."""
    mock_server.response_status = 500
    # Create large 100KB payload
    mock_server.response_body = b"X" * 100000

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        max_response_bytes=512,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-large-body", "event_type": "tool.approval_rejected"}
    result = handler.send_notification(event)

    assert result.success is False
    assert result.retryable is True
    assert len(result.error or "") <= 200  # sanitize_error bounds to 200 chars
    handler.close()


def test_secret_redaction_in_errors(mock_server: MockServer) -> None:
    """Verify auth secrets echoed in response errors are redacted."""
    secret_token = "my-ultra-secret-token-xyz789"
    mock_server.response_status = 500
    mock_server.response_body = f"Server rejected auth token: {secret_token}".encode()

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/hook"

    handler = HttpWebhookNotificationHandler(
        url=url,
        auth_token=secret_token,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    event = {"event_id": "evt-redact", "event_type": "tool.approval_requested"}
    result = handler.send_notification(event)

    assert result.success is False
    assert secret_token not in (result.error or "")
    assert "[REDACTED" in (result.error or "")
    handler.close()


def test_provider_registry_and_factory() -> None:
    """Verify provider registry resolution and custom provider registration."""
    # Test built-in 'test' provider
    test_h = get_notification_handler("test")
    assert isinstance(test_h, TestNotificationHandler)

    # Test built-in 'webhook' provider with kwargs
    webhook_h = get_notification_handler(
        "webhook",
        url="https://example.com/webhook",
        auth_token="token-abc",
    )
    assert isinstance(webhook_h, HttpWebhookNotificationHandler)
    assert webhook_h.url == "https://example.com/webhook"
    webhook_h.close()

    # Test unknown provider raises ValueError
    with pytest.raises(ValueError, match="Unknown notification provider"):
        get_notification_handler("non_existent_provider")

    # Test custom provider registration
    class CustomHandler(TestNotificationHandler):
        pass

    register_notification_provider("custom_mock", lambda settings, **kwargs: CustomHandler())
    custom_h = get_notification_handler("custom_mock")
    assert isinstance(custom_h, CustomHandler)


def test_outbox_dispatcher_integration_with_webhook(
    mock_server: MockServer, temp_store: CheckpointStore
) -> None:
    """Verify end-to-end outbox dispatcher cycle delivering an event via HttpWebhookHandler."""
    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/notifications/outbox"

    # Insert an outbox event in database
    event_row = temp_store.insert_outbox_event(
        event_type="tool.approval_granted",
        payload={"approval_id": "appr-int-1", "tool_name": "search"},
        idempotency_key="idemp-int-1",
    )
    event_id = event_row["event_id"]

    handler = HttpWebhookNotificationHandler(
        url=url,
        auth_token="dispatcher-secret-token",
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    dispatcher = OutboxDispatcher(
        store=temp_store,
        handler=handler,
        owner_id="webhook-dispatcher-test",
    )

    # Run single dispatch cycle
    summary = dispatcher.run_dispatch_cycle()

    assert summary["claimed_count"] == 1
    assert summary["delivered_count"] == 1
    assert summary["failed_count"] == 0
    assert summary["dead_letter_count"] == 0

    # Verify outbox event transitioned to 'delivered' in store
    history = temp_store.get_outbox_event_history(event_id)
    assert len(history) >= 2
    assert history[-1]["status"] == "delivered"

    delivered = temp_store.get_outbox_events(status="delivered")
    assert len(delivered) == 1
    assert delivered[0]["event_id"] == event_id

    # Verify mock server received the event
    assert len(mock_server.recorded_requests) == 1
    req = mock_server.recorded_requests[0]
    assert req["headers"].get("X-Event-ID") == event_id
    assert req["headers"].get("X-Idempotency-Key") == "idemp-int-1"
    handler.close()


def test_outbox_dispatcher_dead_letter_on_non_retryable_webhook_error(
    mock_server: MockServer, temp_store: CheckpointStore
) -> None:
    """Verify dispatcher immediately moves an event to dead_letter on non-retryable 4xx error."""
    mock_server.response_status = 400
    mock_server.response_body = b'{"error": "Bad Request: malformed JSON"}'

    port = mock_server.server_port
    url = f"http://127.0.0.1:{port}/notifications/outbox"

    event_row = temp_store.insert_outbox_event(
        event_type="tool.approval_rejected",
        payload={"approval_id": "appr-int-2"},
        idempotency_key="idemp-int-2",
    )
    event_id = event_row["event_id"]

    handler = HttpWebhookNotificationHandler(
        url=url,
        allow_private_ips=True,
        allow_insecure_http=True,
    )

    dispatcher = OutboxDispatcher(
        store=temp_store,
        handler=handler,
        owner_id="deadletter-dispatcher-test",
        max_attempts=5,
    )

    summary = dispatcher.run_dispatch_cycle()

    # Event moved to dead_letter immediately without 5 retries because 400 is terminal
    assert summary["claimed_count"] == 1
    assert summary["delivered_count"] == 0
    assert summary["failed_count"] == 0
    assert summary["dead_letter_count"] == 1

    history = temp_store.get_outbox_event_history(event_id)
    assert len(history) >= 2
    assert history[-1]["status"] == "dead_letter"
    assert "HTTP 400" in (history[-1]["error_message"] or "")

    dead_letters = temp_store.get_outbox_events(status="dead_letter")
    assert len(dead_letters) == 1
    assert dead_letters[0]["event_id"] == event_id
    handler.close()


def test_dispatcher_instantiation_uses_provider_from_settings() -> None:
    """Verify OutboxDispatcher instantiates the provider specified in settings by default."""
    cfg = Settings(notification_provider="test")
    dispatcher = OutboxDispatcher(settings=cfg)
    assert isinstance(dispatcher.handler, TestNotificationHandler)
