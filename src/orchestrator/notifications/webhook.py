"""Production HTTP webhook notification handler for transactional outbox."""

from __future__ import annotations

import datetime
import json
import logging
import threading
import time
import urllib.parse
from collections.abc import Callable
from typing import Any

import httpx

from ..graph.supervisor import sanitize_error
from .handler import NotificationDeliveryResult, NotificationHandler
from .security import SsrfProtectionError, validate_webhook_url
from .signing import format_signature_header, generate_webhook_signature

logger = logging.getLogger(__name__)


class HttpWebhookNotificationHandler(NotificationHandler):
    """Production HTTP webhook notification delivery handler.

    Sends POST requests containing JSON-serialized outbox events to a configurable
    webhook destination with:
    - Strict SSRF protection rejecting loopback, private, link-local, and reserved IPs.
    - Redirect blocking (follow_redirects=False) to prevent redirect SSRF bypasses.
    - Stable event headers: X-Event-ID, X-Event-Type, X-Idempotency-Key.
    - Secret-safe authorization header injection.
    - Response classification (2xx success, 429/5xx retryable, 4xx/3xx terminal).
    - Bounded response body extraction and sensitive secret redaction.
    """

    def __init__(
        self,
        url: str,
        auth_token: str | None = None,
        auth_header: str = "Authorization",
        timeout_seconds: float = 10.0,
        tls_verify: bool = True,
        max_response_bytes: int = 65536,
        allow_private_ips: bool = False,
        allow_insecure_http: bool = False,
        client: httpx.Client | None = None,
        custom_headers: dict[str, str] | None = None,
        signing_secret: str | None = None,
        signing_secret_previous: str | None = None,
        signature_header: str = "X-Webhook-Signature",
        signature_tolerance_seconds: float = 300.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        # Validate URL syntax, scheme, and credentials at construction time
        validate_webhook_url(
            url,
            allow_private_ips=True,
            allow_insecure_http=allow_insecure_http,
            resolve_dns=False,
        )

        self._url = url.strip()
        self._auth_token = auth_token.strip() if auth_token else None
        self._auth_header = (auth_header or "Authorization").strip()
        self._timeout_seconds = max(0.1, float(timeout_seconds))
        self._tls_verify = bool(tls_verify)
        self._max_response_bytes = max(256, min(int(max_response_bytes), 1048576))
        self._allow_private_ips = bool(allow_private_ips)
        self._allow_insecure_http = bool(allow_insecure_http)
        self._custom_headers = dict(custom_headers or {})

        self._signing_secret = signing_secret.strip() if signing_secret else None
        self._signing_secret_previous = (
            signing_secret_previous.strip() if signing_secret_previous else None
        )
        self._signature_header = (signature_header or "X-Webhook-Signature").strip()
        self._signature_tolerance_seconds = max(1.0, float(signature_tolerance_seconds))
        self._clock = clock or time.time

        self._external_client = client is not None
        self._client = client
        self._client_lock = threading.Lock()

    @property
    def url(self) -> str:
        """Destination webhook URL."""
        return self._url

    @property
    def signing_enabled(self) -> bool:
        """Whether HMAC payload signing is enabled."""
        return self._signing_secret is not None

    @property
    def signature_header(self) -> str:
        """HTTP header name used for HMAC payload signatures."""
        return self._signature_header

    @property
    def redacted_url(self) -> str:
        """Destination webhook URL sanitized for log output."""
        try:
            p = urllib.parse.urlsplit(self._url)
            netloc = p.hostname or ""
            if p.port:
                netloc += f":{p.port}"
            return urllib.parse.urlunsplit((p.scheme, netloc, p.path, "", ""))
        except Exception:
            return "<redacted-url>"

    def _get_client(self) -> httpx.Client:
        """Return or lazily initialize the HTTP client."""
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    self._client = httpx.Client(
                        timeout=httpx.Timeout(self._timeout_seconds),
                        verify=self._tls_verify,
                        follow_redirects=False,
                    )
        return self._client

    def _redact(self, text: str) -> str:
        """Redact auth tokens, signing secrets, and sanitize internal error strings."""
        if not text:
            return ""
        if self._auth_token and len(self._auth_token) >= 4:
            text = text.replace(self._auth_token, "[REDACTED_SECRET]")
        if self._signing_secret and len(self._signing_secret) >= 4:
            text = text.replace(self._signing_secret, "[REDACTED_SECRET]")
        if self._signing_secret_previous and len(self._signing_secret_previous) >= 4:
            text = text.replace(self._signing_secret_previous, "[REDACTED_SECRET]")
        return sanitize_error(text)

    def send_notification(self, event: dict[str, Any]) -> NotificationDeliveryResult:
        """Deliver an outbox event via HTTP POST to the configured destination."""
        event_id = str(event.get("event_id", ""))
        event_type = str(event.get("event_type", ""))
        idempotency_key = event.get("idempotency_key") or event_id
        attempt_count = int(event.get("attempt_count", 1))

        # Runtime SSRF check (with DNS resolution)
        try:
            dest_url = validate_webhook_url(
                self._url,
                allow_private_ips=self._allow_private_ips,
                allow_insecure_http=self._allow_insecure_http,
                resolve_dns=True,
            )
        except SsrfProtectionError as exc:
            logger.warning(
                "Webhook delivery blocked by SSRF protection for url %s: %s",
                self.redacted_url,
                exc,
            )
            return NotificationDeliveryResult(
                success=False,
                error=f"SSRF validation blocked delivery: {self._redact(str(exc))}",
                retryable=False,
            )
        except Exception as exc:
            logger.warning("Webhook URL validation error for %s: %s", self.redacted_url, exc)
            return NotificationDeliveryResult(
                success=False,
                error=f"URL validation error: {self._redact(str(exc))}",
                retryable=False,
            )

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "User-Agent": "Multi-Agent-Orchestrator-Webhook/1.0",
            "X-Event-ID": event_id,
            "X-Event-Type": event_type,
            "X-Idempotency-Key": str(idempotency_key),
        }
        if self._custom_headers:
            headers.update(self._custom_headers)

        if self._auth_token:
            token_val = self._auth_token
            if (
                self._auth_header.lower() == "authorization"
                and not token_val.lower().startswith("bearer ")
            ):
                token_val = f"Bearer {token_val}"
            headers[self._auth_header] = token_val

        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "payload": event.get("payload") or {},
            "idempotency_key": event.get("idempotency_key"),
            "attempt_count": attempt_count,
            "created_at": event.get("created_at"),
        }
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")

        if self._signing_secret:
            now_ts = int(self._clock())
            sigs: list[str] = []
            _, sig_curr = generate_webhook_signature(
                payload_bytes=body_bytes,
                secret=self._signing_secret,
                timestamp=now_ts,
                event_id=event_id,
            )
            sigs.append(sig_curr)

            if self._signing_secret_previous:
                _, sig_prev = generate_webhook_signature(
                    payload_bytes=body_bytes,
                    secret=self._signing_secret_previous,
                    timestamp=now_ts,
                    event_id=event_id,
                )
                sigs.append(sig_prev)

            headers[self._signature_header] = format_signature_header(now_ts, sigs)

        try:
            client = self._get_client()
            response = client.post(dest_url, content=body_bytes, headers=headers)
        except httpx.TimeoutException as exc:
            logger.warning(
                "Webhook delivery timed out for event %s to %s: %s",
                event_id,
                self.redacted_url,
                exc,
            )
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook request timed out: {sanitize_error(str(exc))}"),
                retryable=True,
            )
        except httpx.NetworkError as exc:
            logger.warning(
                "Webhook network error for event %s to %s: %s",
                event_id,
                self.redacted_url,
                exc,
            )
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook network error: {sanitize_error(str(exc))}"),
                retryable=True,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "Webhook HTTP error for event %s to %s: %s",
                event_id,
                self.redacted_url,
                exc,
            )
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook HTTP error: {sanitize_error(str(exc))}"),
                retryable=False,
            )
        except Exception as exc:
            logger.error(
                "Unexpected webhook delivery exception for event %s: %s",
                event_id,
                exc,
                exc_info=True,
            )
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Unexpected webhook error: {sanitize_error(str(exc))}"),
                retryable=False,
            )

        status_code = response.status_code

        # 2xx Success
        if 200 <= status_code < 300:
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            return NotificationDeliveryResult(
                success=True,
                delivered_at=now_iso,
                metadata={
                    "status_code": status_code,
                    "attempt": attempt_count,
                },
            )

        # Bounded body reading for error preview
        body_preview = ""
        try:
            content_bytes = response.content[: self._max_response_bytes]
            body_preview = content_bytes.decode("utf-8", errors="replace").strip()
            if len(response.content) > self._max_response_bytes:
                body_preview += " ... [truncated]"
        except Exception:
            body_preview = "<unreadable response body>"

        snippet = self._redact(body_preview[:120]) if body_preview else "empty response"

        # 3xx Redirects: follow_redirects=False returns 3xx directly
        if 300 <= status_code < 400:
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(
                    f"Webhook returned unexpected redirect HTTP {status_code}. "
                    f"Redirects are disabled for security."
                ),
                retryable=False,
                metadata={"status_code": status_code},
            )

        # 429 Too Many Requests: retryable
        if status_code == 429:
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook rate limited (HTTP 429): {snippet}"),
                retryable=True,
                metadata={"status_code": status_code},
            )

        # 5xx Server Errors: retryable
        if 500 <= status_code < 600:
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook server error (HTTP {status_code}): {snippet}"),
                retryable=True,
                metadata={"status_code": status_code},
            )

        # 4xx Client Errors: non-retryable terminal failure
        if 400 <= status_code < 500:
            return NotificationDeliveryResult(
                success=False,
                error=self._redact(f"Webhook client error (HTTP {status_code}): {snippet}"),
                retryable=False,
                metadata={"status_code": status_code},
            )

        # Any other unexpected status code
        return NotificationDeliveryResult(
            success=False,
            error=self._redact(f"Webhook unexpected status HTTP {status_code}: {snippet}"),
            retryable=False,
            metadata={"status_code": status_code},
        )

    def close(self) -> None:
        """Close the underlying HTTP client if opened internally."""
        if self._client is not None and not self._external_client:
            with self._client_lock:
                if self._client is not None and not self._external_client:
                    self._client.close()
                    self._client = None

    def __enter__(self) -> HttpWebhookNotificationHandler:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
