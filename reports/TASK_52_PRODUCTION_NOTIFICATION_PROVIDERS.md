# Task 52: Production Notification Providers & Webhook Integration

## Executive Summary
Task 52 implements production-grade external HTTP webhook notification delivery for the transactional
outbox introduced in Tasks 50 and 51. The implementation introduces an extensible notification provider
registry, strict Server-Side Request Forgery (SSRF) defense, non-following redirect protection, bounded
response buffering, secret-safe authorization headers, and deterministic offline mock server test suites.

All functionality operates strictly within bounded, non-destructive transactional safety rules, preserving
at-least-once external notification semantics without altering committed tool approval states or
auto-approving blocked executions.

---

## Architecture & Provider Registry

### 1. Provider Abstraction
The notification subsystem decouples outbox persistence and dispatching from external delivery mechanisms:
- **`NotificationHandler`**: Base interface (`send_notification(event) -> NotificationDeliveryResult`).
- **`TestNotificationHandler`**: In-memory test provider supporting simulated transient/permanent failures,
  counting, duplicate recording, and call inspections without real network activity.
- **`HttpWebhookNotificationHandler`**: Production HTTP webhook provider built with `httpx`, delivering
  JSON payloads with stable event headers, SSRF filtering, and bounded response parsing.

### 2. Extensible Provider Registry & Factory
The provider registry (`orchestrator.notifications.registry`) enables modular provider resolution:
- **`register_notification_provider(name, factory)`**: Registers custom notification delivery handlers.
- **`get_notification_handler(provider=None, settings=None, **kwargs)`**: Instantiates providers dynamically
  based on `Settings.notification_provider` (`"test"` or `"webhook"`) or explicit constructor parameters.
- **`OutboxDispatcher` Integration**: `OutboxDispatcher` defaults to `get_notification_handler()` when no
  explicit handler is passed, ensuring zero-configuration production deployment and complete backward
  compatibility with existing dev and test harnesses.

```mermaid
flowchart TD
    Store["CheckpointStore (SQLite)"] -->|claim_outbox_events| Dispatcher["OutboxDispatcher"]
    Dispatcher -->|send_notification| Registry["get_notification_handler()"]
    Registry -.->|provider='test'| TestHandler["TestNotificationHandler (In-Memory)"]
    Registry -.->|provider='webhook'| WebhookHandler["HttpWebhookNotificationHandler"]
    WebhookHandler -->|SSRF & URL Check| Security["security.validate_webhook_url()"]
    Security -->|HTTPS POST| RemoteWebhook["External Webhook Destination"]
    RemoteWebhook -->|HTTP Response| WebhookHandler
    WebhookHandler -->|NotificationDeliveryResult| Dispatcher
    Dispatcher -->|update_outbox_event_delivery| Store
```

---

## HTTP Webhook Delivery Mechanics

### 1. Stable Headers & JSON Payloads
Each HTTP notification POST includes standard identification headers:
- `Content-Type: application/json`
- `User-Agent: Multi-Agent-Orchestrator-Webhook/1.0`
- `X-Event-ID`: Unique persistent UUID of the outbox record (`event_id`).
- `X-Event-Type`: Approval lifecycle event type (e.g. `tool.approval_requested`, `tool.approval_granted`).
- `X-Idempotency-Key`: Stable deduplication key enabling receiver-side idempotent processing.
- Authorization: Configurable auth header (defaults to `Authorization: Bearer <token>`).

The request body contains structured JSON:
```json
{
  "event_id": "8905b736-...",
  "event_type": "tool.approval_requested",
  "payload": {
    "run_id": "run-001",
    "operation_key": "op-bash-1",
    "tool_name": "bash"
  },
  "idempotency_key": "idemp-001",
  "attempt_count": 1,
  "created_at": "2026-10-31T12:00:00Z"
}
```

### 2. HTTP Response Classification
Delivery results are classified into distinct outcomes:
- **`2xx Success`**: `NotificationDeliveryResult(success=True, retryable=False, delivered_at=ISO8601)`.
- **`429 Too Many Requests`**: `NotificationDeliveryResult(success=False, retryable=True)` (triggers backoff).
- **`5xx Server Errors & Timeouts`**: Transient failures (`success=False, retryable=True`), backoff scheduled.
- **`4xx Client Errors (except 429)`**: Terminal (`success=False, retryable=False`), transitioning directly
  to `dead_letter` status in SQLite without wasting retry attempts.
- **`3xx Redirects`**: Terminal (`success=False, retryable=False`), rejected for SSRF safety.

### 3. Bounded Buffering & Secret Redaction
- **Bounded Response Body**: Error responses are read up to `notification_webhook_max_response_bytes`
  (default 64 KB, bounds 256 B to 1 MB). Bodies exceeding this threshold append an explicit `... [truncated]`.
- **Secret Redaction**: Webhook auth tokens, API keys, passwords, and authorization headers are scrubbed from
  all error logs, delivery history entries, and exception messages using `_redact()` and `sanitize_error()`.

---

## SSRF Defense & Security Hardening

The SSRF protection module (`orchestrator.notifications.security`) enforces strict destination constraints:

1. **HTTPS Enforcement by Default**:
   Unencrypted `http://` URLs are rejected in production. Plain HTTP is only permitted when
   `notification_webhook_allow_insecure_http=True` is explicitly configured for local testing.
2. **Credential Stripping**:
   Embedded user credentials (`https://user:password@host/`) are blocked at parse time.
3. **Comprehensive IP Range Filtering**:
   Destination hostnames and IP addresses are evaluated against forbidden ranges:
   - Loopback: `127.0.0.0/8`, `::1`
   - Private RFC 1918: `10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`, `fc00::/7`
   - Link-Local & Cloud Metadata: `169.254.0.0/16` (including metadata `169.254.169.254`), `fe80::/10`
   - Carrier-Grade NAT (RFC 6598): `100.64.0.0/10`
   - Benchmark (RFC 2544): `198.18.0.0/15`
   - IPv4-Mapped IPv6: `::ffff:127.0.0.1` and `::ffff:169.254.169.254` are unpacked and blocked.
   - Multicast, Reserved, and Unspecified (`0.0.0.0`, `::`).
4. **Runtime DNS Verification**:
   Hostname destinations are resolved at runtime via `socket.getaddrinfo`, and every resolved IP is
   validated prior to transmission, defeating DNS rebinding attacks.
5. **Redirect Defense**:
   `httpx.Client(..., follow_redirects=False)` ensures the HTTP client never follows HTTP 3xx redirects,
   closing internal network pivoting vulnerabilities.

---

## Configuration Settings Reference

Added settings in `orchestrator.config.Settings`:

| Field Name | Default | Description |
| :--- | :--- | :--- |
| `notification_provider` | `"test"` | Active provider (`"test"`, `"webhook"`). |
| `notification_webhook_url` | `None` | Destination HTTPS webhook endpoint. |
| `notification_webhook_auth_token` | `None` | Webhook bearer or secret auth token. |
| `notification_webhook_auth_header` | `"Authorization"` | Authorization header name. |
| `notification_webhook_timeout_seconds` | `10.0` | Connection and read timeout (0.5–120s). |
| `notification_webhook_tls_verify` | `True` | Enforce TLS certificate verification. |
| `notification_webhook_max_response_bytes` | `65536` | Max error response body bytes logged. |
| `notification_webhook_allow_private_ips` | `False` | SSRF bypass (testing only). |
| `notification_webhook_allow_insecure_http` | `False` | Allow http:// scheme (testing only). |

---

## Files Created & Modified

### Modified Files
- `src/orchestrator/config.py`: Added notification provider settings, field validators, cross-field
  validation in `validate_configuration()`, and secret-redacted diagnostics in `get_startup_diagnostics()`.
- `src/orchestrator/notifications/dispatcher.py`: Updated `OutboxDispatcher` to resolve default handlers
  dynamically via `get_notification_handler(settings=cfg)`.
- `src/orchestrator/notifications/__init__.py`: Exported handler, security, and registry symbols.
- `README.md`: Added notification settings to configuration reference table.
- `docs/DEVELOPER_GUIDE.md`: Added Section 12.17 ("Production Notification Providers & Webhook Integration").

### Created Files
- `src/orchestrator/notifications/security.py`: SSRF protection, IP range checking, and URL validation.
- `src/orchestrator/notifications/webhook.py`: `HttpWebhookNotificationHandler` with response classification.
- `src/orchestrator/notifications/registry.py`: Provider registry and factory methods.
- `tests/test_notification_webhook.py`: Comprehensive test suite using a local mock HTTP server.
- `reports/TASK_52_PRODUCTION_NOTIFICATION_PROVIDERS.md`: This deliverable report.

---

## Verification & Test Results

### 1. Webhook Test Suite (`tests/test_notification_webhook.py`)
Deterministic test suite using a thread-safe local HTTP mock server without external network dependencies:
- **`test_successful_webhook_delivery`**: 200 OK delivery with stable headers (`X-Event-ID`,
  `X-Event-Type`, `X-Idempotency-Key`, `Authorization`) and JSON payload verification.
- **`test_custom_auth_header`**: Custom header name without Bearer prefix.
- **`test_retryable_status_codes`**: 429, 500, 502, 503, 504 classified as `retryable=True`.
- **`test_non_retryable_client_error_codes`**: 400, 401, 403, 404, 422 classified as `retryable=False`.
- **`test_redirect_rejection`**: 301/302 redirects rejected without following.
- **`test_request_timeout`**: Network timeouts classified as `retryable=True`.
- **`test_connection_failure`**: Unreachable host/port classified as `retryable=True`.
- **`test_ssrf_blocking_private_and_reserved_ips`**: Blocks `127.0.0.1`, `localhost`, `10.0.0.1`,
  `192.168.1.1`, `172.16.0.1`, `169.254.169.254`, `[::1]`, `[::ffff:127.0.0.1]`.
- **`test_ssrf_scheme_and_credential_validation`**: Rejects `ftp://`, `file://`, and embedded credentials.
- **`test_forbidden_ip_checker`**: Unit testing IP range classifications.
- **`test_response_body_truncation`**: Large 100 KB error bodies bounded to `max_response_bytes`.
- **`test_secret_redaction_in_errors`**: Sensitive auth tokens scrubbed from error logs.
- **`test_provider_registry_and_factory`**: Provider registry lookup and custom handler registration.
- **`test_outbox_dispatcher_integration_with_webhook`**: End-to-end integration: dispatcher claims event,
  webhook delivers to server, status transitions to `delivered` in database.
- **`test_outbox_dispatcher_dead_letter_on_non_retryable_webhook_error`**: 400 error immediately transitions
  to `dead_letter` without retrying.

**Result**: 31 passed in 10.97s.

### 2. Full Outbox Regression Suite
- `tests/test_notification_webhook.py`: 31 passed
- `tests/test_transactional_outbox.py`: 12 passed
- `tests/test_outbox_observability_recovery.py`: 10 passed
- **Combined Result**: 53 passed in 14.95s.

### 3. Core Subsystem Regression Suites
- **Approval Lifecycle Suite** (90 tests): 90 passed in 10.43s.
- **API & Security Suite** (62 tests): 62 passed in 36.12s.
- **Configuration Validation Suite** (52 tests): 52 passed in 1.62s.
- **Static Analysis**: `python -m ruff check src tests` passed with 0 errors.
- **Line Length Check**: 0 lines exceed 110 characters across all Task 52 files.

---

## Operational Invariants & Limitations

1. **At-Least-Once Delivery**:
   External webhooks operate under at-least-once delivery guarantees. Receiving endpoints must use
   `X-Idempotency-Key` or `X-Event-ID` to deduplicate incoming messages.
2. **Approval Decoupling**:
   External notification delivery failure never alters or invalidates the underlying approval record.
3. **Fenced Updates**:
   Database transitions continue to enforce monotonic fencing tokens and lease ownership checks,
   preventing split-brain commits from zombie workers.
4. **No Git Commit / Push**:
   In strict accordance with task instructions, no git commit or push actions were performed.
