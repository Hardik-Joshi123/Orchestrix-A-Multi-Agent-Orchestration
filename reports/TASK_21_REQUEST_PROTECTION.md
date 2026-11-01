# Task 21: Request Body Enforcement & API Rate Limiting Report

## 1. Executive Summary

Task 21 hardens the Multi-Agent Orchestration FastAPI service against Denial-of-Service (DoS), memory exhaustion, and brute-force/resource-drain attacks. Building directly upon the security foundation introduced in Task 20, Task 21 addresses the vulnerability where requests lacking `Content-Length` headers (such as chunked or streamed transfers) or providing misleading/malformed headers could bypass payload size limits. Additionally, configurable in-process sliding-window rate limiting is introduced to protect compute-heavy mutation endpoints (`POST /runs` and `POST /runs/{run_id}/resume`) without obstructing vital health checks or status polling.

---

## 2. Key Implementations and Modifications

### 2.1 Configurable Settings (`src/orchestrator/config.py`)
Configured via standard environment variables and typed Pydantic fields:
- `api_max_body_bytes` (`ORCHESTRATOR_API_MAX_BODY_BYTES`, default `1_000_000` = 1 MB): Hard limit on total incoming request body bytes.
- `api_rate_limit_enabled` (`ORCHESTRATOR_RATE_LIMIT_ENABLED`, default `True`): Toggles in-process rate limiting on or off.
- `api_rate_limit_requests` (`ORCHESTRATOR_RATE_LIMIT_REQUESTS`, default `60`): Maximum allowed mutation requests per window.
- `api_rate_limit_window_seconds` (`ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS`, default `60.0`): Sliding window duration in seconds.
- `api_trusted_proxies` (`ORCHESTRATOR_TRUSTED_PROXIES`, default `"127.0.0.1,::1,localhost"`): Comma-separated list of trusted upstream proxy IP addresses.
- `effective_trusted_proxies`: Dynamic property yielding a `frozenset[str]` of lowercase trusted proxy addresses.

### 2.2 Streaming ASGI Body Enforcement Middleware (`src/orchestrator/api/middleware.py`)
Replaced the previous superficial `Content-Length` inspection with an ASGI middleware (`RequestBodyLimitMiddleware`):
1. **Early Content-Length Check**: Fast pre-flight check inspecting `Content-Length`. If explicitly exceeding `effective_max`, returns HTTP 413 immediately without consuming body data.
2. **Malformed & Negative Length Rejection**: Non-integer or negative `Content-Length` headers are rejected cleanly with HTTP 400 Bad Request.
3. **Streamed / Chunked Body Counting**: Intercepts the ASGI `receive()` callable to aggregate byte counts chunk-by-chunk across all `http.request` messages. If the cumulative byte count exceeds `effective_max`, it halts streaming immediately, preventing memory exhaustion or reading the body twice.
4. **Clean HTTP 413 JSON Response**: Emits standard JSON `{"detail": "Request payload exceeds maximum allowed size of ... bytes."}` without leaking server tracebacks or internal error details.
5. **CORS Integration**: Positioned before `CORSMiddleware` in `create_app()` so that `CORSMiddleware` wraps the response, ensuring allowed origins receive `Access-Control-Allow-Origin` even on 413 responses.

### 2.3 Thread-Safe In-Process Rate Limiter (`src/orchestrator/api/limiter.py`)
Implemented a sliding-window rate limiter (`RateLimiter`) with the following guarantees:
1. **Sliding-Window Deque**: Maintains timestamps in `collections.deque` per client key, pruning timestamps outside `[now - window, now]`.
2. **Thread-Safety**: All access to client timestamp queues is synchronized via `threading.Lock()`, validated under high-concurrency multi-threaded tests.
3. **Bounded Memory Usage**: Prunes expired keys if client registry size exceeds `max_keys` (10,000 keys default).
4. **IP Spoofing Prevention**: `get_client_ip()` inspects `X-Forwarded-For` **only** if the direct peer client (`request.client.host`) is in `api_trusted_proxies`. If the peer is an untrusted remote client, `X-Forwarded-For` is ignored to prevent IP spoofing.
5. **Accurate Retry-After**: Calculates exact integer seconds until the oldest request within the window expires (`max(1, int(oldest + window - now + 0.999))`) and attaches standard `Retry-After` header.
6. **Non-Blocking Telemetry**: Rate limiting is attached via `Depends(check_rate_limit)` exclusively to mutation endpoints (`POST /runs`, `POST /runs/{run_id}/resume`). `GET /health` and status polling `GET /runs/{run_id}` are strictly exempt from rate limiting.

### 2.4 API Client & UI Dashboard Resiliency (`src/orchestrator/api/client.py`, `ui/review_app.py`)
1. **Typed Client Exceptions**:
   - `ApiRateLimitError(ApiClientError)` with `retry_after: int | None`.
   - `ApiPayloadTooLargeError(ApiClientError)` with `status_code = 413`.
2. **Client Header Parsing**: `OrchestratorApiClient._handle_response_error` parses the `Retry-After` response header and raises `ApiRateLimitError`.
3. **UI Dashboard Handling**: `ui/review_app.py` catches `ApiRateLimitError` and `ApiPayloadTooLargeError` in both Workflow Submission (Tab 1) and Resumption (Tab 2), informing the user with user-friendly retry notices (`⏳ Rate limit exceeded: Please retry in X seconds.`).

---

## 3. Test Coverage and Verification

A comprehensive test suite was implemented in `tests/test_request_protection.py` alongside regression testing across all existing test suites.

### 3.1 Test Suite Breakdown (`tests/test_request_protection.py` - 22 Tests)
| Test | Objective | Status |
|---|---|---|
| `test_body_under_limit_success` | Valid payload below size limit accepted | PASSED |
| `test_content_length_exceeding_limit_returns_413` | Declared Content-Length > limit rejected immediately with 413 | PASSED |
| `test_streamed_chunked_body_exceeding_limit_returns_413` | Chunked stream without Content-Length rejected mid-stream with 413 | PASSED |
| `test_misleading_content_length_returns_413` | Small declared Content-Length with excess streaming body rejected with 413 | PASSED |
| `test_malformed_content_length_returns_400` | Non-integer Content-Length rejected with HTTP 400 | PASSED |
| `test_negative_content_length_returns_400` | Negative Content-Length rejected with HTTP 400 | PASSED |
| `test_413_response_no_traceback_leakage` | Clean JSON detail without server tracebacks | PASSED |
| `test_cors_headers_preserved_on_413` | Outermost CORS middleware preserves Access-Control headers | PASSED |
| `test_rate_limiter_sliding_window_basics` | Sliding window quota enforcement per key | PASSED |
| `test_rate_limiter_window_expiry` | Expired window resets allowed tokens | PASSED |
| `test_rate_limiter_disabled_allows_all` | Rate limiting disabled flag bypasses throttling | PASSED |
| `test_rate_limiter_reset` | State reset purges tracking queues | PASSED |
| `test_rate_limit_exceeded_returns_429_with_retry_after` | POST /runs throttled with HTTP 429 and Retry-After header | PASSED |
| `test_resume_endpoint_is_rate_limited` | POST /runs/{run_id}/resume throttled when quota exhausted | PASSED |
| `test_health_and_polling_not_blocked_by_rate_limit` | GET /health and polling succeed when runs are rate limited | PASSED |
| `test_ip_spoofing_prevention_untrusted_client` | Untrusted peer cannot spoof IP via X-Forwarded-For | PASSED |
| `test_trusted_proxy_parses_x_forwarded_for` | Trusted upstream proxy allows client IP extraction | PASSED |
| `test_trusted_proxy_empty_or_whitespace_header` | Fallback to peer IP when proxy header is blank | PASSED |
| `test_rate_limiter_thread_safety` | Multi-threaded concurrent execution maintains exact quota | PASSED |
| `test_client_handles_429_rate_limit_error` | OrchestratorApiClient maps 429 to ApiRateLimitError with retry_after | PASSED |
| `test_client_handles_413_payload_too_large_error` | OrchestratorApiClient maps 413 to ApiPayloadTooLargeError | PASSED |
| `test_ui_sanitizes_rate_limit_and_payload_errors` | Dashboard error sanitization prevents secret leaks | PASSED |

### 3.2 Regression Verification Summary
- `pytest tests/test_request_protection.py tests/test_api_security.py tests/test_dashboard.py tests/test_api.py -v`: **67 passed in 31.55s**.
- `pytest tests/test_persistence_resume.py tests/test_cli.py tests/test_evaluation.py tests/test_concurrency.py -v`: **47 passed in 66.52s**.
- **Grand Total**: **114 passed tests**, 0 failures, 0 regressions.
- **Linting (`ruff check`)**: All checks passed cleanly with zero warnings.

---

## 4. Preservation of Prior Tasks
All features, models, endpoints, and behaviors from Tasks 01–20 remain completely operational and preserved. No uncommitted work was overwritten or deleted.
