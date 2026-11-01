"""Comprehensive tests for Request Body Size Enforcement and API Rate Limiting.

Covers:
- Request body limits for standard Content-Length, chunked streaming, and misleading headers.
- HTTP 413 and 400 status codes with clean, non-leaking error responses.
- In-process sliding-window rate limiting on expensive mutation endpoints (POST /runs, resume).
- Immunity of public health and status polling endpoints from rate limiting.
- Retry-After header calculation and HTTP 429 semantics.
- IP spoofing prevention via trusted proxy filtering.
- Thread-safety of rate limiter under concurrent access.
- OrchestratorApiClient mapping of 429 to ApiRateLimitError and 413 to ApiPayloadTooLargeError.
"""

from __future__ import annotations

import concurrent.futures
import time
from unittest.mock import MagicMock

import httpx
import pytest
from starlette.testclient import TestClient
from ui.dashboard_helpers import sanitize_ui_error

from orchestrator.api.client import (
    ApiPayloadTooLargeError,
    ApiRateLimitError,
    OrchestratorApiClient,
)
from orchestrator.api.limiter import (
    RateLimiter,
    get_client_ip,
    reset_rate_limiter,
)
from orchestrator.api.manager import RunManager
from orchestrator.api.models import RunResponse
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.persistence.store import CheckpointStore

# -----------------------------------------------------------------------------
# Fixtures & Test Setup
# -----------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_api_environment(monkeypatch):
    """Reset settings, memory, and rate limiter around each test."""
    reset_stm()
    reset_ltm()
    reset_rate_limiter()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_API_MAX_BODY_BYTES", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_RATE_LIMIT_ENABLED", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_TRUSTED_PROXIES", raising=False)
    yield
    reset_stm()
    reset_ltm()
    reset_rate_limiter()
    get_settings.cache_clear()


@pytest.fixture
def protected_api_client(tmp_path):
    """Provide an isolated TestClient backed by a mock RunManager and CheckpointStore."""
    db_file = tmp_path / "test_protection_checkpoints.db"
    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)
    try:
        yield client, store, manager
    finally:
        manager.shutdown()
        store.close()


# -----------------------------------------------------------------------------
# 1. Request Body Size Limit Tests
# -----------------------------------------------------------------------------


def test_body_under_limit_success(protected_api_client):
    """Normal requests below the maximum allowed payload size must succeed."""
    client, _, manager = protected_api_client
    manager.start_run = MagicMock(
        return_value=RunResponse(
            run_id="run-test-success",
            task="Short analysis task",
            status="pending",
            created_at="2026-10-22T08:00:00Z",
            updated_at="2026-10-22T08:00:00Z",
            step_count=0,
        )
    )

    resp = client.post("/runs", json={"task": "Short analysis task", "background": True})
    assert resp.status_code == 202
    assert resp.json()["run_id"] == "run-test-success"


def test_content_length_exceeding_limit_returns_413(protected_api_client, monkeypatch):
    """Requests with Content-Length header exceeding max_bytes must return 413 immediately."""
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "500")
    client, _, _ = protected_api_client

    resp = client.post(
        "/runs",
        headers={"Content-Length": "1000", "Content-Type": "application/json"},
        content=b"{}",
    )
    assert resp.status_code == 413
    data = resp.json()
    assert "exceeds maximum allowed size" in data["detail"].lower()
    assert "500 bytes" in data["detail"]


def test_streamed_chunked_body_exceeding_limit_returns_413(protected_api_client, monkeypatch):
    """Streamed chunked request without Content-Length exceeding limit must return 413 mid-stream."""
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "200")
    client, _, _ = protected_api_client

    def stream_chunks():
        # 5 chunks of 60 bytes = 300 bytes total > 200 limit
        for _ in range(5):
            yield b"X" * 60

    resp = client.post(
        "/runs",
        content=stream_chunks(),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 413
    assert "exceeds maximum allowed size of 200 bytes" in resp.json()["detail"]


def test_misleading_content_length_returns_413(protected_api_client, monkeypatch):
    """Requests declaring small Content-Length but streaming excess bytes must return 413."""
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "150")
    client, _, _ = protected_api_client

    def stream_excess_chunks():
        for _ in range(4):
            yield b"Y" * 50  # 200 bytes total

    resp = client.post(
        "/runs",
        content=stream_excess_chunks(),
        headers={"Content-Type": "application/json", "Content-Length": "20"},
    )
    assert resp.status_code == 413
    assert "exceeds maximum allowed size of 150 bytes" in resp.json()["detail"]


def test_malformed_content_length_returns_400(protected_api_client):
    """Non-integer Content-Length header must return 400 Bad Request."""
    client, _, _ = protected_api_client

    resp = client.post(
        "/runs",
        headers={"Content-Length": "not-an-integer", "Content-Type": "application/json"},
        content=b"{}",
    )
    assert resp.status_code == 400
    assert "Malformed Content-Length" in resp.json()["detail"]


def test_negative_content_length_returns_400(protected_api_client):
    """Negative Content-Length header is invalid HTTP and must return 400 Bad Request."""
    client, _, _ = protected_api_client

    resp = client.post(
        "/runs",
        headers={"Content-Length": "-50", "Content-Type": "application/json"},
        content=b"{}",
    )
    assert resp.status_code == 400
    assert "Malformed Content-Length" in resp.json()["detail"]


def test_413_response_no_traceback_leakage(protected_api_client, monkeypatch):
    """HTTP 413 responses must be clean JSON without stack traces or internal server details."""
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "100")
    client, _, _ = protected_api_client

    resp = client.post(
        "/runs",
        headers={"Content-Length": "5000", "Content-Type": "application/json"},
        content=b"{}",
    )
    assert resp.status_code == 413
    text = resp.text
    assert "Traceback" not in text
    assert "orchestrator" not in text.lower()
    assert "detail" in resp.json()


def test_cors_headers_preserved_on_413(protected_api_client, monkeypatch):
    """CORS headers must be attached to 413 responses for allowed origins."""
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "100")
    client, _, _ = protected_api_client

    resp = client.post(
        "/runs",
        headers={
            "Origin": "http://localhost:8501",
            "Content-Length": "2000",
            "Content-Type": "application/json",
        },
        content=b"{}",
    )
    assert resp.status_code == 413
    assert resp.headers.get("access-control-allow-origin") == "http://localhost:8501"


# -----------------------------------------------------------------------------
# 2. Rate Limiting Tests
# -----------------------------------------------------------------------------


def test_rate_limiter_sliding_window_basics():
    """RateLimiter acquire and acquire_info must enforce request quotas within window."""
    limiter = RateLimiter(requests_per_window=3, window_seconds=10.0, enabled=True)

    # 3 allowed requests
    allowed, retry_after = limiter.acquire_info("user-1")
    assert allowed is True
    assert retry_after == 0

    allowed, retry_after = limiter.acquire_info("user-1")
    assert allowed is True
    assert retry_after == 0

    allowed, retry_after = limiter.acquire_info("user-1")
    assert allowed is True
    assert retry_after == 0

    # 4th request must be rejected
    allowed, retry_after = limiter.acquire_info("user-1")
    assert allowed is False
    assert 1 <= retry_after <= 10

    # Different key is unaffected
    allowed_other, _ = limiter.acquire_info("user-2")
    assert allowed_other is True


def test_rate_limiter_window_expiry():
    """RateLimiter must clear old request timestamps once the window elapses."""
    limiter = RateLimiter(requests_per_window=2, window_seconds=0.1, enabled=True)

    assert limiter.acquire("client-x") is True
    assert limiter.acquire("client-x") is True
    assert limiter.acquire("client-x") is False

    # Sleep past the window
    time.sleep(0.12)

    # Window expired, requests allowed again
    assert limiter.acquire("client-x") is True


def test_rate_limiter_disabled_allows_all():
    """When disabled, RateLimiter must always allow requests without delay."""
    limiter = RateLimiter(requests_per_window=1, window_seconds=60.0, enabled=False)

    for _ in range(10):
        allowed, retry_after = limiter.acquire_info("test-key")
        assert allowed is True
        assert retry_after == 0


def test_rate_limiter_reset():
    """RateLimiter reset must purge all tracked histories."""
    limiter = RateLimiter(requests_per_window=1, window_seconds=60.0, enabled=True)

    assert limiter.acquire("test-key") is True
    assert limiter.acquire("test-key") is False

    limiter.reset()
    assert limiter.acquire("test-key") is True


def test_rate_limit_exceeded_returns_429_with_retry_after(protected_api_client, monkeypatch):
    """POST /runs must return 429 Too Many Requests with Retry-After when limit exceeded."""
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "2")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "30")
    reset_rate_limiter()

    client, _, manager = protected_api_client
    manager.start_run = MagicMock(
        return_value=RunResponse(
            run_id="run-test-quota",
            task="Quota test task",
            status="pending",
            created_at="2026-10-22T08:00:00Z",
            updated_at="2026-10-22T08:00:00Z",
            step_count=0,
        )
    )

    # Request 1: OK
    r1 = client.post("/runs", json={"task": "task 1", "background": True})
    assert r1.status_code == 202

    # Request 2: OK
    r2 = client.post("/runs", json={"task": "task 2", "background": True})
    assert r2.status_code == 202

    # Request 3: Throttled (429)
    r3 = client.post("/runs", json={"task": "task 3", "background": True})
    assert r3.status_code == 429
    assert "Rate limit exceeded" in r3.json()["detail"]

    retry_after = r3.headers.get("retry-after")
    assert retry_after is not None
    assert int(retry_after) >= 1


def test_resume_endpoint_is_rate_limited(protected_api_client, monkeypatch):
    """POST /runs/{run_id}/resume must also enforce the rate limit."""
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "1")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "30")
    reset_rate_limiter()

    client, store, manager = protected_api_client
    manager.resume_run = MagicMock(
        return_value=RunResponse(
            run_id="run-test-id",
            task="Resume test",
            status="pending",
            created_at="2026-10-22T08:00:00Z",
            updated_at="2026-10-22T08:00:00Z",
            step_count=0,
        )
    )

    # 1st request to /resume consumes quota
    r1 = client.post("/runs/run-test-id/resume", json={"background": True})
    assert r1.status_code in (200, 202)

    # 2nd request must be blocked with 429 before reaching route logic
    r2 = client.post("/runs/run-test-id/resume", json={"background": True})
    assert r2.status_code == 429
    assert r2.headers.get("retry-after") is not None


def test_health_and_polling_not_blocked_by_rate_limit(protected_api_client, monkeypatch):
    """GET /health and GET /runs/{run_id} must NEVER be blocked by rate limits."""
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "1")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "60")
    reset_rate_limiter()

    client, _, manager = protected_api_client
    manager.start_run = MagicMock(
        return_value=RunResponse(
            run_id="run-poll-test",
            task="Poll test",
            status="running",
            created_at="2026-10-22T08:00:00Z",
            updated_at="2026-10-22T08:00:00Z",
            step_count=1,
        )
    )
    manager.get_run = MagicMock(
        return_value=RunResponse(
            run_id="run-poll-test",
            task="Poll test",
            status="running",
            created_at="2026-10-22T08:00:00Z",
            updated_at="2026-10-22T08:00:00Z",
            step_count=1,
        )
    )

    # Exhaust rate limit on /runs
    r_post1 = client.post("/runs", json={"task": "Initial task", "background": True})
    assert r_post1.status_code == 202
    run_id = r_post1.json()["run_id"]

    r_post2 = client.post("/runs", json={"task": "Blocked task", "background": True})
    assert r_post2.status_code == 429

    # GET /health must succeed (no rate limit check)
    r_health = client.get("/health")
    assert r_health.status_code == 200
    assert r_health.json()["status"] == "healthy"

    # GET /runs/{run_id} polling must succeed (no rate limit check)
    r_poll = client.get(f"/runs/{run_id}")
    assert r_poll.status_code == 200
    assert r_poll.json()["run_id"] == run_id

    # GET /runs/{run_id}/checkpoints must not return 429 (no rate limit check)
    r_cp = client.get(f"/runs/{run_id}/checkpoints")
    assert r_cp.status_code != 429


# -----------------------------------------------------------------------------
# 3. IP Spoofing Prevention & Trusted Proxies Tests
# -----------------------------------------------------------------------------


def test_ip_spoofing_prevention_untrusted_client():
    """X-Forwarded-For must be ignored when the peer client is not a trusted proxy."""
    req = MagicMock()
    req.client.host = "198.51.100.25"  # Public untrusted client
    req.headers = {"x-forwarded-for": "10.0.0.1, 1.1.1.1"}

    # Default trusted proxies: 127.0.0.1, ::1, localhost
    ip = get_client_ip(req)
    assert ip == "198.51.100.25"  # Direct peer IP, spoofed header ignored!


def test_trusted_proxy_parses_x_forwarded_for():
    """X-Forwarded-For must be parsed when the direct peer client is in trusted proxies."""
    req = MagicMock()
    req.client.host = "127.0.0.1"  # Loopback trusted proxy
    req.headers = {"x-forwarded-for": "203.0.113.195, 127.0.0.1"}

    ip = get_client_ip(req)
    assert ip == "203.0.113.195"  # Client IP parsed from header


def test_trusted_proxy_empty_or_whitespace_header():
    """If X-Forwarded-For is present but empty, fall back safely to peer host."""
    req = MagicMock()
    req.client.host = "127.0.0.1"
    req.headers = {"x-forwarded-for": "   "}

    ip = get_client_ip(req)
    assert ip == "127.0.0.1"


# -----------------------------------------------------------------------------
# 4. Concurrency & Thread-Safety Tests
# -----------------------------------------------------------------------------


def test_rate_limiter_thread_safety():
    """RateLimiter must safely count concurrent requests across multiple threads."""
    total_quota = 50
    limiter = RateLimiter(requests_per_window=total_quota, window_seconds=60.0, enabled=True)

    num_threads = 20
    requests_per_thread = 5  # Total attempted = 100 requests

    results = []

    def make_requests():
        thread_results = []
        for _ in range(requests_per_thread):
            allowed = limiter.acquire("shared-client-key")
            thread_results.append(allowed)
        return thread_results

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(make_requests) for _ in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            results.extend(f.result())

    # Exactly 50 requests must be allowed, exactly 50 rejected
    assert len(results) == 100
    allowed_count = sum(1 for r in results if r is True)
    rejected_count = sum(1 for r in results if r is False)

    assert allowed_count == total_quota
    assert rejected_count == 50


# -----------------------------------------------------------------------------
# 5. Client & UI Handling Tests
# -----------------------------------------------------------------------------


def test_client_handles_429_rate_limit_error():
    """OrchestratorApiClient must map HTTP 429 to ApiRateLimitError with retry_after."""

    class Mock429Transport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=429,
                headers={"Retry-After": "45"},
                json={"detail": "Rate limit exceeded. Maximum of 5 requests per 60s."},
            )

    client = OrchestratorApiClient(
        base_url="http://mock-api.test",
        client=httpx.Client(transport=Mock429Transport(), base_url="http://mock-api.test"),
    )

    with pytest.raises(ApiRateLimitError) as exc_info:
        client.create_run("Some task")

    err = exc_info.value
    assert err.status_code == 429
    assert err.retry_after == 45
    assert "Rate limit exceeded" in str(err)


def test_client_handles_413_payload_too_large_error():
    """OrchestratorApiClient must map HTTP 413 to ApiPayloadTooLargeError."""

    class Mock413Transport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=413,
                json={"detail": "Request payload exceeds maximum allowed size of 1000000 bytes."},
            )

    client = OrchestratorApiClient(
        base_url="http://mock-api.test",
        client=httpx.Client(transport=Mock413Transport(), base_url="http://mock-api.test"),
    )

    with pytest.raises(ApiPayloadTooLargeError) as exc_info:
        client.create_run("Some task")

    err = exc_info.value
    assert err.status_code == 413
    assert "exceeds maximum allowed size" in str(err)


def test_ui_sanitizes_rate_limit_and_payload_errors():
    """sanitize_ui_error must safely format 429 and 413 errors without leaking sensitive data."""
    rate_err = ApiRateLimitError(
        "Rate limit exceeded for Authorization Bearer secret-token-xyz. Try again later.",
        retry_after=30,
    )
    clean_rate = sanitize_ui_error(rate_err)
    assert "secret-token-xyz" not in clean_rate
    assert "Rate limit exceeded" in clean_rate

    payload_err = ApiPayloadTooLargeError(
        "Payload exceeds 1000000 bytes. sk-live-1234567890abcdef is redacted."
    )
    clean_payload = sanitize_ui_error(payload_err)
    assert "sk-live-1234567890abcdef" not in clean_payload
    assert "Payload exceeds" in clean_payload
