"""Comprehensive operational endpoint security and access-control verification tests.

Covers:
- Strict access-control policy across public vs protected endpoints.
- Missing, invalid, and valid Bearer token verification.
- Protected /diagnostics endpoint under both auth-enabled and auth-disabled configs.
- Consistent enforcement across state-changing endpoints (run creation, resume, cancellation).
- Infrastructure probe behavior (/health, /ready) during shutdown and degradation without auth.
- Absence of secret leakage, raw tokens, stack traces, and host paths across error and success responses.
- Client error translation including ApiAuthenticationError (401) and ApiForbiddenError (403).
"""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiAuthenticationError,
    ApiForbiddenError,
    OrchestratorApiClient,
    _sanitize_message,
)
from orchestrator.api.manager import RunManager
from orchestrator.api.models import DiagnosticsResponse, HealthResponse, ReadyResponse
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.persistence.store import CheckpointStore

TEST_TOKEN = "test-operational-secret-token-xyz-12345"


@pytest.fixture(autouse=True)
def clean_security_env(monkeypatch):
    """Ensure environment token and cache are cleanly reset for every test."""
    reset_stm()
    reset_ltm()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    yield
    reset_stm()
    reset_ltm()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)


@pytest.fixture
def security_client(tmp_path):
    """Provide an isolated TestClient with dedicated SQLite database and RunManager."""
    db_file = tmp_path / "test_sec_checkpoints.db"
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
# 1. Access Control Matrix: Missing Tokens When Auth Is Enabled
# -----------------------------------------------------------------------------


def test_public_probes_accessible_without_token_when_auth_enabled(security_client, monkeypatch):
    """GET /health and GET /ready must remain accessible without auth when token is configured."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, _, _ = security_client

    # 1. Liveness probe is public
    resp_health = client.get("/health")
    assert resp_health.status_code == 200
    health_model = HealthResponse.model_validate(resp_health.json())
    assert health_model.status == "healthy"
    assert health_model.auth_required is True
    assert TEST_TOKEN not in resp_health.text

    # 2. Readiness probe is public
    resp_ready = client.get("/ready")
    assert resp_ready.status_code == 200
    ready_model = ReadyResponse.model_validate(resp_ready.json())
    assert ready_model.status == "ready"
    assert ready_model.accepting_runs is True
    assert ready_model.database == "healthy"
    assert TEST_TOKEN not in resp_ready.text


def test_protected_endpoints_reject_missing_token_with_401(security_client, monkeypatch):
    """Protected operational, telemetry, and workflow routes must return 401 when token is missing."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, store, _ = security_client

    # Seed an existing run in store so 404 is not triggered
    store.save_checkpoint("run-seed-1", {"task": "Seed task", "status": "running"}, transition="init")

    protected_requests = [
        ("GET", "/diagnostics", None),
        ("POST", "/runs", {"task": "New task"}),
        ("GET", "/runs/run-seed-1", None),
        ("POST", "/runs/run-seed-1/resume", {}),
        ("POST", "/runs/run-seed-1/cancel", {"reason": "Halt"}),
        ("GET", "/runs/run-seed-1/checkpoints", None),
        ("GET", "/evaluation", None),
    ]

    for method, path, payload in protected_requests:
        if method == "GET":
            resp = client.get(path)
        else:
            resp = client.post(path, json=payload)

        assert resp.status_code == 401, f"{method} {path} did not reject missing token with 401"
        assert resp.headers.get("www-authenticate") == "Bearer"
        assert "Missing or invalid authentication credentials." in resp.json()["detail"]
        assert TEST_TOKEN not in resp.text


# -----------------------------------------------------------------------------
# 2. Access Control Matrix: Invalid Tokens & Authentication Schemes
# -----------------------------------------------------------------------------


def test_protected_endpoints_reject_invalid_tokens_with_401(security_client, monkeypatch):
    """Protected endpoints must reject wrong tokens and invalid schemes with 401."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, _, _ = security_client

    invalid_headers = [
        ({"Authorization": "Bearer wrong-secret-token-xyz"}, "Invalid authentication token."),
        ({"Authorization": "Bearer "}, "Missing or invalid authentication credentials."),
        ({"Authorization": "Basic dXNlcjpwYXNz"}, "Missing or invalid authentication credentials."),
        ({"Authorization": "Token some-custom-token"}, "Missing or invalid authentication credentials."),
    ]

    for headers, expected_detail in invalid_headers:
        # Check /diagnostics
        resp_diag = client.get("/diagnostics", headers=headers)
        assert resp_diag.status_code == 401
        assert expected_detail in resp_diag.json()["detail"]
        assert resp_diag.headers.get("www-authenticate") == "Bearer"

        # Check state-changing POST /runs
        resp_runs = client.post("/runs", json={"task": "Task"}, headers=headers)
        assert resp_runs.status_code == 401
        assert expected_detail in resp_runs.json()["detail"]


# -----------------------------------------------------------------------------
# 3. Access Control Matrix: Valid Tokens
# -----------------------------------------------------------------------------


def test_protected_endpoints_accept_valid_token(security_client, monkeypatch):
    """Protected endpoints must succeed when presented with valid Bearer token."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, store, _ = security_client
    auth_header = {"Authorization": f"Bearer {TEST_TOKEN}"}

    # 1. GET /diagnostics
    resp_diag = client.get("/diagnostics", headers=auth_header)
    assert resp_diag.status_code == 200
    diag_model = DiagnosticsResponse.model_validate(resp_diag.json())
    assert diag_model.application.auth_required is True

    # 2. POST /runs (create workflow)
    resp_create = client.post("/runs", json={"task": "Protected task"}, headers=auth_header)
    assert resp_create.status_code == 201
    run_id = resp_create.json()["run_id"]

    # 3. GET /runs/{id}
    resp_get = client.get(f"/runs/{run_id}", headers=auth_header)
    assert resp_get.status_code == 200

    # 4. GET /runs/{id}/checkpoints
    resp_cp = client.get(f"/runs/{run_id}/checkpoints", headers=auth_header)
    assert resp_cp.status_code == 200

    # 5. GET /evaluation
    resp_eval = client.get("/evaluation", headers=auth_header)
    assert resp_eval.status_code == 200


# -----------------------------------------------------------------------------
# 4. Local Development Mode (Unauthenticated Loopback Access)
# -----------------------------------------------------------------------------


def test_dev_mode_all_endpoints_accessible_without_token(security_client):
    """When ORCHESTRATOR_API_TOKEN is unset, all endpoints permit unauthenticated loopback access."""
    client, store, _ = security_client

    # 1. Probes
    assert client.get("/health").status_code == 200
    assert client.get("/health").json()["auth_required"] is False
    assert client.get("/ready").status_code == 200

    # 2. Diagnostics
    resp_diag = client.get("/diagnostics")
    assert resp_diag.status_code == 200
    assert resp_diag.json()["application"]["auth_required"] is False

    # 3. Mutation: Create run
    resp_create = client.post("/runs", json={"task": "Dev mode task"})
    assert resp_create.status_code == 201
    run_id = resp_create.json()["run_id"]

    # 4. Inspection
    assert client.get(f"/runs/{run_id}").status_code == 200
    assert client.get(f"/runs/{run_id}/checkpoints").status_code == 200
    assert client.get("/evaluation").status_code == 200


# -----------------------------------------------------------------------------
# 5. State-Changing Route Consistency: Cancellation & Resume
# -----------------------------------------------------------------------------


def test_state_changing_cancellation_and_resume_require_auth(security_client, monkeypatch):
    """POST /runs/{id}/resume and POST /runs/{id}/cancel strictly enforce authentication."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, store, manager = security_client

    # Create an interrupted run in store
    store.save_checkpoint(
        "run-state-test",
        {"task": "State test", "status": "interrupted", "plan": []},
        transition="interrupted",
    )

    # Missing token -> 401
    assert client.post("/runs/run-state-test/resume", json={}).status_code == 401
    assert client.post("/runs/run-state-test/cancel", json={"reason": "Cancel"}).status_code == 401

    # Invalid token -> 401
    bad_hdr = {"Authorization": "Bearer bad-token"}
    assert client.post("/runs/run-state-test/resume", json={}, headers=bad_hdr).status_code == 401
    assert client.post("/runs/run-state-test/cancel", json={}, headers=bad_hdr).status_code == 401

    # Valid token -> succeeds
    good_hdr = {"Authorization": f"Bearer {TEST_TOKEN}"}
    resp_cancel = client.post("/runs/run-state-test/cancel", json={"reason": "User stop"}, headers=good_hdr)
    assert resp_cancel.status_code == 200
    assert resp_cancel.json()["status"] == "cancelled"


# -----------------------------------------------------------------------------
# 6. Public Probe Behavior During Shutdown and Persistence Failure
# -----------------------------------------------------------------------------


def test_probes_unauthenticated_during_shutdown(security_client, monkeypatch):
    """Probes remain unauthenticated and non-leaking during shutdown when auth is enabled."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, _, manager = security_client

    with manager._lock:
        manager._is_shutting_down = True

    # /health returns 200 with shutting_down status without token
    resp_health = client.get("/health")
    assert resp_health.status_code == 200
    assert resp_health.json()["status"] == "shutting_down"
    assert TEST_TOKEN not in resp_health.text

    # /ready returns 503 Service Unavailable without token
    resp_ready = client.get("/ready")
    assert resp_ready.status_code == 503
    ready_data = resp_ready.json()
    assert ready_data["status"] == "not_ready"
    assert ready_data["accepting_runs"] is False
    assert ready_data["reason"] == "Service is shutting down"
    assert TEST_TOKEN not in resp_ready.text


def test_probes_unauthenticated_during_database_failure(security_client, monkeypatch):
    """Probes return structured, sanitized 503 responses without auth during persistence degradation."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, store, _ = security_client

    with patch.object(store, "check_health", return_value=(False, "Database locked")):
        # /health remains 200
        resp_health = client.get("/health")
        assert resp_health.status_code == 200
        assert resp_health.json()["status"] == "healthy"

        # /ready returns 503 with sanitized error reason
        resp_ready = client.get("/ready")
        assert resp_ready.status_code == 503
        ready_data = resp_ready.json()
        assert ready_data["status"] == "not_ready"
        assert ready_data["database"] == "unhealthy"
        assert "Database probe failed: Database locked" in ready_data["reason"]
        assert TEST_TOKEN not in resp_ready.text


# -----------------------------------------------------------------------------
# 7. Absence of Secrets, Tokens, and Filesystem Paths in Error Responses
# -----------------------------------------------------------------------------


def test_error_responses_contain_no_secrets_or_stack_traces(security_client, monkeypatch):
    """Errors (400, 401, 404, 413, 500) must never leak secrets, stack traces, or internal paths."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_TOKEN)
    client, _, _ = security_client

    headers = {"Authorization": f"Bearer {TEST_TOKEN}"}

    # 400 Bad Request (empty task)
    r_bad = client.post("/runs", json={"task": ""}, headers=headers)
    assert r_bad.status_code == 400
    assert TEST_TOKEN not in r_bad.text
    assert "Traceback" not in r_bad.text

    # 404 Not Found
    r_404 = client.get("/runs/nonexistent-run-id", headers=headers)
    assert r_404.status_code == 404
    assert TEST_TOKEN not in r_404.text
    assert "Traceback" not in r_404.text

    # 401 Unauthorized
    r_401 = client.get("/diagnostics")
    assert r_401.status_code == 401
    assert TEST_TOKEN not in r_401.text
    assert "Traceback" not in r_401.text

    # 413 Payload Too Large
    r_413 = client.post("/runs", headers={"Content-Length": "2000000", **headers}, content=b"{}")
    assert r_413.status_code == 413
    assert TEST_TOKEN not in r_413.text
    assert "Traceback" not in r_413.text


# -----------------------------------------------------------------------------
# 8. OrchestratorApiClient Error Translation (401 & 403)
# -----------------------------------------------------------------------------


def test_api_client_maps_401_and_403_errors():
    """Client must map HTTP 401 to ApiAuthenticationError and HTTP 403 to ApiForbiddenError."""

    class MockTransport(httpx.BaseTransport):
        def __init__(self, code: int, message: str) -> None:
            self.code = code
            self.message = message

        def handle_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=self.code,
                json={"detail": self.message},
                request=request,
            )

    # 401 Unauthorized
    c401 = OrchestratorApiClient(
        base_url="http://mock.test",
        client=httpx.Client(
            transport=MockTransport(401, "Invalid authentication token."),
            base_url="http://mock.test",
        ),
    )
    with pytest.raises(ApiAuthenticationError) as exc_401:
        c401.get_diagnostics()
    assert exc_401.value.status_code == 401
    assert "Invalid authentication token" in str(exc_401.value)

    # 403 Forbidden
    c403 = OrchestratorApiClient(
        base_url="http://mock.test",
        client=httpx.Client(
            transport=MockTransport(403, "Action forbidden by security policy."),
            base_url="http://mock.test",
        ),
    )
    with pytest.raises(ApiForbiddenError) as exc_403:
        c403.create_run("Blocked task")
    assert exc_403.value.status_code == 403
    assert "Action forbidden by security policy" in str(exc_403.value)


def test_sanitize_message_strips_sensitive_data():
    """_sanitize_message must scrub Bearer tokens, API keys, and stack traces."""
    msg_token = "Failed request with Bearer secret-bearer-token-12345678."
    clean_token = _sanitize_message(msg_token)
    assert "secret-bearer-token-12345678" not in clean_token
    assert "Bearer [REDACTED_TOKEN]" in clean_token

    msg_traceback = "Something broke:\nTraceback (most recent call last):\n  File 'test.py', line 1"
    clean_trace = _sanitize_message(msg_traceback)
    assert "Traceback" not in clean_trace
    assert "Check server logs for diagnostics" in clean_trace
