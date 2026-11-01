"""Comprehensive security tests for API authentication, safe network binding, and CORS."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiAuthenticationError,
    OrchestratorApiClient,
    _sanitize_message,
)
from orchestrator.api.manager import RunManager
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import (
    create_app,
    is_loopback_host,
    validate_network_binding,
)
from orchestrator.main import cli
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm
from orchestrator.persistence.store import CheckpointStore

TEST_AUTH_TOKEN = "test-secret-token-398472"


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Ensure environment token is cleanly reset around each test."""
    reset_stm()
    reset_ltm()
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    yield
    reset_stm()
    reset_ltm()
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)


@pytest.fixture
def auth_api_client(tmp_path):
    """Provide an isolated TestClient with dedicated database and manager."""
    db_file = tmp_path / "test_security_checkpoints.db"
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
# 1. Local Development Unauthenticated Loopback Access
# -----------------------------------------------------------------------------


def test_loopback_unauthenticated_dev_mode(auth_api_client):
    """When ORCHESTRATOR_API_TOKEN is unset, loopback requests proceed without credentials."""
    client, _, _ = auth_api_client

    # 1. Health check is open
    resp_health = client.get("/health")
    assert resp_health.status_code == 200
    assert resp_health.json()["status"] == "healthy"
    assert resp_health.json()["auth_required"] is False

    # 2. Workflow creation is permitted
    resp_run = client.post("/runs", json={"task": "Local dev unauthenticated test"})
    assert resp_run.status_code == 201
    assert resp_run.json()["run_id"]


# -----------------------------------------------------------------------------
# 2. Token Authentication Enforcement on Protected Endpoints
# -----------------------------------------------------------------------------


def test_protected_endpoints_missing_token_returns_401(auth_api_client, monkeypatch):
    """When token is configured, requests without Authorization header must return 401."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_AUTH_TOKEN)
    client, store, _ = auth_api_client

    # 1. POST /runs
    r1 = client.post("/runs", json={"task": "Secret task"})
    assert r1.status_code == 401
    assert "WWW-Authenticate" in r1.headers
    assert "Missing or invalid authentication credentials." in r1.json()["detail"]

    # 2. GET /runs/{run_id}
    r2 = client.get("/runs/run-12345")
    assert r2.status_code == 401

    # 3. POST /runs/{run_id}/resume
    r3 = client.post("/runs/run-12345/resume", json={})
    assert r3.status_code == 401

    # 4. GET /runs/{run_id}/checkpoints
    r4 = client.get("/runs/run-12345/checkpoints")
    assert r4.status_code == 401

    # 5. GET /evaluation
    r5 = client.get("/evaluation")
    assert r5.status_code == 401


def test_protected_endpoints_invalid_token_returns_401(auth_api_client, monkeypatch):
    """Requests with incorrect token or invalid scheme must return 401."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_AUTH_TOKEN)
    client, _, _ = auth_api_client

    # 1. Wrong token
    bad_headers = {"Authorization": "Bearer wrong-token-xyz"}
    r1 = client.post("/runs", json={"task": "Secret task"}, headers=bad_headers)
    assert r1.status_code == 401
    assert "Invalid authentication token." in r1.json()["detail"]

    # 2. Wrong scheme (Basic instead of Bearer)
    basic_headers = {"Authorization": "Basic dXNlcjpwYXNz"}
    r2 = client.post("/runs", json={"task": "Secret task"}, headers=basic_headers)
    assert r2.status_code == 401
    assert "Missing or invalid authentication credentials." in r2.json()["detail"]


def test_protected_endpoints_valid_token_success(auth_api_client, monkeypatch):
    """Requests with valid Bearer token must succeed on all protected endpoints."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_AUTH_TOKEN)
    client, _, _ = auth_api_client
    headers = {"Authorization": f"Bearer {TEST_AUTH_TOKEN}"}

    # 1. POST /runs
    r_create = client.post(
        "/runs",
        json={"task": "Authorized analysis workflow"},
        headers=headers,
    )
    assert r_create.status_code == 201
    run_id = r_create.json()["run_id"]

    # 2. GET /runs/{run_id}
    r_get = client.get(f"/runs/{run_id}", headers=headers)
    assert r_get.status_code == 200
    assert r_get.json()["run_id"] == run_id

    # 3. GET /runs/{run_id}/checkpoints
    r_cp = client.get(f"/runs/{run_id}/checkpoints", headers=headers)
    assert r_cp.status_code == 200
    assert r_cp.json()["total_steps"] >= 1

    # 4. GET /evaluation
    r_eval = client.get("/evaluation", headers=headers)
    assert r_eval.status_code == 200
    assert r_eval.json()["dataset_version"] == "1.0.0"


# -----------------------------------------------------------------------------
# 3. Health Endpoint Privacy & Telemetry
# -----------------------------------------------------------------------------


def test_health_endpoint_remains_public_with_privacy(auth_api_client, monkeypatch):
    """GET /health must remain accessible without auth while exposing zero secrets."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", TEST_AUTH_TOKEN)
    client, _, _ = auth_api_client

    resp = client.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    assert data["status"] == "healthy"
    assert data["auth_required"] is True
    # Secrets must NEVER appear in health telemetry
    assert TEST_AUTH_TOKEN not in str(data)
    assert "token" not in data


# -----------------------------------------------------------------------------
# 4. Safe Network Binding Rejection
# -----------------------------------------------------------------------------


def test_loopback_host_detection():
    """is_loopback_host must accurately distinguish loopback from remote interfaces."""
    assert is_loopback_host("127.0.0.1") is True
    assert is_loopback_host("localhost") is True
    assert is_loopback_host("::1") is True
    assert is_loopback_host(None) is True

    assert is_loopback_host("0.0.0.0") is False
    assert is_loopback_host("192.168.1.50") is False
    assert is_loopback_host("10.0.0.1") is False
    assert is_loopback_host("api.mycompany.internal") is False


def test_network_binding_validation_unauthenticated_rejection():
    """validate_network_binding must reject non-loopback hosts when token is empty."""
    # Loopback bindings allowed without token
    validate_network_binding("127.0.0.1", token="")
    validate_network_binding("localhost", token="")

    # Non-loopback binding without token MUST raise ValueError
    with pytest.raises(ValueError) as exc1:
        validate_network_binding("0.0.0.0", token="")
    assert "Unsafe network binding rejected" in str(exc1.value)
    assert "ORCHESTRATOR_API_TOKEN" in str(exc1.value)

    with pytest.raises(ValueError) as exc2:
        validate_network_binding("192.168.1.100", token="")
    assert "192.168.1.100" in str(exc2.value)

    # Non-loopback binding with token is permitted
    validate_network_binding("0.0.0.0", token="valid-secure-token")


def test_cli_rejects_unsafe_non_loopback_serve(capsys):
    """CLI orchestrator --serve must exit with error if bound to 0.0.0.0 without token."""
    with patch.dict(os.environ, {}, clear=True):
        rc = cli(["--serve", "--host", "0.0.0.0"])
        assert rc == 1
        captured = capsys.readouterr().err
        assert "Error starting API server" in captured
        assert "Unsafe network binding rejected" in captured


# -----------------------------------------------------------------------------
# 5. Dashboard API Client Token Handling and Redaction
# -----------------------------------------------------------------------------


def test_client_attaches_bearer_token(monkeypatch):
    """OrchestratorApiClient must attach Authorization header when token is configured."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "client-token-9988")
    client = OrchestratorApiClient(base_url="http://127.0.0.1:8000")

    assert client.api_token == "client-token-9988"
    assert "token=configured" in repr(client)
    assert "client-token-9988" not in repr(client)  # Token never in repr


def test_client_handles_401_authentication_error(monkeypatch):
    """Client must map HTTP 401 to ApiAuthenticationError."""
    import httpx

    class Mock401Transport(httpx.BaseTransport):
        def handle_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                status_code=401,
                json={"detail": "Missing or invalid authentication credentials."},
            )

    client = OrchestratorApiClient(
        base_url="http://mock-api.test",
        client=httpx.Client(transport=Mock401Transport(), base_url="http://mock-api.test"),
    )

    with pytest.raises(ApiAuthenticationError) as exc_info:
        client.create_run("Some task")
    assert exc_info.value.status_code == 401
    assert "Missing or invalid authentication credentials" in str(exc_info.value)


def test_client_error_message_redacts_tokens():
    """Client error sanitizer must redact bearer tokens from exception text."""
    raw = "Failed request: Authorization Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.token"
    clean = _sanitize_message(raw)
    assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in clean
    assert "Bearer [REDACTED_TOKEN]" in clean


# -----------------------------------------------------------------------------
# 6. CORS Policy and Payload Size Protection
# -----------------------------------------------------------------------------


def test_cors_policy_allowed_and_disallowed_origins(auth_api_client):
    """CORS middleware must allow configured dashboard origin and reject unknown origins."""
    client, _, _ = auth_api_client

    # Allowed Streamlit origin
    resp_allowed = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:8501",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp_allowed.status_code == 200
    assert resp_allowed.headers.get("access-control-allow-origin") == "http://localhost:8501"

    # Disallowed untrusted origin
    resp_blocked = client.options(
        "/health",
        headers={
            "Origin": "http://malicious-tracker.evil.com",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert resp_blocked.headers.get("access-control-allow-origin") is None


def test_payload_size_limit_protection(auth_api_client):
    """Requests with Content-Length exceeding 1MB must return 413 Payload Too Large."""
    client, _, _ = auth_api_client

    # Send oversized content-length header
    resp = client.post(
        "/runs",
        headers={"Content-Length": "2000000"},
        content=b"{}",
    )
    assert resp.status_code == 413
    assert "exceeds maximum allowed size" in resp.json()["detail"].lower()
