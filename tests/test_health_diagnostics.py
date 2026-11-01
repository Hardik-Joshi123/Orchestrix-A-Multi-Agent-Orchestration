"""Tests for production health checks, readiness probes, and runtime diagnostics.

Verifies:
- GET /health lightweight liveness probe and backward compatibility
- GET /ready readiness probe accounting for shutdown state and SQLite health
- GET /diagnostics structured operational telemetry without sensitive leakage
- Graceful failure handling (persistence down, lock contention, corrupted db)
- Client helper methods (get_readiness, is_ready, get_diagnostics)
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.manager import RunManager
from orchestrator.api.models import (
    DiagnosticsResponse,
    HealthResponse,
    ReadyResponse,
)
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture
def test_setup(tmp_path):
    """Provide an isolated TestClient with dedicated SQLite database and RunManager."""
    db_file = tmp_path / "test_diagnostics.db"
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
# 1. Healthy Service and Ready Database
# -----------------------------------------------------------------------------


def test_health_liveness_healthy(test_setup):
    """GET /health must return 200 with status 'healthy' and standard schema."""
    client, store, manager = test_setup
    resp = client.get("/health")
    assert resp.status_code == 200

    data = resp.json()
    model = HealthResponse.model_validate(data)
    assert model.status == "healthy"
    assert model.checkpoint_backend == "sqlite"
    assert model.persistence_enabled is True
    assert model.active_runs_count == 0
    assert bool(model.version) is True
    assert bool(model.timestamp) is True


def test_readiness_healthy(test_setup):
    """GET /ready must return 200 with status 'ready' and accepting_runs=True."""
    client, store, manager = test_setup
    resp = client.get("/ready")
    assert resp.status_code == 200

    data = resp.json()
    model = ReadyResponse.model_validate(data)
    assert model.status == "ready"
    assert model.accepting_runs is True
    assert model.database == "healthy"
    assert model.active_runs == 0
    assert model.reason is None


def test_diagnostics_healthy(test_setup):
    """GET /diagnostics must return 200 with comprehensive operational diagnostics."""
    client, store, manager = test_setup
    resp = client.get("/diagnostics")
    assert resp.status_code == 200

    data = resp.json()
    model = DiagnosticsResponse.model_validate(data)
    assert model.application.status == "healthy"
    assert model.application.is_shutting_down is False
    assert model.execution.active_runs_count == 0
    assert isinstance(model.execution.lifecycle_counts, dict)
    assert model.persistence.enabled is True
    assert model.persistence.backend == "sqlite"
    assert model.persistence.status == "healthy"
    assert model.persistence.db_name == "test_diagnostics.db"
    assert model.limits.shutdown_grace_seconds > 0
    assert model.limits.max_body_bytes > 0


# -----------------------------------------------------------------------------
# 2. Database Failure Handling
# -----------------------------------------------------------------------------


def test_readiness_database_failure(test_setup):
    """GET /ready must return 503 if persistence health check fails."""
    client, store, manager = test_setup

    with patch.object(store, "check_health", return_value=(False, "Database connection failure")):
        resp = client.get("/ready")
        assert resp.status_code == 503

        data = resp.json()
        model = ReadyResponse.model_validate(data)
        assert model.status == "not_ready"
        assert model.accepting_runs is False
        assert model.database == "unhealthy"
        assert "Database probe failed" in (model.reason or "")

    # Liveness probe (/health) must still respond 200 despite degraded persistence
    liveness_resp = client.get("/health")
    assert liveness_resp.status_code == 200
    assert liveness_resp.json()["status"] == "healthy"


def test_diagnostics_database_failure(test_setup):
    """GET /diagnostics must return 200 with persistence status 'unhealthy' without crashing."""
    client, store, manager = test_setup

    with patch.object(store, "check_health", return_value=(False, "Database locked")):
        with patch.object(store, "get_status_counts", return_value={}):
            resp = client.get("/diagnostics")
            assert resp.status_code == 200

            data = resp.json()
            model = DiagnosticsResponse.model_validate(data)
            assert model.persistence.status == "unhealthy"
            assert model.persistence.detail == "Database locked"


def test_readiness_persistence_disabled(test_setup):
    """GET /ready returns 200 when persistence is explicitly disabled."""
    client, store, manager = test_setup
    store.enabled = False

    resp = client.get("/ready")
    assert resp.status_code == 200

    data = resp.json()
    model = ReadyResponse.model_validate(data)
    assert model.status == "ready"
    assert model.accepting_runs is True
    assert model.database == "disabled"


# -----------------------------------------------------------------------------
# 3. Shutdown State in Health and Readiness
# -----------------------------------------------------------------------------


def test_readiness_during_shutdown(test_setup):
    """GET /ready must return 503 when manager is shutting down."""
    client, store, manager = test_setup
    with manager._lock:
        manager._is_shutting_down = True

    resp = client.get("/ready")
    assert resp.status_code == 503

    data = resp.json()
    model = ReadyResponse.model_validate(data)
    assert model.status == "not_ready"
    assert model.accepting_runs is False
    assert model.reason == "Service is shutting down"

    # Liveness probe (/health) reflects shutting_down status
    health_resp = client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["status"] == "shutting_down"

    # Diagnostics reflects shutting_down status
    diag_resp = client.get("/diagnostics")
    assert diag_resp.status_code == 200
    diag_model = DiagnosticsResponse.model_validate(diag_resp.json())
    assert diag_model.application.status == "shutting_down"
    assert diag_model.application.is_shutting_down is True


# -----------------------------------------------------------------------------
# 4. Runtime Counts and Diagnostics Consistency
# -----------------------------------------------------------------------------


def test_runtime_counts_consistency(test_setup):
    """Lifecycle counts and active run counts must accurately match persistence and memory."""
    client, store, manager = test_setup

    # Insert runs with known lifecycle statuses directly into store
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute("INSERT INTO runs (run_id, status) VALUES ('run-1', 'completed');")
                conn.execute("INSERT INTO runs (run_id, status) VALUES ('run-2', 'completed');")
                conn.execute("INSERT INTO runs (run_id, status) VALUES ('run-3', 'failed');")
                conn.execute("INSERT INTO runs (run_id, status) VALUES ('run-4', 'interrupted');")
                conn.execute("INSERT INTO runs (run_id, status) VALUES ('run-5', 'cancelled');")
        finally:
            conn.close()

    # Register an in-flight run in manager
    manager._register_active("run-active-1", "Test active task")

    resp = client.get("/diagnostics")
    assert resp.status_code == 200

    data = resp.json()
    model = DiagnosticsResponse.model_validate(data)
    assert model.execution.active_runs_count == 1
    assert model.execution.lifecycle_counts.get("completed") == 2
    assert model.execution.lifecycle_counts.get("failed") == 1
    assert model.execution.lifecycle_counts.get("interrupted") == 1
    assert model.execution.lifecycle_counts.get("cancelled") == 1

    # Check that ready and health endpoints also reflect active_runs_count
    assert client.get("/health").json()["active_runs_count"] == 1
    assert client.get("/ready").json()["active_runs"] == 1


# -----------------------------------------------------------------------------
# 5. Recovery Diagnostics
# -----------------------------------------------------------------------------


def test_recovery_diagnostics_tracking(test_setup):
    """Startup recovery summary must be reflected in /diagnostics."""
    client, store, manager = test_setup

    # Simulate recovery execution
    with manager._lock:
        manager._last_recovery_summary = {
            "status": "completed",
            "recovered_count": 2,
            "recovered_run_ids": ["run-rec-1", "run-rec-2"],
            "timestamp": "2026-10-23T12:00:00Z",
        }

    resp = client.get("/diagnostics")
    assert resp.status_code == 200

    data = resp.json()
    model = DiagnosticsResponse.model_validate(data)
    assert model.recovery.status == "completed"
    assert model.recovery.recovered_count == 2
    assert model.recovery.recovered_run_ids == ["run-rec-1", "run-rec-2"]
    assert model.recovery.timestamp == "2026-10-23T12:00:00Z"


# -----------------------------------------------------------------------------
# 6. Safety and Absence of Sensitive Data
# -----------------------------------------------------------------------------


def test_absence_of_sensitive_data(test_setup, monkeypatch):
    """Ensure sensitive tokens, passwords, and file paths are never exposed."""
    client, store, manager = test_setup
    secret_token = "ultra-secret-test-token-99999"
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", secret_token)

    endpoints = ["/health", "/ready", "/diagnostics"]
    for endpoint in endpoints:
        resp = client.get(endpoint)
        body = resp.text
        # Secret token must not appear in any response body
        assert secret_token not in body, f"Secret leaked in {endpoint}"

    # Verify unauthenticated /diagnostics returns 401 when token is configured
    assert client.get("/diagnostics").status_code == 401

    # Verify authenticated /diagnostics returns 200 with sanitized db_name
    auth_resp = client.get(
        "/diagnostics",
        headers={"Authorization": f"Bearer {secret_token}"},
    )
    assert auth_resp.status_code == 200
    diag = auth_resp.json()
    assert secret_token not in str(diag)
    db_name = diag["persistence"]["db_name"]
    assert os.sep not in db_name
    assert db_name == "test_diagnostics.db"


# -----------------------------------------------------------------------------
# 7. OrchestratorApiClient Integration
# -----------------------------------------------------------------------------


def test_api_client_health_and_readiness_methods(test_setup):
    """OrchestratorApiClient helper methods (get_health, get_readiness, is_ready, get_diagnostics) work."""
    client, store, manager = test_setup

    # Point OrchestratorApiClient to the TestClient via custom transport
    api_cli = OrchestratorApiClient(base_url="http://testserver")
    api_cli._client = client

    # Healthy checks
    health = api_cli.get_health()
    assert health.status == "healthy"

    ready = api_cli.get_readiness()
    assert ready.status == "ready"
    assert ready.accepting_runs is True

    assert api_cli.is_ready() is True

    diag = api_cli.get_diagnostics()
    assert diag.application.status == "healthy"
    assert diag.persistence.status == "healthy"

    # Shutdown check
    with manager._lock:
        manager._is_shutting_down = True

    ready_shutdown = api_cli.get_readiness()
    assert ready_shutdown.status == "not_ready"
    assert ready_shutdown.accepting_runs is False
    assert api_cli.is_ready() is False
