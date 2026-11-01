"""Tests for idempotent workflow submission, deduplication, retry safety, and lifecycle recovery."""

from __future__ import annotations

import concurrent.futures
import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import (
    ApiConflictError,
    ApiValidationError,
    OrchestratorApiClient,
)
from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import RunConflictError, RunManager
from orchestrator.api.models import CreateRunRequest, RunResponse
from orchestrator.api.routes import (
    compute_request_fingerprint,
    get_manager,
    get_store,
    validate_idempotency_key,
)
from orchestrator.api.server import create_app
from orchestrator.config import Settings, reset_settings
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture
def clean_environment(monkeypatch, tmp_path):
    """Ensure clean isolated configuration and database for every test."""
    db_file = str(tmp_path / "test_idempotency.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.setenv("ORCHESTRATOR_IDEMPOTENCY_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_IDEMPOTENCY_RETENTION_HOURS", "24")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield db_file
    reset_settings()
    reset_rate_limiter()


@pytest.fixture
def test_app(clean_environment):
    """Provide a configured test application, CheckpointStore, and RunManager."""
    db_file = clean_environment
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
# 1. Configuration & Key Validation Tests
# -----------------------------------------------------------------------------


def test_idempotency_config_validation():
    """Verify settings validation for idempotency retention and feature enablement."""
    s = Settings()
    assert s.idempotency_enabled is True
    assert s.idempotency_retention_hours == 24

    s_custom = Settings(idempotency_enabled=False, idempotency_retention_hours=48)
    assert s_custom.idempotency_enabled is False
    assert s_custom.idempotency_retention_hours == 48

    with pytest.raises(ValueError, match="idempotency_retention_hours must be an integer >= 1"):
        Settings(idempotency_retention_hours=0)

    with pytest.raises(ValueError, match="idempotency_retention_hours cannot exceed 87600"):
        Settings(idempotency_retention_hours=90000)


def test_idempotency_key_format_validation():
    """Verify key validation rules (length 1..128, printable ASCII, non-empty)."""
    # Valid keys
    assert validate_idempotency_key(None) is None
    assert validate_idempotency_key("req-12345-abc") == "req-12345-abc"
    assert validate_idempotency_key("  trimmed-key  ") == "trimmed-key"
    assert validate_idempotency_key("a" * 128) == "a" * 128

    # Empty or whitespace keys
    with pytest.raises(Exception) as exc1:
        validate_idempotency_key("")
    assert exc1.value.status_code == 400
    assert "empty or whitespace" in exc1.value.detail

    with pytest.raises(Exception) as exc2:
        validate_idempotency_key("   ")
    assert exc2.value.status_code == 400

    # Key too long
    with pytest.raises(Exception) as exc3:
        validate_idempotency_key("a" * 129)
    assert exc3.value.status_code == 400
    assert "exceeds maximum allowed limit" in exc3.value.detail

    # Non-printable characters
    with pytest.raises(Exception) as exc4:
        validate_idempotency_key("bad\nkey")
    assert exc4.value.status_code == 400
    assert "invalid characters" in exc4.value.detail


def test_request_fingerprint_determinism():
    """Verify deterministic request hashing across parameter order and whitespace."""
    req1 = CreateRunRequest(task="Perform analysis  ", max_concurrency=2, background=False)
    req2 = CreateRunRequest(task="Perform analysis", max_concurrency=2, background=False)
    assert compute_request_fingerprint(req1) == compute_request_fingerprint(req2)

    req_diff = CreateRunRequest(task="Perform analysis", max_concurrency=4, background=False)
    assert compute_request_fingerprint(req1) != compute_request_fingerprint(req_diff)


# -----------------------------------------------------------------------------
# 2. Basic Idempotency & Safe Retry Tests
# -----------------------------------------------------------------------------


def test_first_submission_creates_run(test_app):
    """First submission with an Idempotency-Key creates a new workflow run."""
    client, store, _ = test_app
    key = "idem-test-01"
    payload = {"task": "Analyze distributed caching architectures.", "max_concurrency": 2}

    resp = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp.status_code == 201
    data = resp.json()
    assert data["run_id"].startswith("run-")
    assert data["task"] == payload["task"]
    assert "Idempotent-Replayed" not in resp.headers

    # Verify database record in idempotency_keys
    rec = store.get_idempotency_record(key=key)
    assert rec is not None
    assert rec["run_id"] == data["run_id"]
    assert rec["status"] == "completed"
    assert rec["response_code"] == 201


def test_repeated_key_returns_identical_run(test_app):
    """Retrying with the same Idempotency-Key and payload returns the original run."""
    client, store, _ = test_app
    key = "idem-test-02"
    payload = {"task": "Evaluate database indexing strategies."}

    # 1. First submission
    resp1 = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    data1 = resp1.json()

    # 2. Retry with same key and payload
    resp2 = client.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 201
    data2 = resp2.json()

    # Must return exact same run identity and results
    assert data2["run_id"] == data1["run_id"]
    assert data2["task"] == data1["task"]
    assert data2["status"] == data1["status"]
    assert resp2.headers.get("Idempotent-Replayed") == "true"

    # Verify no duplicate run was created in SQLite runs table
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM runs;")
            assert cursor.fetchone()[0] == 1
        finally:
            conn.close()


def test_reusing_key_with_different_payload_returns_conflict(test_app):
    """Reusing an Idempotency-Key with a materially different payload returns 409 Conflict."""
    client, store, _ = test_app
    key = "idem-test-conflict"
    payload1 = {"task": "Initial task: Review system architecture."}
    payload2 = {"task": "Modified task: Draft marketing newsletter."}

    # First submission succeeds
    resp1 = client.post("/runs", json=payload1, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    original_run_id = resp1.json()["run_id"]

    # Reusing with different task returns 409
    resp2 = client.post("/runs", json=payload2, headers={"Idempotency-Key": key})
    assert resp2.status_code == 409
    assert "previously used with a different request payload" in resp2.json()["detail"]

    # Reusing with different max_concurrency returns 409
    payload3 = {"task": "Initial task: Review system architecture.", "max_concurrency": 8}
    resp3 = client.post("/runs", json=payload3, headers={"Idempotency-Key": key})
    assert resp3.status_code == 409

    # Original run remains unaltered
    rec = store.get_idempotency_record(key=key)
    assert rec["run_id"] == original_run_id


def test_requests_without_key_create_distinct_runs(test_app):
    """Requests without an Idempotency-Key retain existing behavior and create distinct runs."""
    client, store, _ = test_app
    payload = {"task": "Standard non-idempotent task."}

    resp1 = client.post("/runs", json=payload)
    assert resp1.status_code == 201
    run1 = resp1.json()["run_id"]

    resp2 = client.post("/runs", json=payload)
    assert resp2.status_code == 201
    run2 = resp2.json()["run_id"]

    assert run1 != run2
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM runs;")
            assert cursor.fetchone()[0] == 2
        finally:
            conn.close()


# -----------------------------------------------------------------------------
# 3. Concurrency Safety Tests
# -----------------------------------------------------------------------------


def test_concurrent_submissions_with_same_key(test_app):
    """Simultaneous concurrent requests using the same key create at most one workflow run."""
    client, store, _ = test_app
    key = "idem-concurrent-key"
    payload = {"task": "Parallel concurrent submission safety check."}

    num_threads = 6
    results = []

    def submit_request():
        # New client instance sharing the app for thread safety
        c = TestClient(client.app)
        return c.post("/runs", json=payload, headers={"Idempotency-Key": key})

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(submit_request) for _ in range(num_threads)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    # All responses must succeed (201 Created)
    assert len(results) == num_threads
    for r in results:
        assert r.status_code == 201

    # All responses must return the EXACT same run_id
    run_ids = {r.json()["run_id"] for r in results}
    assert len(run_ids) == 1

    # Exactly one run stored in SQLite
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM runs;")
            assert cursor.fetchone()[0] == 1
            cursor2 = conn.execute("SELECT COUNT(*) FROM idempotency_keys;")
            assert cursor2.fetchone()[0] == 1
        finally:
            conn.close()


# -----------------------------------------------------------------------------
# 4. Persistence & Application Restart Safety
# -----------------------------------------------------------------------------


def test_idempotency_persists_across_restart(clean_environment):
    """Idempotency records survive application/manager restart, preventing duplicates."""
    db_file = clean_environment
    key = "idem-restart-key"
    payload = {"task": "Task executed before restart."}

    # Process 1: Submit and create run
    store1 = CheckpointStore(db_path=db_file, enabled=True)
    manager1 = RunManager()
    app1 = create_app()
    app1.dependency_overrides[get_store] = lambda: store1
    app1.dependency_overrides[get_manager] = lambda: manager1
    client1 = TestClient(app1)

    resp1 = client1.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp1.status_code == 201
    run_id1 = resp1.json()["run_id"]

    manager1.shutdown()
    store1.close()

    # Process 2: Restart application with fresh managers pointing to same DB
    store2 = CheckpointStore(db_path=db_file, enabled=True)
    manager2 = RunManager()
    app2 = create_app()
    app2.dependency_overrides[get_store] = lambda: store2
    app2.dependency_overrides[get_manager] = lambda: manager2
    client2 = TestClient(app2)

    # Retry same key
    resp2 = client2.post("/runs", json=payload, headers={"Idempotency-Key": key})
    assert resp2.status_code == 201
    assert resp2.json()["run_id"] == run_id1
    assert resp2.headers.get("Idempotent-Replayed") == "true"

    # Conflicting payload after restart still rejected
    resp3 = client2.post("/runs", json={"task": "Different task"}, headers={"Idempotency-Key": key})
    assert resp3.status_code == 409

    manager2.shutdown()
    store2.close()


def test_startup_recovery_of_interrupted_idempotency_key(clean_environment):
    """Startup recovery finalizes in-progress keys if run exists, or marks failed if incomplete."""
    db_file = clean_environment
    store = CheckpointStore(db_path=db_file, enabled=True)

    # 1. Simulate key where run WAS created before unexpected crash
    run_id_created = "run-crashed-created"
    store.save_checkpoint(
        run_id=run_id_created,
        state={"task": "Crashed run that exists", "status": "interrupted"},
        transition="interrupted",
    )
    store.acquire_idempotency_key("ip:127.0.0.1", "key-crashed-1", "hash-1", run_id_created)

    # 2. Simulate key where process crashed BEFORE run was persisted
    run_id_uncreated = "run-crashed-uncreated"
    store.acquire_idempotency_key("ip:127.0.0.1", "key-crashed-2", "hash-2", run_id_uncreated)

    # Execute startup recovery
    recovered = store.recover_in_progress_idempotency_keys()
    assert recovered == 2

    # Key 1 should be completed with run_id_created
    rec1 = store.get_idempotency_record("ip:127.0.0.1", "key-crashed-1")
    assert rec1["status"] == "completed"
    assert rec1["run_id"] == run_id_created

    # Key 2 should be marked failed, allowing client to safely retry
    rec2 = store.get_idempotency_record("ip:127.0.0.1", "key-crashed-2")
    assert rec2["status"] == "failed"

    store.close()


# -----------------------------------------------------------------------------
# 5. Authentication Scope & Isolation Tests
# -----------------------------------------------------------------------------


def test_authentication_scope_isolation(monkeypatch, clean_environment):
    """Idempotency keys are isolated per authenticated client identity."""
    db_file = clean_environment
    token_a = "token-client-alpha-12345"
    token_b = "token-client-bravo-67890"

    # Configure API token requirement
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token_a)
    reset_settings()

    store = CheckpointStore(db_path=db_file, enabled=True)
    manager = RunManager()
    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager
    client = TestClient(app)

    key = "shared-idempotency-key"
    payload = {"task": "Isolated client task"}

    # Client A creates with key
    resp_a = client.post(
        "/runs",
        json=payload,
        headers={"Idempotency-Key": key, "Authorization": f"Bearer {token_a}"},
    )
    assert resp_a.status_code == 201
    run_a = resp_a.json()["run_id"]

    # Replay by Client A returns run_a
    resp_a2 = client.post(
        "/runs",
        json=payload,
        headers={"Idempotency-Key": key, "Authorization": f"Bearer {token_a}"},
    )
    assert resp_a2.status_code == 201
    assert resp_a2.json()["run_id"] == run_a

    # Switch server token to token_b
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token_b)
    reset_settings()

    # Client B uses the same key string, but has distinct client scope
    resp_b = client.post(
        "/runs",
        json=payload,
        headers={"Idempotency-Key": key, "Authorization": f"Bearer {token_b}"},
    )
    assert resp_b.status_code == 201
    run_b = resp_b.json()["run_id"]

    # Must NOT collide with Client A's run
    assert run_b != run_a

    manager.shutdown()
    store.close()


# -----------------------------------------------------------------------------
# 6. Failure Handling & Retry After Error
# -----------------------------------------------------------------------------


def test_failed_creation_allows_subsequent_retry(test_app):
    """If creation fails (e.g. invalid parameter or conflict), the key can be retried."""
    client, store, manager = test_app
    key = "idem-retry-after-fail"

    # Simulate an error on first attempt by patching start_run
    with patch.object(manager, "start_run", side_effect=RunConflictError("Simulated resource collision")):
        resp1 = client.post("/runs", json={"task": "Failing task"}, headers={"Idempotency-Key": key})
        assert resp1.status_code == 409

    # Record in DB should be marked failed
    rec = store.get_idempotency_record(key=key)
    assert rec is not None
    assert rec["status"] == "failed"

    # Subsequent retry with same key now succeeds!
    resp2 = client.post("/runs", json={"task": "Failing task"}, headers={"Idempotency-Key": key})
    assert resp2.status_code == 201
    assert resp2.json()["run_id"].startswith("run-")

    # Record transitioned to completed
    rec2 = store.get_idempotency_record(key=key)
    assert rec2["status"] == "completed"


# -----------------------------------------------------------------------------
# 7. Retention & Storage Purge Tests
# -----------------------------------------------------------------------------


def test_purge_idempotency_keys_retention(clean_environment):
    """Purge operation removes records older than retention cutoff while preserving active ones."""
    db_file = clean_environment
    store = CheckpointStore(db_path=db_file, enabled=True)
    now = datetime.datetime.now(datetime.timezone.utc)

    # Insert old record (48 hours ago)
    t_old = (now - datetime.timedelta(hours=48)).isoformat()
    # Insert fresh record (2 hours ago)
    t_new = (now - datetime.timedelta(hours=2)).isoformat()

    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO idempotency_keys (
                        key, client_scope, request_hash, run_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'completed', ?, ?);
                    """,
                    ("key-old", "scope-1", "hash-1", "run-old", t_old, t_old),
                )
                conn.execute(
                    """
                    INSERT INTO idempotency_keys (
                        key, client_scope, request_hash, run_id, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'completed', ?, ?);
                    """,
                    ("key-new", "scope-1", "hash-2", "run-new", t_new, t_new),
                )
        finally:
            conn.close()

    # Purge older than 24 hours
    res = store.purge_idempotency_keys(max_age_hours=24)
    assert res["success"] is True
    assert res["deleted_count"] == 1

    # Old record removed, new record kept
    assert store.get_idempotency_record("scope-1", "key-old") is None
    assert store.get_idempotency_record("scope-1", "key-new") is not None

    store.close()


# -----------------------------------------------------------------------------
# 8. Client SDK Integration Tests
# -----------------------------------------------------------------------------


def test_client_sdk_idempotent_creation(test_app):
    """Verify OrchestratorApiClient supports passing idempotency_key for safe retries."""
    client_http, store, _ = test_app
    api_client = OrchestratorApiClient(base_url="http://testserver", client=client_http)
    key = "idem-sdk-key-01"

    # 1. Initial creation
    run1 = api_client.create_run(
        task="Client SDK task with idempotency key",
        max_concurrency=2,
        idempotency_key=key,
    )
    assert isinstance(run1, RunResponse)
    assert run1.run_id.startswith("run-")

    # 2. Retry with same key returns identical RunResponse
    run2 = api_client.create_run(
        task="Client SDK task with idempotency key",
        max_concurrency=2,
        idempotency_key=key,
    )
    assert run2.run_id == run1.run_id
    assert run2.task == run1.task

    # 3. Payload mismatch raises ApiConflictError (409)
    with pytest.raises(ApiConflictError, match="different request payload"):
        api_client.create_run(
            task="Different task instruction",
            idempotency_key=key,
        )

    # 4. Invalid key validation in client SDK
    with pytest.raises(ApiValidationError, match="empty or whitespace"):
        api_client.create_run(task="Valid task", idempotency_key="   ")

    with pytest.raises(ApiValidationError, match="exceeds maximum limit"):
        api_client.create_run(task="Valid task", idempotency_key="X" * 129)
