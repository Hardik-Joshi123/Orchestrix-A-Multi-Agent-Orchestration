"""Tests for audit retention, storage management, safe cleanup, and multi-format export."""

from __future__ import annotations

import csv
import datetime
import io
import json
import sqlite3
import threading
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import ApiValidationError, OrchestratorApiClient
from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import RunManager
from orchestrator.api.models import AuditCleanupResponse
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings, reset_settings
from orchestrator.observability.audit import (
    AuditEventType,
    AuditOutcome,
    record_audit_event,
    serialize_audit_events_csv,
    serialize_audit_events_json,
    serialize_audit_events_jsonl,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch, tmp_path):
    """Ensure clean isolated configuration for every test."""
    db_file = str(tmp_path / "test_retention.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.setenv("ORCHESTRATOR_AUDIT_RETENTION_DAYS", "30")
    monkeypatch.setenv("ORCHESTRATOR_AUDIT_CLEANUP_ON_STARTUP", "false")
    monkeypatch.setenv("ORCHESTRATOR_AUDIT_EXPORT_MAX_LIMIT", "1000")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield
    reset_settings()
    reset_rate_limiter()


def _insert_custom_event(
    store: CheckpointStore,
    timestamp_str: str,
    run_id: str = "run-test-01",
    event_type: str = AuditEventType.WORKFLOW_CREATED,
    metadata: dict | None = None,
) -> str:
    """Helper to insert an audit event with an explicit timestamp."""
    event_id = str(uuid.uuid4())
    store.record_audit_event(
        event_id=event_id,
        timestamp=timestamp_str,
        event_type=event_type,
        run_id=run_id,
        correlation_id=None,
        actor="system",
        outcome=AuditOutcome.SUCCESS,
        metadata=metadata or {"key": "val"},
    )
    return event_id


# ---------------------------------------------------------------------------
# 1. Retention Configuration Validation
# ---------------------------------------------------------------------------


def test_audit_retention_config_validation():
    """Verify settings validation for audit retention, startup cleanup, and export limits."""
    # Defaults
    s = Settings()
    assert s.audit_retention_days == 30
    assert s.audit_cleanup_on_startup is False
    assert s.audit_export_max_limit == 1000

    # Custom valid values
    s_valid = Settings(
        audit_retention_days=0,  # 0 means retain indefinitely
        audit_cleanup_on_startup=True,
        audit_export_max_limit=5000,
    )
    assert s_valid.audit_retention_days == 0
    assert s_valid.audit_cleanup_on_startup is True
    assert s_valid.audit_export_max_limit == 5000

    # Negative retention days rejected
    with pytest.raises(ValueError, match="audit_retention_days must be an integer >= 0"):
        Settings(audit_retention_days=-5)

    # Invalid export limits rejected
    with pytest.raises(ValueError, match="audit_export_max_limit must be an integer >= 1"):
        Settings(audit_export_max_limit=0)

    with pytest.raises(ValueError, match="audit_export_max_limit cannot exceed 10000"):
        Settings(audit_export_max_limit=15000)


# ---------------------------------------------------------------------------
# 2. Selective Purge & Boundary Testing
# ---------------------------------------------------------------------------


def test_purge_audit_events_selective_cutoff():
    """Verify that records older than cutoff are purged while newer records are preserved."""
    store = CheckpointStore()
    now = datetime.datetime.now(datetime.timezone.utc)

    # Insert events at different ages
    t_60d = (now - datetime.timedelta(days=60)).isoformat()
    t_40d = (now - datetime.timedelta(days=40)).isoformat()
    t_20d = (now - datetime.timedelta(days=20)).isoformat()
    t_1d = (now - datetime.timedelta(days=1)).isoformat()

    _insert_custom_event(store, t_60d, run_id="run-old")
    _insert_custom_event(store, t_40d, run_id="run-old")
    _insert_custom_event(store, t_20d, run_id="run-new")
    _insert_custom_event(store, t_1d, run_id="run-new")

    assert store.count_audit_events() == 4

    # Purge events older than 30 days
    result = store.purge_audit_events(max_age_days=30)

    assert result["success"] is True
    assert result["deleted_count"] == 2
    assert result["max_age_days"] == 30
    assert result["cutoff_timestamp"] is not None

    # Remaining events should be exactly the 2 newer events
    assert store.count_audit_events() == 2
    remaining = store.get_audit_events(limit=10)
    remaining_timestamps = [e["timestamp"] for e in remaining]
    assert t_20d in remaining_timestamps
    assert t_1d in remaining_timestamps
    assert t_60d not in remaining_timestamps
    assert t_40d not in remaining_timestamps


def test_purge_audit_events_with_explicit_timestamp():
    """Verify purging using an explicit ISO UTC timestamp."""
    store = CheckpointStore()
    t1 = "2025-01-01T00:00:00+00:00"
    t2 = "2025-02-01T00:00:00+00:00"
    t3 = "2025-03-01T00:00:00+00:00"

    _insert_custom_event(store, t1)
    _insert_custom_event(store, t2)
    _insert_custom_event(store, t3)

    # Purge before 2025-02-01T00:00:00+00:00 (strictly older)
    res = store.purge_audit_events(before_timestamp=t2)
    assert res["success"] is True
    assert res["deleted_count"] == 1
    assert store.count_audit_events() == 2

    # Remaining are t2 and t3
    remaining = [e["timestamp"] for e in store.get_audit_events(limit=10)]
    assert t1 not in remaining
    assert t2 in remaining
    assert t3 in remaining


def test_purge_audit_events_with_limit():
    """Verify bounded deletion when limit is specified."""
    store = CheckpointStore()
    now = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now - datetime.timedelta(days=100)).isoformat()

    for _ in range(8):
        _insert_custom_event(store, old_time)

    assert store.count_audit_events() == 8

    # Purge with limit=3
    res = store.purge_audit_events(max_age_days=30, limit=3)
    assert res["success"] is True
    assert res["deleted_count"] == 3
    assert store.count_audit_events() == 5

    # Purge remaining with limit=10
    res2 = store.purge_audit_events(max_age_days=30, limit=10)
    assert res2["success"] is True
    assert res2["deleted_count"] == 5
    assert store.count_audit_events() == 0


# ---------------------------------------------------------------------------
# 3. Non-Audit Table Preservation
# ---------------------------------------------------------------------------


def test_purge_preserves_checkpoints_and_runs():
    """Verify that purging audit_events never touches runs, checkpoints, or other tables."""
    store = CheckpointStore()
    run_id = "run-preserve-test"

    # Create run and checkpoint
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Do important work", "status": "completed", "data": "persisted-state"},
        transition="completed",
    )

    # Insert old audit events
    now = datetime.datetime.now(datetime.timezone.utc)
    old_t = (now - datetime.timedelta(days=90)).isoformat()
    _insert_custom_event(store, old_t, run_id=run_id)

    assert store.count_audit_events() == 1
    assert store.get_run_summary(run_id) is not None
    assert store.load_latest_checkpoint(run_id) is not None

    # Purge audit events
    res = store.purge_audit_events(max_age_days=10)
    assert res["deleted_count"] == 1
    assert store.count_audit_events() == 0

    # Verify run summary and checkpoints remain completely intact
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["task"] == "Do important work"
    assert summary["status"] == "completed"

    cp = store.load_latest_checkpoint(run_id)
    assert cp is not None
    assert cp.state["data"] == "persisted-state"


# ---------------------------------------------------------------------------
# 4. Idempotence & Safe Handling
# ---------------------------------------------------------------------------


def test_purge_idempotence_and_empty_store():
    """Verify purge on empty database and idempotent repeated purges."""
    store = CheckpointStore()

    # Empty store
    res1 = store.purge_audit_events(max_age_days=30)
    assert res1["success"] is True
    assert res1["deleted_count"] == 0

    # Add an event and purge it
    now = datetime.datetime.now(datetime.timezone.utc)
    old_t = (now - datetime.timedelta(days=45)).isoformat()
    _insert_custom_event(store, old_t)

    res2 = store.purge_audit_events(max_age_days=30)
    assert res2["success"] is True
    assert res2["deleted_count"] == 1

    # Immediate second purge should delete 0 and succeed idempotently
    res3 = store.purge_audit_events(max_age_days=30)
    assert res3["success"] is True
    assert res3["deleted_count"] == 0


def test_purge_and_export_disabled_persistence():
    """Verify safe handling when persistence is disabled."""
    store = CheckpointStore()
    store.enabled = False

    res = store.purge_audit_events(max_age_days=30)
    assert res["success"] is False
    assert res["deleted_count"] == 0
    assert "disabled" in res["reason"].lower()

    export_res = store.export_audit_events()
    assert export_res == []


def test_purge_safe_failure_on_operational_error():
    """Verify safe failure reporting when an operational database error occurs."""
    store = CheckpointStore()

    with patch.object(store, "_get_connection", side_effect=sqlite3.OperationalError("Simulated DB Lock")):
        res = store.purge_audit_events(max_age_days=30)
        assert res["success"] is False
        assert res["deleted_count"] == 0
        assert "Simulated DB Lock" in str(res["error"])


# ---------------------------------------------------------------------------
# 5. Concurrency Race Safety
# ---------------------------------------------------------------------------


def test_concurrent_writes_and_purge():
    """Verify concurrent thread execution writing audit events while purge runs."""
    store = CheckpointStore()
    now = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now - datetime.timedelta(days=50)).isoformat()

    # Seed with some old events
    for _ in range(20):
        _insert_custom_event(store, old_time)

    errors: list[Exception] = []

    def writer_worker(worker_id: int):
        try:
            for i in range(15):
                record_audit_event(
                    event_type=AuditEventType.WORKFLOW_STARTED,
                    outcome=AuditOutcome.SUCCESS,
                    run_id=f"run-worker-{worker_id}",
                    actor=f"worker-{worker_id}",
                    metadata={"step": f"step-{i}"},
                    store=store,
                )
        except Exception as exc:
            errors.append(exc)

    def purger_worker():
        try:
            for _ in range(5):
                store.purge_audit_events(max_age_days=30, limit=5)
        except Exception as exc:
            errors.append(exc)

    threads = []
    # 4 writers and 2 purgers running concurrently
    for w_id in range(4):
        threads.append(threading.Thread(target=writer_worker, args=(w_id,)))
    for _ in range(2):
        threads.append(threading.Thread(target=purger_worker))

    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    # Concurrency must not raise any SQLite locking errors
    assert len(errors) == 0
    # Store must remain readable and functional
    total_remaining = store.count_audit_events()
    assert total_remaining >= 60  # All 4 * 15 = 60 freshly written events remain


# ---------------------------------------------------------------------------
# 6. Serialization: JSON, JSONL, and CSV Formats
# ---------------------------------------------------------------------------


def test_audit_export_serializers():
    """Verify serialization formats: JSON, JSONL, and CSV with complex metadata."""
    events = [
        {
            "event_id": "evt-001",
            "event_type": AuditEventType.WORKFLOW_CREATED,
            "timestamp": "2026-01-01T12:00:00+00:00",
            "run_id": "run-001",
            "correlation_id": "corr-001",
            "actor": "user_1",
            "outcome": AuditOutcome.SUCCESS,
            "metadata": {"task": "Analyze data", "priority": 1, "nested": {"active": True}},
        },
        {
            "event_id": "evt-002",
            "event_type": AuditEventType.WORKFLOW_COMPLETED,
            "timestamp": "2026-01-01T12:05:00+00:00",
            "run_id": "run-001",
            "correlation_id": "corr-002",
            "actor": "orchestrator",
            "outcome": AuditOutcome.SUCCESS,
            "metadata": {"result_code": 0},
        },
    ]

    # JSON serialization
    json_out = serialize_audit_events_json(events)
    parsed_json = json.loads(json_out)
    assert len(parsed_json) == 2
    assert parsed_json[0]["event_id"] == "evt-001"
    assert parsed_json[0]["metadata"]["nested"]["active"] is True

    # JSONL serialization
    jsonl_out = serialize_audit_events_jsonl(events)
    lines = [line.strip() for line in jsonl_out.strip().split("\n") if line.strip()]
    assert len(lines) == 2
    parsed_line_0 = json.loads(lines[0])
    assert parsed_line_0["event_id"] == "evt-001"
    parsed_line_1 = json.loads(lines[1])
    assert parsed_line_1["event_id"] == "evt-002"

    # CSV serialization
    csv_out = serialize_audit_events_csv(events)
    reader = csv.DictReader(io.StringIO(csv_out))
    rows = list(reader)
    assert len(rows) == 2
    assert rows[0]["event_id"] == "evt-001"
    assert rows[0]["event_type"] == AuditEventType.WORKFLOW_CREATED
    # Metadata should be valid JSON in CSV
    meta_parsed = json.loads(rows[0]["metadata"])
    assert meta_parsed["task"] == "Analyze data"
    assert meta_parsed["priority"] == 1


# ---------------------------------------------------------------------------
# 7. API Export & Cleanup Endpoints
# ---------------------------------------------------------------------------


def test_api_export_run_audit_endpoints():
    """Verify GET /runs/{run_id}/audit/export in json, jsonl, and csv formats."""
    store = CheckpointStore()
    manager = RunManager()
    run_id = "run-api-export-01"

    # Seed run and events
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Test export", "status": "completed"},
        transition="completed",
    )
    _insert_custom_event(
        store, "2026-02-01T10:00:00+00:00", run_id=run_id, event_type=AuditEventType.WORKFLOW_STARTED
    )
    _insert_custom_event(
        store, "2026-02-01T10:05:00+00:00", run_id=run_id, event_type=AuditEventType.WORKFLOW_COMPLETED
    )

    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # 1. JSON export
    resp_json = client.get(f"/runs/{run_id}/audit/export?format=json")
    assert resp_json.status_code == 200
    assert resp_json.headers["content-type"].startswith("application/json")
    assert f'filename="audit-{run_id}.json"' in resp_json.headers.get("content-disposition", "")
    data = resp_json.json()
    assert len(data) == 2

    # 2. JSONL export
    resp_jsonl = client.get(f"/runs/{run_id}/audit/export?format=jsonl")
    assert resp_jsonl.status_code == 200
    assert resp_jsonl.headers["content-type"].startswith("application/x-ndjson")
    assert f'filename="audit-{run_id}.jsonl"' in resp_jsonl.headers.get("content-disposition", "")
    lines = resp_jsonl.text.strip().split("\n")
    assert len(lines) == 2

    # 3. CSV export
    resp_csv = client.get(f"/runs/{run_id}/audit/export?format=csv")
    assert resp_csv.status_code == 200
    assert resp_csv.headers["content-type"].startswith("text/csv")
    assert f'filename="audit-{run_id}.csv"' in resp_csv.headers.get("content-disposition", "")
    rows = list(csv.DictReader(io.StringIO(resp_csv.text)))
    assert len(rows) == 2

    # 4. Invalid format error
    resp_inv = client.get(f"/runs/{run_id}/audit/export?format=xml")
    assert resp_inv.status_code == 400
    assert "Unsupported format" in resp_inv.json()["detail"]

    # 5. Non-existent run
    resp_404 = client.get("/runs/non-existent-run/audit/export")
    assert resp_404.status_code == 404

    # 6. Invalid time range
    resp_range = client.get(
        f"/runs/{run_id}/audit/export?start_time=2026-03-01T00:00:00Z&end_time=2026-01-01T00:00:00Z"
    )
    assert resp_range.status_code == 400
    assert "start_time must be earlier than or equal to end_time" in resp_range.json()["detail"]


def test_api_export_system_audit_endpoint():
    """Verify GET /audit/export system-wide export with filtering and pagination."""
    store = CheckpointStore()
    manager = RunManager()

    _insert_custom_event(
        store, "2026-02-01T10:00:00+00:00", run_id="run-A", event_type=AuditEventType.WORKFLOW_CREATED
    )
    _insert_custom_event(
        store, "2026-02-01T10:02:00+00:00", run_id="run-B", event_type=AuditEventType.SECURITY_AUTH_FAILURE
    )
    _insert_custom_event(
        store, "2026-02-01T10:04:00+00:00", run_id="run-A", event_type=AuditEventType.WORKFLOW_COMPLETED
    )

    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # Filter by run_id
    resp_run = client.get("/audit/export?run_id=run-A")
    assert resp_run.status_code == 200
    assert len(resp_run.json()) == 2

    # Filter by event_type
    resp_type = client.get(f"/audit/export?event_type={AuditEventType.SECURITY_AUTH_FAILURE}")
    assert resp_type.status_code == 200
    data = resp_type.json()
    assert len(data) == 1
    assert data[0]["event_type"] == AuditEventType.SECURITY_AUTH_FAILURE

    # Pagination: limit=1, offset=1
    resp_page = client.get("/audit/export?limit=1&offset=1")
    assert resp_page.status_code == 200
    assert len(resp_page.json()) == 1


def test_api_cleanup_audit_endpoint():
    """Verify POST /audit/cleanup with various parameters and validation."""
    store = CheckpointStore()
    manager = RunManager()
    now = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now - datetime.timedelta(days=45)).isoformat()

    _insert_custom_event(store, old_time)
    assert store.count_audit_events() == 1

    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # Clean up with explicit max_age_days
    resp = client.post("/audit/cleanup", json={"max_age_days": 30})
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["deleted_count"] == 1
    assert store.count_audit_events() == 0

    # Validation: negative max_age_days
    resp_neg = client.post("/audit/cleanup", json={"max_age_days": -1})
    assert resp_neg.status_code == 400
    assert "non-negative" in resp_neg.json()["detail"]

    # Validation: invalid before_timestamp
    resp_ts = client.post("/audit/cleanup", json={"before_timestamp": "invalid-timestamp-value"})
    assert resp_ts.status_code == 400
    assert "Expected ISO 8601" in resp_ts.json()["detail"]

    # Validation: limit < 1
    resp_lim = client.post("/audit/cleanup", json={"limit": 0})
    assert resp_lim.status_code == 400
    assert "greater than or equal to 1" in resp_lim.json()["detail"]


# ---------------------------------------------------------------------------
# 8. Authentication & Operational Security
# ---------------------------------------------------------------------------


def test_authentication_enforcement_on_retention_and_export(monkeypatch):
    """Verify unauthenticated requests are rejected when ORCHESTRATOR_API_TOKEN is set."""
    token = "secret-retention-test-token"
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token)
    reset_settings()

    store = CheckpointStore()
    manager = RunManager()

    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # 1. Unauthenticated requests must fail with 401
    assert client.get("/runs/test/audit/export").status_code == 401
    assert client.get("/audit/export").status_code == 401
    assert client.post("/audit/cleanup", json={}).status_code == 401

    # 2. Authenticated requests with valid Bearer token succeed
    headers = {"Authorization": f"Bearer {token}"}
    resp_cleanup = client.post("/audit/cleanup", json={"max_age_days": 30}, headers=headers)
    assert resp_cleanup.status_code == 200

    resp_export = client.get("/audit/export", headers=headers)
    assert resp_export.status_code == 200


# ---------------------------------------------------------------------------
# 9. Startup Cleanup Lifecycle
# ---------------------------------------------------------------------------


def test_startup_cleanup_lifecycle(monkeypatch):
    """Verify startup cleanup is skipped by default, but executed when enabled."""
    store = CheckpointStore()
    manager = RunManager()
    now = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now - datetime.timedelta(days=60)).isoformat()

    # Seed an old event
    _insert_custom_event(store, old_time, run_id="run-seeded-old")
    assert store.count_audit_events(run_id="run-seeded-old") == 1

    # 1. By default, audit_cleanup_on_startup is False -> old event preserved on startup
    app1 = create_app()
    app1.dependency_overrides[get_manager] = lambda: manager
    app1.dependency_overrides[get_store] = lambda: store
    with TestClient(app1):
        assert store.count_audit_events(run_id="run-seeded-old") == 1

    # 2. When enabled, startup cleanup deletes events older than retention cutoff
    monkeypatch.setenv("ORCHESTRATOR_AUDIT_CLEANUP_ON_STARTUP", "true")
    monkeypatch.setenv("ORCHESTRATOR_AUDIT_RETENTION_DAYS", "30")
    reset_settings()

    app2 = create_app()
    app2.dependency_overrides[get_manager] = lambda: manager
    app2.dependency_overrides[get_store] = lambda: store
    with TestClient(app2):
        assert store.count_audit_events(run_id="run-seeded-old") == 0


# ---------------------------------------------------------------------------
# 10. Client SDK Methods
# ---------------------------------------------------------------------------


def test_client_sdk_retention_and_export():
    """Verify OrchestratorApiClient methods for retention cleanup and multi-format export."""
    store = CheckpointStore()
    manager = RunManager()
    run_id = "run-sdk-01"

    store.save_checkpoint(
        run_id=run_id,
        state={"task": "SDK test", "status": "completed"},
        transition="completed",
    )
    _insert_custom_event(store, "2026-02-01T12:00:00+00:00", run_id=run_id)

    app = create_app()
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store

    test_http = TestClient(app)
    client = OrchestratorApiClient(base_url="http://testserver", client=test_http)

    # export_run_audit
    json_out = client.export_run_audit(run_id, format="json")
    assert "event_id" in json_out

    csv_out = client.export_run_audit(run_id, format="csv")
    assert "event_id" in csv_out

    # Client validation for invalid format
    with pytest.raises(ApiValidationError, match="Invalid format 'xml'"):
        client.export_run_audit(run_id, format="xml")

    # export_audit system-wide
    sys_out = client.export_audit(format="jsonl")
    assert "event_id" in sys_out

    # cleanup_audit_events
    cleanup_res = client.cleanup_audit_events(max_age_days=1)
    assert isinstance(cleanup_res, AuditCleanupResponse)
    assert cleanup_res.success is True
