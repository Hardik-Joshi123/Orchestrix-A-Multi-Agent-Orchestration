"""Tests for structured audit logging, event history persistence, and privacy protection."""

from __future__ import annotations

import sqlite3
import uuid
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.limiter import reset_rate_limiter
from orchestrator.api.manager import RunManager
from orchestrator.api.models import AuditEventModel, RunAuditResponse
from orchestrator.api.routes import get_manager, get_store
from orchestrator.api.server import create_app
from orchestrator.config import reset_settings
from orchestrator.hitl.queue import ReviewItem, ReviewQueue
from orchestrator.observability.audit import (
    AuditEvent,
    AuditEventType,
    AuditOutcome,
    record_audit_event,
    sanitize_audit_metadata,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch, tmp_path):
    """Ensure clean isolated configuration for every test."""
    db_file = str(tmp_path / "test_audit.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_DEV_MODE", "true")
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    reset_settings()
    reset_rate_limiter()
    yield
    reset_rate_limiter()
    reset_settings()


def test_sanitize_audit_metadata_redaction():
    """Verify sensitive keys, Bearer tokens, OpenAI API keys, and tracebacks are sanitized."""
    raw_meta = {
        "api_key": "sk-proj-1234567890abcdef123456",
        "authorization": "Bearer secret-token-xyz-12345678",
        "password": "super-secret-password",
        "user_token": "token-12345",
        "nested": {
            "secret_val": "nested-secret",
            "safe_counter": 42,
            "error_msg": (
                "Traceback (most recent call last):\n"
                '  File "test.py", line 10, in foo\n'
                "ValueError: Specialist failed to execute\n"
            ),
        },
        "task": "Perform a sensitive operation with key sk-998877665544332211",
    }

    clean = sanitize_audit_metadata(raw_meta)

    # Sensitive key names must be redacted
    assert clean["api_key"] == "[REDACTED]"
    assert clean["authorization"] == "[REDACTED]"
    assert clean["password"] == "[REDACTED]"
    assert clean["user_token"] == "[REDACTED]"
    assert clean["nested"]["secret_val"] == "[REDACTED]"

    # Safe primitives preserved
    assert clean["nested"]["safe_counter"] == 42

    # Traceback reduced to final line
    assert clean["nested"]["error_msg"] == "ValueError: Specialist failed to execute"

    # Full task converted to length and sanitized preview
    assert "task_chars" in clean
    assert "task_preview" in clean
    assert "[REDACTED_KEY]" in clean["task_preview"]
    assert "sk-9988" not in clean["task_preview"]


def test_audit_event_dataclass_and_serialization():
    """Verify AuditEvent structure and dictionary serialization."""
    event = AuditEvent(
        event_id="aud-123456",
        timestamp="2026-10-24T12:00:00Z",
        event_type=AuditEventType.WORKFLOW_CREATED,
        outcome=AuditOutcome.SUCCESS,
        run_id="run-100",
        actor="operator",
        metadata={"step": 1},
    )

    d = event.to_dict()
    assert d["event_id"] == "aud-123456"
    assert d["event_type"] == "workflow.created"
    assert d["outcome"] == "success"
    assert d["run_id"] == "run-100"
    assert d["actor"] == "operator"
    assert d["metadata"] == {"step": 1}

    # Verify Pydantic model compatibility
    model = AuditEventModel(**d)
    assert model.event_id == "aud-123456"
    assert model.event_type == "workflow.created"


def test_store_audit_event_persistence_and_ordering(tmp_path):
    """Verify CheckpointStore records, bounds, orders, and filters audit events."""
    db_path = str(tmp_path / "ordering.db")
    store = CheckpointStore(db_path=db_path, enabled=True)

    run_a = "run-test-a"
    run_b = "run-test-b"

    # Record multiple events
    store.record_audit_event(
        event_id="evt-1",
        timestamp="2026-10-24T10:00:01Z",
        event_type=AuditEventType.WORKFLOW_CREATED,
        run_id=run_a,
        correlation_id=None,
        actor="system",
        outcome=AuditOutcome.SUCCESS,
        metadata={"m": 1},
    )
    store.record_audit_event(
        event_id="evt-2",
        timestamp="2026-10-24T10:00:02Z",
        event_type=AuditEventType.WORKFLOW_STARTED,
        run_id=run_a,
        correlation_id=None,
        actor="system",
        outcome=AuditOutcome.SUCCESS,
        metadata={"m": 2},
    )
    store.record_audit_event(
        event_id="evt-3",
        timestamp="2026-10-24T10:00:03Z",
        event_type=AuditEventType.WORKFLOW_COMPLETED,
        run_id=run_a,
        correlation_id=None,
        actor="system",
        outcome=AuditOutcome.SUCCESS,
        metadata={"m": 3},
    )
    store.record_audit_event(
        event_id="evt-4",
        timestamp="2026-10-24T10:00:04Z",
        event_type=AuditEventType.WORKFLOW_CREATED,
        run_id=run_b,
        correlation_id=None,
        actor="user",
        outcome=AuditOutcome.SUCCESS,
        metadata={"m": 4},
    )

    # 1. Query for run_a
    events_a = store.get_audit_events(run_id=run_a)
    assert len(events_a) == 3
    assert [e["event_id"] for e in events_a] == ["evt-1", "evt-2", "evt-3"]
    assert store.count_audit_events(run_id=run_a) == 3

    # 2. Query pagination (limit=2, offset=1)
    page = store.get_audit_events(run_id=run_a, limit=2, offset=1)
    assert len(page) == 2
    assert [e["event_id"] for e in page] == ["evt-2", "evt-3"]

    # 3. Filter by event_type
    filtered = store.get_audit_events(run_id=run_a, event_type=AuditEventType.WORKFLOW_STARTED)
    assert len(filtered) == 1
    assert filtered[0]["event_id"] == "evt-2"

    # 4. Total count across all runs
    assert store.count_audit_events() == 4


def test_safe_failure_policy_on_broken_database():
    """Verify that when persistence fails, record_audit_event logs warning and returns cleanly."""
    store = CheckpointStore(db_path=":memory:", enabled=True)

    # Simulate database operational failure by corrupting connection
    with patch.object(store, "_get_connection", side_effect=sqlite3.OperationalError("Disk I/O failure")):
        # Must not raise an exception
        evt = record_audit_event(
            event_type=AuditEventType.WORKFLOW_CREATED,
            outcome=AuditOutcome.SUCCESS,
            run_id="run-fail-test",
            store=store,
        )
        assert evt is not None
        assert evt.run_id == "run-fail-test"


def test_workflow_lifecycle_audit_events(tmp_path):
    """Verify manager records workflow.created, started, completed, and cancelled audit events."""
    db_path = str(tmp_path / "lifecycle.db")
    store = CheckpointStore(db_path=db_path, enabled=True)
    manager = RunManager()

    # 1. Start and complete a workflow
    run_res = manager.start_run(
        task="Audit lifecycle test task",
        store=store,
    )
    run_id = run_res.run_id

    events = store.get_audit_events(run_id=run_id)
    event_types = [e["event_type"] for e in events]

    assert AuditEventType.WORKFLOW_CREATED in event_types
    assert AuditEventType.WORKFLOW_STARTED in event_types
    assert AuditEventType.WORKFLOW_COMPLETED in event_types

    # 2. Test cancellation audit event
    active_id = "run-cancel-target"
    manager._register_active(active_id, "Target task for cancel")
    # Save a pending checkpoint so cancel can operate
    store.save_checkpoint(active_id, {"task": "Target", "status": "running"}, transition="start")

    cancel_res = manager.cancel_run(active_id, reason="User test cancellation", store=store)
    assert cancel_res.status == "cancelled"

    cancel_events = store.get_audit_events(run_id=active_id, event_type=AuditEventType.WORKFLOW_CANCELLED)
    assert len(cancel_events) >= 1
    assert cancel_events[0]["metadata"]["reason"] == "User test cancellation"


def test_shutdown_and_startup_recovery_deduplication(tmp_path):
    """Verify startup recovery records workflow.recovered with idempotent deduplication."""
    db_path = str(tmp_path / "recovery.db")
    store = CheckpointStore(db_path=db_path, enabled=True)
    manager = RunManager()

    # Insert a simulated run left in 'running' state by a previous process
    orphan_id = f"run-{uuid.uuid4().hex[:12]}"
    store.save_checkpoint(
        orphan_id,
        {"task": "Interrupted workflow", "status": "running", "plan": [{"id": "s1", "status": "pending"}]},
        transition="step_1",
    )

    # First recovery run
    recovered_1 = manager.recover_interrupted_runs(store=store, reason="Process died")
    assert orphan_id in recovered_1

    events_1 = store.get_audit_events(run_id=orphan_id, event_type=AuditEventType.WORKFLOW_RECOVERED)
    assert len(events_1) == 1
    assert events_1[0]["outcome"] == AuditOutcome.SUCCESS

    # Second recovery run (idempotent retry)
    recovered_2 = manager.recover_interrupted_runs(store=store, reason="Process died")
    assert orphan_id not in recovered_2  # already recovered

    # Audit events count must remain exactly 1 (no duplicate recovery events)
    events_2 = store.get_audit_events(run_id=orphan_id, event_type=AuditEventType.WORKFLOW_RECOVERED)
    assert len(events_2) == 1


def test_hitl_escalation_and_resolution_audit_events():
    """Verify ReviewQueue.push and resolve emit hitl.escalated and hitl.approved/edited audit events."""
    store = CheckpointStore(enabled=True)

    import uuid

    task_id = f"run-hitl-audit-{uuid.uuid4().hex[:8]}"
    q = ReviewQueue()
    # Ensure queue is clean
    q._client = None
    from orchestrator.hitl import queue as q_mod

    q_mod._LOCAL.clear()

    item = ReviewItem(
        task_id=task_id,
        task="HITL audit task",
        reason="low_confidence",
        level="approve_action",
        proposed_action="Execute shell script",
        confidence=0.45,
        quality=0.55,
    )

    # Push to queue -> emits hitl.escalated
    q.push(item)

    esc_events = store.get_audit_events(run_id=task_id, event_type=AuditEventType.HITL_ESCALATED)
    assert len(esc_events) == 1
    assert esc_events[0]["actor"] == "system"
    assert esc_events[0]["outcome"] == AuditOutcome.ESCALATED
    assert esc_events[0]["metadata"]["reason"] == "low_confidence"

    # Resolve with edit -> emits hitl.edited
    res = q.resolve(
        identifier=item.item_id,
        decision="edit",
        approver="security-admin",
        note="Approved with restricted sandbox parameters",
    )
    assert res is not None

    edit_events = store.get_audit_events(run_id=task_id, event_type=AuditEventType.HITL_EDITED)
    assert len(edit_events) == 1
    assert edit_events[0]["actor"] == "security-admin"
    assert edit_events[0]["outcome"] == AuditOutcome.SUCCESS
    assert edit_events[0]["metadata"]["decision"] == "edit"


def test_security_auth_failure_audit_events(monkeypatch, tmp_path):
    """Verify missing and invalid credentials record security.auth_failure without secret leakage."""
    test_db = str(tmp_path / "test_auth_audit.db")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", test_db)
    monkeypatch.setenv("CHECKPOINT_DB_PATH", test_db)
    store = CheckpointStore(enabled=True, db_path=test_db)
    manager = RunManager()

    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "valid-secret-token-999")

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)

    # 1. Request with missing credentials
    resp1 = client.get("/diagnostics")
    assert resp1.status_code == 401

    events1 = store.get_audit_events(event_type=AuditEventType.SECURITY_AUTH_FAILURE)
    assert len(events1) == 1
    assert events1[0]["outcome"] == AuditOutcome.DENIED
    assert events1[0]["metadata"]["reason"] == "missing_credentials"

    # 2. Request with invalid credentials
    invalid_token = "invalid-token-leaked-attempt-sk-12345678"
    resp2 = client.get("/diagnostics", headers={"Authorization": f"Bearer {invalid_token}"})
    assert resp2.status_code == 401

    events2 = store.get_audit_events(event_type=AuditEventType.SECURITY_AUTH_FAILURE)
    assert len(events2) == 2
    assert events2[1]["metadata"]["reason"] == "invalid_token"

    # Guarantee raw token was NEVER persisted in audit table
    conn = store._get_connection()
    try:
        cursor = conn.execute("SELECT * FROM audit_events;")
        rows = cursor.fetchall()
        for r in rows:
            for col in r:
                assert invalid_token not in str(col)
                assert "valid-secret-token-999" not in str(col)
    finally:
        conn.close()


def test_security_rate_limit_exceeded_audit_event(monkeypatch):
    """Verify rate limit violations record security.rate_limit_exceeded with client IP."""
    store = CheckpointStore(enabled=True)
    manager = RunManager()

    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "2")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "60")
    reset_rate_limiter()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)

    # Send 2 allowed requests, 3rd must trigger 429
    r1 = client.post("/runs", json={"task": "Rate limit task 1"})
    assert r1.status_code in (200, 201)
    r2 = client.post("/runs", json={"task": "Rate limit task 2"})
    assert r2.status_code in (200, 201)
    r3 = client.post("/runs", json={"task": "Rate limit task 3"})
    assert r3.status_code == 429

    events = store.get_audit_events(event_type=AuditEventType.SECURITY_RATE_LIMIT_EXCEEDED)
    assert len(events) >= 1
    assert events[0]["outcome"] == AuditOutcome.DENIED
    assert events[0]["actor"] in ("127.0.0.1", "testclient")
    assert events[0]["metadata"]["path"] == "/runs"


def test_api_audit_query_endpoint_and_client(monkeypatch):
    """Verify GET /runs/{run_id}/audit endpoint security, pagination, and SDK client integration."""
    store = CheckpointStore(enabled=True)
    manager = RunManager()

    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-auth-token")

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager

    client = TestClient(app)

    run_id = "run-api-audit-target"
    # Seed events
    for i in range(5):
        store.record_audit_event(
            event_id=f"evt-{i}",
            timestamp=f"2026-10-24T12:00:0{i}Z",
            event_type=AuditEventType.WORKFLOW_STARTED if i == 0 else AuditEventType.WORKFLOW_COMPLETED,
            run_id=run_id,
            correlation_id=None,
            actor="test",
            outcome=AuditOutcome.SUCCESS,
            metadata={"step": i},
        )

    # 1. Unauthenticated request -> 401
    resp_unauth = client.get(f"/runs/{run_id}/audit")
    assert resp_unauth.status_code == 401

    auth_headers = {"Authorization": "Bearer test-auth-token"}

    # 2. Query non-existent run -> 404
    resp_404 = client.get("/runs/non-existent-run/audit", headers=auth_headers)
    assert resp_404.status_code == 404

    # 3. Authenticated query -> 200 with RunAuditResponse
    resp_ok = client.get(f"/runs/{run_id}/audit?limit=3&offset=1", headers=auth_headers)
    assert resp_ok.status_code == 200
    data = resp_ok.json()
    assert data["run_id"] == run_id
    assert data["total_events"] == 5
    assert data["limit"] == 3
    assert data["offset"] == 1
    assert len(data["events"]) == 3
    assert data["events"][0]["event_id"] == "evt-1"

    # 4. Filter by event_type
    resp_filt = client.get(
        f"/runs/{run_id}/audit?event_type={AuditEventType.WORKFLOW_STARTED}",
        headers=auth_headers,
    )
    assert resp_filt.status_code == 200
    filt_data = resp_filt.json()
    assert len(filt_data["events"]) == 1
    assert filt_data["events"][0]["event_id"] == "evt-0"

    # 5. SDK Client integration
    api_client = OrchestratorApiClient(
        base_url="http://testserver",
        api_token="test-auth-token",
        client=client,
    )
    sdk_resp = api_client.get_run_audit(run_id=run_id, limit=2, offset=0)
    assert isinstance(sdk_resp, RunAuditResponse)
    assert sdk_resp.total_events == 5
    assert len(sdk_resp.events) == 2
