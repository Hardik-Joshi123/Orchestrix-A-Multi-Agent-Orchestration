"""Deterministic test suite for Outbox Delivery Observability & Operational Recovery (Task 51)."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.routes import get_settings, get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture
def temp_db(tmp_path: Any) -> str:
    """Return path to a fresh SQLite database file for testing."""
    return str(tmp_path / "test_outbox_obs.db")


@pytest.fixture
def store(temp_db: str) -> CheckpointStore:
    """Return a CheckpointStore instance bound to temp_db."""
    return CheckpointStore(db_path=temp_db)


@pytest.fixture
def test_settings(temp_db: str) -> Settings:
    """Return configured settings for test environment."""
    return Settings(
        checkpoint_db_path=temp_db,
        api_token="test-secret-token",
        outbox_enabled=True,
        outbox_dispatcher_enabled=False,
        outbox_max_attempts=5,
    )


@pytest.fixture
def app_client(
    temp_db: str,
    store: CheckpointStore,
    test_settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
) -> TestClient:
    """Return a TestClient bound to a configured test app."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-secret-token")
    with patch("orchestrator.api.routes.get_settings", return_value=test_settings), patch(
        "orchestrator.api.server.get_settings", return_value=test_settings
    ), patch(
        "orchestrator.notifications.dispatcher.get_settings", return_value=test_settings
    ):
        app = create_app()
        app.dependency_overrides[get_settings] = lambda: test_settings
        app.dependency_overrides[get_store] = lambda: store
        return TestClient(app)


def test_durable_delivery_metrics_and_latency(store: CheckpointStore) -> None:
    """Verify durable status counts, retry metrics, and latency statistics from DB state."""
    # 1. Insert 3 events
    e1 = store.insert_outbox_event(
        event_type="tool.approval_requested",
        payload={"op": "1"},
        created_at="2026-10-31T10:00:00+00:00",
    )
    e2 = store.insert_outbox_event(
        event_type="tool.approval_granted",
        payload={"op": "2"},
        created_at="2026-10-31T10:05:00+00:00",
    )
    store.insert_outbox_event(
        event_type="tool.approval_rejected",
        payload={"op": "3"},
        created_at="2026-10-31T10:10:00+00:00",
    )

    # 2. Claim e1 and mark delivered with known delivered_at (latency: 120s)
    c1 = store.claim_outbox_events(owner_id="w1", batch_size=1, now="2026-10-31T10:01:00+00:00")[0]
    assert c1["event_id"] == e1["event_id"]
    store.update_outbox_event_delivery(
        event_id=e1["event_id"],
        claimed_by="w1",
        fence_token=c1["fence_token"],
        success=True,
        now="2026-10-31T10:02:00+00:00",
    )

    # 3. Claim e2 and mark failed (attempt 1)
    c2 = store.claim_outbox_events(owner_id="w1", batch_size=1, now="2026-10-31T10:06:00+00:00")[0]
    assert c2["event_id"] == e2["event_id"]
    store.update_outbox_event_delivery(
        event_id=e2["event_id"],
        claimed_by="w1",
        fence_token=c2["fence_token"],
        success=False,
        error="Gateway timeout",
        retryable=True,
        now="2026-10-31T10:07:00+00:00",
    )

    # 4. Check diagnostics
    diag = store.get_outbox_diagnostics(max_attempts=5)
    assert diag["enabled"] is True
    assert diag["total_events"] == 3
    assert diag["counts"]["delivered"] == 1
    assert diag["counts"]["pending"] == 2
    assert diag["counts"]["dead_letter"] == 0
    assert diag["counts"]["claimed"] == 0

    # Oldest pending should be e2 created at 10:05:00
    assert diag["oldest_pending_at"] == "2026-10-31T10:05:00+00:00"
    assert diag["oldest_pending_age_seconds"] is not None
    assert diag["oldest_pending_age_seconds"] > 0

    # Retry metrics
    assert diag["retry_metrics"]["total_attempts"] == 2  # 1 for e1, 1 for e2
    assert diag["retry_metrics"]["retried_events_count"] == 0  # not yet retried > 1

    # Delivery latency
    lat = diag["delivery_latency"]
    assert lat is not None
    assert lat["delivered_samples_count"] == 1
    assert lat["avg_latency_seconds"] == 120.0
    assert lat["min_latency_seconds"] == 120.0
    assert lat["max_latency_seconds"] == 120.0
    assert lat["p95_latency_seconds"] == 120.0


def test_metrics_persist_across_store_restarts(temp_db: str) -> None:
    """Verify metrics are derived from SQLite tables and survive process/store restarts."""
    # Process A writes data
    store_a = CheckpointStore(db_path=temp_db)
    ev = store_a.insert_outbox_event(
        event_type="tool.approval_requested",
        payload={"op": "persist-test"},
    )
    claimed = store_a.claim_outbox_events(owner_id="worker-a", batch_size=1)[0]
    store_a.update_outbox_event_delivery(
        event_id=ev["event_id"],
        claimed_by="worker-a",
        fence_token=claimed["fence_token"],
        success=False,
        error="Network error",
        retryable=False,  # forces dead_letter
    )
    store_a.close()

    # Process B starts fresh with same database
    store_b = CheckpointStore(db_path=temp_db)
    diag = store_b.get_outbox_diagnostics()
    assert diag["total_events"] == 1
    assert diag["counts"]["dead_letter"] == 1
    assert len(diag["recent_failures"]) == 1
    assert diag["recent_failures"][0]["event_id"] == ev["event_id"]
    assert diag["recent_failures"][0]["last_error"] == "Network error"
    store_b.close()


def test_expired_claims_diagnostics(store: CheckpointStore) -> None:
    """Verify expired claim leases are accurately detected and reported in diagnostics."""
    ev = store.insert_outbox_event(
        event_type="tool.approval_requested",
        payload={"op": "claim-exp"},
        created_at="2026-10-31T08:59:00+00:00",
    )

    # Claim event with claim expiring in the past
    past_now = "2026-10-31T09:00:00+00:00"
    claimed = store.claim_outbox_events(
        owner_id="crashed-worker",
        batch_size=1,
        claim_ttl_seconds=10.0,
        now=past_now,
    )
    assert len(claimed) == 1
    assert claimed[0]["event_id"] == ev["event_id"]

    # Now inspect diagnostics
    diag = store.get_outbox_diagnostics()
    assert diag["expired_claims_count"] == 1
    assert len(diag["expired_claims"]) == 1
    exp_item = diag["expired_claims"][0]
    assert exp_item["event_id"] == ev["event_id"]
    assert exp_item["claimed_by"] == "crashed-worker"


def test_retry_limit_diagnostics_and_approaching_events(store: CheckpointStore) -> None:
    """Verify diagnostics categorize events approaching or exceeding retry attempt limits."""
    base_t1 = datetime.datetime(2026, 10, 31, 10, 0, 0, tzinfo=datetime.timezone.utc)
    # Event 1: approaching limit (4 attempts out of max 5)
    e1 = store.insert_outbox_event(
        event_type="tool.appr_limit",
        payload={"op": "lim-1"},
        created_at=base_t1.isoformat(),
    )
    for i in range(4):
        step_now = (base_t1 + datetime.timedelta(seconds=100 * (i + 1))).isoformat()
        c = store.claim_outbox_events(
            owner_id=f"w-{i}",
            batch_size=1,
            event_types=["tool.appr_limit"],
            now=step_now,
        )[0]
        store.update_outbox_event_delivery(
            event_id=e1["event_id"],
            claimed_by=f"w-{i}",
            fence_token=c["fence_token"],
            success=False,
            error=f"Failure {i + 1}",
            retryable=True,
            max_attempts=5,
            now=step_now,
        )

    # Event 2: exceeded limit (5 attempts -> dead_letter)
    base_t2 = datetime.datetime(2026, 10, 31, 12, 0, 0, tzinfo=datetime.timezone.utc)
    e2 = store.insert_outbox_event(
        event_type="tool.exc_limit",
        payload={"op": "lim-2"},
        created_at=base_t2.isoformat(),
    )
    for i in range(5):
        step_now = (base_t2 + datetime.timedelta(seconds=100 * (i + 1))).isoformat()
        c = store.claim_outbox_events(
            owner_id=f"w2-{i}",
            batch_size=1,
            event_types=["tool.exc_limit"],
            now=step_now,
        )[0]
        store.update_outbox_event_delivery(
            event_id=e2["event_id"],
            claimed_by=f"w2-{i}",
            fence_token=c["fence_token"],
            success=False,
            error=f"Failure {i + 1}",
            retryable=True,
            max_attempts=5,
            now=step_now,
        )

    diag = store.get_outbox_diagnostics(max_attempts=5)
    limit_diag = diag["retry_limit_diagnostics"]
    assert limit_diag["max_configured_attempts"] == 5
    assert limit_diag["approaching_limit_count"] >= 1
    assert limit_diag["exceeded_limit_count"] >= 1

    appr_ids = [item["event_id"] for item in limit_diag["events_approaching_limit"]]
    assert e1["event_id"] in appr_ids

    exc_ids = [item["event_id"] for item in limit_diag["events_exceeded_limit"]]
    assert e2["event_id"] in exc_ids


def test_recent_failures_sanitization(store: CheckpointStore) -> None:
    """Verify error messages with sensitive credentials or tokens are sanitized in diagnostics."""
    ev = store.insert_outbox_event(event_type="tool.approval_requested", payload={"op": "sec-1"})
    c = store.claim_outbox_events(owner_id="w-sec", batch_size=1)[0]

    sensitive_error = "Connection failed with Bearer sk-secret1234567890abcdef at https://api.internal"
    store.update_outbox_event_delivery(
        event_id=ev["event_id"],
        claimed_by="w-sec",
        fence_token=c["fence_token"],
        success=False,
        error=sensitive_error,
        retryable=False,
    )

    diag = store.get_outbox_diagnostics()
    assert len(diag["recent_failures"]) == 1
    sanitized = diag["recent_failures"][0]["last_error"]
    assert "sk-secret1234567890abcdef" not in sanitized
    assert "[REDACTED]" in sanitized or "Bearer" in sanitized


def test_manual_recovery_requires_reason(store: CheckpointStore, app_client: TestClient) -> None:
    """Verify operator manual recovery rejects empty or whitespace reasons."""
    ev = store.insert_outbox_event(event_type="tool.approval_requested", payload={"op": "reas-1"})
    c = store.claim_outbox_events(owner_id="w-r", batch_size=1)[0]
    store.update_outbox_event_delivery(
        event_id=ev["event_id"],
        claimed_by="w-r",
        fence_token=c["fence_token"],
        success=False,
        error="Simulated failure",
        retryable=False,
    )

    # 1. Direct store call with empty reason raises ValueError
    with pytest.raises(ValueError, match="A non-empty recovery reason must be provided"):
        store.recover_outbox_events(reason="   ", event_ids=[ev["event_id"]])

    # 2. HTTP POST endpoint with empty reason returns 400 Bad Request
    headers = {"Authorization": "Bearer test-secret-token"}
    resp = app_client.post(
        f"/approvals/outbox/{ev['event_id']}/retry",
        headers=headers,
        json={"reason": "   "},
    )
    assert resp.status_code == 400
    assert "Recovery reason cannot be empty" in resp.json()["detail"]


def test_manual_recovery_protects_active_claims(store: CheckpointStore, app_client: TestClient) -> None:
    """Verify recovery does not silently reset an active claim unless force=True is set."""
    ev = store.insert_outbox_event(event_type="tool.approval_requested", payload={"op": "act-1"})
    # Claim with future expiration
    store.claim_outbox_events(owner_id="active-worker", batch_size=1, claim_ttl_seconds=600.0)

    # 1. Normal retry fails because claim is active
    with pytest.raises(ValueError, match="active claim held"):
        store.retry_outbox_event(
            event_id=ev["event_id"],
            reason="Operator trying to recover active event",
            force=False,
        )

    # 2. HTTP API normal retry returns 409 Conflict
    headers = {"Authorization": "Bearer test-secret-token"}
    resp = app_client.post(
        f"/approvals/outbox/{ev['event_id']}/retry",
        headers=headers,
        json={"reason": "Normal retry", "force": False},
    )
    assert resp.status_code == 409
    assert "active claim held" in resp.json()["detail"]

    # 3. Forced recovery with force=True succeeds
    resp_force = app_client.post(
        f"/approvals/outbox/{ev['event_id']}/retry",
        headers=headers,
        json={"reason": "Forced recovery due to hung worker", "force": True},
    )
    assert resp_force.status_code == 200
    assert resp_force.json()["retried"] is True
    assert resp_force.json()["event"]["status"] == "pending"
    assert resp_force.json()["event"]["claimed_by"] is None


def test_manual_recovery_emits_audit_and_preserves_history(
    store: CheckpointStore, app_client: TestClient
) -> None:
    """Verify recovery actions record structured audit events and append to delivery history."""
    ev = store.insert_outbox_event(
        event_type="tool.approval_granted",
        payload={"op": "audit-test", "run_id": "run-audit-outbox-1"},
    )
    c = store.claim_outbox_events(owner_id="w-aud", batch_size=1)[0]
    store.update_outbox_event_delivery(
        event_id=ev["event_id"],
        claimed_by="w-aud",
        fence_token=c["fence_token"],
        success=False,
        error="Downstream outage",
        retryable=False,
    )

    # Operator recovers event via API
    headers = {"Authorization": "Bearer test-secret-token"}
    resp = app_client.post(
        f"/approvals/outbox/{ev['event_id']}/retry",
        headers=headers,
        json={"reason": "Service restored by downstream team", "operator": "ops-admin"},
    )
    assert resp.status_code == 200

    # 1. Verify audit event was logged
    audit_events = store.get_audit_events(
        run_id="run-audit-outbox-1",
        event_type=AuditEventType.OUTBOX_RETRY_REQUESTED,
    )
    assert len(audit_events) == 1
    aud = audit_events[0]
    assert aud["actor"] == "ops-admin"
    assert aud["metadata"]["event_id"] == ev["event_id"]
    assert aud["metadata"]["reason"] == "Service restored by downstream team"

    # 2. Verify history endpoint returns chronological log
    resp_hist = app_client.get(f"/approvals/outbox/{ev['event_id']}/history", headers=headers)
    assert resp_hist.status_code == 200
    hist = resp_hist.json()
    assert hist["total"] >= 3  # created, claim, dead_letter, retried
    actions = [entry["action"] for entry in hist["history"]]
    assert "created" in actions
    assert "claim" in actions
    assert "dead_letter" in actions
    assert "retried" in actions


def test_batch_operator_recovery_and_authorization(
    store: CheckpointStore, app_client: TestClient
) -> None:
    """Verify batch recovery endpoint filters, recovers dead letters, and requires authentication."""
    # Create 3 dead_letter events
    ids: list[str] = []
    for i in range(3):
        ev = store.insert_outbox_event(
            event_type="tool.approval_requested",
            payload={"op": f"batch-{i}"},
        )
        c = store.claim_outbox_events(owner_id="w-b", batch_size=1)[0]
        store.update_outbox_event_delivery(
            event_id=ev["event_id"],
            claimed_by="w-b",
            fence_token=c["fence_token"],
            success=False,
            error="Temporary fail",
            retryable=False,
        )
        ids.append(ev["event_id"])

    # 1. Unauthenticated batch recovery rejected with 401/403
    resp_unauth = app_client.post(
        "/approvals/outbox/recover",
        json={"reason": "Batch retry", "status_filter": "dead_letter"},
    )
    assert resp_unauth.status_code in (401, 403)

    # 2. Authenticated batch recovery succeeds
    headers = {"Authorization": "Bearer test-secret-token"}
    resp_auth = app_client.post(
        "/approvals/outbox/recover",
        headers=headers,
        json={
            "reason": "Operator bulk recovery after connectivity fix",
            "status_filter": "dead_letter",
            "max_events": 10,
        },
    )
    assert resp_auth.status_code == 200
    res_data = resp_auth.json()
    assert res_data["recovered_count"] == 3
    assert res_data["skipped_count"] == 0
    assert len(res_data["recovered_events"]) == 3

    # All events now pending
    pending = store.get_outbox_events(status="pending")
    assert len(pending) == 3


def test_sdk_methods_for_observability_and_recovery(app_client: TestClient) -> None:
    """Verify OrchestratorApiClient methods for diagnostics, history, and recovery."""
    client = OrchestratorApiClient(base_url=str(app_client.base_url), api_token="test-secret-token")
    client._client = app_client

    # 1. Trigger an approval to generate an outbox event
    headers = {"Authorization": "Bearer test-secret-token"}
    app_client.post(
        "/runs/run-sdk-obs/tools/op-sdk-obs/approve",
        headers=headers,
        json={"tool_name": "bash", "approver": "alice", "decision": "approved"},
    )

    # 2. SDK get_outbox_diagnostics
    diag = client.get_outbox_diagnostics()
    assert diag.enabled is True
    assert diag.total_events >= 1
    assert "pending" in diag.counts

    # 3. SDK list_outbox_events
    events_resp = client.list_outbox_events()
    assert events_resp.total >= 1
    ev_id = events_resp.items[0].event_id

    # 4. SDK get_outbox_event_history
    hist_resp = client.get_outbox_event_history(event_id=ev_id)
    assert hist_resp.total >= 1
    assert hist_resp.history[0].event_id == ev_id

    # 5. SDK retry_outbox_event
    retry_resp = client.retry_outbox_event(
        event_id=ev_id,
        reason="Manual SDK verification",
        operator="alice",
        force=True,
    )
    assert retry_resp.retried is True
    assert retry_resp.event.status == "pending"

    # 6. SDK recover_outbox_events
    rec_resp = client.recover_outbox_events(
        reason="SDK batch test",
        event_ids=[ev_id],
    )
    assert rec_resp.recovered_count == 1
