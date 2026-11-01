"""Deterministic test suite for Transactional Outbox (Task 50)."""

from __future__ import annotations

import datetime
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.routes import get_settings, get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings
from orchestrator.notifications.dispatcher import OutboxDispatcher
from orchestrator.notifications.handler import TestNotificationHandler
from orchestrator.persistence.store import (
    CheckpointStore,
    StaleDispatcherError,
)


@pytest.fixture
def temp_db(tmp_path: Any) -> str:
    """Return path to a fresh SQLite database file for testing."""
    return str(tmp_path / "test_outbox.db")


@pytest.fixture
def store(temp_db: str) -> CheckpointStore:
    """Return a CheckpointStore instance bound to temp_db."""
    return CheckpointStore(db_path=temp_db)


@pytest.fixture
def app_client(temp_db: str, store: CheckpointStore, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """Return a TestClient bound to a configured test app."""
    settings = Settings(
        checkpoint_db_path=temp_db,
        api_token="test-secret-token",
        outbox_enabled=True,
        outbox_dispatcher_enabled=False,
    )
    monkeypatch.setattr("orchestrator.config.get_settings", lambda: settings)
    monkeypatch.setattr("orchestrator.api.routes.get_settings", lambda: settings)
    monkeypatch.setattr("orchestrator.api.server.get_settings", lambda: settings)
    monkeypatch.setattr("orchestrator.notifications.dispatcher.get_settings", lambda: settings)
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_store] = lambda: store
    return TestClient(app)


def test_outbox_atomic_insert_with_approval(store: CheckpointStore) -> None:
    """Verify that approval request, approval decision, and outbox records commit atomically."""
    # 1. Record pending approval request
    req_res = store.record_tool_approval_request(
        run_id="run-outbox-1",
        operation_key="op-outbox-1",
        tool_name="bash",
        reason="Needs human approval",
    )
    assert req_res["decision"] == "pending"

    # Verify tool.approval_requested outbox record exists
    outbox_req = store.get_outbox_events(event_type="tool.approval_requested")
    assert len(outbox_req) == 1
    assert outbox_req[0]["status"] == "pending"
    assert outbox_req[0]["attempt_count"] == 0
    assert outbox_req[0]["payload"]["operation_key"] == "op-outbox-1"
    assert outbox_req[0]["payload"]["tool_name"] == "bash"

    # 2. Record approval decision
    dec_res = store.record_tool_approval(
        run_id="run-outbox-1",
        operation_key="op-outbox-1",
        tool_name="bash",
        approver="alice",
        decision="approved",
        reason="Approved by operator",
    )
    assert dec_res["decision"] == "approved"

    # Verify tool.approval_granted outbox record exists
    outbox_dec = store.get_outbox_events(event_type="tool.approval_granted")
    assert len(outbox_dec) == 1
    assert outbox_dec[0]["status"] == "pending"
    assert outbox_dec[0]["payload"]["approver"] == "alice"
    assert outbox_dec[0]["payload"]["decision"] == "approved"


def test_outbox_rollback_on_outbox_failure(store: CheckpointStore) -> None:
    """Verify approval state transition rolls back completely if outbox write fails."""
    store.record_tool_approval_request(
        run_id="run-outbox-2",
        operation_key="op-outbox-2",
        tool_name="bash",
    )

    # Monkeypatch insert_outbox_event to fail
    with patch.object(store, "insert_outbox_event", side_effect=RuntimeError("Disk full for outbox")):
        with pytest.raises(RuntimeError, match="Disk full for outbox"):
            store.record_tool_approval(
                run_id="run-outbox-2",
                operation_key="op-outbox-2",
                tool_name="bash",
                approver="bob",
                decision="approved",
            )

    # Verify approval state reverted to pending
    appr = store.get_tool_approval("op-outbox-2")
    assert appr is not None
    assert appr["decision"] == "pending"

    # Verify no tool.approval_granted outbox record was committed
    granted = store.get_outbox_events(event_type="tool.approval_granted")
    assert len(granted) == 0


def test_batch_expiration_savepoint_outbox_rollback(store: CheckpointStore) -> None:
    """Verify single-row outbox failure in a batch scan rolls back only that row."""
    now = datetime.datetime.now(datetime.timezone.utc)
    past = (now - datetime.timedelta(hours=2)).isoformat()

    for idx in range(1, 4):
        store.record_tool_approval_request(
            run_id=f"run-batch-{idx}",
            operation_key=f"op-batch-{idx}",
            tool_name="bash",
            expires_at=past,
        )

    orig_insert = store.insert_outbox_event

    def conditional_insert(*args: Any, **kwargs: Any) -> Any:
        payload = kwargs.get("payload") or (args[1] if len(args) > 1 else {})
        if payload.get("operation_key") == "op-batch-2":
            raise RuntimeError("Simulated outbox failure on row 2")
        return orig_insert(*args, **kwargs)

    with patch.object(store, "insert_outbox_event", side_effect=conditional_insert):
        res = store.check_and_expire_tool_approvals()

    assert res["expired_count"] == 2
    assert "op-batch-1" in res["expired_operation_keys"]
    assert "op-batch-3" in res["expired_operation_keys"]
    assert res["failed_count"] == 1
    assert "op-batch-2" in res["failed_operation_keys"]

    # Verify database state
    appr1 = store.get_tool_approval("op-batch-1")
    assert appr1 is not None and appr1["decision"] == "expired"
    appr2 = store.get_tool_approval("op-batch-2")
    assert appr2 is not None and appr2["decision"] == "pending"
    appr3 = store.get_tool_approval("op-batch-3")
    assert appr3 is not None and appr3["decision"] == "expired"


def test_dispatcher_successful_delivery(store: CheckpointStore) -> None:
    """Verify dispatcher claims pending events, delivers them to handler, and marks delivered."""
    store.record_tool_approval_request(
        run_id="run-dispatch-1",
        operation_key="op-dispatch-1",
        tool_name="bash",
    )

    handler = TestNotificationHandler()
    dispatcher = OutboxDispatcher(
        store=store,
        handler=handler,
        owner_id="test-worker-1",
        batch_size=10,
    )

    cycle_res = dispatcher.run_dispatch_cycle()
    assert cycle_res["claimed_count"] == 1
    assert cycle_res["delivered_count"] == 1
    assert cycle_res["failed_count"] == 0

    assert len(handler.delivered_events) == 1
    assert handler.delivered_events[0]["payload"]["operation_key"] == "op-dispatch-1"

    # Verify database status is delivered
    events = store.get_outbox_events(status="delivered")
    assert len(events) == 1
    assert events[0]["delivered_at"] is not None
    assert events[0]["claimed_by"] is None


def test_dispatcher_retry_with_bounded_backoff(store: CheckpointStore) -> None:
    """Verify transient handler failures trigger bounded exponential backoff retries."""
    store.record_tool_approval_request(
        run_id="run-retry-1",
        operation_key="op-retry-1",
        tool_name="bash",
    )

    # Fail the first 2 attempts, then succeed
    handler = TestNotificationHandler(fail_events={"tool.approval_requested": 2})
    dispatcher = OutboxDispatcher(
        store=store,
        handler=handler,
        owner_id="test-worker-2",
        backoff_base_seconds=5.0,
        backoff_max_seconds=60.0,
    )

    # Cycle 1: Fails attempt 1
    c1 = dispatcher.run_dispatch_cycle()
    assert c1["claimed_count"] == 1
    assert c1["failed_count"] == 1
    assert c1["delivered_count"] == 0

    events = store.get_outbox_events(status="pending")
    assert len(events) == 1
    assert events[0]["attempt_count"] == 1
    assert events[0]["last_error"] is not None
    retry_time_1 = events[0]["next_retry_at"]

    # Simulating clock before retry_time: no events should be claimed
    c1_early = dispatcher.run_dispatch_cycle()
    assert c1_early["claimed_count"] == 0

    # Advance clock past retry_time_1: Cycle 2 fails attempt 2
    future_time_1 = (
        datetime.datetime.fromisoformat(retry_time_1) + datetime.timedelta(seconds=1)
    ).isoformat()
    claimed_2 = store.claim_outbox_events(
        owner_id="test-worker-2",
        now=future_time_1,
    )
    assert len(claimed_2) == 1

    # Delivery fails second time
    store.update_outbox_event_delivery(
        event_id=claimed_2[0]["event_id"],
        claimed_by="test-worker-2",
        fence_token=claimed_2[0]["fence_token"],
        success=False,
        error="Transient error attempt 2",
        now=future_time_1,
        backoff_base=5.0,
        backoff_max=60.0,
    )

    ev_after_2 = store.get_outbox_events(status="pending")[0]
    assert ev_after_2["attempt_count"] == 2
    retry_time_2 = ev_after_2["next_retry_at"]

    # Advance clock past retry_time_2: Cycle 3 succeeds
    future_time_2 = (
        datetime.datetime.fromisoformat(retry_time_2) + datetime.timedelta(seconds=1)
    ).isoformat()
    claimed_3 = store.claim_outbox_events(
        owner_id="test-worker-2",
        now=future_time_2,
    )
    assert len(claimed_3) == 1

    store.update_outbox_event_delivery(
        event_id=claimed_3[0]["event_id"],
        claimed_by="test-worker-2",
        fence_token=claimed_3[0]["fence_token"],
        success=True,
        now=future_time_2,
    )

    ev_final = store.get_outbox_events(status="delivered")[0]
    assert ev_final["status"] == "delivered"
    assert ev_final["attempt_count"] == 3


def test_dispatcher_dead_letter_after_max_attempts(store: CheckpointStore) -> None:
    """Verify exceeding max_attempts permanently transitions event to dead_letter."""
    store.record_tool_approval_request(
        run_id="run-dead-1",
        operation_key="op-dead-1",
        tool_name="bash",
    )

    handler = TestNotificationHandler(fail_all=True)
    dispatcher = OutboxDispatcher(
        store=store,
        handler=handler,
        owner_id="dead-letter-worker",
        max_attempts=3,
        backoff_base_seconds=0.1,
    )

    for i in range(1, 4):
        now_str = (
            datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=i * 10)
        ).isoformat()
        dispatcher._clock = lambda s=now_str: datetime.datetime.fromisoformat(s)
        dispatcher.run_dispatch_cycle()

    # Event should now be in dead_letter state
    dead = store.get_outbox_events(status="dead_letter")
    assert len(dead) == 1
    assert dead[0]["attempt_count"] == 3
    assert "Simulated global notification delivery failure" in (dead[0]["last_error"] or "")


def test_dispatcher_non_retryable_dead_letter(store: CheckpointStore) -> None:
    """Verify non-retryable failure transitions immediately to dead_letter on attempt 1."""
    store.record_tool_approval_request(
        run_id="run-nonretry-1",
        operation_key="op-nonretry-1",
        tool_name="bash",
    )

    events = store.get_outbox_events(status="pending")
    ev_id = events[0]["event_id"]

    handler = TestNotificationHandler(non_retryable_event_ids={ev_id})
    dispatcher = OutboxDispatcher(
        store=store,
        handler=handler,
        owner_id="non-retry-worker",
        max_attempts=5,
    )

    res = dispatcher.run_dispatch_cycle()
    assert res["dead_letter_count"] == 1

    dead = store.get_outbox_events(status="dead_letter")
    assert len(dead) == 1
    assert dead[0]["attempt_count"] == 1


def test_dispatcher_stale_owner_fencing(store: CheckpointStore) -> None:
    """Verify stale dispatcher cannot update delivery state after losing ownership."""
    store.record_tool_approval_request(
        run_id="run-fence-1",
        operation_key="op-fence-1",
        tool_name="bash",
    )

    now = datetime.datetime.now(datetime.timezone.utc)
    # 1. Worker A claims the event with TTL of 5 seconds
    claimed_a = store.claim_outbox_events(
        owner_id="worker-A",
        claim_ttl_seconds=5.0,
        now=now.isoformat(),
    )
    assert len(claimed_a) == 1
    fence_a = claimed_a[0]["fence_token"]

    # 2. 10 seconds elapse: claim expires, Worker B takes over the event
    expired_time = (now + datetime.timedelta(seconds=10)).isoformat()
    claimed_b = store.claim_outbox_events(
        owner_id="worker-B",
        claim_ttl_seconds=30.0,
        now=expired_time,
    )
    assert len(claimed_b) == 1
    fence_b = claimed_b[0]["fence_token"]
    assert fence_b > fence_a

    # 3. Worker A (stale) wakes up and attempts to report delivery success
    with pytest.raises(StaleDispatcherError, match="claim is stale or superseded"):
        store.update_outbox_event_delivery(
            event_id=claimed_a[0]["event_id"],
            claimed_by="worker-A",
            fence_token=fence_a,
            success=True,
        )

    # 4. Worker B successfully reports delivery
    res_b = store.update_outbox_event_delivery(
        event_id=claimed_b[0]["event_id"],
        claimed_by="worker-B",
        fence_token=fence_b,
        success=True,
    )
    assert res_b["status"] == "delivered"


def test_concurrent_dispatchers_claim_disjoint_events(store: CheckpointStore) -> None:
    """Verify two dispatchers claim disjoint subsets of pending events without collision."""
    for idx in range(10):
        store.record_tool_approval_request(
            run_id=f"run-conc-{idx}",
            operation_key=f"op-conc-{idx}",
            tool_name="bash",
        )

    claimed_1 = store.claim_outbox_events(owner_id="worker-1", batch_size=4)
    claimed_2 = store.claim_outbox_events(owner_id="worker-2", batch_size=4)

    assert len(claimed_1) == 4
    assert len(claimed_2) == 4

    ids_1 = {ev["event_id"] for ev in claimed_1}
    ids_2 = {ev["event_id"] for ev in claimed_2}

    # Set intersection must be empty
    assert len(ids_1.intersection(ids_2)) == 0


def test_handler_idempotency_deduplication(store: CheckpointStore) -> None:
    """Verify notification handlers can deduplicate redelivered events using idempotency_key."""
    store.record_tool_approval_request(
        run_id="run-dedup-1",
        operation_key="op-dedup-1",
        tool_name="bash",
    )

    handler = TestNotificationHandler()
    dispatcher = OutboxDispatcher(store=store, handler=handler, owner_id="dedup-worker")

    # Run dispatch cycle 1
    dispatcher.run_dispatch_cycle()
    assert len(handler.delivered_events) == 1

    # Manually reset event back to pending simulating uncommitted recovery crash
    ev = store.get_outbox_events()[0]
    store.retry_outbox_event(ev["event_id"])

    # Run dispatch cycle 2
    dispatcher.run_dispatch_cycle()

    # Verify idempotency key was seen twice, allowing receiver to deduplicate
    idemp_key = ev["idempotency_key"]
    assert idemp_key is not None
    assert handler.seen_idempotency_keys[idemp_key] == 2


def test_manual_retry_of_dead_letter(store: CheckpointStore) -> None:
    """Verify calling retry_outbox_event resets a dead_letter event back to pending."""
    store.record_tool_approval_request(
        run_id="run-retry-dl-1",
        operation_key="op-retry-dl-1",
        tool_name="bash",
    )

    # Manually put into dead_letter state
    claimed = store.claim_outbox_events(owner_id="test", batch_size=1)[0]
    store.update_outbox_event_delivery(
        event_id=claimed["event_id"],
        claimed_by="test",
        fence_token=claimed["fence_token"],
        success=False,
        retryable=False,
    )

    dead = store.get_outbox_events(status="dead_letter")
    assert len(dead) == 1

    # Manually retry
    res = store.retry_outbox_event(dead[0]["event_id"], reset_attempts=True)
    assert res["status"] == "pending"
    assert res["attempt_count"] == 0
    assert res["last_error"] is None

    pending = store.get_outbox_events(status="pending")
    assert len(pending) == 1


def test_outbox_api_routes(app_client: TestClient) -> None:
    """Verify HTTP API endpoints for listing outbox events, diagnostics, retry, and dispatch."""
    headers = {"Authorization": "Bearer test-secret-token"}

    # 1. Trigger an approval request via API
    resp_appr = app_client.post(
        "/runs/test-api-run/tools/op-api-outbox-1/approve",
        json={
            "tool_name": "bash",
            "approver": "alice",
            "decision": "approved",
            "reason": "Approved via test",
        },
        headers=headers,
    )
    assert resp_appr.status_code == 200

    # 2. GET /approvals/outbox
    resp_list = app_client.get("/approvals/outbox", headers=headers)
    assert resp_list.status_code == 200
    data = resp_list.json()
    assert data["total"] >= 1
    assert data["items"][0]["event_type"] == "tool.approval_granted"

    # 3. GET /approvals/outbox/diagnostics
    resp_diag = app_client.get("/approvals/outbox/diagnostics", headers=headers)
    assert resp_diag.status_code == 200
    diag_data = resp_diag.json()
    assert diag_data["enabled"] is True
    assert diag_data["counts"]["pending"] >= 1

    # 4. POST /approvals/outbox/dispatch
    resp_disp = app_client.post("/approvals/outbox/dispatch", headers=headers)
    assert resp_disp.status_code == 200
    disp_data = resp_disp.json()
    assert disp_data["claimed_count"] >= 1
    assert disp_data["delivered_count"] >= 1

    # 5. POST /approvals/outbox/{event_id}/retry
    event_id = data["items"][0]["event_id"]
    resp_retry = app_client.post(f"/approvals/outbox/{event_id}/retry", headers=headers)
    assert resp_retry.status_code == 200
    assert resp_retry.json()["retried"] is True
    assert resp_retry.json()["event"]["status"] == "pending"
