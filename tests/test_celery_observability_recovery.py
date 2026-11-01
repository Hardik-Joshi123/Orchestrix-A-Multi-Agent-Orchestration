"""Tests for Celery worker observability, queue metrics, and operational recovery (Task 57)."""

from __future__ import annotations

import datetime
import json
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.client import OrchestratorClient
from orchestrator.api.manager import RunManager
from orchestrator.api.models import (
    CeleryQueueMetricsResponse,
    CeleryWorkersResponse,
    RecoverWorkflowsResponse,
    StuckWorkflowsResponse,
)
from orchestrator.api.server import create_app
from orchestrator.celery_app import (
    CeleryWorkerHeartbeatTracker,
)
from orchestrator.config import (
    Settings,
    get_startup_diagnostics,
    reset_settings,
    validate_configuration,
)
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import CheckpointStore, RecoveryError


@pytest.fixture
def temp_db(tmp_path: Any) -> str:
    """Create a temporary SQLite database path."""
    return str(tmp_path / "test_celery_obs.db")


@pytest.fixture(autouse=True)
def configure_test_environment(temp_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure environment for isolated Celery execution and testing."""
    monkeypatch.setenv("CHECKPOINT_DB_PATH", temp_db)
    monkeypatch.setenv("CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-secret-token")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_WORKER_HEARTBEAT_INTERVAL_SECONDS", "15.0")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_WORKER_STALE_THRESHOLD_SECONDS", "60.0")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_QUEUE_MONITORING_INTERVAL_SECONDS", "30.0")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_WORKFLOW_STUCK_THRESHOLD_SECONDS", "300.0")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_RECOVERY_BATCH_SIZE", "50")
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def store(temp_db: str) -> CheckpointStore:
    """Initialized CheckpointStore with temporary database."""
    return CheckpointStore(db_path=temp_db)


@pytest.fixture
def test_app(store: CheckpointStore) -> Any:
    """FastAPI test client with injected dependencies."""
    app = create_app()
    from orchestrator.api.routes import get_manager, get_store

    manager = RunManager(
        max_active_runs=10,
        store=store,
        owner_id="test-api-node",
    )
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    return app


@pytest.fixture
def client(test_app: Any) -> TestClient:
    """FastAPI TestClient fixture."""
    return TestClient(test_app)


def test_record_and_get_worker_heartbeat(store: CheckpointStore) -> None:
    """Worker heartbeats are accurately recorded, updated, and queried."""
    rec = store.record_worker_heartbeat(
        worker_id="worker-node-1",
        status="active",
        active_tasks=2,
        queues="orchestrator_workflows",
        metadata={"hostname": "worker-host", "pid": 1234},
    )
    assert rec["worker_id"] == "worker-node-1"
    assert rec["status"] == "active"
    assert rec["active_tasks"] == 2

    # Query worker
    w = store.get_worker_heartbeat("worker-node-1", stale_threshold_seconds=60.0)
    assert w is not None
    assert w["worker_id"] == "worker-node-1"
    assert w["status"] == "active"
    assert w["is_stale"] is False
    assert w["active_tasks"] == 2
    assert w["queues"] == "orchestrator_workflows"
    assert w["metadata"]["hostname"] == "worker-host"

    # Update on completion
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    store.record_worker_heartbeat(
        worker_id="worker-node-1",
        status="active",
        active_tasks=1,
        last_completed_at=now_iso,
    )
    w_updated = store.get_worker_heartbeat("worker-node-1")
    assert w_updated is not None
    assert w_updated["active_tasks"] == 1
    assert w_updated["last_completed_at"] == now_iso
    # Previous metadata preserved
    assert w_updated["metadata"]["hostname"] == "worker-host"


def test_stale_worker_detection(store: CheckpointStore) -> None:
    """Workers missing heartbeats beyond the configured threshold are marked stale."""
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now_dt - datetime.timedelta(seconds=90)).isoformat()

    store.record_worker_heartbeat(
        worker_id="worker-stale-node",
        status="active",
        now_iso=old_time,
    )

    # 1. Stale with threshold 60.0
    w = store.get_worker_heartbeat(
        "worker-stale-node", stale_threshold_seconds=60.0, now_dt=now_dt
    )
    assert w is not None
    assert w["is_stale"] is True
    assert w["status"] == "stale"
    assert w["heartbeat_age_seconds"] >= 89.0

    # 2. Not stale with threshold 120.0
    w_fresh = store.get_worker_heartbeat(
        "worker-stale-node", stale_threshold_seconds=120.0, now_dt=now_dt
    )
    assert w_fresh is not None
    assert w_fresh["is_stale"] is False
    assert w_fresh["status"] == "active"


def test_worker_heartbeat_tracker_lifecycle(store: CheckpointStore) -> None:
    """Tracker manages task start, success, failure, and graceful shutdown."""
    tracker = CeleryWorkerHeartbeatTracker(
        worker_id="tracker-test-worker",
        store=store,
    )

    # Task start increments active tasks
    tracker.on_task_start()
    w1 = store.get_worker_heartbeat("tracker-test-worker")
    assert w1 is not None
    assert w1["active_tasks"] == 1

    # Task success decrements active tasks and records completion
    tracker.on_task_success()
    w2 = store.get_worker_heartbeat("tracker-test-worker")
    assert w2 is not None
    assert w2["active_tasks"] == 0
    assert w2["last_completed_at"] is not None

    # Task failure sanitizes secrets from error text
    tracker.on_task_start()
    tracker.on_task_failure("Error connecting with api_key=super-secret-123")
    w3 = store.get_worker_heartbeat("tracker-test-worker")
    assert w3 is not None
    assert w3["active_tasks"] == 0
    assert w3["last_failure_at"] is not None
    assert "super-secret-123" not in str(w3["last_failure_reason"])
    assert "[REDACTED]" in str(w3["last_failure_reason"])

    # Graceful stop marks offline
    tracker.stop()
    w4 = store.get_worker_heartbeat("tracker-test-worker")
    assert w4 is not None
    assert w4["status"] == "offline"


def test_prune_worker_heartbeats(store: CheckpointStore) -> None:
    """Pruning deletes offline workers older than retention days while preserving recent."""
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now_dt - datetime.timedelta(days=10)).isoformat()
    recent_time = (now_dt - datetime.timedelta(days=2)).isoformat()

    store.record_worker_heartbeat(
        worker_id="worker-old-offline",
        status="offline",
        now_iso=old_time,
    )
    store.record_worker_heartbeat(
        worker_id="worker-recent-offline",
        status="offline",
        now_iso=recent_time,
    )
    store.record_worker_heartbeat(
        worker_id="worker-active-preserved",
        status="active",
        now_iso=old_time,
    )

    deleted = store.prune_worker_heartbeats(offline_retention_days=7)
    assert deleted == 1

    assert store.get_worker_heartbeat("worker-old-offline") is None
    assert store.get_worker_heartbeat("worker-recent-offline") is not None
    # Active worker is preserved even if old
    assert store.get_worker_heartbeat("worker-active-preserved") is not None


def test_celery_queue_metrics_and_sanitization(store: CheckpointStore) -> None:
    """Queue metrics derived from durable state with sanitized recent failure summaries."""
    # Register workers
    store.record_worker_heartbeat(worker_id="worker-1", status="active")
    store.record_worker_heartbeat(worker_id="worker-2", status="offline")

    # Insert runs with various statuses
    store.save_checkpoint(
        run_id="run-queued-1",
        state={"task": "Queued task", "status": "queued"},
    )
    store.save_checkpoint(
        run_id="run-running-1",
        state={"task": "Running task", "status": "running"},
    )
    store.save_checkpoint(
        run_id="run-completed-1",
        state={"task": "Completed task", "status": "completed"},
    )
    # Failed run with sensitive token
    store.save_checkpoint(
        run_id="run-failed-1",
        state={
            "task": "Failed task with token",
            "status": "failed",
            "error": "Failed request with Authorization: Bearer secret-auth-token-xyz",
        },
    )

    metrics = store.get_celery_queue_metrics()
    assert metrics["queued_count"] == 1
    assert metrics["running_count"] == 1
    assert metrics["completed_count"] == 1
    assert metrics["failed_count"] == 1
    assert metrics["active_worker_count"] == 1
    assert metrics["offline_worker_count"] == 1
    assert metrics["total_worker_count"] == 2

    # Verify sanitized failure output
    failures = metrics["recent_failures"]
    assert len(failures) == 1
    assert failures[0]["run_id"] == "run-failed-1"
    assert "secret-auth-token-xyz" not in failures[0]["error"]
    assert "[REDACTED]" in failures[0]["error"]


def test_stuck_workflow_detection_categories(store: CheckpointStore) -> None:
    """Stuck workflows properly categorized into queued, orphaned, expired, or deadline exceeded."""
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    old_time = (now_dt - datetime.timedelta(seconds=600)).isoformat()
    future_time = (now_dt + datetime.timedelta(seconds=300)).isoformat()
    past_time = (now_dt - datetime.timedelta(seconds=30)).isoformat()

    # 1. Stuck queued
    store.save_checkpoint(
        run_id="run-stuck-queued",
        state={"task": "Queued long ago", "status": "queued"},
    )
    # Manually backdate created_at in database
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    "UPDATE runs SET created_at = ? WHERE run_id = ?;",
                    (old_time, "run-stuck-queued"),
                )
        finally:
            conn.close()

    # 2. Orphaned running (no admission lease)
    store.save_checkpoint(
        run_id="run-orphaned-running",
        state={"task": "Running without lease", "status": "running"},
    )

    # 3. Running with expired admission lease and dead worker
    store.save_checkpoint(
        run_id="run-expired-lease",
        state={"task": "Running with expired lease", "status": "running"},
    )
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO workflow_admissions (
                        run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    )
                    VALUES (?, 'dead-worker-1', 'active', ?, ?, ?, 1);
                    """,
                    ("run-expired-lease", old_time, old_time, past_time),
                )
        finally:
            conn.close()

    # 4. Running with ACTIVE unexpired admission lease (NOT recovery eligible!)
    store.save_checkpoint(
        run_id="run-active-lease",
        state={"task": "Running with valid active lease", "status": "running"},
    )
    store.record_worker_heartbeat(worker_id="live-worker-1", status="active")
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO workflow_admissions (
                        run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    )
                    VALUES (?, 'live-worker-1', 'active', ?, ?, ?, 1);
                    """,
                    ("run-active-lease", old_time, old_time, future_time),
                )
                conn.execute(
                    "UPDATE runs SET created_at = ? WHERE run_id = ?;",
                    (old_time, "run-active-lease"),
                )
        finally:
            conn.close()

    # Detect stuck workflows
    stuck = store.get_stuck_workflows(stuck_threshold_seconds=120.0, now_dt=now_dt)
    stuck_by_id = {s["run_id"]: s for s in stuck}

    # Verify stuck queued
    assert "run-stuck-queued" in stuck_by_id
    assert stuck_by_id["run-stuck-queued"]["stuck_reason"] == "stuck_queued"
    assert stuck_by_id["run-stuck-queued"]["recovery_eligible"] is True

    # Verify orphaned running
    assert "run-orphaned-running" in stuck_by_id
    assert stuck_by_id["run-orphaned-running"]["stuck_reason"] == "orphaned_lease"
    assert stuck_by_id["run-orphaned-running"]["recovery_eligible"] is True

    # Verify expired lease with dead worker
    assert "run-expired-lease" in stuck_by_id
    assert stuck_by_id["run-expired-lease"]["stuck_reason"] in (
        "expired_lease_dead_worker",
        "lease_expired",
    )
    assert stuck_by_id["run-expired-lease"]["recovery_eligible"] is True

    # Verify active lease: flagged as deadline_exceeded but NOT recovery_eligible
    assert "run-active-lease" in stuck_by_id
    assert stuck_by_id["run-active-lease"]["stuck_reason"] == "deadline_exceeded"
    assert stuck_by_id["run-active-lease"]["recovery_eligible"] is False


def test_recover_stuck_workflow_requeue_and_fence(store: CheckpointStore) -> None:
    """Recovery safely resets state to queued, bumps fence token, and records audit event."""
    store.save_checkpoint(
        run_id="run-recover-01",
        state={"task": "Recoverable job", "status": "running"},
    )

    rec = store.recover_stuck_workflow(
        run_id="run-recover-01",
        operator="ops-alice",
        reason="Recovering orphaned task after host reboot",
        action="requeue",
    )
    assert rec["run_id"] == "run-recover-01"
    assert rec["previous_status"] == "running"
    assert rec["new_status"] == "queued"
    assert rec["action"] == "requeue"
    assert rec["fence_token"] >= 1
    assert rec["recovered_by"] == "ops-alice"

    # Verify state in runs table
    summary = store.get_run_summary("run-recover-01")
    assert summary is not None
    assert summary["status"] == "queued"
    assert summary["fence_token"] == rec["fence_token"]

    # Verify audit event
    audits = store.get_audit_events(
        run_id="run-recover-01",
        event_type=AuditEventType.WORKFLOW_RECOVERED,
    )
    assert len(audits) >= 1
    audit = audits[0]
    assert audit["actor"] == "ops-alice"
    assert audit["outcome"] == "recovered"
    assert "reboot" in json.dumps(audit["metadata"])


def test_recover_stuck_workflow_safety_checks(store: CheckpointStore) -> None:
    """Recovery strictly enforces safety constraints: reason length, terminal, lease, and HITL."""
    # 1. Short reason rejected
    store.save_checkpoint(run_id="run-safe-01", state={"status": "running"})
    with pytest.raises(RecoveryError, match="at least 5 characters"):
        store.recover_stuck_workflow(
            run_id="run-safe-01",
            operator="ops",
            reason="bad",
        )

    # 2. Terminal runs rejected
    store.save_checkpoint(run_id="run-terminal-01", state={"status": "completed"})
    with pytest.raises(RecoveryError, match="terminal status"):
        store.recover_stuck_workflow(
            run_id="run-terminal-01",
            operator="ops",
            reason="valid operator reason",
        )

    # 3. Active unexpired lease rejected (cannot overwrite valid active ownership)
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    future_time = (now_dt + datetime.timedelta(seconds=120)).isoformat()
    store.save_checkpoint(run_id="run-leased-01", state={"status": "running"})
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO workflow_admissions (
                        run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    )
                    VALUES (?, 'live-worker-x', 'active', ?, ?, ?, 1);
                    """,
                    ("run-leased-01", now_dt.isoformat(), now_dt.isoformat(), future_time),
                )
        finally:
            conn.close()

    with pytest.raises(RecoveryError, match="active unexpired"):
        store.recover_stuck_workflow(
            run_id="run-leased-01",
            operator="ops",
            reason="attempting to steal lease",
        )

    # 4. HITL waiting_approval rejected for requeue action (must never auto-approve)
    store.save_checkpoint(
        run_id="run-hitl-01",
        state={"status": "waiting_approval"},
    )
    with pytest.raises(RecoveryError, match="waiting for human approval"):
        store.recover_stuck_workflow(
            run_id="run-hitl-01",
            operator="ops",
            reason="requeue approval waiting task",
            action="requeue",
        )


def test_api_celery_workers_and_queue_endpoints(
    client: TestClient, store: CheckpointStore
) -> None:
    """HTTP endpoints /celery/workers and /celery/queue return valid schemas and enforce auth."""
    headers = {"Authorization": "Bearer test-secret-token"}

    # Unauthorized access rejected
    r_unauth = client.get("/celery/workers")
    assert r_unauth.status_code in (401, 403)

    # Register worker
    store.record_worker_heartbeat(
        worker_id="api-test-worker",
        status="active",
        active_tasks=3,
    )

    # GET /celery/workers
    r_workers = client.get("/celery/workers", headers=headers)
    assert r_workers.status_code == 200
    data_workers = r_workers.json()
    assert data_workers["total_workers"] >= 1
    assert data_workers["active_workers"] >= 1
    worker_item = next(w for w in data_workers["workers"] if w["worker_id"] == "api-test-worker")
    assert worker_item["active_tasks"] == 3

    # GET /celery/queue
    r_queue = client.get("/celery/queue", headers=headers)
    assert r_queue.status_code == 200
    data_queue = r_queue.json()
    assert "queued_count" in data_queue
    assert "active_worker_count" in data_queue
    assert data_queue["active_worker_count"] >= 1


def test_api_celery_stuck_and_recover_endpoints(
    client: TestClient, store: CheckpointStore
) -> None:
    """HTTP endpoints /celery/stuck and /celery/recover diagnose and safely recover runs."""
    headers = {"Authorization": "Bearer test-secret-token"}

    store.save_checkpoint(
        run_id="api-stuck-run-01",
        state={"task": "Orphaned background run", "status": "running"},
    )

    # GET /celery/stuck
    r_stuck = client.get("/celery/stuck", headers=headers)
    assert r_stuck.status_code == 200
    data_stuck = r_stuck.json()
    assert data_stuck["total_stuck"] >= 1
    stuck_item = next(s for s in data_stuck["workflows"] if s["run_id"] == "api-stuck-run-01")
    assert stuck_item["recovery_eligible"] is True

    # POST /celery/recover with invalid short reason rejected
    r_bad = client.post(
        "/celery/recover",
        headers=headers,
        json={"run_id": "api-stuck-run-01", "reason": "bad"},
    )
    assert r_bad.status_code in (400, 422)

    # POST /celery/recover with valid request
    r_rec = client.post(
        "/celery/recover",
        headers=headers,
        json={
            "run_id": "api-stuck-run-01",
            "action": "requeue",
            "reason": "Operator manual cluster recovery",
        },
    )
    assert r_rec.status_code == 200
    data_rec = r_rec.json()
    assert data_rec["success"] is True
    assert data_rec["recovered_count"] == 1
    assert data_rec["results"][0]["new_status"] == "queued"

    # Verify run updated in store
    summary = store.get_run_summary("api-stuck-run-01")
    assert summary is not None
    assert summary["status"] in ("queued", "completed")


def test_sdk_celery_observability_methods(store: CheckpointStore) -> None:
    """Python SDK client provides convenient typed access to Celery observability APIs."""
    store.record_worker_heartbeat(worker_id="sdk-worker-1", status="active")
    store.save_checkpoint(run_id="sdk-stuck-run", state={"status": "running"})

    # Patch _request on OrchestratorApiClient to simulate SDK calls
    client = OrchestratorClient(base_url="http://testserver", token="test-secret-token")

    with patch.object(client, "_request") as mock_req:
        # Mock /celery/workers
        mock_resp_workers = MagicMock()
        mock_resp_workers.json.return_value = {
            "total_workers": 1,
            "active_workers": 1,
            "stale_workers": 0,
            "offline_workers": 0,
            "stale_threshold_seconds": 60.0,
            "workers": [
                {
                    "worker_id": "sdk-worker-1",
                    "status": "active",
                    "registered_at": "2026-11-01T00:00:00Z",
                    "last_heartbeat_at": "2026-11-01T00:00:00Z",
                    "heartbeat_age_seconds": 5.0,
                    "is_stale": False,
                    "active_tasks": 0,
                    "queues": "orchestrator_workflows",
                    "metadata": {},
                }
            ],
        }
        mock_req.return_value = mock_resp_workers
        workers_resp = client.get_celery_workers()
        assert isinstance(workers_resp, CeleryWorkersResponse)
        assert workers_resp.total_workers == 1

        # Mock /celery/queue
        mock_resp_queue = MagicMock()
        mock_resp_queue.json.return_value = {
            "queued_count": 0,
            "running_count": 1,
            "waiting_approval_count": 0,
            "completed_count": 2,
            "failed_count": 0,
            "retry_count": 0,
            "oldest_queued_age_seconds": 0.0,
            "active_worker_count": 1,
            "stale_worker_count": 0,
            "offline_worker_count": 0,
            "total_worker_count": 1,
            "recent_failures": [],
        }
        mock_req.return_value = mock_resp_queue
        queue_resp = client.get_celery_queue()
        assert isinstance(queue_resp, CeleryQueueMetricsResponse)
        assert queue_resp.running_count == 1

        # Mock /celery/stuck
        mock_resp_stuck = MagicMock()
        mock_resp_stuck.json.return_value = {
            "total_stuck": 1,
            "eligible_count": 1,
            "stuck_threshold_seconds": 300.0,
            "workflows": [
                {
                    "run_id": "sdk-stuck-run",
                    "task": "Test stuck task",
                    "status": "running",
                    "created_at": "2026-11-01T00:00:00Z",
                    "updated_at": "2026-11-01T00:00:00Z",
                    "age_seconds": 350.0,
                    "stuck_reason": "orphaned_lease",
                    "recovery_eligible": True,
                }
            ],
        }
        mock_req.return_value = mock_resp_stuck
        stuck_resp = client.get_celery_stuck()
        assert isinstance(stuck_resp, StuckWorkflowsResponse)
        assert stuck_resp.total_stuck == 1

        # Mock /celery/recover
        mock_resp_rec = MagicMock()
        mock_resp_rec.json.return_value = {
            "success": True,
            "action": "requeue",
            "recovered_count": 1,
            "failed_count": 0,
            "operator": "sdk-user",
            "reason": "SDK automated test recovery",
            "results": [
                {
                    "run_id": "sdk-stuck-run",
                    "success": True,
                    "new_status": "queued",
                    "action": "requeue",
                    "fence_token": 2,
                }
            ],
        }
        mock_req.return_value = mock_resp_rec
        rec_resp = client.recover_celery_workflows(
            run_id="sdk-stuck-run",
            reason="SDK automated test recovery",
        )
        assert isinstance(rec_resp, RecoverWorkflowsResponse)
        assert rec_resp.success is True


def test_configuration_celery_observability_validation() -> None:
    """Configuration validators enforce heartbeat < stale threshold and valid batch limits."""
    # 1. Invalid: heartbeat interval >= stale threshold
    with pytest.raises(ValueError, match="strictly less than"):
        validate_configuration(
            Settings(
                checkpoint_db_path=":memory:",
                celery_worker_heartbeat_interval_seconds=60.0,
                celery_worker_stale_threshold_seconds=60.0,
            )
        )

    # 2. Invalid: recovery batch size <= 0
    with pytest.raises(ValueError, match="celery_recovery_batch_size"):
        Settings(
            checkpoint_db_path=":memory:",
            celery_recovery_batch_size=0,
        )

    # 3. Diagnostics contain Celery observability settings
    diag = get_startup_diagnostics(
        Settings(
            checkpoint_db_path=":memory:",
            celery_worker_heartbeat_interval_seconds=15.0,
            celery_worker_stale_threshold_seconds=60.0,
            celery_queue_monitoring_interval_seconds=30.0,
            celery_workflow_stuck_threshold_seconds=300.0,
            celery_recovery_batch_size=50,
        )
    )
    celery_diag = diag["celery"]
    assert celery_diag["worker_heartbeat_interval_seconds"] == 15.0
    assert celery_diag["worker_stale_threshold_seconds"] == 60.0
    assert celery_diag["queue_monitoring_interval_seconds"] == 30.0
    assert celery_diag["workflow_stuck_threshold_seconds"] == 300.0
    assert celery_diag["recovery_batch_size"] == 50
