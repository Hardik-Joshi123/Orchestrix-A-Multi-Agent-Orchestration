"""Integration and failure-injection tests for Celery distributed workflow execution (Task 58)."""

from __future__ import annotations

import datetime
import json
import time
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.manager import (
    CapacityExhaustedError,
    RunManager,
)
from orchestrator.api.server import create_app
from orchestrator.celery_app import (
    execute_workflow_run,
    resume_workflow_run,
)
from orchestrator.config import (
    Settings,
    reset_settings,
)
from orchestrator.hitl.queue import ReviewQueue, apply_resolution
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import (
    AdmissionFencingError,
    CheckpointStore,
    RecoveryError,
)
from orchestrator.tools import REGISTRY, RiskLevel, Tool


@pytest.fixture
def temp_db(tmp_path: Any) -> str:
    """Create a temporary SQLite database path."""
    return str(tmp_path / "test_celery_fail_injection.db")


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
    monkeypatch.setenv("ORCHESTRATOR_CELERY_WORKFLOW_STUCK_THRESHOLD_SECONDS", "300.0")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_RECOVERY_BATCH_SIZE", "50")
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def eager_settings(temp_db: str) -> Settings:
    """Settings configured for eager execution and temporary persistence."""
    return Settings(
        checkpoint_db_path=temp_db,
        checkpoint_enabled=True,
        celery_enabled=True,
        workflow_execution_backend="celery",
        celery_always_eager=True,
        celery_task_default_queue="orchestrator_workflows",
        celery_worker_concurrency=2,
        api_token="test-secret-token",
        idempotency_enabled=True,
    )


@pytest.fixture
def store(temp_db: str) -> CheckpointStore:
    """Initialized CheckpointStore with temporary database."""
    return CheckpointStore(db_path=temp_db)


@pytest.fixture
def test_app(eager_settings: Settings, store: CheckpointStore) -> Any:
    """FastAPI test app configured with eager Celery execution and injected dependencies."""
    app = create_app()
    from orchestrator.api.routes import get_manager, get_store

    manager = RunManager(
        max_active_runs=10,
        store=store,
        owner_id="api-node-master",
        lease_ttl_seconds=30.0,
    )
    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    return app


@pytest.fixture
def client(test_app: Any) -> TestClient:
    """FastAPI TestClient with Bearer auth header."""
    return TestClient(test_app, headers={"Authorization": "Bearer test-secret-token"})


# ── 1. End-to-End Execution Tests ─────────────────────────────────────────────


def test_e2e_celery_submission_execution_and_persisted_result(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """API submission -> Celery queue -> worker execution -> persisted terminal result."""
    run_id = "test-e2e-run-001"
    task_desc = "Analyze multi-region failover architecture"

    resp = client.post(
        "/runs",
        json={
            "run_id": run_id,
            "task": task_desc,
            "background": True,
            "execution_backend": "celery",
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["run_id"] == run_id

    # Verify execution finished and status is completed
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "completed"
    assert summary["latest_step"] >= 1

    # Verify GET /runs/{run_id} reports completed result and assembled answer
    get_resp = client.get(f"/runs/{run_id}")
    assert get_resp.status_code == 200
    get_body = get_resp.json()
    assert get_body["status"] == "completed"
    assert get_body["answer"] != ""

    # Verify admission lease was cleanly released upon terminal completion
    adm = store.get_workflow_admission(run_id)
    assert adm is None


def test_multiple_workers_competing_for_same_queue_under_admission_limit(
    store: CheckpointStore,
) -> None:
    """Multiple workers consuming the same queue strictly enforce global admission limits."""
    # Configure tight admission limit: max 2 active runs across all workers
    max_active = 2

    mgr_worker_3 = RunManager(owner_id="celery-worker-3", store=store, max_active_runs=max_active)

    # Worker 1 and Worker 2 acquire admission slots for their runs
    admitted_1 = store.acquire_workflow_admission("run-multi-1", "celery-worker-1", max_active)
    admitted_2 = store.acquire_workflow_admission("run-multi-2", "celery-worker-2", max_active)
    assert admitted_1 is True
    assert admitted_2 is True

    # Worker 3 attempts to execute third run: capacity limit reached, admission rejected
    with pytest.raises(CapacityExhaustedError, match="capacity limit"):
        mgr_worker_3.execute_dispatched_run(
            run_id="run-multi-3",
            task="Third parallel task",
            store=store,
        )

    # Worker 1 finishes and releases admission
    store.release_workflow_admission("run-multi-1", "celery-worker-1")

    # Worker 3 can now acquire admission and execute
    res3 = mgr_worker_3.execute_dispatched_run(
        run_id="run-multi-3",
        task="Third parallel task",
        store=store,
    )
    assert res3.get("status") in ("completed", "approved")
    summary3 = store.get_run_summary("run-multi-3")
    assert summary3 is not None
    assert summary3["status"] == "completed"


def test_workflow_state_consistency_across_separate_api_and_worker_processes(
    store: CheckpointStore,
) -> None:
    """Workflow state remains consistent across decoupled API and worker process lifecycles."""
    run_id = "test-process-decoupled-run"

    # 1. API Process: writes initial queued state with fence_token = 0
    state_queued = {"run_id": run_id, "task": "Cross-process consistency task", "status": "queued"}
    step_0 = store.save_checkpoint(run_id, state_queued, transition="enqueue")
    assert step_0 == 1

    summary_api_1 = store.get_run_summary(run_id)
    assert summary_api_1 is not None
    assert summary_api_1["status"] == "queued"
    assert summary_api_1["fence_token"] == 0

    # 2. Worker Process: picks up task, acquires lease (fence_token becomes 1)
    worker_mgr = RunManager(owner_id="detached-celery-worker-pid-456", store=store)
    result = worker_mgr.execute_dispatched_run(
        run_id=run_id,
        task="Cross-process consistency task",
        store=store,
    )
    assert result.get("status") in ("completed", "approved")

    # 3. API Query Process: reads from persistence, verifying updated step and monotonic fence
    summary_api_2 = store.get_run_summary(run_id)
    assert summary_api_2 is not None
    assert summary_api_2["status"] == "completed"
    assert summary_api_2["fence_token"] == 1
    assert summary_api_2["latest_step"] > step_0


def test_idempotent_submission_and_duplicate_delivery(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """Idempotency-Key caches 202 and duplicate Celery delivery avoids redundant execution."""
    idempotency_key = "idemp-celery-fail-inject-999"
    payload = {
        "task": "Deduplicated computation workflow",
        "background": True,
        "execution_backend": "celery",
    }
    headers = {"Idempotency-Key": idempotency_key}

    # Initial submission returns 202
    resp1 = client.post("/runs", json=payload, headers=headers)
    assert resp1.status_code == 202
    run_id = resp1.json()["run_id"]

    # Replay with same Idempotency-Key returns cached 202 with Idempotent-Replayed header
    resp2 = client.post("/runs", json=payload, headers=headers)
    assert resp2.status_code == 202
    assert resp2.headers.get("Idempotent-Replayed") == "true"
    assert resp2.json()["run_id"] == run_id

    # Verify run completed in eager mode
    summary1 = store.get_run_summary(run_id)
    assert summary1 is not None
    assert summary1["status"] == "completed"
    initial_step = summary1["latest_step"]

    # Celery broker redelivers the task (at-least-once delivery duplicate)
    dup_result = execute_workflow_run(
        run_id=run_id,
        task="Deduplicated computation workflow",
    )
    assert dup_result.get("run_id") == run_id
    assert dup_result.get("status") == "completed"

    # Step count did not change: no duplicate execution occurred
    summary2 = store.get_run_summary(run_id)
    assert summary2 is not None
    assert summary2["latest_step"] == initial_step


def test_workflow_pause_and_resume_after_hitl_approval(
    store: CheckpointStore,
) -> None:
    """Workflow stops in waiting_approval, releases lease, and resumes safely upon approval."""
    approval_called = False

    def database_drop_table(table_name: str) -> str:
        nonlocal approval_called
        approval_called = True
        return f"Dropped {table_name}"

    drop_tool = Tool(
        name="celery_hitl_drop_table",
        fn=database_drop_table,
        schema={"type": "object", "properties": {"table_name": {"type": "string"}}},
        side_effecting=True,
        requires_approval=True,
        risk_level=RiskLevel.HIGH,
    )
    REGISTRY.register(drop_tool)

    try:
        run_id = "test-hitl-pause-resume-run"
        plan = [
            {
                "id": "s1",
                "specialist": "analysis",
                "instruction": "Drop table users via celery_hitl_drop_table",
                "tools": ["celery_hitl_drop_table"],
                "status": "pending",
            },
            {
                "id": "s2",
                "specialist": "analysis",
                "instruction": "Confirm table drop in audit logs",
                "tools": [],
                "status": "pending",
                "depends_on": ["s1"],
            },
        ]

        # 1. Celery task executes up to approval gate
        _res = execute_workflow_run(
            run_id=run_id,
            task="Drop user table",
            plan=plan,
        )

        # 2. Verify workflow stopped in waiting_approval and sensitive tool was NOT executed
        summary = store.get_run_summary(run_id)
        assert summary is not None
        assert approval_called is False
        assert summary["status"] in ("waiting_approval", "pending", "in_progress")

        # Admission lease was cleanly released during pause
        adm = store.get_workflow_admission(run_id)
        assert adm is None

        # 3. Simulate human decision approval in persistence
        approvals = store.list_tool_approvals(run_id=run_id)
        if approvals:
            op_key = approvals[0]["operation_key"]
            store.record_approval_decision(
                operation_key=op_key,
                decision="approved",
                approver="sec-admin",
                reason="Authorized by security lead",
            )
        rq = ReviewQueue()
        for item in rq.pending():
            if item.task_id == run_id:
                res = rq.resolve(item.item_id, decision="approve", approver="sec-admin")
                chk = store.load_latest_checkpoint(run_id)
                if chk and res:
                    apply_resolution(chk.state, res)

        # 4. Resume task dispatched to Celery worker
        resume_res = resume_workflow_run(
            run_id=run_id,
        )
        assert resume_res.get("run_id") == run_id

        # Verify workflow resumed and finished
        summary_final = store.get_run_summary(run_id)
        assert summary_final is not None
        assert summary_final["status"] in ("completed", "approved")

    finally:
        if "celery_hitl_drop_table" in REGISTRY._tools:
            del REGISTRY._tools["celery_hitl_drop_table"]


# ── 2. Failure-Injection Scenarios ─────────────────────────────────────────────


def test_worker_termination_and_orphaned_lease_diagnosis(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """Worker dies during execution; observability detects stuck run and orphaned lease."""
    run_id = "test-crash-stuck-run"
    old_time = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=600)
    ).isoformat()
    past_expiry = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60)
    ).isoformat()

    # Worker started run, acquired lease, then terminated abruptly
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Batch report processing", "status": "running"},
    )
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO workflow_admissions (
                        run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    ) VALUES (?, 'dead-worker-pid-999', 'active', ?, ?, ?, 1);
                    """,
                    (run_id, old_time, old_time, past_expiry),
                )
                conn.execute(
                    "UPDATE runs SET created_at = ? WHERE run_id = ?;",
                    (old_time, run_id),
                )
        finally:
            conn.close()

    # GET /celery/stuck detects the stuck run with expired lease and dead worker
    resp = client.get("/celery/stuck")
    assert resp.status_code == 200
    data = resp.json()
    stuck_run = next((s for s in data["workflows"] if s["run_id"] == run_id), None)
    assert stuck_run is not None
    assert stuck_run["stuck_reason"] in ("expired_lease_dead_worker", "lease_expired")
    assert stuck_run["recovery_eligible"] is True


def test_worker_restart_and_safe_recovery(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """Interrupted workflow recovered via POST /celery/recover and resumed by fresh worker."""
    run_id = "test-recover-resume-run"
    old_time = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=400)
    ).isoformat()

    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Critical analytics compilation", "status": "running"},
    )
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    "UPDATE runs SET created_at = ? WHERE run_id = ?;",
                    (old_time, run_id),
                )
        finally:
            conn.close()

    # Operator recovers the stuck run
    rec_resp = client.post(
        "/celery/recover",
        json={
            "run_id": run_id,
            "action": "requeue",
            "reason": "Recovering orphaned task after worker host failure",
        },
    )
    assert rec_resp.status_code == 200
    rec_data = rec_resp.json()
    assert rec_data["success"] is True
    assert rec_data["results"][0]["new_status"] == "queued"
    new_fence = rec_data["results"][0]["fence_token"]
    assert new_fence >= 1

    # In eager mode, recovery automatically re-enqueued and executed the task
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] in ("queued", "completed")

    # Verify audit trail records the recovery event
    audits = store.get_audit_events(
        run_id=run_id,
        event_type=AuditEventType.WORKFLOW_RECOVERED,
    )
    assert len(audits) >= 1
    assert "host failure" in json.dumps(audits[0]["metadata"])


def test_stale_fenced_out_worker_rejected_on_commit(
    store: CheckpointStore,
) -> None:
    """Stale worker attempting to commit with an outdated fence token is strictly rejected."""
    run_id = "test-fencing-conflict-run"

    # Worker A acquires lease with fence_token = 1
    store.acquire_workflow_admission(run_id, "worker-a-partitioned", max_active_runs=5)
    fenced_store_a = store.with_fencing("worker-a-partitioned", fence_token=1)
    fenced_store_a.save_checkpoint(
        run_id,
        {"task": "Partition test task", "status": "running"},
        transition="start",
    )

    # Worker A's lease expires while Worker A is partitioned
    past_expiry = (
        datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=60)
    ).isoformat()
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    "UPDATE workflow_admissions SET expires_at = ? WHERE run_id = ?;",
                    (past_expiry, run_id),
                )
        finally:
            conn.close()

    # Operator recovers the workflow, deleting old lease and bumping fence_token to 2
    store.recover_stuck_workflow(
        run_id=run_id,
        operator="admin-operator",
        reason="Fencing out partitioned worker A",
        action="requeue",
    )

    summary_recovered = store.get_run_summary(run_id)
    assert summary_recovered is not None
    assert summary_recovered["fence_token"] == 2

    # Worker A wakes up and attempts to commit with outdated fence_token = 1
    with pytest.raises(AdmissionFencingError, match="superseded"):
        fenced_store_a.save_checkpoint(
            run_id,
            {"task": "Partition test task", "status": "completed", "result": "stale data"},
            transition="complete",
        )

    # Verify stale write did NOT overwrite database
    summary_final = store.get_run_summary(run_id)
    assert summary_final is not None
    assert summary_final["status"] == "queued"
    assert summary_final["fence_token"] == 2


def test_recovery_does_not_override_active_worker_ownership(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """Recovery rejected when an active, unexpired admission lease is held by a live worker."""
    run_id = "test-active-lease-protected"
    now_dt = datetime.datetime.now(datetime.timezone.utc)
    future_expiry = (now_dt + datetime.timedelta(seconds=120)).isoformat()

    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Long running simulation", "status": "running"},
    )
    # Worker B is actively holding an unexpired lease
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO workflow_admissions (
                        run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    ) VALUES (?, 'active-worker-b', 'active', ?, ?, ?, 1);
                    """,
                    (run_id, now_dt.isoformat(), now_dt.isoformat(), future_expiry),
                )
        finally:
            conn.close()

    # Operator attempts recovery: must fail because lease is active
    rec_resp = client.post(
        "/celery/recover",
        json={
            "run_id": run_id,
            "action": "requeue",
            "reason": "Attempting to steal active lease",
        },
    )
    assert rec_resp.status_code == 200
    data = rec_resp.json()
    assert data["failed_count"] == 1
    assert data["recovered_count"] == 0
    assert "active unexpired" in data["results"][0]["error"].lower()

    # Lease and run remain intact
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "running"


def test_broker_temporary_unavailability_handling_and_rollback(
    client: TestClient,
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Broker connection failure returns HTTP 503 and rolls back SQLite queued state."""
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "false")
    reset_settings()

    run_id = "test-broker-rollback-run"

    # Mock connection_for_write to raise ConnectionError
    mock_conn = MagicMock()
    mock_conn.ensure_connection.side_effect = ConnectionRefusedError("Broker offline at 127.0.0.1:6379")
    mock_app = MagicMock()
    mock_app.conf.task_always_eager = False
    mock_app.connection_for_write.return_value.__enter__.return_value = mock_conn

    with patch("orchestrator.celery_app.get_celery_app", return_value=mock_app):
        resp = client.post(
            "/runs",
            json={
                "run_id": run_id,
                "task": "Data pipeline ingestion",
                "background": True,
                "execution_backend": "celery",
            },
        )
        assert resp.status_code == 503
        assert "broker" in resp.json()["detail"].lower()

    # Verify database was cleaned up: no orphaned run or checkpoint left in SQLite
    summary = store.get_run_summary(run_id)
    assert summary is None


def test_api_process_restart_with_queued_and_running_workflows(
    store: CheckpointStore,
) -> None:
    """API process termination and restart seamlessly resumes from durable SQLite state."""
    run_id_1 = "test-restart-queued"
    run_id_2 = "test-restart-running"

    # 1. First API process lifetime: records queued and running workflows
    store.save_checkpoint(run_id_1, {"status": "queued", "task": "Queued task 1"})
    store.save_checkpoint(run_id_2, {"status": "running", "task": "Running task 2"})

    # 2. API process dies (RunManager instance destroyed, memory cache gone)
    # 3. New API process starts up
    new_manager = RunManager(owner_id="api-node-restarted", store=store)
    assert new_manager.owner_id == "api-node-restarted"

    # New API process reads runs directly from durable SQLite
    sum_1 = store.get_run_summary(run_id_1)
    sum_2 = store.get_run_summary(run_id_2)
    assert sum_1 is not None and sum_1["status"] == "queued"
    assert sum_2 is not None and sum_2["status"] == "running"

    # New API process detects stuck runs without needing previous memory state
    stuck_list = store.get_stuck_workflows(stuck_threshold_seconds=10.0)
    assert any(s["run_id"] == run_id_2 for s in stuck_list)


# ── 3. Recovery, Security & HITL Invariants ───────────────────────────────────


def test_hitl_workflows_never_bypass_human_approval_during_recovery(
    store: CheckpointStore,
) -> None:
    """Recovery action requeue is strictly rejected for runs in waiting_approval status."""
    run_id = "test-hitl-no-bypass-run"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Sensitive bank transfer", "status": "waiting_approval"},
    )

    # Attempting to requeue waiting approval run raises RecoveryError
    with pytest.raises(RecoveryError, match="waiting for human approval"):
        store.recover_stuck_workflow(
            run_id=run_id,
            operator="ops-malicious",
            reason="Bypassing human review via requeue",
            action="requeue",
        )

    # Workflow remains in waiting_approval
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "waiting_approval"


def test_terminal_workflows_not_reexecuted_or_recovered(
    store: CheckpointStore,
) -> None:
    """Workflows in terminal status completed or cancelled cannot be recovered."""
    store.save_checkpoint(run_id="run-term-done", state={"status": "completed"})
    store.save_checkpoint(run_id="run-term-abort", state={"status": "cancelled"})

    # Completed run recovery rejected
    with pytest.raises(RecoveryError, match="terminal status"):
        store.recover_stuck_workflow(
            run_id="run-term-done",
            operator="ops",
            reason="Attempting to re-run completed task",
            action="requeue",
        )

    # Cancelled run recovery rejected
    with pytest.raises(RecoveryError, match="terminal status"):
        store.recover_stuck_workflow(
            run_id="run-term-abort",
            operator="ops",
            reason="Attempting to re-run cancelled task",
            action="requeue",
        )


def test_cancellation_persists_across_worker_restarts(
    store: CheckpointStore,
) -> None:
    """Administrative cancellation before or during worker dispatch aborts task execution."""
    run_id = "test-cancel-persist-run"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Heavy ML training job", "status": "queued"},
    )

    # Cancel administratively before worker runs
    store.cancel_run_administratively(run_id, reason="Operator stopped queue processing")

    # Worker executes task
    result = execute_workflow_run(
        run_id=run_id,
        task="Heavy ML training job",
    )
    assert result.get("status") == "cancelled"

    # Terminal state remains cancelled
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "cancelled"


def test_uncertain_external_tool_operations_not_blindly_repeated(
    store: CheckpointStore,
) -> None:
    """Recovery rejects requeue when in-flight or uncertain tool executions exist."""
    run_id = "test-uncertain-tool-run"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "External payment gateway call", "status": "running"},
    )

    # Record in-flight/uncertain tool execution
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with store._get_db_lock(store.db_path):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO tool_executions (
                        operation_key, run_id, subtask_id, tool_name, fence_token,
                        status, args_json, created_at, updated_at
                    ) VALUES ('op-pay-123', ?, 's1', 'stripe_charge', 1, 'uncertain', '{}', ?, ?);
                    """,
                    (run_id, now_iso, now_iso),
                )
        finally:
            conn.close()

    # Requeue recovery rejected due to uncertain external tool state
    with pytest.raises(RecoveryError, match="uncertain in-flight tool operations"):
        store.recover_stuck_workflow(
            run_id=run_id,
            operator="ops-finance",
            reason="Recovering payment workflow",
            action="requeue",
        )

    # Operator reconciles tool execution to completed
    store.reconcile_tool_execution(
        run_id=run_id,
        operation_key="op-pay-123",
        outcome="completed",
        actor="ops-finance",
        reason="Charge verified with Stripe dashboard",
    )

    # After reconciliation, recovery succeeds safely
    rec = store.recover_stuck_workflow(
        run_id=run_id,
        operator="ops-finance",
        reason="Reconciliation completed; requeuing remainder of workflow",
        action="requeue",
    )
    assert rec["new_status"] == "queued"


# ── 4. Outbox, Webhook, and Replay Reliability ────────────────────────────────


def test_recovery_outbox_audit_and_webhook_reliability(
    store: CheckpointStore,
) -> None:
    """Recovery records structured audit event, outbox dispatches, and replay protection works."""
    run_id = "test-outbox-recovery-run"
    store.save_checkpoint(run_id=run_id, state={"status": "running", "task": "Audited job"})

    # Recover workflow
    rec = store.recover_stuck_workflow(
        run_id=run_id,
        operator="audit-auditor",
        reason="Scheduled nightly failure-injection recovery",
        action="requeue",
    )
    assert rec["new_status"] == "queued"

    # Verify structured audit event exists
    audits = store.get_audit_events(
        run_id=run_id,
        event_type=AuditEventType.WORKFLOW_RECOVERED,
    )
    assert len(audits) >= 1
    assert audits[0]["actor"] == "audit-auditor"

    # Verify durable replay protection store records and rejects duplicates
    now_ts = time.time()
    event_id = "evt-recovery-notification-999"

    # First record succeeds
    recorded_1 = store.record_webhook_replay(
        event_id=event_id,
        timestamp=int(now_ts),
        expires_at=now_ts + 300.0,
    )
    assert recorded_1 is True

    # Duplicate replay rejected
    recorded_2 = store.record_webhook_replay(
        event_id=event_id,
        timestamp=int(now_ts),
        expires_at=now_ts + 300.0,
    )
    assert recorded_2 is False
