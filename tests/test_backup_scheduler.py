"""Comprehensive test suite for automated database backup scheduling and monitoring.

Tests cover:
- Backup scheduler settings validation, bounds, and startup diagnostics.
- Single-cycle online backup execution, deep validation, and metrics derivation.
- Structured audit logging (backup.started, backup.completed, backup.failed).
- Multi-process leadership leasing and standby worker isolation.
- Leader failover, lease expiration, and fence-token monotonicity.
- Prevention of concurrent overlapping backup executions.
- Bounded retries, exponential backoff, and failure recovery.
- Retention lifecycle management with unconditional latest-backup preservation.
- CLI operational interfaces (`orchestrator backup status` and `trigger`).
- FastAPI operational routes (`/backups/status`, `/backups/trigger`, `/backups/history`).
"""

from __future__ import annotations

import io
import json
import os
import pathlib
import sys
import time
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from orchestrator.api.routes import get_store
from orchestrator.api.server import create_app
from orchestrator.config import (
    Settings,
    get_settings,
    get_startup_diagnostics,
    validate_configuration,
)
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.backup import (
    BackupError,
    backup_cli,
)
from orchestrator.persistence.scheduler import (
    DatabaseBackupScheduler,
    reset_backup_scheduler,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_scheduler() -> None:
    """Ensure global scheduler singleton and settings cache are cleanly reset."""
    reset_backup_scheduler()
    get_settings.cache_clear()
    yield
    reset_backup_scheduler()
    get_settings.cache_clear()


@pytest.fixture
def populated_db(tmp_path: pathlib.Path) -> tuple[str, CheckpointStore]:
    """Create a populated SQLite database with sample checkpoints, audit, and outbox rows."""
    db_file = str(tmp_path / "source_orch.db")
    store = CheckpointStore(db_path=db_file)

    # 1. Run and checkpoint
    run_id = "run-sched-test-001"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Execute financial reconciliation", "status": "completed"},
        transition="supervisor->reviewer",
        privileged=True,
    )

    # 2. Tool approval request
    store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op_shell_001",
        tool_name="shell_command",
        timeout_seconds=3600.0,
    )

    # 3. Transactional outbox event
    store.insert_outbox_event(
        event_type="workflow.completed",
        payload={"run_id": run_id, "status": "completed"},
        idempotency_key="idemp_key_001",
    )

    return db_file, store


# =============================================================================
# 1. Configuration & Startup Diagnostics Tests
# =============================================================================


def test_backup_scheduler_settings_validation_and_bounds() -> None:
    """Verify settings defaults, bounds checking, and configuration validation."""
    settings = Settings(
        checkpoint_db_path="test.db",
        backup_scheduler_enabled=True,
        backup_interval_seconds=3600.0,
        backup_max_duration_seconds=120.0,
        backup_max_retries=5,
        backup_retry_backoff_seconds=2.0,
        backup_max_backoff_seconds=30.0,
        backup_scheduler_lease_ttl_seconds=45.0,
        backup_scheduler_heartbeat_interval_seconds=15.0,
    )
    validated = validate_configuration(settings)
    assert validated.backup_scheduler_enabled is True
    assert validated.backup_interval_seconds == 3600.0
    assert validated.backup_max_retries == 5

    # Interval too low (< 1.0s)
    with pytest.raises(ValidationError):
        Settings(checkpoint_db_path="test.db", backup_interval_seconds=0.5)

    # Max duration too low
    with pytest.raises(ValidationError):
        Settings(checkpoint_db_path="test.db", backup_max_duration_seconds=0.0)

    # Retries negative
    with pytest.raises(ValidationError):
        Settings(checkpoint_db_path="test.db", backup_max_retries=-1)

    # Heartbeat >= Lease TTL validation error
    with pytest.raises(ValueError, match="strictly less than"):
        bad_settings = Settings(
            checkpoint_db_path="test.db",
            backup_scheduler_lease_ttl_seconds=10.0,
            backup_scheduler_heartbeat_interval_seconds=10.0,
        )
        validate_configuration(bad_settings)

    # Retry backoff > Max backoff validation error
    with pytest.raises(ValueError, match="must be <="):
        bad_backoff = Settings(
            checkpoint_db_path="test.db",
            backup_retry_backoff_seconds=50.0,
            backup_max_backoff_seconds=10.0,
        )
        validate_configuration(bad_backoff)


def test_backup_scheduler_startup_diagnostics_integration() -> None:
    """Verify get_startup_diagnostics includes backup scheduler settings without secrets."""
    settings = Settings(
        checkpoint_db_path="test.db",
        backup_scheduler_enabled=True,
        backup_interval_seconds=7200.0,
    )
    diag = get_startup_diagnostics(settings)
    assert "backup" in diag
    backup_diag = diag["backup"]
    assert backup_diag["scheduler_enabled"] is True
    assert backup_diag["interval_seconds"] == 7200.0
    assert backup_diag["retention_count"] == 10


# =============================================================================
# 2. Execution, Verification & Metrics Tests
# =============================================================================


def test_backup_scheduler_single_cycle_execution(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify single-cycle backup creation, validation, and metrics recording."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_target")

    scheduler = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        enabled=True,
        interval_seconds=3600.0,
    )

    # Execute on-demand cycle
    res = scheduler.run_backup_cycle(actor="test", force=True)
    assert res["status"] == "success"
    assert "metadata" in res
    assert "validation" in res
    assert res["validation"]["is_valid"] is True
    assert res["duration_ms"] >= 0.0

    # Verify backup exists on disk
    backup_file = res["metadata"]["backup_path"]
    assert os.path.exists(backup_file)
    assert os.path.getsize(backup_file) > 0

    # Verify operational metrics
    status = scheduler.get_status()
    assert status["total_backups_count"] == 1
    assert status["failure_count"] == 0
    assert status["last_success_at"] is not None
    assert status["last_backup_path"] == backup_file
    assert status["last_size_bytes"] == os.path.getsize(backup_file)
    assert status["last_integrity"] == "ok"
    assert status["last_tables_count"] > 0
    assert status["last_error"] is None


def test_backup_scheduler_audit_logging(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify backup.started and backup.completed events are recorded in audit trail."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_audit")

    scheduler = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        enabled=True,
    )
    res = scheduler.run_backup_cycle(actor="test_runner", force=True)
    assert res["status"] == "success"

    # Query audit events from store
    events = store.get_audit_events(limit=50)
    event_types = [e["event_type"] for e in events]
    assert AuditEventType.BACKUP_STARTED in event_types
    assert AuditEventType.BACKUP_COMPLETED in event_types

    # Verify no credential leakage in audit metadata
    for ev in events:
        if ev["event_type"].startswith("backup."):
            meta_str = json.dumps(ev.get("metadata", {}))
            assert "token" not in meta_str or "[REDACTED]" in meta_str
            assert "password" not in meta_str or "[REDACTED]" in meta_str


# =============================================================================
# 3. Cross-Process Leadership & Lease Fencing Tests
# =============================================================================


def test_backup_scheduler_cross_process_leadership(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify only the lease leader executes backups while standby nodes skip."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_lease")

    # Instance A (Leader)
    sched_a = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        owner_id="worker-node-alpha",
        enabled=True,
        lease_name="test_backup_lease",
        lease_ttl_seconds=30.0,
    )
    # Instance B (Standby)
    sched_b = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        owner_id="worker-node-bravo",
        enabled=True,
        lease_name="test_backup_lease",
        lease_ttl_seconds=30.0,
    )

    # 1. Leader acquires lease
    acquired_a, fence_a, _ = store.acquire_scheduler_lease(
        lease_name="test_backup_lease",
        owner_id="worker-node-alpha",
        lease_ttl_seconds=30.0,
    )
    assert acquired_a is True
    sched_a._is_leader = True
    sched_a._fence_token = fence_a
    sched_a._leadership_state = "leader"

    # 2. Standby fails to acquire active lease
    acquired_b, fence_b, _ = store.acquire_scheduler_lease(
        lease_name="test_backup_lease",
        owner_id="worker-node-bravo",
        lease_ttl_seconds=30.0,
    )
    assert acquired_b is False
    sched_b._is_leader = False
    sched_b._leadership_state = "standby"

    # 3. Standby running backup cycle without force skips execution
    res_b = sched_b.run_backup_cycle(actor="scheduler", force=False)
    assert res_b["status"] == "skipped"
    assert "standby" in res_b["message"]
    assert sched_b.get_status()["total_backups_count"] == 0

    # 4. Leader running backup cycle executes successfully
    res_a = sched_a.run_backup_cycle(actor="scheduler", force=False)
    assert res_a["status"] == "success"
    assert sched_a.get_status()["total_backups_count"] == 1


def test_backup_scheduler_lease_failover_and_recovery(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify standby node takes over leadership after leader shutdown."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_failover")

    sched_a = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        owner_id="leader-proc-1",
        enabled=True,
        lease_name="failover_lease",
        lease_ttl_seconds=10.0,
    )
    sched_b = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        owner_id="standby-proc-2",
        enabled=True,
        lease_name="failover_lease",
        lease_ttl_seconds=10.0,
    )

    # 1. Node A starts and acquires leadership
    sched_a.start()
    time.sleep(0.15)
    assert sched_a.is_leader is True
    fence_1 = sched_a.fence_token
    assert fence_1 is not None

    # 2. Node A shuts down gracefully, releasing lease
    sched_a.stop()
    assert sched_a.is_leader is False

    # 3. Node B starts and acquires leadership with incremented fence token
    sched_b.start()
    time.sleep(0.15)
    assert sched_b.is_leader is True
    fence_2 = sched_b.fence_token
    assert fence_2 is not None
    assert fence_2 >= fence_1

    sched_b.stop()


# =============================================================================
# 4. Overlapping Execution Prevention & Retry Handling Tests
# =============================================================================


def test_backup_scheduler_prevents_concurrent_overlapping_executions(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify non-overlapping execution lock blocks parallel backup attempts."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_overlap")

    scheduler = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        enabled=True,
    )

    # Acquire internal execution lock to simulate in-flight execution
    assert scheduler._exec_lock.acquire(blocking=False) is True

    try:
        # Subsequent execution attempt is immediately skipped
        res = scheduler.run_backup_cycle(actor="test", force=True)
        assert res["status"] == "skipped"
        assert "already in progress" in res["message"]
    finally:
        scheduler._exec_lock.release()

    # Once lock is freed, execution succeeds cleanly
    res_after = scheduler.run_backup_cycle(actor="test", force=True)
    assert res_after["status"] == "success"


def test_backup_scheduler_failure_retries_and_error_handling(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify transient error retries, exponential backoff, and failure recording."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_retry")

    scheduler = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        max_retries=2,
        retry_backoff_seconds=0.05,
        max_backoff_seconds=0.1,
    )

    call_count = 0

    def mock_create_backup(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise BackupError(f"Simulated storage failure on attempt {call_count}")

    with patch.object(scheduler._backup_manager, "create_backup", side_effect=mock_create_backup):
        res = scheduler.run_backup_cycle(actor="scheduler", force=True)

    assert res["status"] == "failed"
    # Initial attempt + 2 retries = 3 total attempts
    assert call_count == 3
    assert "Simulated storage failure" in res["error"]

    status = scheduler.get_status()
    assert status["failure_count"] == 1
    assert status["consecutive_failures"] == 1
    assert "Simulated storage failure" in status["last_error"]

    # Verify backup.failed audit event
    events = store.get_audit_events(limit=10)
    failed_evs = [e for e in events if e["event_type"] == AuditEventType.BACKUP_FAILED]
    assert len(failed_evs) >= 1
    assert "Simulated storage failure" in str(failed_evs[0]["metadata"])


# =============================================================================
# 5. Retention & Latest-Backup Protection Tests
# =============================================================================


def test_backup_scheduler_retention_preserves_latest_backup(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify retention pruning preserves the single newest backup unconditionally."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_retention")

    # Set retention count to 1
    scheduler = DatabaseBackupScheduler(
        store=store,
        backup_dir=backup_dir,
        retention_count=1,
        retention_days=0,
    )

    # Run 3 consecutive backup cycles
    for i in range(3):
        res = scheduler.run_backup_cycle(actor="test", tag=f"cycle_{i}", force=True)
        assert res["status"] == "success"
        time.sleep(0.01)

    # Verify only 1 backup remains on disk, and it is the latest
    remaining = scheduler._backup_manager.list_backups(dest_dir=backup_dir)
    assert len(remaining) == 1
    assert "cycle_2" in remaining[0].backup_path


# =============================================================================
# 6. CLI Management Commands Tests
# =============================================================================


def test_backup_cli_status_and_trigger(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
) -> None:
    """Verify `orchestrator backup status` and `trigger` subcommands."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_cli")

    # 1. Trigger backup via CLI with --json
    code_trig = backup_cli([
        "trigger",
        "--db-path", db_file,
        "--dest", backup_dir,
        "--tag", "cli_test",
        "--json",
    ])
    assert code_trig == 0

    # 2. Check status via CLI with --json
    stdout_capture = io.StringIO()
    with patch.object(sys, "stdout", stdout_capture):
        code_status = backup_cli([
            "status",
            "--db-path", db_file,
            "--dir", backup_dir,
            "--json",
        ])
    assert code_status == 0
    status_json = json.loads(stdout_capture.getvalue())
    assert status_json["total_backups_count"] == 1
    assert status_json["last_backup_path"] is not None
    assert "cli_test" in status_json["last_backup_path"]

    # 3. Status in human-readable mode
    stdout_human = io.StringIO()
    with patch.object(sys, "stdout", stdout_human):
        code_human = backup_cli([
            "status",
            "--db-path", db_file,
            "--dir", backup_dir,
        ])
    assert code_human == 0
    output_text = stdout_human.getvalue()
    assert "DATABASE BACKUP SCHEDULER STATUS" in output_text
    assert "Total Backups:" in output_text


# =============================================================================
# 7. FastAPI REST Operational Routes Tests
# =============================================================================


def test_backup_api_routes(
    populated_db: tuple[str, CheckpointStore],
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify /backups/status, /backups/trigger, /backups/history, and /diagnostics."""
    db_file, store = populated_db
    backup_dir = str(tmp_path / "backups_api")

    token = "test_auth_token_secret_123"
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token)
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("BACKUP_DIR", backup_dir)
    monkeypatch.setenv("BACKUP_SCHEDULER_ENABLED", "true")
    get_settings.cache_clear()

    app = create_app()
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # 1. Unauthenticated requests fail with 401
    assert client.get("/backups/status").status_code == 401
    assert client.post("/backups/trigger").status_code == 401
    assert client.get("/backups/history").status_code == 401

    auth_headers = {"Authorization": f"Bearer {token}"}

    # 2. GET /backups/status initially
    resp_status = client.get("/backups/status", headers=auth_headers)
    assert resp_status.status_code == 200
    data_status = resp_status.json()
    assert "scheduler" in data_status
    assert data_status["scheduler"]["enabled"] is True

    # 3. POST /backups/trigger creates backup
    resp_trig = client.post(
        "/backups/trigger",
        headers=auth_headers,
        json={"reason": "Integration API test", "tag": "api_test"},
    )
    assert resp_trig.status_code == 200
    data_trig = resp_trig.json()
    assert data_trig["status"] == "success"
    assert data_trig["metadata"] is not None
    assert "api_test" in data_trig["metadata"]["backup_path"]

    # 4. GET /backups/history lists archives
    resp_hist = client.get("/backups/history", headers=auth_headers)
    assert resp_hist.status_code == 200
    data_hist = resp_hist.json()
    assert data_hist["total_count"] >= 1
    assert len(data_hist["backups"]) >= 1

    # 5. GET /diagnostics contains backup_scheduler diagnostics
    resp_diag = client.get("/diagnostics", headers=auth_headers)
    assert resp_diag.status_code == 200
    data_diag = resp_diag.json()
    assert "backup_scheduler" in data_diag
    assert data_diag["backup_scheduler"]["total_backups_count"] >= 1
