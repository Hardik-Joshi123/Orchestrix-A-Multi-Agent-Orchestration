"""Comprehensive test suite for automated database backup restore drills & verification.

Tests cover:
- Isolated restore verification: non-destructive restoration into temporary SQLite database.
- SQLite integrity verification: PRAGMA integrity_check, quick_check, foreign_key_check.
- Recovery verification: table inventory comparison and critical record JSON deserialization.
- Rejection of corrupt, truncated, or invalid backup archives.
- Live database protection: ensuring live production data is never touched or modified.
- Safe cleanup of temporary databases and journals even on failure.
- Scheduler integration: cross-process leadership fencing and concurrency prevention.
- CheckpointStore operational history persistence and retention pruning.
- CLI subcommands: `orchestrator backup drill run|status|history` with JSON formatting.
- REST API authenticated endpoints: `/backups/drill/status`, `/trigger`, `/history`.
"""

from __future__ import annotations

import datetime
import json
import os
import pathlib
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.persistence.backup import (
    BackupError,
    BackupManager,
    RestoreDrillResult,
    backup_cli,
)
from orchestrator.persistence.scheduler import (
    DatabaseBackupScheduler,
    reset_backup_scheduler,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture
def populated_db(tmp_path: pathlib.Path) -> tuple[str, CheckpointStore]:
    """Create a fully populated SQLite database with all critical subsystem tables."""
    db_file = str(tmp_path / "production_live.db")
    store = CheckpointStore(db_path=db_file)
    
    # 1. runs & checkpoints
    run_id = "run-drill-001"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Security analysis", "status": "completed", "step": 3},
        transition="supervisor->reviewer",
        privileged=True,
    )

    # Update runs record status
    with store._get_db_lock(db_file):
        conn = store._get_connection()
        try:
            with conn:
                conn.execute(
                    "UPDATE runs SET status='completed' WHERE run_id=?;",
                    (run_id,),
                )
        finally:
            conn.close()

    # 2. audit_events
    from orchestrator.observability.audit import (
        AuditEventType,
        AuditOutcome,
        record_audit_event,
    )

    record_audit_event(
        event_type=AuditEventType.WORKFLOW_CREATED,
        outcome=AuditOutcome.SUCCESS,
        actor="drill-tester",
        metadata={"run_id": run_id, "mode": "isolated"},
        store=store,
    )

    # 3. outbox_events
    with store._get_db_lock(db_file):
        conn = store._get_connection()
        try:
            with conn:
                now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
                conn.execute(
                    """
                    INSERT INTO outbox_events (
                        event_id, event_type, payload, created_at, status, next_retry_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (
                        "evt-drill-100",
                        "workflow.completed",
                        json.dumps({"event": "completed", "run_id": run_id}),
                        now_str,
                        "pending",
                        now_str,
                    ),
                )
        finally:
            conn.close()

    # 4. scheduler_leases
    store.acquire_scheduler_lease(
        lease_name="database_backup",
        owner_id="worker-node-1",
        lease_ttl_seconds=120.0,
    )

    # 5. workflow_admissions
    store.acquire_workflow_admission(
        run_id=run_id,
        owner_id="worker-node-1",
        max_active_runs=10,
    )

    return db_file, store


@pytest.fixture
def backup_dir(tmp_path: pathlib.Path) -> str:
    """Provide a dedicated backup directory."""
    bdir = tmp_path / "backups"
    bdir.mkdir(parents=True, exist_ok=True)
    return str(bdir)


class TestIsolatedRestoreDrill:
    """Tests for core isolated restore verification and recovery checks."""

    def test_run_restore_drill_success(self, populated_db: tuple[str, CheckpointStore], backup_dir: str):
        """A valid backup restores cleanly, verifies schema and records, and cleans up."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        # Create valid backup
        meta = mgr.create_backup(dest=backup_dir, tag="drill_valid")
        assert os.path.isfile(meta.backup_path)

        # Run restore drill
        res = mgr.run_restore_drill(backup_path=meta.backup_path, sample_records=True)

        assert isinstance(res, RestoreDrillResult)
        assert res.success is True
        assert res.integrity_check == "ok"
        assert res.quick_check == "ok"
        assert res.foreign_key_check == "ok"
        assert len(res.errors) == 0
        assert res.cleaned_up is True
        assert res.records_verified > 0
        assert res.queries_verified > 0
        assert "runs" in res.tables_verified
        assert "checkpoints" in res.tables_verified
        assert "audit_events" in res.tables_verified
        assert "outbox_events" in res.tables_verified
        assert res.duration_ms > 0.0
        assert res.backup_age_seconds >= 0.0

        # Verify isolated temporary database was deleted
        assert res.target_temp_path is not None
        assert not os.path.exists(res.target_temp_path)

    def test_run_restore_drill_selects_latest_by_default(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """When backup_path is None, run_restore_drill picks the latest valid archive."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        mgr.create_backup(dest=backup_dir, tag="first")
        time.sleep(0.05)
        meta2 = mgr.create_backup(dest=backup_dir, tag="second")

        res = mgr.run_restore_drill(backup_path=None)
        assert res.success is True
        assert res.backup_path == meta2.backup_path

    def test_run_restore_drill_no_backups_found(self, tmp_path: pathlib.Path, backup_dir: str):
        """Drill returns failure gracefully when backup directory contains no archives."""
        db_file = str(tmp_path / "empty.db")
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        res = mgr.run_restore_drill()
        assert res.success is False
        assert any("No backup archives found" in err for err in res.errors)

    def test_run_restore_drill_corrupt_header_rejected(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Corrupt or truncated file without valid SQLite header fails drill immediately."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        corrupt_path = os.path.join(backup_dir, "corrupt_backup.db")
        with open(corrupt_path, "wb") as f:
            f.write(b"NOT_A_SQLITE_DATABASE_HEADER_GARBAGE_BYTES")

        res = mgr.run_restore_drill(backup_path=corrupt_path)
        assert res.success is False
        assert res.integrity_check == "header_invalid"
        assert any("magic header" in err or "too small" in err for err in res.errors)
        assert res.cleaned_up is True

    def test_run_restore_drill_damaged_btree_rejected(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """A backup file with corrupted internal pages fails SQLite integrity check."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        meta = mgr.create_backup(dest=backup_dir, tag="damage_source")
        damaged_path = os.path.join(backup_dir, "damaged_backup.db")

        with open(meta.backup_path, "rb") as fin, open(damaged_path, "wb") as fout:
            content = bytearray(fin.read())
            # Overwrite middle pages while keeping 16-byte magic header intact
            if len(content) > 200:
                content[100:200] = b"\x00" * 100
            fout.write(content)

        res = mgr.run_restore_drill(backup_path=damaged_path)
        assert res.success is False
        assert res.integrity_check != "ok" or len(res.errors) > 0
        assert res.cleaned_up is True

    def test_restore_drill_live_database_untouched(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Restoration drill strictly guarantees production database is never modified."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        meta = mgr.create_backup(dest=backup_dir, tag="protect_live")

        # Snapshot live database state
        conn = sqlite3.connect(db_file)
        live_runs_before = conn.execute("SELECT run_id, status FROM runs;").fetchall()
        live_audit_before = conn.execute("SELECT COUNT(*) FROM audit_events;").fetchone()[0]
        live_size_before = os.path.getsize(db_file)
        conn.close()

        # Execute restore drill
        res = mgr.run_restore_drill(backup_path=meta.backup_path)
        assert res.success is True

        # Verify live database is completely identical
        conn = sqlite3.connect(db_file)
        live_runs_after = conn.execute("SELECT run_id, status FROM runs;").fetchall()
        live_audit_after = conn.execute("SELECT COUNT(*) FROM audit_events;").fetchone()[0]
        live_size_after = os.path.getsize(db_file)
        conn.close()

        assert live_runs_before == live_runs_after
        assert live_audit_before == live_audit_after
        assert live_size_before == live_size_after

    def test_safety_check_prevents_targeting_live_db(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch: pytest.MonkeyPatch
    ):
        """Safety check raises BackupError if drill target path matches live DB."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="live_target_test")

        # Patch temp path calculation to collide with live DB path

        real_join = os.path.join

        def fake_join(a: str, b: str) -> str:
            if "restore_drill_" in b:
                return os.path.abspath(db_file)
            return real_join(a, b)

        monkeypatch.setattr(os.path, "join", fake_join)

        with pytest.raises(BackupError, match="CRITICAL: Drill target matches live production database path"):
            mgr.run_restore_drill(backup_path=meta.backup_path)

    def test_restore_drill_custom_temp_dir_cleanup(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, tmp_path: pathlib.Path
    ):
        """Custom target_temp_dir safely removes drill db and journal files."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="custom_dir")

        custom_temp = tmp_path / "custom_drill_workspace"
        custom_temp.mkdir(parents=True, exist_ok=True)

        res = mgr.run_restore_drill(
            backup_path=meta.backup_path,
            target_temp_dir=str(custom_temp),
        )
        assert res.success is True
        assert res.cleaned_up is True
        # Check that no .db or journal files remain in custom_temp
        remaining = list(custom_temp.glob("*.db*"))
        assert len(remaining) == 0

    def test_restore_drill_detects_invalid_json_state(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Restore drill flags JSON deserialization failure on corrupted record state."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="json_check")

        # Intentionally inject invalid JSON into the backup's checkpoints table
        conn = sqlite3.connect(meta.backup_path)
        conn.execute("UPDATE checkpoints SET state_json='INVALID_NON_JSON_MALFORMED{{{';")
        conn.commit()
        conn.close()
        # Regenerate manifest so archive checksum matches and drill tests JSON parsing
        mgr.create_manifest(meta.backup_path)

        res = mgr.run_restore_drill(backup_path=meta.backup_path, sample_records=True)
        assert res.success is False
        assert any("JSON deserialization failure" in err for err in res.errors)


class TestSchedulerRestoreDrills:
    """Tests for scheduler integration, cross-process fencing, and concurrency."""

    def test_scheduler_run_restore_drill_as_leader(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Leader scheduler executes drill, records SQLite history and audit events."""
        db_file, store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="sched_leader")

        scheduler = DatabaseBackupScheduler(
            store=store,
            backup_manager=mgr,
            backup_dir=backup_dir,
            restore_drill_enabled=True,
            owner_id="leader-worker-1",
        )
        scheduler._is_leader = True
        scheduler._fence_token = 42

        result = scheduler.run_restore_drill(actor="scheduler")
        assert result["status"] == "success"
        assert result["fence_token"] == 42
        assert "drill" in result
        assert result["drill"]["success"] is True

        # Verify operational metrics updated
        status = scheduler.get_status()
        assert status["total_drills_count"] >= 1
        assert status["last_drill_status"] == "success"
        assert status["last_drill_at"] is not None

        # Verify SQLite drill history record
        latest_drill = store.get_latest_restore_drill()
        assert latest_drill is not None
        assert latest_drill["status"] == "success"
        assert latest_drill["fence_token"] == 42
        assert latest_drill["tables_verified"] > 0

    def test_scheduler_standby_skips_unforced_drill(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Standby scheduler instance skips unforced periodic drill execution."""
        db_file, store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="standby")

        scheduler = DatabaseBackupScheduler(
            store=store,
            backup_manager=mgr,
            backup_dir=backup_dir,
            restore_drill_enabled=True,
            owner_id="standby-worker-2",
        )
        scheduler._is_leader = False

        result = scheduler.run_restore_drill(actor="scheduler", force=False)
        assert result["status"] == "skipped"
        assert "standby mode" in result["message"]

    def test_scheduler_trigger_restore_drill_forces_execution(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Manual trigger_restore_drill forces execution even on standby instance."""
        db_file, store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="force_drill")

        scheduler = DatabaseBackupScheduler(
            store=store,
            backup_manager=mgr,
            backup_dir=backup_dir,
            restore_drill_enabled=False,
            owner_id="manual-worker",
        )
        scheduler._is_leader = False

        result = scheduler.trigger_restore_drill(
            actor="manual",
            operator="admin_alice",
            reason="Ad-hoc compliance verification",
        )
        assert result["status"] == "success"
        assert result["drill"]["success"] is True

    def test_scheduler_prevents_overlapping_drills(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Non-overlapping lock prevents concurrent drills within the same scheduler."""
        db_file, store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="overlap")

        scheduler = DatabaseBackupScheduler(
            store=store,
            backup_manager=mgr,
            backup_dir=backup_dir,
            restore_drill_enabled=True,
            owner_id="lock-worker",
        )
        scheduler._is_leader = True

        # Artificially acquire drill exec lock
        assert scheduler._drill_exec_lock.acquire(blocking=False)
        try:
            result = scheduler.run_restore_drill()
            assert result["status"] == "skipped"
            assert "already in progress" in result["message"]
        finally:
            scheduler._drill_exec_lock.release()

    def test_store_history_retention_pruning(
        self, populated_db: tuple[str, CheckpointStore]
    ):
        """CheckpointStore prunes older drill records beyond configured limit."""
        _db_file, store = populated_db

        # Insert 6 drill records with retention_limit=3
        for i in range(1, 7):
            store.record_restore_drill(
                drill_id=f"drill-prune-{i}",
                started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                status="success",
                backup_path=f"/fake/backup_{i}.db",
                retention_limit=3,
            )

        drills = store.list_restore_drills(limit=10)
        assert len(drills) == 3
        # Ensure latest drills are retained
        drill_ids = [d["drill_id"] for d in drills]
        assert drill_ids == ["drill-prune-6", "drill-prune-5", "drill-prune-4"]


class TestCLIRestoreDrills:
    """Tests for CLI subcommands `orchestrator backup drill run|status|history`."""

    def test_cli_drill_run_success(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys: pytest.CaptureFixture[str]
    ):
        """CLI `backup drill run` executes drill and prints human-readable summary."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="cli_run")

        code = backup_cli(["drill", "run", "--db-path", db_file, "--dir", backup_dir])
        captured = capsys.readouterr()

        assert code == 0
        assert "=== DATABASE RESTORE DRILL: SUCCESS ===" in captured.out
        assert "Integrity check:   ok" in captured.out
        assert "Cleaned up:        True" in captured.out

    def test_cli_drill_run_json(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys: pytest.CaptureFixture[str]
    ):
        """CLI `backup drill run --json` outputs parseable JSON."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        mgr.create_backup(dest=backup_dir, tag="cli_json")

        code = backup_cli(["drill", "run", "--db-path", db_file, "--dir", backup_dir, "--json"])
        captured = capsys.readouterr()

        assert code == 0
        data = json.loads(captured.out)
        assert data["success"] is True
        assert data["integrity_check"] == "ok"
        assert "drill_id" in data

    def test_cli_drill_status_and_history(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys: pytest.CaptureFixture[str]
    ):
        """CLI `backup drill status` and `backup drill history` display stored history."""
        db_file, store = populated_db
        store.record_restore_drill(
            drill_id="drill-cli-hist-1",
            started_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            status="success",
            backup_path="/fake/backup.db",
            duration_ms=45.2,
        )

        code_stat = backup_cli(["drill", "status", "--db-path", db_file, "--json"])
        cap_stat = capsys.readouterr()
        assert code_stat == 0
        stat_data = json.loads(cap_stat.out)
        assert stat_data["drill_id"] == "drill-cli-hist-1"

        code_hist = backup_cli(["drill", "history", "--db-path", db_file, "--json"])
        cap_hist = capsys.readouterr()
        assert code_hist == 0
        hist_data = json.loads(cap_hist.out)
        assert len(hist_data) >= 1
        assert hist_data[0]["drill_id"] == "drill-cli-hist-1"


class TestAPIRestoreDrills:
    """Tests for FastAPI endpoints under `/backups/drill/`."""

    @pytest.fixture
    def test_client(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch: pytest.MonkeyPatch
    ) -> TestClient:
        """Create authenticated TestClient with backing store and scheduler."""
        db_file, store = populated_db
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "valid-test-token-drill")
        get_settings.cache_clear()

        app = create_app()
        app.state.store = store
        reset_backup_scheduler()
        scheduler = DatabaseBackupScheduler(
            store=store,
            backup_dir=backup_dir,
            restore_drill_enabled=True,
        )
        app.state.backup_scheduler = scheduler
        return TestClient(app)

    def test_api_status_requires_auth(self, test_client: TestClient):
        """GET /backups/drill/status returns 401 without valid bearer token."""
        resp = test_client.get("/backups/drill/status")
        assert resp.status_code == 401

    def test_api_drill_status_and_trigger(
        self, test_client: TestClient, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """POST /backups/drill/trigger executes drill, reflected in GET /backups/drill/status."""
        db_file, _store = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="api_drill")

        headers = {"Authorization": "Bearer valid-test-token-drill"}

        # 1. Trigger drill
        post_resp = test_client.post(
            "/backups/drill/trigger",
            headers=headers,
            json={"backup_path": meta.backup_path, "reason": "Automated API drill test"},
        )
        assert post_resp.status_code == 200
        post_data = post_resp.json()
        assert post_data["status"] == "success"
        assert post_data["drill"] is not None
        assert post_data["drill"]["success"] is True

        # 2. Inspect status
        get_resp = test_client.get("/backups/drill/status", headers=headers)
        assert get_resp.status_code == 200
        get_data = get_resp.json()
        assert get_data["enabled"] is True
        assert get_data["last_drill_status"] == "success"
        assert get_data["total_drills_count"] >= 1

        # 3. List history
        hist_resp = test_client.get("/backups/drill/history", headers=headers)
        assert hist_resp.status_code == 200
        hist_data = hist_resp.json()
        assert hist_data["count"] >= 1
        assert hist_data["drills"][0]["status"] == "success"

        # 4. Plural alias check
        alias_resp = test_client.get("/backups/drills/status", headers=headers)
        assert alias_resp.status_code == 200
