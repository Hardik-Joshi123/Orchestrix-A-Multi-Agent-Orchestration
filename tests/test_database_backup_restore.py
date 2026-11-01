"""Comprehensive test suite for database backup, restore, validation, and disaster recovery.

Tests cover:
- Backup creation with WAL journal mode and active connections.
- Destination directory vs custom file paths and tag naming.
- Overwrite protection against accidental destruction of existing backups.
- Pre-restore safety backup generation.
- Rejection of corrupt or invalid backup files without affecting target database.
- Active database lock and running workflow collision prevention.
- Backup retention management: count-based, age-based, dry-run, and latest preservation.
- Full subsystem table and record preservation across backup and restore.
- Interrupted workflow recovery from restored database.
- Complete CLI operational interface coverage across all subcommands.
"""

from __future__ import annotations

import json
import os
import pathlib
import sqlite3
import time

import pytest

from orchestrator.api.manager import RunManager
from orchestrator.main import cli
from orchestrator.persistence.backup import (
    EXPECTED_TABLES,
    BackupError,
    BackupManager,
    BackupOverwriteError,
    CorruptBackupError,
    DatabaseInUseError,
    create_backup,
    list_backups,
    restore_backup,
    validate_backup,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture
def test_db(tmp_path: pathlib.Path) -> str:
    """Create and initialize a populated SQLite database with WAL enabled."""
    db_file = str(tmp_path / "test_source.db")
    store = CheckpointStore(db_path=db_file)

    # 1. Create a run and checkpoint
    run_id = "run-backup-test-001"
    store.save_checkpoint(
        run_id=run_id,
        state={"task": "Analyze quarterly earnings", "status": "completed"},
        transition="supervisor->specialist",
        privileged=True,
    )

    # 2. Insert audit event
    from orchestrator.observability.audit import record_audit_event

    record_audit_event(
        event_type="workflow_started",
        outcome="success",
        actor="tester",
        metadata={"run_id": run_id},
        store=store,
    )

    # 3. Insert tool approval
    store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op_execute_shell_01",
        tool_name="shell_command",
        timeout_seconds=3600.0,
    )

    # 4. Insert transactional outbox event
    store.insert_outbox_event(
        event_type="workflow.completed",
        payload={"run_id": run_id, "status": "completed"},
        idempotency_key="idemp_outbox_001",
    )

    # 5. Insert webhook replay record
    store.record_webhook_replay(
        event_id="evt_replay_001",
        timestamp=int(time.time()),
        expires_at=time.time() + 3600.0,
    )

    store.close()
    return db_file


# ── 1. Backup Creation & WAL Consistency ─────────────────────────────────────


def test_backup_wal_database_consistency(test_db: str, tmp_path: pathlib.Path) -> None:
    """Backup an active WAL-mode database and verify completeness and integrity."""
    backup_dir = tmp_path / "backups"
    meta = create_backup(db_path=test_db, dest=backup_dir, tag="wal_test")

    assert os.path.exists(meta.backup_path)
    assert meta.size_bytes > 0
    assert meta.integrity == "ok"
    assert meta.tag == "wal_test"
    assert "runs" in meta.table_counts
    assert meta.table_counts["runs"] >= 1
    assert meta.table_counts["checkpoints"] >= 1

    # Verify backup is a valid standalone SQLite database
    conn = sqlite3.connect(meta.backup_path)
    chk = conn.execute("PRAGMA integrity_check;").fetchone()
    assert chk and chk[0] == "ok"
    run_row = conn.execute("SELECT run_id, task FROM runs;").fetchone()
    assert run_row[0] == "run-backup-test-001"
    conn.close()


def test_backup_destination_directory_and_custom_path(test_db: str, tmp_path: pathlib.Path) -> None:
    """Verify backups can be written to a directory (timestamped) or exact file path."""
    backup_dir = tmp_path / "auto_named"
    meta1 = create_backup(db_path=test_db, dest=backup_dir)
    assert os.path.dirname(meta1.backup_path) == str(backup_dir)
    assert os.path.basename(meta1.backup_path).startswith("backup_")

    exact_file = tmp_path / "exact_destination.db"
    meta2 = create_backup(db_path=test_db, dest=exact_file)
    assert meta2.backup_path == str(exact_file)
    assert os.path.exists(exact_file)


def test_backup_overwrite_protection(test_db: str, tmp_path: pathlib.Path) -> None:
    """Accidental overwriting of an existing backup file raises BackupOverwriteError."""
    target_backup = tmp_path / "protected_backup.db"
    create_backup(db_path=test_db, dest=target_backup)

    with pytest.raises(BackupOverwriteError, match="already exists"):
        create_backup(db_path=test_db, dest=target_backup, overwrite=False)

    # Allowed with explicit overwrite=True
    meta = create_backup(db_path=test_db, dest=target_backup, overwrite=True)
    assert meta.backup_path == str(target_backup)


def test_backup_rejects_in_memory_database() -> None:
    """Attempting file backup of ':memory:' without a persistent path raises BackupError."""
    with pytest.raises(BackupError, match="in-memory"):
        create_backup(db_path=":memory:")


def test_backup_rejects_non_existent_source(tmp_path: pathlib.Path) -> None:
    """Attempting backup of a non-existent database file raises BackupError."""
    missing = tmp_path / "does_not_exist.db"
    with pytest.raises(BackupError, match="not found"):
        create_backup(db_path=missing)


# ── 2. Backup Validation ─────────────────────────────────────────────────────


def test_validate_backup_detects_valid_archive(test_db: str, tmp_path: pathlib.Path) -> None:
    """validate_backup confirms integrity, foreign keys, and tables on valid archives."""
    backup_file = tmp_path / "valid_archive.db"
    create_backup(db_path=test_db, dest=backup_file)

    val = validate_backup(backup_file, deep=True)
    assert val.is_valid is True
    assert val.integrity_check == "ok"
    assert val.quick_check == "ok"
    assert val.foreign_key_check == "ok"
    assert "runs" in val.tables
    assert "checkpoints" in val.tables
    assert val.total_records > 0
    assert not val.errors


def test_validate_backup_detects_corrupt_file(tmp_path: pathlib.Path) -> None:
    """validate_backup identifies corrupt byte headers or invalid file data."""
    corrupt_file = tmp_path / "corrupt.db"
    corrupt_file.write_bytes(b"NOT_A_SQLITE_FILE_CONTENT_AT_ALL_" * 10)

    val = validate_backup(corrupt_file)
    assert val.is_valid is False
    assert val.integrity_check == "invalid_header"
    assert any("magic header" in err for err in val.errors)


def test_validate_backup_detects_missing_file(tmp_path: pathlib.Path) -> None:
    """validate_backup returns invalid status for non-existent files."""
    missing_file = tmp_path / "missing.db"
    val = validate_backup(missing_file)
    assert val.is_valid is False
    assert val.integrity_check == "file_not_found"


# ── 3. Database Restoration & Disaster Recovery ──────────────────────────────


def test_restore_into_clean_target(test_db: str, tmp_path: pathlib.Path) -> None:
    """Restore into a non-existent target path correctly initializes SQLite WAL."""
    backup_file = tmp_path / "source_backup.db"
    create_backup(db_path=test_db, dest=backup_file)

    clean_target = tmp_path / "restored_target.db"
    res = restore_backup(backup_path=backup_file, target_db_path=clean_target)

    assert res.success is True
    assert res.integrity == "ok"
    assert os.path.exists(clean_target)
    assert "runs" in res.restored_tables
    assert res.table_counts["runs"] >= 1

    # Verify restored records
    store = CheckpointStore(db_path=clean_target)
    summary = store.get_run_summary("run-backup-test-001")
    assert summary is not None
    assert summary["task"] == "Analyze quarterly earnings"
    store.close()


def test_restore_corrupt_backup_rejected_and_leaves_target_untouched(
    test_db: str,
    tmp_path: pathlib.Path,
) -> None:
    """Attempting restore with a corrupt backup is rejected and leaves target intact."""
    corrupt_backup = tmp_path / "bad_backup.db"
    corrupt_backup.write_bytes(b"GARBAGE_PAYLOAD_NOT_SQLITE")

    with pytest.raises(CorruptBackupError, match="validation failed"):
        restore_backup(backup_path=corrupt_backup, target_db_path=test_db)

    # Verify original target is unmodified and healthy
    store = CheckpointStore(db_path=test_db)
    summary = store.get_run_summary("run-backup-test-001")
    assert summary is not None
    store.close()


def test_restore_creates_pre_restore_safety_backup(tmp_path: pathlib.Path) -> None:
    """Restoring over an existing database automatically creates a safety backup."""
    target_db = tmp_path / "active_target.db"
    store1 = CheckpointStore(db_path=target_db)
    store1.save_checkpoint(
        run_id="run-initial-state",
        state={"task": "Original task", "status": "completed"},
        privileged=True,
    )
    store1.close()

    source_db = tmp_path / "incoming_source.db"
    store2 = CheckpointStore(db_path=source_db)
    store2.save_checkpoint(
        run_id="run-incoming-state",
        state={"task": "New task", "status": "completed"},
        privileged=True,
    )
    store2.close()

    backup_file = tmp_path / "incoming_backup.db"
    create_backup(db_path=source_db, dest=backup_file)

    safety_dir = tmp_path / "safety_vault"
    res = restore_backup(
        backup_path=backup_file,
        target_db_path=target_db,
        safety_backup=True,
        safety_backup_dest=safety_dir,
    )

    assert res.success is True
    assert res.safety_backup_path is not None
    assert os.path.exists(res.safety_backup_path)

    # Verify safety backup has original state
    conn_safety = sqlite3.connect(res.safety_backup_path)
    safety_row = conn_safety.execute("SELECT run_id FROM runs;").fetchone()
    assert safety_row[0] == "run-initial-state"
    conn_safety.close()

    # Verify target database now has incoming state
    conn_target = sqlite3.connect(target_db)
    target_row = conn_target.execute("SELECT run_id FROM runs;").fetchone()
    assert target_row[0] == "run-incoming-state"
    conn_target.close()


def test_restore_prevents_active_database_overwrite_without_force(tmp_path: pathlib.Path) -> None:
    """Restoring over a database with active runs is rejected unless force=True."""
    target_db = tmp_path / "busy_target.db"
    store = CheckpointStore(db_path=target_db)
    # Save a run with active 'running' status
    store.save_checkpoint(
        run_id="run-active-001",
        state={"task": "In-flight run", "status": "running"},
        privileged=True,
    )
    store.close()

    source_db = tmp_path / "fresh_source.db"
    store_src = CheckpointStore(db_path=source_db)
    store_src.save_checkpoint(
        run_id="run-fresh-002",
        state={"task": "Finished run", "status": "completed"},
        privileged=True,
    )
    store_src.close()

    backup_file = tmp_path / "fresh_backup.db"
    create_backup(db_path=source_db, dest=backup_file)

    # Without force: must be rejected with DatabaseInUseError
    with pytest.raises(DatabaseInUseError, match="active workflows"):
        restore_backup(backup_path=backup_file, target_db_path=target_db, force=False)

    # With force=True: permitted
    res = restore_backup(backup_path=backup_file, target_db_path=target_db, force=True)
    assert res.success is True


# ── 4. Backup Retention Management ───────────────────────────────────────────


def test_retention_count_cleanup(test_db: str, tmp_path: pathlib.Path) -> None:
    """clean_backups removes oldest backups exceeding keep_count."""
    vault = tmp_path / "vault_count"
    manager = BackupManager(db_path=test_db, backup_dir=vault)

    # Generate 5 backups with slight timestamp spacing
    for i in range(5):
        manager.create_backup(tag=f"batch_{i}")
        time.sleep(0.05)

    assert len(manager.list_backups()) == 5

    # Retain only the 3 newest backups
    res = manager.clean_backups(keep_count=3, keep_days=0, dry_run=False)
    assert res.total_retained == 3
    assert res.total_deleted == 2
    assert len(manager.list_backups()) == 3


def test_retention_age_cleanup(test_db: str, tmp_path: pathlib.Path) -> None:
    """clean_backups purges backups whose mtime is older than keep_days."""
    vault = tmp_path / "vault_age"
    manager = BackupManager(db_path=test_db, backup_dir=vault)

    b1 = manager.create_backup(tag="old_backup")
    time.sleep(0.05)
    b2 = manager.create_backup(tag="new_backup")

    # Set mtime of b1 to 40 days in the past
    past_mtime = time.time() - (40 * 86400)
    os.utime(b1.backup_path, (past_mtime, past_mtime))

    res = manager.clean_backups(keep_count=10, keep_days=30, dry_run=False)
    assert res.total_deleted == 1
    assert b1.backup_path in res.deleted
    assert b2.backup_path in res.retained


def test_retention_protects_latest_backup(test_db: str, tmp_path: pathlib.Path) -> None:
    """clean_backups never purges the single latest backup, even if expired by age."""
    vault = tmp_path / "vault_protect"
    manager = BackupManager(db_path=test_db, backup_dir=vault)

    b1 = manager.create_backup(tag="sole_backup")
    past_mtime = time.time() - (60 * 86400)
    os.utime(b1.backup_path, (past_mtime, past_mtime))

    # keep_days=1, keep_count=0: single backup must be preserved
    res = manager.clean_backups(keep_count=0, keep_days=1, dry_run=False)
    assert res.total_deleted == 0
    assert res.total_retained == 1
    assert os.path.exists(b1.backup_path)


def test_retention_dry_run(test_db: str, tmp_path: pathlib.Path) -> None:
    """dry_run=True returns candidate files for deletion without removing them."""
    vault = tmp_path / "vault_dry"
    manager = BackupManager(db_path=test_db, backup_dir=vault)

    for i in range(4):
        manager.create_backup(tag=f"dry_{i}")
        time.sleep(0.05)

    res = manager.clean_backups(keep_count=2, dry_run=True)
    assert res.dry_run is True
    assert res.total_deleted == 2
    # All 4 files must still exist on disk
    for path in res.deleted:
        assert os.path.exists(path)


# ── 5. End-to-End Subsystem Preservation & Recovery ──────────────────────────


def test_preservation_across_all_subsystems(tmp_path: pathlib.Path) -> None:
    """Verify that all 13 core tables and their records are preserved across restore."""
    source_db = str(tmp_path / "all_subsystems_src.db")
    store = CheckpointStore(db_path=source_db)

    # Populate every table
    conn = sqlite3.connect(source_db)
    with conn:
        conn.execute(
            "INSERT INTO runs (run_id, task, status, latest_step, created_at) "
            "VALUES ('r1', 't1', 'completed', 1, '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO checkpoints (run_id, step, transition, state_json, created_at) "
            "VALUES ('r1', 1, 'tr1', '{}', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO audit_events (event_id, timestamp, event_type, outcome, created_at) "
            "VALUES ('a1', '2026-01-01', 'test_event', 'ok', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO idempotency_keys (key, client_scope, request_hash, run_id, status, "
            "created_at, updated_at) VALUES ('k1', 'default', 'h1', 'r1', 'completed', "
            "'2026-01-01', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO workflow_admissions (run_id, owner_id, status, acquired_at, heartbeat_at, "
            "expires_at, fence_token) VALUES ('r1', 'owner1', 'active', '2026-01-01', '2026-01-01', "
            "'2026-01-01', 1);"
        )
        conn.execute(
            "INSERT INTO tool_executions (operation_key, run_id, subtask_id, tool_name, fence_token, "
            "status, args_json, created_at, updated_at) "
            "VALUES ('e1', 'r1', 's1', 'tool1', 1, 'completed', '{}', '2026-01-01', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO tool_approvals (operation_key, run_id, tool_name, approver, decision, created_at) "
            "VALUES ('ap1', 'r1', 'tool1', 'admin', 'pending', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO scheduler_leases (lease_name, owner_id, status, acquired_at, heartbeat_at, "
            "expires_at, fence_token) VALUES ('s1', 'owner1', 'active', '2026-01-01', '2026-01-01', "
            "'2026-01-01', 1);"
        )
        conn.execute(
            "INSERT INTO outbox_events (event_id, event_type, payload, created_at, status, "
            "attempt_count, next_retry_at, fence_token, retry_count) "
            "VALUES ('o1', 'ev1', '{}', '2026-01-01', 'pending', 0, '2026-01-01', 1, 0);"
        )
        conn.execute(
            "INSERT INTO outbox_delivery_history (event_id, attempt_number, action, status, recorded_at) "
            "VALUES ('o1', 1, 'attempt', 'success', '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO webhook_replay_records (event_id, timestamp, expires_at, recorded_at) "
            "VALUES ('w1', 1000, 2000.0, '2026-01-01');"
        )
        conn.execute(
            "INSERT INTO webhook_replay_cleanup_log (cleaned_at, deleted_count, duration_ms, trigger_type) "
            "VALUES ('2026-01-01', 5, 10.0, 'manual');"
        )
        conn.execute(
            "INSERT INTO celery_worker_heartbeats (worker_id, status, registered_at, "
            "last_heartbeat_at, active_tasks, queues, metadata_json) "
            "VALUES ('w1', 'online', '2026-01-01', '2026-01-01', 2, '[]', '{}');"
        )
    conn.close()
    store.close()

    # Backup source
    backup_file = tmp_path / "all_subsystems.bak"
    create_backup(db_path=source_db, dest=backup_file)

    # Restore into fresh target
    dest_db = str(tmp_path / "all_subsystems_dest.db")
    res = restore_backup(backup_path=backup_file, target_db_path=dest_db)

    assert res.success is True
    for expected_table in EXPECTED_TABLES:
        assert expected_table in res.restored_tables
        assert res.table_counts[expected_table] == 1


def test_recovery_after_process_interruption_from_restored_backup(tmp_path: pathlib.Path) -> None:
    """Interrupted runs survive backup/restore and are safely recovered on startup."""
    source_db = str(tmp_path / "crash_source.db")
    store = CheckpointStore(db_path=source_db)

    # Simulate an in-flight run interrupted by process crash (no lease owner active)
    interrupted_id = "run-crashed-123"
    store.save_checkpoint(
        run_id=interrupted_id,
        state={"task": "Critical financial computation", "status": "running"},
        transition="supervisor->analysis",
        privileged=True,
    )
    store.close()

    # Backup and restore to simulated new server node
    backup_file = tmp_path / "crash_snapshot.db"
    create_backup(db_path=source_db, dest=backup_file)

    new_node_db = str(tmp_path / "new_node.db")
    restore_backup(backup_path=backup_file, target_db_path=new_node_db)

    # Startup recovery on the new node
    recovered_store = CheckpointStore(db_path=new_node_db)
    manager = RunManager()
    recovered = manager.recover_interrupted_runs(store=recovered_store)

    assert interrupted_id in recovered
    summary = recovered_store.get_run_summary(interrupted_id)
    assert summary is not None
    assert summary["status"] == "interrupted"
    recovered_store.close()


# ── 6. Operational CLI Interfaces ────────────────────────────────────────────


def test_cli_backup_create_and_list(test_db: str, tmp_path: pathlib.Path, capsys) -> None:
    """CLI 'backup create' and 'backup list' execute and output JSON."""
    vault = tmp_path / "cli_vault"

    # Create backup via CLI
    rc_create = cli(
        ["backup", "create", "--db-path", test_db, "--dest", str(vault), "--tag", "cli_test", "--json"]
    )
    assert rc_create == 0
    out_create = json.loads(capsys.readouterr().out)
    assert out_create["integrity"] == "ok"
    assert "backup_path" in out_create

    # List backups via CLI
    rc_list = cli(["backup", "list", "--dir", str(vault), "--json"])
    assert rc_list == 0
    out_list = json.loads(capsys.readouterr().out)
    assert len(out_list) == 1
    assert out_list[0]["tag"] == "cli_test"


def test_cli_backup_validate_and_restore(test_db: str, tmp_path: pathlib.Path, capsys) -> None:
    """CLI 'backup validate' and 'backup restore' execute properly."""
    backup_file = str(tmp_path / "cli_val_backup.db")
    create_backup(db_path=test_db, dest=backup_file)

    # Validate CLI
    rc_val = cli(["backup", "validate", backup_file, "--json"])
    assert rc_val == 0
    out_val = json.loads(capsys.readouterr().out)
    assert out_val["is_valid"] is True
    assert out_val["integrity_check"] == "ok"

    # Restore CLI
    target_file = str(tmp_path / "cli_restored.db")
    rc_res = cli(["backup", "restore", backup_file, "--target", target_file, "--json"])
    assert rc_res == 0
    out_res = json.loads(capsys.readouterr().out)
    assert out_res["success"] is True
    assert out_res["integrity"] == "ok"
    assert os.path.exists(target_file)


def test_cli_backup_clean(test_db: str, tmp_path: pathlib.Path, capsys) -> None:
    """CLI 'backup clean' supports --dry-run and active deletion."""
    vault = tmp_path / "cli_clean_vault"
    manager = BackupManager(db_path=test_db, backup_dir=vault)
    for i in range(3):
        manager.create_backup(tag=f"t_{i}")
        time.sleep(0.05)

    rc_dry = cli(["backup", "clean", "--dir", str(vault), "--keep-count", "1", "--dry-run", "--json"])
    assert rc_dry == 0
    out_dry = json.loads(capsys.readouterr().out)
    assert out_dry["dry_run"] is True
    assert out_dry["total_deleted"] == 2

    # Active clean
    rc_clean = cli(["backup", "clean", "--dir", str(vault), "--keep-count", "1", "--json"])
    assert rc_clean == 0
    out_clean = json.loads(capsys.readouterr().out)
    assert out_clean["dry_run"] is False
    assert out_clean["total_deleted"] == 2
    assert len(list_backups(dest_dir=vault)) == 1
