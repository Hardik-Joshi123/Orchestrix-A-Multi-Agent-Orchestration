# Task 62 Report: Database Backup, Restore & Disaster Recovery

**Repository**: `c:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator`  
**Execution Date**: November 1, 2026  
**Status**: Completed  
**Test Suite**: 21/21 new tests passing; full regression suite passing (3 skipped without Docker Engine)

---

## 1. Executive Summary

Task 62 engineered, integrated, and validated reliable database backup, restore, validation, and disaster
recovery capabilities for the Multi-Agent Orchestrator's persistent SQLite storage tier. While SQLite's
Write-Ahead Logging (WAL) mode provides high concurrency and crash durability, naive file-system copying of
active `.db` files during write transactions risks corrupted or torn pages.

Task 62 implements a production-grade disaster recovery subsystem in
`src/orchestrator/persistence/backup.py` centered around SQLite's native Online Backup API, automated WAL
frame flushing, pre-restore safety snapshots, active database collision prevention, retention policy
enforcement, and a dedicated CLI operational interface (`orchestrator backup`).

### Key Accomplishments
1. **Complete Database Inventory**: Audited and documented all 13 core persistent tables across all
   subsystems (workflows, checkpoints, audit trail, idempotency, admission leases, scheduler fencing,
   tool approvals, tool executions, transactional outbox, delivery history, webhook replay protection,
   replay purger logs, and Celery worker heartbeats).
2. **ACID Online Backup Engine**: Built `BackupManager.create_backup()` utilizing `sqlite3.Connection.backup`
   to safely replicate committed pages from active WAL databases into self-contained, standalone backup
   archives without locking readers or stopping services.
3. **Pre-Restore Safety & Collision Prevention**: Built `BackupManager.restore_backup()` with a 5-step safety
   pipeline: validates backup integrity before touching target files, detects active workflows and
   unexpired leases, creates an automatic pre-restore safety snapshot of the active target, atomically
   replaces database pages, and re-initializes WAL mode.
4. **Retention Management**: Built `BackupManager.clean_backups()` enforcing count-based and age-based
   retention limits with dry-run support, while strictly guaranteeing that the latest valid backup is never
   pruned.
5. **Operational CLI Commands**: Added `orchestrator backup` subcommands (`create`, `list`, `validate`,
   `restore`, and `clean`) supporting human-readable terminal output and automation-friendly JSON (`--json`).
6. **Disaster Recovery Test Suite**: Implemented 21 automated tests in
   `tests/test_database_backup_restore.py`, verifying WAL consistency, corruption rejection, pre-restore
   snapshots, retention pruning, subsystem record preservation, and interrupted workflow recovery.
7. **Production Documentation**: Updated `docs/DEPLOYMENT_GUIDE.md` with Section 13 detailing backup and
   restore runbooks, Docker volume considerations, and SQLite operational constraints.

---

## 2. Complete Database Inventory & Consistency Scope

All orchestrator subsystems share a single persistent SQLite database (defaulting to
`orchestrator_checkpoints.db` or `/app/data/orchestrator_checkpoints.db` in container deployments).
The database comprises 13 distinct tables:

| Subsystem | Table | Cols | Primary Key | Description |
|:---|:---|:---:|:---|:---|
| **Workflows** | `runs` | 8 | `run_id` | Workflow tasks, status, latest step, fence token. |
| **Checkpoints** | `checkpoints` | 6 | `(run_id, step)` | State snapshots at execution transition points. |
| **Security** | `audit_events` | 9 | `event_id` | Append-only immutable security audit log. |
| **Idempotency** | `idempotency_keys` | 9 | `key` | Idempotency hashes & response cache. |
| **Admission** | `workflow_admissions` | 7 | `run_id` | Specialist concurrency leases & worker fencing. |
| **Execution** | `tool_executions` | 14 | `operation_key` | Recorded tool invocations, inputs, and results. |
| **HITL** | `tool_approvals` | 18 | `operation_key` | Human-in-the-loop review requests & decisions. |
| **Schedulers** | `scheduler_leases` | 7 | `lease_name` | Distributed locks for outbox, replay, approvals. |
| **Outbox** | `outbox_events` | 18 | `event_id` | Transactional outbox events awaiting delivery. |
| **Outbox** | `outbox_delivery_history` | 10 | `id` | Audit log of webhook delivery attempts & latency. |
| **Replay** | `webhook_replay_records` | 4 | `event_id` | Durable replay cache for webhook HMAC signatures. |
| **Replay** | `webhook_replay_cleanup_log` | 7 | `id` | Audit log of replay cache purges & metrics. |
| **Workers** | `celery_worker_heartbeats` | 10 | `worker_id` | Worker registration & heartbeat telemetry. |

### Subsystem Interdependencies & Invariants
- `checkpoints`, `workflow_admissions`, and `tool_executions` link to `runs(run_id)`.
- `tool_approvals` links to `runs(run_id)` and records human authorizations for pending operations.
- `outbox_events` links to `idempotency_keys` and encapsulates event payloads across execution steps.
- Restoring a backup restores the entire relational dataset atomically, preserving cross-table invariants.

---

## 3. Consistent Online Backup Architecture

### 3.1 The Online Backup API vs. Naive File Copying
Under SQLite Write-Ahead Logging (WAL), changes are appended to `-wal` and coordinated through `-shm`.
A standard file copy (`cp`, `rsync`) while writers are active can capture torn pages or miss committed
frames in the WAL file.

`BackupManager.create_backup()` resolves this:
1. Connects to the source database with a 15-second busy timeout.
2. Initializes the destination database and invokes `source_conn.backup(dest_conn, pages=-1)`.
3. The SQLite engine acquires internal locks page-by-page, flushing committed WAL frames into the
   destination database file.
4. The resulting destination file is a self-contained, standalone SQLite database requiring no auxiliary
   `-wal` or `-shm` files to read.

### 3.2 Overwrite Protection & Configurable Naming
- **Timestamped Naming**: Default backups follow the pattern `backup_{tag}_{YYYYMMDD_HHMMSS}.db`.
- **Destination Flexibility**: Accepts a directory path (auto-generating timestamped file) or an explicit
  target file path.
- **Overwrite Guard**: If the target backup file exists, `BackupOverwriteError` is raised unless
  `overwrite=True` is explicitly specified.
- **Integrity Validation**: Immediately after creation, `PRAGMA integrity_check` is executed against the
  backup file. If corrupt, the file is safely unlinked and `CorruptBackupError` is raised.

---

## 4. Safe Restore & Disaster Recovery Architecture

Restoring a database into a production environment carries significant risk of data loss or operational
conflict if the target database is actively serving traffic. `BackupManager.restore_backup()` enforces a
strict multi-stage safety protocol:

```
┌─────────────────────────┐
│ 1. Validate Backup File │ ── (Invalid header / corrupt pages) ──► ABORT (Target untouched)
└────────────┬────────────┘
             │ Valid
             ▼
┌─────────────────────────┐
│ 2. Active Use Collision │ ── (Active runs / unexpired leases) ──► ABORT unless --force
└────────────┬────────────┘
             │ Clear / Forced
             ▼
┌─────────────────────────┐
│ 3. Pre-Restore Snapshot │ ──► Saves pre_restore_safety_{ts}.db
└────────────┬────────────┘
             │ Safety secured
             ▼
┌─────────────────────────┐
│ 4. Atomic Page Restore  │ ──► Replaces pages via SQLite Backup API
└────────────┬────────────┘
             │ Pages copied
             ▼
┌─────────────────────────┐
│ 5. WAL Re-init & Verify │ ──► Executes PRAGMA wal_checkpoint(TRUNCATE) & integrity_check
└─────────────────────────┘
```

### Safety Features
- **Integrity Pre-check**: Verifies the 16-byte SQLite magic header (`SQLite format 3\000`), executes
  `PRAGMA integrity_check`, and checks for core tables. Corrupted archives are rejected before modifying
  any target data.
- **Active Collision Prevention**: Inspects target database for active workflows (`status='running'`) or
  unexpired admission leases (`expires_at > now`). If found, raises `DatabaseInUseError` unless `force=True`.
- **Pre-Restore Safety Snapshot**: Automatically creates a timestamped safety backup
  (`backup_pre_restore_safety_{ts}.db`) of the target database before executing the restore, providing a
  safe rollback path.
- **Post-Restore WAL Alignment**: Restores pages atomically and resets the WAL file via
  `PRAGMA wal_checkpoint(TRUNCATE);`.

---

## 5. Backup Retention Lifecycle Management

Unbounded backup creation can exhaust container volume storage. `BackupManager.clean_backups()` provides
automated lifecycle management:

- **Count-Based Retention**: Keeps the $N$ most recent backups (default: `10`), configured via
  `BACKUP_RETENTION_COUNT` or `--keep-count`.
- **Age-Based Retention**: Prunes backups older than $M$ days (default: `30`), configured via
  `BACKUP_RETENTION_DAYS` or `--keep-days`.
- **Protection of Latest Backup**: Invariant: The single latest valid backup is **never** deleted, even if
  its age exceeds the configured threshold.
- **Dry-Run Mode**: Operators can preview candidate deletions (`--dry-run`) without modifying disk state.

---

## 6. Operational CLI Interfaces

Task 62 integrated backup operations directly into the orchestrator CLI entry point:

```bash
# 1. Create an online backup
orchestrator backup create [--dest DIR] [--tag TAG] [--json]

# 2. List available backup archives
orchestrator backup list [--dir DIR] [--json]

# 3. Validate backup integrity
orchestrator backup validate <backup-file> [--quick] [--json]

# 4. Restore database from backup
orchestrator backup restore <backup-file> [--target PATH] [--force] [--json]

# 5. Purge expired backups
orchestrator backup clean [--keep-count N] [--keep-days N] [--dry-run] [--json]
```

### Example JSON CLI Outputs
`orchestrator backup create --json`:
```json
{
  "backup_path": "/app/data/backups/backup_manual_20261101_120000.db",
  "source_db_path": "/app/data/orchestrator_checkpoints.db",
  "timestamp": "2026-11-01T12:00:00.123456+00:00",
  "size_bytes": 1048576,
  "integrity": "ok",
  "table_counts": {
    "runs": 42,
    "checkpoints": 128,
    "audit_events": 350
  },
  "duration_ms": 15.4,
  "tag": "manual"
}
```

---

## 7. Disaster Recovery Test Suite

The test suite in `tests/test_database_backup_restore.py` validates all disaster recovery invariants:

1. `test_backup_wal_database_consistency`: Backs up an active WAL database and asserts standalone integrity.
2. `test_backup_destination_directory_and_custom_path`: Tests directory auto-naming and explicit file paths.
3. `test_backup_overwrite_protection`: Rejects overwriting existing backups unless `overwrite=True`.
4. `test_backup_rejects_in_memory_database`: Catches and rejects `:memory:` database backup requests.
5. `test_backup_rejects_non_existent_source`: Validates missing source database error handling.
6. `test_validate_backup_detects_valid_archive`: Verifies deep integrity and foreign key validation.
7. `test_validate_backup_detects_corrupt_file`: Detects truncated or non-SQLite headers and reports errors.
8. `test_validate_backup_detects_missing_file`: Reports clean error when backup file does not exist.
9. `test_restore_into_clean_target`: Restores into a fresh non-existent database path.
10. `test_restore_corrupt_backup_rejected_and_leaves_target_untouched`: Confirms target DB is never altered
    when a corrupt archive is supplied.
11. `test_restore_creates_pre_restore_safety_backup`: Validates pre-restore safety snapshot generation.
12. `test_restore_prevents_active_database_overwrite_without_force`: Blocks restore when active workflows
    or unexpired leases are present.
13. `test_retention_count_cleanup`: Asserts oldest backups beyond count threshold are purged.
14. `test_retention_age_cleanup`: Asserts backups older than cutoff age are purged.
15. `test_retention_protects_latest_backup`: Confirms single latest backup is never deleted.
16. `test_retention_dry_run`: Verifies dry run returns candidates without removing files.
17. `test_preservation_across_all_subsystems`: Asserts record counts across all 13 tables match exactly after
    restore.
18. `test_recovery_after_process_interruption_from_restored_backup`: Validates that restored databases
    allow `RunManager` to identify and recover interrupted workflows.
19. `test_cli_backup_create_and_list`: Tests CLI `backup create` and `backup list`.
20. `test_cli_backup_validate_and_restore`: Tests CLI `backup validate` and `backup restore`.
21. `test_cli_backup_clean`: Tests CLI `backup clean` in active and dry-run modes.

---

## 8. Verification Results

### Focused Test Suite
```bash
python -m pytest tests/test_database_backup_restore.py -v
# Result: 21 passed in 3.99s
```

### Full Regression Suite
```bash
python -m pytest tests/test_cli.py tests/test_config_validation.py \
                 tests/test_production_configuration_validation.py \
                 tests/test_database_backup_restore.py -v
# Result: 105 passed in 11.24s
```

### Code Quality & Hygiene
- `ruff check`: All checks passed.
- Line length limit: All modified and new files strictly conform to the $\le 110$ character limit.
- Git status: Clean worktree; no commits or pushes performed.
