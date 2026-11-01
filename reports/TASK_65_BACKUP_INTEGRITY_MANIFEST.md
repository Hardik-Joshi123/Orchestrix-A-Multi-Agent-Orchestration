# Task 65: Backup Integrity Manifest & Tamper Detection Report

## 1. Executive Summary

Task 65 introduced cryptographic integrity manifests for database backup archives in the Multi-Agent
Orchestrator. While Task 62 implemented online SQLite backups, Task 63 automated scheduling and retention,
and Task 64 established automated isolated restore drills, backup archives remained vulnerable to silent
bit rot, transmission truncation, and unauthorized manual modification prior to recovery.

With Task 65, every generated database backup archive is paired with an atomic, deterministic cryptographic
integrity manifest (`<backup_path>.manifest.json`). The orchestrator now detects single-byte modifications,
archive truncation, appended unauthorized payloads, path traversal attempts, and malformed metadata prior to
validation or recovery drills.

---

## 2. Cryptographic Manifest Architecture & Schema

### 2.1 Manifest Schema

The manifest is defined by the `BackupManifest` dataclass and serializes into standard, deterministic JSON:

```json
{
  "backup_id": "backup_20261101_105953_7f8a9b",
  "created_at": "2026-11-01T10:59:53.123456+00:00",
  "filename": "backup_scheduled_20261101_105953.db",
  "manifest_version": 1,
  "schema_version": 2,
  "sha256": "f74d19777f87bbe29c9c1efd0d7dde597c7763e82c6ea7298d21bd8cc9c13b1b",
  "size_bytes": 225280,
  "tag": "scheduled"
}
```

### 2.2 Deterministic Serialization & Atomic Writes

To prevent corrupt or partial manifest files from filesystem interrupts or process crashes:
- Serialization enforces UTF-8 encoding, 2-space indentation, and lexicographically sorted keys.
- `write_atomic_json` writes content to a unique sibling temporary file (`<target>.tmp.<uuid>`) and performs
  an atomic rename (`os.replace`) to the final `<backup_path>.manifest.json`.

### 2.3 Streaming SHA-256 Hashing

- To avoid memory bloat when validating multi-gigabyte production databases, `compute_sha256` reads files in
  configurable 64 KiB (65,536 bytes) chunks.
- Digest comparisons use standard constant-time string comparisons to prevent timing side-channel attacks.

### 2.4 Three-Valued Verification Status

The `verify_manifest` method returns a `ManifestVerificationResult` distinguishing three distinct states:
1. `verified`: Manifest exists, schema is valid, file size matches, and SHA-256 matches (`is_valid = True`).
2. `unverified`: Legacy backup archive created prior to Task 65 without a companion manifest. Backward
   compatibility is preserved: SQLite integrity checks proceed, but manifest status is logged as unverified.
3. `failed`: Cryptographic checksum mismatch, truncated file, path traversal attempt, or malformed manifest.

---

## 3. Backup Lifecycle Integration

### 3.1 Backup Creation (`BackupManager.create_backup`)
- Generates SQLite backup online via `sqlite3.Connection.backup()`.
- Computes SHA-256 checksum and file size.
- Atomically writes companion manifest `<backup>.manifest.json`.
- Emits structured audit event `BACKUP_MANIFEST_CREATED`.

### 3.2 Backup Validation (`BackupManager.validate_backup`)
- Evaluates companion manifest if present.
- If manifest verification fails (`status = "failed"`), validation immediately fails (`is_valid = False`).
- If manifest is missing, marks `manifest_status = "unverified"` and permits SQLite checks to evaluate health.

### 3.3 Restore Drill Gating (`BackupManager.run_restore_drill`)
- Inspects candidate backup manifest before provisioning temporary isolated SQLite databases.
- If candidate manifest verification fails (`status = "failed"`), the drill aborts immediately with
  `RestoreDrillResult(success=False, manifest_status="failed", ...)`.
- Emits `BACKUP_INTEGRITY_FAILED` and `BACKUP_RESTORE_DRILL_FAILED` audit events.

### 3.4 Retention Pruning (`BackupManager.clean_backups`)
- When pruning expired backups by count or age, companion `.manifest.json` files are deleted with archives.
- Dedicated orphaned manifest cleaner scans storage and deletes `.manifest.json` files whose `.db` file was
  manually removed.
- The latest backup archive is unconditionally preserved regardless of manifest status.

---

## 4. Security Architecture & Threat Analysis

| Threat / Vulnerability | Mitigation Strategy | Status |
| :--- | :--- | :--- |
| **Silent Bit Rot / Decay** | Streaming SHA-256 recomputation detects single-bit corruption | Verified |
| **Transmission Truncation** | Exact byte length validation against manifest `size_bytes` | Verified |
| **Path Traversal Attacks** | Manifest `filename` strictly rejects `..`, `/`, and `\\` | Verified |
| **Information Disclosure** | Manifest contains zero row data, state records, or secrets | Verified |
| **Concurrent Write Corruption** | Atomic temp file creation with `os.replace` rename | Verified |
| **Tampered Archive Gating** | Restore drills abort before executing SQLite attach/restore | Verified |

### 4.1 Security Limitation & Defense-in-Depth

> [!IMPORTANT]
> The SHA-256 integrity manifest protects against hardware bit rot, network truncation, and uncoordinated
> modifications. However, because manifests are currently unsigned cryptographic hashes, an adversary with
> write access to both the backup directory and manifest files could alter an archive and recompute its
> manifest. In high-threat environments, backup directories should reside on write-once-read-many (WORM)
> volumes or object storage with Object Lock enabled.

---

## 5. Operational Interfaces & Audit

### 5.1 CLI Subcommands

```bash
# Verify integrity of a backup archive with formatted summary
python -m orchestrator backup verify backups/backup_20261101_105953.db

# Verify integrity and emit machine-readable JSON
python -m orchestrator backup verify backups/backup_20261101_105953.db --json
```

### 5.2 REST API Endpoints

Authenticated endpoints protected by Bearer token (`ORCHESTRATOR_API_TOKEN`):
- `GET /backups/verify?backup_path=...`: Verifies specified backup archive (or latest backup if omitted).
- `POST /backups/verify`: Verifies backup archive with JSON payload `{"backup_path": "..."}`.

### 5.3 Structured Audit Logging

Emits auditable events with full sanitization and metadata:
- `BACKUP_MANIFEST_CREATED` (`backup.manifest_created`): Emitted upon manifest generation.
- `BACKUP_INTEGRITY_VERIFIED` (`backup.integrity_verified`): Emitted upon successful verification.
- `BACKUP_INTEGRITY_FAILED` (`backup.integrity_failed`): Emitted on checksum mismatch, truncation, or tamper.

---

## 6. Test Suite & Verification Results

A comprehensive test suite of 24 tests was developed in `tests/test_backup_manifest.py`:

```text
- TestManifestCreationAndHashing:
  * test_create_backup_generates_valid_manifest .................................... PASSED
  * test_manifest_deterministic_formatting ......................................... PASSED
  * test_manifest_preserves_privacy ................................................ PASSED
  * test_atomic_manifest_creation_via_temp_file .................................... PASSED
- TestManifestVerificationAndTamperDetection:
  * test_verify_manifest_success ................................................... PASSED
  * test_tamper_detection_modified_byte ............................................ PASSED
  * test_tamper_detection_truncated_file ........................................... PASSED
  * test_tamper_detection_appended_data ............................................ PASSED
  * test_legacy_backup_without_manifest ............................................ PASSED
  * test_malformed_manifest_json ................................................... PASSED
  * test_manifest_missing_required_fields .......................................... PASSED
  * test_manifest_filename_mismatch ................................................ PASSED
  * test_path_traversal_rejection .................................................. PASSED
- TestRetentionAndRestoreDrillIntegration:
  * test_clean_backups_prunes_companion_manifest ................................... PASSED
  * test_clean_backups_cleans_orphaned_manifests ................................... PASSED
  * test_clean_backups_preserves_latest_even_if_manifest_unverified ................ PASSED
  * test_restore_drill_aborts_on_failed_manifest ................................... PASSED
- TestOperationalInterfacesAndAudit:
  * test_cli_backup_verify_success_text ............................................ PASSED
  * test_cli_backup_verify_success_json ............................................ PASSED
  * test_cli_backup_verify_tampered_fails .......................................... PASSED
  * test_api_verify_backup_get ..................................................... PASSED
  * test_api_verify_backup_post .................................................... PASSED
  * test_api_verify_tampered_returns_failed_status ................................. PASSED
  * test_api_verify_requires_authentication ........................................ PASSED

============================= 24 passed in 4.62s ==============================
```

### Full Backup Subsystem Regression Suite

All 75 backup, scheduler, restore drill, and manifest tests passed:
- `tests/test_backup_manifest.py`: 24 passed.
- `tests/test_restore_drill.py`: 19 passed.
- `tests/test_backup_scheduler.py`: 11 passed.
- `tests/test_database_backup_restore.py`: 21 passed.
**Total: 75 passed in 12.38s.**

---

## 7. Deliverables & Artifacts

1. `src/orchestrator/observability/audit.py`: Added manifest audit event types.
2. `src/orchestrator/persistence/backup.py`: Added manifest generation, verification, atomic writes,
   orphan cleanup, restore drill gating, and CLI `backup verify`.
3. `src/orchestrator/persistence/__init__.py`: Exported manifest data classes and functions.
4. `src/orchestrator/config.py`: Added `backup_require_manifest` configuration and diagnostics.
5. `src/orchestrator/api/models.py`: Added request/response schemas for manifest verification.
6. `src/orchestrator/api/routes.py`: Added authenticated `GET /backups/verify` and `POST /backups/verify`.
7. `tests/test_backup_manifest.py`: 24 comprehensive unit and integration tests.
8. `docs/DEPLOYMENT_GUIDE.md`: Section 13.9 added covering manifest schema, verification, and runbooks.
9. `reports/TASK_65_BACKUP_INTEGRITY_MANIFEST.md`: Complete engineering and verification report.
