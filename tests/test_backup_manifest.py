"""Comprehensive test suite for backup integrity manifest generation and tamper detection.

Validates Task 65 requirements:
- Deterministic cryptographic manifest generation (SHA-256).
- Tamper and bit rot detection (single byte changes, truncation, appending).
- Security controls: path traversal rejection and strict input sanitization.
- Backward compatibility: legacy unverified backups without companion manifests.
- Safe lifecycle integration: atomic writes, restore drill gating, companion/orphan retention.
- Operational interfaces: CLI verify (text/JSON) and REST API endpoints (GET/POST) with audit.
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.server import create_app
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.backup import (
    BackupManager,
    ManifestVerificationResult,
    backup_cli,
    compute_sha256,
    write_atomic_json,
)
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_manifest_settings():
    from orchestrator.config import reset_settings
    from orchestrator.persistence.scheduler import reset_backup_scheduler

    reset_settings()
    reset_backup_scheduler()
    yield
    reset_settings()
    reset_backup_scheduler()


@pytest.fixture
def populated_db(tmp_path: pathlib.Path) -> tuple[str, CheckpointStore]:
    """Create a fully populated SQLite database with sample records."""
    db_file = str(tmp_path / "manifest_test_live.db")
    store = CheckpointStore(db_path=db_file)

    store.save_checkpoint(
        run_id="run-manifest-001",
        state={"task": "Integrity manifest validation", "status": "active"},
        transition="supervisor->worker",
        privileged=False,
    )
    return db_file, store


@pytest.fixture
def backup_dir(tmp_path: pathlib.Path) -> str:
    """Isolated backup destination directory."""
    b_dir = tmp_path / "backups"
    b_dir.mkdir(parents=True, exist_ok=True)
    return str(b_dir)


class TestManifestCreationAndHashing:
    """Tests for manifest creation, SHA-256 calculation, and atomic writes."""

    def test_create_backup_generates_valid_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Creating a backup automatically creates a valid companion manifest."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="manifest_test")

        assert meta.manifest_path is not None
        assert os.path.exists(meta.manifest_path)
        assert meta.manifest_status == "verified"
        assert meta.sha256 is not None

        # Verify companion manifest file content
        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)

        assert data["manifest_version"] == 1
        assert data["filename"] == os.path.basename(meta.backup_path)
        assert data["size_bytes"] == os.path.getsize(meta.backup_path)
        assert data["sha256"] == compute_sha256(meta.backup_path)
        assert data["tag"] == "manifest_test"
        assert "created_at" in data
        assert "schema_version" in data

    def test_manifest_deterministic_formatting(self, tmp_path: pathlib.Path):
        """Manifest JSON is serialized deterministically with sorted keys."""
        manifest_path = str(tmp_path / "test.manifest.json")
        payload = {
            "zebra": 1,
            "alpha": "first",
            "backup_id": "b-123",
            "sha256": "abc",
        }
        write_atomic_json(manifest_path, payload)

        with open(manifest_path, encoding="utf-8") as fh:
            raw_text = fh.read()

        # With sort_keys=True, alpha precedes backup_id, which precedes sha256, which precedes zebra
        alpha_idx = raw_text.find('"alpha"')
        backup_idx = raw_text.find('"backup_id"')
        sha_idx = raw_text.find('"sha256"')
        zebra_idx = raw_text.find('"zebra"')

        assert alpha_idx < backup_idx < sha_idx < zebra_idx

    def test_manifest_preserves_privacy(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Manifest never leaks sensitive table data, state records, or secrets."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="privacy_test")

        with open(meta.manifest_path, encoding="utf-8") as fh:
            manifest_text = fh.read()

        # Database contents must not appear in the manifest
        assert "Integrity manifest validation" not in manifest_text
        assert "supervisor->worker" not in manifest_text
        assert "checkpoints" not in manifest_text

    def test_atomic_manifest_creation_via_temp_file(self, tmp_path: pathlib.Path):
        """write_atomic_json ensures atomic rename and does not leave temporary files."""
        target = tmp_path / "atomic.json"
        data = {"status": "atomic_ok", "count": 42}
        write_atomic_json(str(target), data)

        assert target.exists()
        # Ensure no lingering .tmp files
        tmp_files = list(tmp_path.glob("*.tmp*"))
        assert len(tmp_files) == 0


class TestManifestVerificationAndTamperDetection:
    """Tests for tamper detection, bit rot, truncation, and corruption."""

    def test_verify_manifest_success(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Valid archive matches manifest checksum and returns verified status."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="verify_ok")

        res: ManifestVerificationResult = mgr.verify_manifest(meta.backup_path)
        assert res.status == "verified"
        assert res.is_valid is True
        assert res.error is None
        assert res.expected_sha256 == res.computed_sha256
        assert res.expected_size_bytes == res.actual_size_bytes
        assert res.manifest is not None
        assert res.duration_ms >= 0.0

    def test_tamper_detection_modified_byte(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Single modified byte in backup file is detected as tamper/corruption."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="tamper_byte")

        # Mutate a byte in the middle of the backup file
        with open(meta.backup_path, "r+b") as fh:
            fh.seek(100)
            orig_byte = fh.read(1)
            new_byte = b"\x00" if orig_byte != b"\x00" else b"\xFF"
            fh.seek(100)
            fh.write(new_byte)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Cryptographic checksum mismatch" in (res.error or "")

        # validate_backup also reports failure
        val = mgr.validate_backup(meta.backup_path)
        assert val.is_valid is False
        assert val.manifest_status == "failed"

    def test_tamper_detection_truncated_file(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Truncated backup archive fails verification on size and checksum."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="tamper_trunc")

        # Truncate 200 bytes from the end
        orig_size = os.path.getsize(meta.backup_path)
        with open(meta.backup_path, "r+b") as fh:
            fh.truncate(orig_size - 200)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Archive size mismatch" in (res.error or "")

    def test_tamper_detection_appended_data(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Appending unauthorized data to backup archive fails verification."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="tamper_append")

        # Append malicious payload to archive
        with open(meta.backup_path, "ab") as fh:
            fh.write(b"MALICIOUS_INJECTED_TAIL_DATA")

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Archive size mismatch" in (res.error or "")

    def test_legacy_backup_without_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Legacy backup without companion manifest returns unverified status."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="legacy_bup")

        # Remove manifest to simulate pre-Task 65 legacy backup
        os.remove(meta.manifest_path)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "unverified"
        assert res.is_valid is False
        assert "Integrity manifest not found" in (res.error or "")

        # validate_backup still succeeds for valid legacy backup
        val = mgr.validate_backup(meta.backup_path)
        assert val.is_valid is True
        assert val.manifest_status == "unverified"

    def test_malformed_manifest_json(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Corrupt or non-JSON manifest content fails verification."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="malformed_json")

        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            fh.write("NOT_VALID_JSON{:::broken")

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Malformed manifest JSON" in (res.error or "")

    def test_manifest_missing_required_fields(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Manifest missing critical security fields is rejected."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="missing_fields")

        # Overwrite manifest without sha256
        bad_manifest = {
            "backup_id": "b-999",
            "filename": os.path.basename(meta.backup_path),
            "size_bytes": 1000,
        }
        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(bad_manifest, fh)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Manifest missing required fields" in (res.error or "")

    def test_manifest_filename_mismatch(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Manifest declaring a different filename fails verification."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="mismatch_name")

        # Edit manifest filename to point to another file
        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)
        data["filename"] = "completely_different_backup.db"
        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Manifest filename mismatch" in (res.error or "")

    def test_path_traversal_rejection(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Manifest filename attempting directory traversal is rejected."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="traversal_test")

        for bad_name in ["../etc/passwd", "..\\boot.ini", "/etc/shadow", "subdir/backup.db"]:
            with open(meta.manifest_path, encoding="utf-8") as fh:
                data = json.load(fh)
            data["filename"] = bad_name
            with open(meta.manifest_path, "w", encoding="utf-8") as fh:
                json.dump(data, fh)

            res = mgr.verify_manifest(meta.backup_path)
            assert res.status == "failed"
            assert res.is_valid is False
            assert "path traversal" in (res.error or "").lower()


class TestRetentionAndRestoreDrillIntegration:
    """Tests for companion manifest retention cleanup and restore drill gating."""

    def test_clean_backups_prunes_companion_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Pruning backup archives also deletes companion .manifest.json files."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        # Create 4 backups
        metas = []
        for i in range(4):
            meta = mgr.create_backup(dest=backup_dir, tag=f"prune_{i}")
            metas.append(meta)

        # Prune down to 2
        result = mgr.clean_backups(keep_count=2, keep_days=30)
        assert result.total_deleted == 2

        # Check that pruned archives and manifests are both gone
        remaining = mgr.list_backups()
        assert len(remaining) == 2
        remaining_paths = {b.backup_path for b in remaining}

        for meta in metas:
            if meta.backup_path not in remaining_paths:
                assert not os.path.exists(meta.backup_path)
                assert not os.path.exists(meta.manifest_path)

    def test_clean_backups_cleans_orphaned_manifests(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Orphaned manifests whose archives were removed are safely cleaned."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="orphan_test")

        # Manually delete the backup archive but leave the manifest
        os.remove(meta.backup_path)
        assert os.path.exists(meta.manifest_path)

        # clean_backups should prune the orphan manifest
        mgr.clean_backups(keep_count=10, keep_days=30)
        assert not os.path.exists(meta.manifest_path)

    def test_clean_backups_preserves_latest_even_if_manifest_unverified(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """The newest backup is unconditionally preserved by retention pruning."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)

        # Create 1 backup and delete its manifest (unverified)
        meta = mgr.create_backup(dest=backup_dir, tag="latest_unverified")
        os.remove(meta.manifest_path)

        res = mgr.clean_backups(keep_count=0, keep_days=0)
        assert res.total_deleted == 0
        assert os.path.exists(meta.backup_path)

    def test_restore_drill_aborts_on_failed_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, caplog
    ):
        """Restore drill immediately aborts when candidate manifest verification fails."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="drill_tamper")

        # Tamper with backup file
        with open(meta.backup_path, "r+b") as fh:
            fh.seek(50)
            fh.write(b"\xaa\xbb\xcc\xdd")

        with caplog.at_level("INFO"):
            res = mgr.run_restore_drill(backup_path=meta.backup_path)

        assert res.success is False
        assert res.manifest_status == "failed"
        assert any("manifest verification failed" in err.lower() for err in res.errors)

        # Check audit event was logged
        assert any(
            AuditEventType.BACKUP_INTEGRITY_FAILED in record.message
            for record in caplog.records
        )


class TestOperationalInterfacesAndAudit:
    """Tests for CLI subcommands, REST API endpoints, and structured audit logs."""

    def test_cli_backup_verify_success_text(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys
    ):
        """CLI `orchestrator backup verify` succeeds with text output."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="cli_test")

        code = backup_cli(["verify", meta.backup_path])
        assert code == 0
        captured = capsys.readouterr().out
        assert "BACKUP INTEGRITY VERIFICATION: VERIFIED" in captured
        assert meta.sha256 in captured

    def test_cli_backup_verify_success_json(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys
    ):
        """CLI `orchestrator backup verify --json` emits valid structured JSON."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="cli_json")

        code = backup_cli(["verify", meta.backup_path, "--json"])
        assert code == 0
        captured = capsys.readouterr().out
        data = json.loads(captured)
        assert data["status"] == "verified"
        assert data["is_valid"] is True
        assert data["expected_sha256"] == meta.sha256

    def test_cli_backup_verify_tampered_fails(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys
    ):
        """CLI `orchestrator backup verify` returns non-zero code on tamper."""
        db_file, _ = populated_db
        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="cli_tamper")

        # Corrupt file
        with open(meta.backup_path, "r+b") as fh:
            fh.seek(10)
            fh.write(b"CORRUPT")

        code = backup_cli(["verify", meta.backup_path])
        assert code == 1
        captured = capsys.readouterr().out
        assert "BACKUP INTEGRITY VERIFICATION: FAILED" in captured

    def test_api_verify_backup_get(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch
    ):
        """REST API GET /backups/verify verifies backup archive."""
        db_file, _ = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-token-123")

        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="api_get")

        app = create_app()
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token-123"}

        # Verify explicit backup_path
        resp = client.get(
            f"/backups/verify?backup_path={meta.backup_path}",
            headers=headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "verified"
        assert data["is_valid"] is True
        assert data["expected_sha256"] == meta.sha256

        # Verify default latest
        resp_latest = client.get("/backups/verify", headers=headers)
        assert resp_latest.status_code == 200
        assert resp_latest.json()["status"] == "verified"

    def test_api_verify_backup_post(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch
    ):
        """REST API POST /backups/verify verifies backup archive via JSON body."""
        db_file, _ = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-token-123")

        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="api_post")

        app = create_app()
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token-123"}

        resp = client.post(
            "/backups/verify",
            json={"backup_path": meta.backup_path},
            headers=headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "verified"
        assert data["is_valid"] is True

    def test_api_verify_tampered_returns_failed_status(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch
    ):
        """REST API returns status 'failed' when verifying tampered archive."""
        db_file, _ = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-token-123")

        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="api_tamper")

        with open(meta.backup_path, "r+b") as fh:
            fh.seek(15)
            fh.write(b"TAMPER")

        app = create_app()
        client = TestClient(app)
        headers = {"Authorization": "Bearer test-token-123"}

        resp = client.get(
            f"/backups/verify?backup_path={meta.backup_path}",
            headers=headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert data["is_valid"] is False
        assert "Cryptographic checksum mismatch" in data["error"]

    def test_api_verify_requires_authentication(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch
    ):
        """REST API /backups/verify rejects requests without valid auth token."""
        db_file, _ = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "secret-key-999")

        app = create_app()
        client = TestClient(app)

        resp = client.get("/backups/verify")
        assert resp.status_code == 401
