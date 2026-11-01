"""Security test suite for signed backup manifests and cryptographic key rotation.

Validates Task 66 requirements:
- HMAC-SHA256 manifest authentication and envelope serialization.
- Constant-time verification rejecting modified signatures, payloads, and versions.
- Key rotation: active signing key + verification keyring for historical backups.
- Explicit retirement of old verification keys and missing-key handling.
- Backward compatibility: unsigned legacy manifests vs authenticated manifests.
- Configurable strict production policy requiring valid signatures.
- Restore drill gating: immediate rejection of invalid signatures before DB attach.
- Secret redaction in diagnostics, API responses, CLI outputs, and logs.
- Atomic cleanup on signing failures without leaving orphaned files.
"""

from __future__ import annotations

import json
import os
import pathlib

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.server import create_app
from orchestrator.config import Settings, get_settings, reset_settings
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.backup import (
    BackupManager,
    BackupSigningError,
    backup_cli,
    canonical_manifest_bytes,
    sign_manifest_payload,
    verify_manifest_signature,
)
from orchestrator.persistence.scheduler import reset_backup_scheduler
from orchestrator.persistence.store import CheckpointStore

KEY_V1_ID = "key-2025-v1"
KEY_V1_SECRET = "super-secret-signing-key-2025-abcdef123456"

KEY_V2_ID = "key-2026-v2"
KEY_V2_SECRET = "ultra-secret-signing-key-2026-7890fedcba"


@pytest.fixture(autouse=True)
def _cleanup_settings():
    yield
    reset_settings()
    reset_backup_scheduler()


@pytest.fixture
def populated_db(tmp_path: pathlib.Path) -> tuple[str, CheckpointStore]:
    """Create a populated SQLite database for backup testing."""
    db_file = str(tmp_path / "signed_manifest_live.db")
    store = CheckpointStore(db_path=db_file)
    store.save_checkpoint(
        run_id="run-signed-001",
        state={"task": "Cryptographic manifest signing", "status": "completed"},
        transition="supervisor->worker",
        privileged=False,
    )
    return db_file, store


@pytest.fixture
def backup_dir(tmp_path: pathlib.Path) -> str:
    """Isolated directory for backups."""
    b_dir = tmp_path / "backups"
    b_dir.mkdir(parents=True, exist_ok=True)
    return str(b_dir)


@pytest.fixture
def signing_settings(backup_dir: str) -> Settings:
    """Settings configured with active signing key and verification keyring."""
    return Settings(
        backup_dir=backup_dir,
        backup_signing_key=KEY_V1_SECRET,
        backup_signing_key_id=KEY_V1_ID,
        backup_require_signature=False,
    )


class TestManifestSigningAndVerification:
    """Unit and integration tests for signing, serialization, and constant-time checks."""

    def test_canonical_manifest_bytes_deterministic(self):
        """canonical_manifest_bytes produces identical UTF-8 bytes regardless of key insertion order."""
        dict1 = {"backup_id": "b1", "filename": "b.db", "size_bytes": 100, "sha256": "abc"}
        dict2 = {"sha256": "abc", "size_bytes": 100, "backup_id": "b1", "filename": "b.db"}

        bytes1 = canonical_manifest_bytes(dict1)
        bytes2 = canonical_manifest_bytes(dict2)
        assert bytes1 == bytes2
        assert b"backup_id" in bytes1

    def test_canonical_manifest_bytes_excludes_signature(self):
        """canonical_manifest_bytes strips signature metadata so payload is unsigned."""
        payload_with_sig = {
            "backup_id": "b1",
            "filename": "b.db",
            "signature": {"key_id": "k1", "value": "xyz"},
        }
        b = canonical_manifest_bytes(payload_with_sig)
        assert b"signature" not in b
        assert b"b1" in b

    def test_sign_and_verify_success(self):
        """sign_manifest_payload generates HMAC-SHA256 verifiable by verify_manifest_signature."""
        payload = {"backup_id": "b-test-1", "filename": "b.db", "size_bytes": 2048, "sha256": "aabbcc"}
        sig_meta = sign_manifest_payload(payload, KEY_V1_SECRET, KEY_V1_ID)

        assert sig_meta["key_id"] == KEY_V1_ID
        assert sig_meta["algorithm"] == "HMAC-SHA256"
        assert sig_meta["signature_version"] == 1
        assert len(sig_meta["value"]) == 64

        keyring = {KEY_V1_ID: KEY_V1_SECRET}
        is_valid, status, err = verify_manifest_signature(payload, sig_meta, keyring)
        assert is_valid is True
        assert status == "verified"
        assert err is None

    def test_create_backup_signs_manifest_envelope(
        self, populated_db: tuple[str, CheckpointStore], signing_settings: Settings
    ):
        """BackupManager.create_backup generates a signed envelope manifest."""
        db_file, _ = populated_db
        mgr = BackupManager(settings=signing_settings, db_path=db_file)
        meta = mgr.create_backup(tag="signed_test")

        assert meta.manifest_path is not None
        assert os.path.exists(meta.manifest_path)
        assert meta.signature_status == "verified"
        assert meta.key_id == KEY_V1_ID

        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)

        # Check envelope structure
        assert "payload" in data
        assert "signature" in data
        assert data["signature"]["key_id"] == KEY_V1_ID
        assert data["signature"]["algorithm"] == "HMAC-SHA256"
        assert data["payload"]["filename"] == os.path.basename(meta.backup_path)

        # Verification through BackupManager
        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "verified"
        assert res.is_valid is True
        assert res.signature_status == "verified"
        assert res.key_id == KEY_V1_ID


class TestTamperAndModificationRejection:
    """Tests rejecting modified signatures, tampered payloads, wrong keys, and bad versions."""

    def test_reject_modified_signature_value(
        self, populated_db: tuple[str, CheckpointStore], signing_settings: Settings
    ):
        """Tampering with signature hex string causes verification failure."""
        db_file, _ = populated_db
        mgr = BackupManager(settings=signing_settings, db_path=db_file)
        meta = mgr.create_backup(tag="tamper_sig")

        # Mutate signature hex in file
        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)
        orig_val = data["signature"]["value"]
        # Flip first character
        new_char = "0" if orig_val[0] != "0" else "1"
        data["signature"]["value"] = new_char + orig_val[1:]
        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert res.signature_status == "invalid"
        assert "Signature mismatch" in (res.error or "")

    def test_reject_modified_payload_metadata(
        self, populated_db: tuple[str, CheckpointStore], signing_settings: Settings
    ):
        """Modifying payload metadata (e.g. tag or size) without updating signature fails."""
        db_file, _ = populated_db
        mgr = BackupManager(settings=signing_settings, db_path=db_file)
        meta = mgr.create_backup(tag="tamper_meta")

        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)
        # Modify tag in payload
        data["payload"]["tag"] = "malicious_injected_tag"
        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

        res = mgr.verify_manifest(meta.backup_path)
        assert res.status == "failed"
        assert res.is_valid is False
        assert res.signature_status == "invalid"

    def test_reject_wrong_signing_key(self):
        """Verifying signature with a different secret key fails."""
        payload = {"backup_id": "b-1", "filename": "b.db", "size_bytes": 100, "sha256": "123"}
        sig_meta = sign_manifest_payload(payload, KEY_V1_SECRET, KEY_V1_ID)

        # Keyring has wrong secret for key_id
        wrong_keyring = {KEY_V1_ID: "wrong-secret-key-that-does-not-match"}
        is_valid, status, err = verify_manifest_signature(payload, sig_meta, wrong_keyring)
        assert is_valid is False
        assert status == "invalid"
        assert "Signature mismatch" in (err or "")

    def test_reject_unsupported_signature_algorithm_and_version(self):
        """Unsupported signature algorithm or format version is rejected."""
        payload = {"backup_id": "b-1", "filename": "b.db", "size_bytes": 100, "sha256": "123"}
        keyring = {KEY_V1_ID: KEY_V1_SECRET}

        # Bad algorithm
        bad_alg_meta = {
            "key_id": KEY_V1_ID,
            "algorithm": "MD5-RSA",
            "signature_version": 1,
            "value": "abcd",
        }
        is_valid, status, err = verify_manifest_signature(payload, bad_alg_meta, keyring)
        assert is_valid is False
        assert status == "unsupported_version"

        # Bad version
        bad_ver_meta = {
            "key_id": KEY_V1_ID,
            "algorithm": "HMAC-SHA256",
            "signature_version": 99,
            "value": "abcd",
        }
        is_valid, status, err = verify_manifest_signature(payload, bad_ver_meta, keyring)
        assert is_valid is False
        assert status == "unsupported_version"


class TestKeyRotationAndRetirement:
    """Tests for active signing key rotation, historical verification, and key retirement."""

    def test_key_rotation_verifies_old_and_new_backups(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Rotation allows verifying historical backups with old keys while signing with new key."""
        db_file, _ = populated_db

        # 1. Create first backup signed with KEY_V1
        settings_v1 = Settings(
            backup_dir=backup_dir,
            backup_signing_key=KEY_V1_SECRET,
            backup_signing_key_id=KEY_V1_ID,
        )
        mgr_v1 = BackupManager(settings=settings_v1, db_path=db_file)
        meta_v1 = mgr_v1.create_backup(tag="bup_v1")
        assert meta_v1.key_id == KEY_V1_ID

        # 2. Rotate to KEY_V2, keeping KEY_V1 in verification keyring
        rotated_settings = Settings(
            backup_dir=backup_dir,
            backup_signing_key=KEY_V2_SECRET,
            backup_signing_key_id=KEY_V2_ID,
            backup_verification_keys=f"{KEY_V1_ID}:{KEY_V1_SECRET}",
        )
        mgr_v2 = BackupManager(settings=rotated_settings, db_path=db_file)

        # Create second backup signed with KEY_V2
        meta_v2 = mgr_v2.create_backup(tag="bup_v2")
        assert meta_v2.key_id == KEY_V2_ID

        # Verify old backup v1 using rotated manager
        res_v1 = mgr_v2.verify_manifest(meta_v1.backup_path)
        assert res_v1.status == "verified"
        assert res_v1.is_valid is True
        assert res_v1.signature_status == "verified"
        assert res_v1.key_id == KEY_V1_ID

        # Verify new backup v2 using rotated manager
        res_v2 = mgr_v2.verify_manifest(meta_v2.backup_path)
        assert res_v2.status == "verified"
        assert res_v2.is_valid is True
        assert res_v2.signature_status == "verified"
        assert res_v2.key_id == KEY_V2_ID

    def test_retired_key_behavior(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """When an old key is retired from keyring, backups signed with it report missing_key."""
        db_file, _ = populated_db

        settings_v1 = Settings(
            backup_dir=backup_dir,
            backup_signing_key=KEY_V1_SECRET,
            backup_signing_key_id=KEY_V1_ID,
        )
        mgr_v1 = BackupManager(settings=settings_v1, db_path=db_file)
        meta = mgr_v1.create_backup(tag="retire_test")

        # Now configure manager where KEY_V1 has been explicitly retired (not in keyring)
        retired_settings = Settings(
            backup_dir=backup_dir,
            backup_signing_key=KEY_V2_SECRET,
            backup_signing_key_id=KEY_V2_ID,
            backup_verification_keys=None,  # KEY_V1 removed
        )
        mgr_retired = BackupManager(settings=retired_settings, db_path=db_file)

        # In non-strict mode: returns unverified / missing_key
        res = mgr_retired.verify_manifest(meta.backup_path)
        assert res.signature_status == "missing_key"
        assert res.status == "unverified"
        assert res.is_valid is False
        assert KEY_V1_ID in (res.error or "")

        # In strict mode: fails
        res_strict = mgr_retired.verify_manifest(meta.backup_path, strict_signature=True)
        assert res_strict.signature_status == "missing_key"
        assert res_strict.status == "failed"
        assert res_strict.is_valid is False


class TestLegacyAndStrictPolicies:
    """Tests distinguishing unsigned legacy manifests and enforcing strict policies."""

    def test_legacy_unsigned_manifest_compatibility(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Legacy flat unsigned manifest from Task 65 is distinguished as signature_status='unsigned'."""
        db_file, _ = populated_db
        # No signing key configured
        mgr = BackupManager(
            settings=Settings(backup_dir=backup_dir, backup_signing_key=None),
            db_path=db_file,
        )
        meta = mgr.create_backup(tag="unsigned_legacy")

        res = mgr.verify_manifest(meta.backup_path)
        # Archive integrity is verified, but signature is explicitly unsigned
        assert res.status == "verified"
        assert res.is_valid is True
        assert res.signature_status == "unsigned"
        assert res.key_id is None

    def test_strict_policy_rejects_unsigned_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Strict signature policy rejects unsigned manifests for recovery."""
        db_file, _ = populated_db
        mgr = BackupManager(
            settings=Settings(
                backup_dir=backup_dir,
                backup_signing_key=None,
                backup_require_signature=False,
            ),
            db_path=db_file,
        )
        meta = mgr.create_backup(tag="strict_reject_unsigned")

        # Verify with strict_signature=True
        res = mgr.verify_manifest(meta.backup_path, strict_signature=True)
        assert res.status == "failed"
        assert res.is_valid is False
        assert res.signature_status == "unsigned"
        assert "Strict signature policy violation" in (res.error or "")

    def test_strict_policy_rejects_missing_manifest(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """Strict policy marks backups without manifests as failed rather than unverified."""
        db_file, _ = populated_db
        mgr = BackupManager(settings=Settings(backup_dir=backup_dir), db_path=db_file)
        meta = mgr.create_backup(tag="no_manifest")
        os.remove(meta.manifest_path)

        res = mgr.verify_manifest(meta.backup_path, strict_signature=True)
        assert res.status == "failed"
        assert res.is_valid is False
        assert "Strict signature policy violation" in (res.error or "")


class TestLifecycleAndRestoreDrillGating:
    """Tests for restore drill rejection, signing failure atomicity, and retention."""

    def test_restore_drill_aborts_on_invalid_signature(
        self, populated_db: tuple[str, CheckpointStore], signing_settings: Settings, caplog
    ):
        """Restore drill immediately aborts when candidate manifest signature is invalid."""
        db_file, _ = populated_db
        mgr = BackupManager(settings=signing_settings, db_path=db_file)
        meta = mgr.create_backup(tag="drill_bad_sig")

        # Tamper with signature
        with open(meta.manifest_path, encoding="utf-8") as fh:
            data = json.load(fh)
        data["signature"]["value"] = "0" * 64
        with open(meta.manifest_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

        with caplog.at_level("INFO"):
            res = mgr.run_restore_drill(backup_path=meta.backup_path)

        assert res.success is False
        assert res.signature_status == "invalid"
        assert any("manifest verification failed" in err.lower() for err in res.errors)

        # Check structured audit event was logged
        assert any(
            AuditEventType.BACKUP_SIGNATURE_FAILED in record.message
            for record in caplog.records
        )

    def test_atomic_failure_when_strict_policy_missing_key(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str
    ):
        """create_backup cleans up destination archive when signing fails."""
        db_file, _ = populated_db
        # Strict mode enabled but signing key missing
        bad_settings = Settings(
            backup_dir=backup_dir,
            backup_signing_key=None,
            backup_require_signature=True,
        )
        mgr = BackupManager(settings=bad_settings, db_path=db_file)

        with pytest.raises(BackupSigningError, match="Strict signature policy enabled"):
            mgr.create_backup(dest=backup_dir, tag="fail_atomic")

        # Ensure no orphan .db or .manifest.json file was left behind
        db_files = list(pathlib.Path(backup_dir).glob("*.db*"))
        assert len(db_files) == 0


class TestSecretRedactionAndOperationalInterfaces:
    """Tests ensuring zero secrets are leaked in diagnostics, logs, CLI, or API."""

    def test_diagnostics_and_keyring_info_never_leak_secrets(self, backup_dir: str):
        """Key secrets are never present in keyring info or startup diagnostics."""
        s = Settings(
            backup_dir=backup_dir,
            backup_signing_key=KEY_V1_SECRET,
            backup_signing_key_id=KEY_V1_ID,
            backup_verification_keys=f"{KEY_V2_ID}:{KEY_V2_SECRET}",
        )

        info = s.get_safe_backup_keyring_info()
        info_json = json.dumps(info)
        assert KEY_V1_SECRET not in info_json
        assert KEY_V2_SECRET not in info_json
        assert KEY_V1_ID in info["active_key_id"]
        assert KEY_V2_ID in info["verification_key_ids"]

    def test_cli_backup_verify_and_keys_commands(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, capsys, monkeypatch
    ):
        """CLI `orchestrator backup verify` and `keys` subcommands display signature telemetry."""
        db_file, _ = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_SIGNING_KEY", KEY_V1_SECRET)
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_SIGNING_KEY_ID", KEY_V1_ID)
        reset_settings()

        mgr = BackupManager(settings=get_settings(), db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(tag="cli_signed")

        # Test verify command
        code = backup_cli(["verify", meta.backup_path, "--db-path", db_file])
        assert code == 0
        captured = capsys.readouterr().out
        assert "Signature:     verified" in captured
        assert f"Key ID:        {KEY_V1_ID}" in captured
        assert KEY_V1_SECRET not in captured

        # Test keys command
        code_keys = backup_cli(["keys", "--db-path", db_file])
        assert code_keys == 0
        captured_keys = capsys.readouterr().out
        assert "BACKUP SIGNING & VERIFICATION KEYS" in captured_keys
        assert f"Active key ID:      {KEY_V1_ID}" in captured_keys
        assert KEY_V1_SECRET not in captured_keys

    def test_api_verify_and_keys_endpoints(
        self, populated_db: tuple[str, CheckpointStore], backup_dir: str, monkeypatch
    ):
        """REST API /backups/verify and /backups/keys expose signature telemetry without secrets."""
        db_file, store = populated_db
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_DIR", backup_dir)
        monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
        monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "api-token-task66")
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_SIGNING_KEY", KEY_V1_SECRET)
        monkeypatch.setenv("ORCHESTRATOR_BACKUP_SIGNING_KEY_ID", KEY_V1_ID)
        reset_settings()

        app = create_app()
        client = TestClient(app)
        headers = {"Authorization": "Bearer api-token-task66"}

        mgr = BackupManager(db_path=db_file, backup_dir=backup_dir)
        meta = mgr.create_backup(dest=backup_dir, tag="api_signed")

        # 1. GET /backups/verify
        resp = client.get(f"/backups/verify?backup_path={meta.backup_path}", headers=headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "verified"
        assert data["signature_status"] == "verified"
        assert data["key_id"] == KEY_V1_ID
        assert KEY_V1_SECRET not in json.dumps(data)

        # 2. GET /backups/keys
        resp_keys = client.get("/backups/keys", headers=headers)
        assert resp_keys.status_code == 200
        keys_data = resp_keys.json()
        assert keys_data["signing_enabled"] is True
        assert keys_data["active_key_id"] == KEY_V1_ID
        assert KEY_V1_ID in keys_data["verification_key_ids"]
        assert KEY_V1_SECRET not in json.dumps(keys_data)
