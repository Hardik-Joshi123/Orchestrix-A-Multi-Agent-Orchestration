"""Example 04: Database Backup Creation, HMAC Signing & Verification.

Demonstrates creating an online SQLite backup, signing its companion manifest
with HMAC-SHA256, and verifying cryptographic archive integrity.
"""

import tempfile

from orchestrator.config import Settings
from orchestrator.persistence.backup import BackupManager
from orchestrator.persistence.store import CheckpointStore


def main() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = f"{temp_dir}/live_orchestrator.db"
        backup_dir = f"{temp_dir}/backups"

        # Initialize live checkpoint store and populate sample checkpoint
        store = CheckpointStore(db_path=db_path)
        store.save_checkpoint(
            run_id="sample_run_id",
            state={"status": "running"},
            transition="supervisor_decompose",
        )

        print("1. Creating BackupManager with HMAC-SHA256 signing key...")
        signing_key = "example-secret-signing-key-minimum-32-chars-long"
        signing_key_id = "prod-2026-v1"
        settings = Settings(
            checkpoint_db_path=db_path,
            backup_dir=backup_dir,
            backup_signing_key=signing_key,
            backup_signing_key_id=signing_key_id,
        )
        mgr = BackupManager(settings=settings, db_path=db_path, backup_dir=backup_dir)

        print("2. Generating online database backup with companion manifest...")
        metadata = mgr.create_backup(tag="example_demo")
        print(f"   Backup Archive: {metadata.backup_path}")
        print(f"   Archive SHA256: {metadata.sha256[:16]}...")
        print(f"   Manifest Path:  {metadata.manifest_path}\n")

        print("3. Verifying archive integrity and cryptographic signature...")
        verification = mgr.verify_manifest(metadata.backup_path, strict_signature=True)
        print(f"   Status:           {verification.status}")
        print(f"   Is Valid:         {verification.is_valid}")
        print(f"   Signature Status: {verification.signature_status}")
        print(f"   Key ID:           {verification.key_id}")
        print(f"   Algorithm:        {verification.signature_algorithm}")
        print(f"   Duration:         {verification.duration_ms} ms")


if __name__ == "__main__":
    main()
