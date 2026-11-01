"""SQLite database online backup, restore, validation, and retention utilities.

Provides ACID-compliant online backups, WAL checkpoint management, pre-restore
safety snapshots, active database lock detection, integrity verification, and
retention lifecycle management for the Multi-Agent Orchestrator.
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import json
import logging
import os
import pathlib
import sqlite3
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

from ..config import Settings, get_settings

logger = logging.getLogger(__name__)

# Magic byte header for SQLite database files
SQLITE_HEADER_MAGIC = b"SQLite format 3\x00"

# Core persistence tables expected across all subsystems
EXPECTED_TABLES: frozenset[str] = frozenset(
    {
        "runs",
        "checkpoints",
        "audit_events",
        "idempotency_keys",
        "workflow_admissions",
        "tool_executions",
        "tool_approvals",
        "scheduler_leases",
        "outbox_events",
        "outbox_delivery_history",
        "webhook_replay_records",
        "webhook_replay_cleanup_log",
        "celery_worker_heartbeats",
    }
)


class BackupError(Exception):
    """Base exception for all backup and disaster recovery operations."""


class BackupValidationError(BackupError):
    """Raised when a backup archive fails integrity, structure, or schema validation."""


class BackupOverwriteError(BackupError):
    """Raised when an existing backup file would be accidentally overwritten."""


class DatabaseInUseError(BackupError):
    """Raised when attempting to restore into a target database that is actively in use."""


class CorruptBackupError(BackupError):
    """Raised when a backup archive is corrupt, truncated, or unreadable."""


class ManifestVerificationError(BackupError):
    """Raised when an integrity manifest is missing, corrupt, or fails verification."""


class BackupSigningError(BackupError):
    """Raised when manifest signing fails or signing keys are invalid or missing."""


SIGNATURE_ALGORITHM_HMAC_SHA256 = "HMAC-SHA256"
SIGNATURE_VERSION_1 = 1


def canonical_manifest_bytes(payload: dict[str, Any]) -> bytes:
    """Return deterministic UTF-8 bytes for an unsigned manifest payload."""
    clean_payload = {k: v for k, v in payload.items() if k != "signature"}
    return json.dumps(clean_payload, indent=2, sort_keys=True).encode("utf-8")


def sign_manifest_payload(
    payload: dict[str, Any],
    secret_key: str,
    key_id: str,
    algorithm: str = SIGNATURE_ALGORITHM_HMAC_SHA256,
    version: int = SIGNATURE_VERSION_1,
) -> dict[str, Any]:
    """Compute HMAC-SHA256 signature metadata for an unsigned manifest payload."""
    if not secret_key or not secret_key.strip():
        raise BackupSigningError("Cannot sign manifest: secret key is empty or missing.")
    if not key_id or not key_id.strip():
        raise BackupSigningError("Cannot sign manifest: key_id is empty or missing.")

    data_bytes = canonical_manifest_bytes(payload)
    sig_hex = hmac.new(
        secret_key.strip().encode("utf-8"), data_bytes, hashlib.sha256
    ).hexdigest()
    return {
        "algorithm": algorithm,
        "key_id": key_id.strip(),
        "signature_version": version,
        "value": sig_hex,
    }


def verify_manifest_signature(
    payload: dict[str, Any],
    signature_meta: dict[str, Any] | None,
    keyring: dict[str, str],
) -> tuple[bool, str, str | None]:
    """Verify manifest signature against a keyring of verification keys.

    Returns:
        tuple of (is_valid, signature_status, error_description)
    """
    if not signature_meta or not isinstance(signature_meta, dict):
        return False, "unsigned", "Manifest does not contain signature metadata."

    kid = signature_meta.get("key_id")
    alg = signature_meta.get("algorithm")
    ver = signature_meta.get("signature_version")
    val = signature_meta.get("value")

    if not kid or not alg or ver is None or not val:
        return False, "invalid", "Malformed signature metadata: missing required fields."

    if alg != SIGNATURE_ALGORITHM_HMAC_SHA256:
        return (
            False,
            "unsupported_version",
            f"Unsupported signature algorithm '{alg}' (expected {SIGNATURE_ALGORITHM_HMAC_SHA256}).",
        )

    if ver != SIGNATURE_VERSION_1:
        return (
            False,
            "unsupported_version",
            f"Unsupported signature format version '{ver}' (expected {SIGNATURE_VERSION_1}).",
        )

    clean_kid = str(kid).strip()
    if clean_kid not in keyring or not keyring[clean_kid]:
        return (
            False,
            "missing_key",
            f"Verification key '{clean_kid}' unavailable in verification keyring.",
        )

    secret_key = keyring[clean_kid]
    data_bytes = canonical_manifest_bytes(payload)
    expected_sig = hmac.new(
        secret_key.strip().encode("utf-8"), data_bytes, hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(str(val).strip().lower(), expected_sig.lower()):
        return (
            False,
            "invalid",
            "Signature mismatch: manifest metadata or signature has been modified.",
        )

    return True, "verified", None


@dataclass
class BackupManifest:
    """Cryptographic integrity manifest for a database backup archive."""

    backup_id: str
    filename: str
    size_bytes: int
    sha256: str
    created_at: str
    manifest_version: int = 1
    schema_version: int | None = None
    tag: str | None = None
    signature: dict[str, Any] | None = None

    def get_unsigned_payload(self) -> dict[str, Any]:
        """Return the unsigned payload dictionary."""
        return {
            "backup_id": self.backup_id,
            "created_at": self.created_at,
            "filename": self.filename,
            "manifest_version": self.manifest_version,
            "schema_version": self.schema_version,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "tag": self.tag,
        }

    def to_dict(self) -> dict[str, Any]:
        """Convert manifest to serializable dictionary representation."""
        payload = self.get_unsigned_payload()
        if self.signature is not None:
            return {
                "payload": payload,
                "signature": self.signature,
            }
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BackupManifest:
        """Parse dictionary from either signed envelope or legacy flat format."""
        if "payload" in data and isinstance(data["payload"], dict):
            payload = data["payload"]
            sig = data.get("signature")
        else:
            payload = data
            sig = data.get("signature")

        return cls(
            backup_id=str(payload.get("backup_id", "")),
            filename=str(payload.get("filename", "")),
            size_bytes=int(payload.get("size_bytes", 0)),
            sha256=str(payload.get("sha256", "")),
            created_at=str(payload.get("created_at", "")),
            manifest_version=int(payload.get("manifest_version", 1)),
            schema_version=payload.get("schema_version"),
            tag=payload.get("tag"),
            signature=sig if isinstance(sig, dict) else None,
        )


@dataclass
class ManifestVerificationResult:
    """Detailed result of verifying a backup archive against its cryptographic manifest."""

    status: str
    is_valid: bool
    backup_path: str
    manifest_path: str | None = None
    expected_sha256: str | None = None
    computed_sha256: str | None = None
    expected_size_bytes: int | None = None
    actual_size_bytes: int | None = None
    manifest: dict[str, Any] | None = None
    error: str | None = None
    duration_ms: float = 0.0
    signature_status: str = "unsigned"
    key_id: str | None = None
    signature_algorithm: str | None = None
    signature_version: int | None = None
    signature_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert verification result to dictionary representation."""
        return asdict(self)


def compute_sha256(file_path: str | pathlib.Path, chunk_size: int = 65536) -> str:
    """Compute standard SHA-256 hex digest for a file in streaming chunks."""
    clean_path = os.path.abspath(str(file_path).strip())
    hasher = hashlib.sha256()
    with open(clean_path, "rb") as fh:
        while chunk := fh.read(chunk_size):
            hasher.update(chunk)
    return hasher.hexdigest().lower()


def write_atomic_json(target_path: str | pathlib.Path, data: dict[str, Any]) -> None:
    """Atomically write JSON data using deterministic serialization via a temp file."""
    abs_target = os.path.abspath(str(target_path).strip())
    parent_dir = os.path.dirname(abs_target)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)

    temp_path = f"{abs_target}.tmp.{uuid.uuid4().hex[:8]}"
    content = json.dumps(data, indent=2, sort_keys=True)
    try:
        with open(temp_path, "w", encoding="utf-8") as fh:
            fh.write(content)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(temp_path, abs_target)
    except Exception:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
        raise


@dataclass
class BackupMetadata:
    """Detailed metadata describing a completed database backup."""

    backup_path: str
    source_db_path: str
    timestamp: str
    size_bytes: int
    integrity: str
    table_counts: dict[str, int] = field(default_factory=dict)
    duration_ms: float = 0.0
    tag: str | None = None
    manifest_path: str | None = None
    sha256: str | None = None
    manifest_status: str = "verified"
    signature_status: str = "unsigned"
    key_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metadata to dictionary representation."""
        return asdict(self)


@dataclass
class BackupValidationResult:
    """Result of structural, integrity, and schema checks on a backup database."""

    is_valid: bool
    backup_path: str
    size_bytes: int
    integrity_check: str
    quick_check: str
    foreign_key_check: str
    tables: list[str] = field(default_factory=list)
    missing_tables: list[str] = field(default_factory=list)
    table_counts: dict[str, int] = field(default_factory=dict)
    total_records: int = 0
    errors: list[str] = field(default_factory=list)
    manifest_status: str = "unverified"
    sha256: str | None = None
    manifest_path: str | None = None
    manifest_error: str | None = None
    signature_status: str = "unsigned"
    key_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert validation result to dictionary representation."""
        return asdict(self)


@dataclass
class RestoreResult:
    """Outcome of a database restore operation."""

    success: bool
    target_db_path: str
    backup_path: str
    safety_backup_path: str | None = None
    restored_tables: list[str] = field(default_factory=list)
    table_counts: dict[str, int] = field(default_factory=dict)
    total_records: int = 0
    integrity: str = "ok"
    duration_ms: float = 0.0
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert restore outcome to dictionary representation."""
        return asdict(self)


@dataclass
class RetentionResult:
    """Outcome of a backup retention cleanup operation."""

    retained: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    total_retained: int = 0
    total_deleted: int = 0
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert retention outcome to dictionary representation."""
        return asdict(self)


@dataclass
class RestoreDrillResult:
    """Outcome of an isolated database restore drill and recovery verification."""

    drill_id: str
    success: bool
    backup_path: str
    backup_timestamp: str | None = None
    backup_age_seconds: float = 0.0
    duration_ms: float = 0.0
    target_temp_path: str | None = None
    integrity_check: str = "ok"
    quick_check: str = "ok"
    foreign_key_check: str = "ok"
    tables_verified: list[str] = field(default_factory=list)
    records_verified: int = 0
    queries_verified: int = 0
    table_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    cleaned_up: bool = True
    manifest_status: str = "unverified"
    sha256: str | None = None
    signature_status: str = "unsigned"
    key_id: str | None = None
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert drill outcome to dictionary representation."""
        return asdict(self)


class BackupManager:
    """Unified manager for database backup, restore, validation, and retention."""

    def __init__(
        self,
        settings: Settings | None = None,
        db_path: str | pathlib.Path | None = None,
        backup_dir: str | pathlib.Path | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.db_path = str(db_path or self.settings.checkpoint_db_path)
        self.backup_dir = str(backup_dir or getattr(self.settings, "backup_dir", "backups"))

    def get_verification_keyring(self) -> dict[str, str]:
        """Retrieve the active verification keyring from settings."""
        if self.settings and hasattr(self.settings, "get_backup_verification_keyring"):
            return self.settings.get_backup_verification_keyring()
        return {}

    def get_manifest_path(self, backup_path: str | pathlib.Path) -> str:
        """Derive standard manifest file path for a given backup archive."""
        clean_path = os.path.abspath(str(backup_path).strip())
        return f"{clean_path}.manifest.json"

    def create_manifest(
        self,
        backup_path: str | pathlib.Path,
        backup_id: str | None = None,
        tag: str | None = None,
        schema_version: int | None = None,
        signing_key: str | None = None,
        signing_key_id: str | None = None,
    ) -> BackupManifest:
        """Generate, optionally sign, and atomically write an integrity manifest."""
        clean_backup_path = os.path.abspath(str(backup_path).strip())
        if not os.path.isfile(clean_backup_path):
            raise BackupError(
                f"Cannot generate manifest: backup file not found ({clean_backup_path})"
            )

        b_id = backup_id or f"backup_{uuid.uuid4().hex[:16]}"
        size_bytes = os.path.getsize(clean_backup_path)
        sha256_val = compute_sha256(clean_backup_path)
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        manifest_path = self.get_manifest_path(clean_backup_path)

        s_version = schema_version
        if s_version is None:
            try:
                conn = sqlite3.connect(clean_backup_path, timeout=5.0)
                try:
                    s_version = conn.execute("PRAGMA user_version;").fetchone()[0] or None
                finally:
                    conn.close()
            except Exception:
                s_version = None

        manifest = BackupManifest(
            backup_id=b_id,
            filename=os.path.basename(clean_backup_path),
            size_bytes=size_bytes,
            sha256=sha256_val,
            created_at=now_iso,
            manifest_version=1,
            schema_version=s_version or 1,
            tag=tag,
        )

        effective_key = (
            signing_key
            if signing_key is not None
            else getattr(self.settings, "backup_signing_key", None)
        )
        effective_key_id = (
            signing_key_id
            if signing_key_id is not None
            else getattr(self.settings, "backup_signing_key_id", "default")
        )
        strict_sig = getattr(self.settings, "backup_require_signature", False)

        if effective_key and effective_key.strip():
            sig_meta = sign_manifest_payload(
                manifest.get_unsigned_payload(),
                effective_key,
                effective_key_id,
            )
            manifest.signature = sig_meta
        elif strict_sig:
            raise BackupSigningError(
                "Strict signature policy enabled but no backup signing key is configured."
            )

        write_atomic_json(manifest_path, manifest.to_dict())
        logger.info(
            "Created backup integrity manifest for %s (sha256=%s, signed=%s)",
            clean_backup_path,
            sha256_val,
            bool(manifest.signature),
        )
        try:
            from ..observability.audit import (
                AuditEventType,
                AuditOutcome,
                record_audit_event,
                sanitize_audit_metadata,
            )
            record_audit_event(
                event_type=AuditEventType.BACKUP_MANIFEST_CREATED,
                outcome=AuditOutcome.SUCCESS,
                actor="system",
                metadata=sanitize_audit_metadata({
                    "backup_id": manifest.backup_id,
                    "filename": manifest.filename,
                    "sha256": manifest.sha256,
                    "schema_version": manifest.schema_version,
                    "size_bytes": manifest.size_bytes,
                    "signed": bool(manifest.signature),
                    "key_id": manifest.signature.get("key_id") if manifest.signature else None,
                }),
            )
            if manifest.signature:
                record_audit_event(
                    event_type=AuditEventType.BACKUP_MANIFEST_SIGNED,
                    outcome=AuditOutcome.SUCCESS,
                    actor="system",
                    metadata=sanitize_audit_metadata({
                        "backup_id": manifest.backup_id,
                        "filename": manifest.filename,
                        "key_id": manifest.signature.get("key_id"),
                        "algorithm": manifest.signature.get("algorithm"),
                    }),
                )
        except Exception:
            pass
        return manifest

    def _record_verify_audit(
        self, res: ManifestVerificationResult
    ) -> ManifestVerificationResult:
        """Emit structured audit events for verified and failed manifest outcomes."""
        try:
            from ..observability.audit import (
                AuditEventType,
                AuditOutcome,
                record_audit_event,
                sanitize_audit_metadata,
            )
            if res.status == "verified":
                record_audit_event(
                    event_type=AuditEventType.BACKUP_INTEGRITY_VERIFIED,
                    outcome=AuditOutcome.SUCCESS,
                    actor="system",
                    metadata=sanitize_audit_metadata({
                        "backup_path": res.backup_path,
                        "manifest_path": res.manifest_path,
                        "sha256": res.computed_sha256,
                        "duration_ms": res.duration_ms,
                        "signature_status": res.signature_status,
                        "key_id": res.key_id,
                    }),
                )
            elif res.status == "failed":
                record_audit_event(
                    event_type=AuditEventType.BACKUP_INTEGRITY_FAILED,
                    outcome=AuditOutcome.FAILURE,
                    actor="system",
                    metadata=sanitize_audit_metadata({
                        "backup_path": res.backup_path,
                        "manifest_path": res.manifest_path,
                        "error": res.error,
                        "duration_ms": res.duration_ms,
                        "signature_status": res.signature_status,
                        "key_id": res.key_id,
                    }),
                )

            if res.signature_status == "verified":
                record_audit_event(
                    event_type=AuditEventType.BACKUP_SIGNATURE_VERIFIED,
                    outcome=AuditOutcome.SUCCESS,
                    actor="system",
                    metadata=sanitize_audit_metadata({
                        "backup_path": res.backup_path,
                        "key_id": res.key_id,
                        "algorithm": res.signature_algorithm,
                    }),
                )
            elif res.signature_status in ("invalid", "missing_key", "unsupported_version"):
                record_audit_event(
                    event_type=AuditEventType.BACKUP_SIGNATURE_FAILED,
                    outcome=AuditOutcome.FAILURE,
                    actor="system",
                    metadata=sanitize_audit_metadata({
                        "backup_path": res.backup_path,
                        "key_id": res.key_id,
                        "signature_status": res.signature_status,
                        "error": res.signature_error or res.error,
                    }),
                )
        except Exception:
            pass
        return res

    def verify_manifest(
        self,
        backup_path: str | pathlib.Path,
        manifest_path: str | pathlib.Path | None = None,
        keyring: dict[str, str] | None = None,
        strict_signature: bool | None = None,
    ) -> ManifestVerificationResult:
        """Verify an archive against its manifest and validate cryptographic signature.

        Guarantees:
        - Resolves backup files safely within their parent directory, rejecting path traversal.
        - Verifies manifest existence, structure, and required schema fields.
        - Compares declared size with actual filesystem size.
        - Computes SHA-256 in streaming chunks and performs constant-time comparison.
        - Evaluates HMAC-SHA256 signature against verification keyring when present.
        - Honors strict signature policy requiring authenticated manifests for recovery.
        """
        start_time = time.perf_counter()
        clean_backup_path = os.path.abspath(str(backup_path).strip())

        clean_manifest_path = (
            os.path.abspath(str(manifest_path).strip())
            if manifest_path is not None
            else self.get_manifest_path(clean_backup_path)
        )

        is_strict = (
            strict_signature
            if strict_signature is not None
            else getattr(self.settings, "backup_require_signature", False)
        )
        effective_keyring = keyring if keyring is not None else self.get_verification_keyring()

        # 1. Check if manifest exists (legacy backup compatibility)
        if not os.path.exists(clean_manifest_path):
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            actual_size = (
                os.path.getsize(clean_backup_path)
                if os.path.exists(clean_backup_path)
                else None
            )
            computed_hash = (
                compute_sha256(clean_backup_path)
                if os.path.exists(clean_backup_path)
                else None
            )
            err_msg = (
                "Strict signature policy violation: Integrity manifest not found."
                if is_strict
                else "Integrity manifest not found (legacy unverified backup archive)."
            )
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed" if is_strict else "unverified",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    computed_sha256=computed_hash,
                    actual_size_bytes=actual_size,
                    signature_status="unverified",
                    error=err_msg,
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 2. Check if backup archive exists
        if not os.path.exists(clean_backup_path) or not os.path.isfile(clean_backup_path):
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    signature_status="unsigned",
                    error=f"Backup archive file not found: {os.path.basename(clean_backup_path)}",
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 3. Read and parse manifest JSON
        try:
            with open(clean_manifest_path, encoding="utf-8") as fh:
                manifest_data = json.load(fh)
        except Exception as exc:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    signature_status="invalid",
                    error=f"Malformed manifest JSON: {exc}",
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 4. Validate schema structure and required fields
        if not isinstance(manifest_data, dict):
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    signature_status="invalid",
                    error="Manifest content must be a JSON object.",
                    duration_ms=round(duration_ms, 2),
                )
            )

        manifest_obj = BackupManifest.from_dict(manifest_data)
        payload = manifest_obj.get_unsigned_payload()
        sig_meta = manifest_obj.signature

        required_fields = ["backup_id", "filename", "size_bytes", "sha256"]
        missing = [f for f in required_fields if not payload.get(f)]
        if missing:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    manifest=manifest_data,
                    signature_status="invalid",
                    error=f"Manifest missing required fields: {', '.join(missing)}",
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 5. Security check: Reject path traversal in manifest filename
        m_filename = str(manifest_obj.filename).strip()
        if not m_filename or ".." in m_filename or "/" in m_filename or "\\" in m_filename:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    manifest=manifest_data,
                    signature_status="invalid",
                    error="Security violation: Manifest filename contains path traversal characters.",
                    duration_ms=round(duration_ms, 2),
                )
            )

        # Check filename match with target archive
        expected_basename = os.path.basename(clean_backup_path)
        if m_filename != expected_basename:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    manifest=manifest_data,
                    signature_status="invalid",
                    error=(
                        f"Manifest filename mismatch: manifest declares '{m_filename}', "
                        f"actual archive is '{expected_basename}'."
                    ),
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 6. Check file size
        actual_size = os.path.getsize(clean_backup_path)
        expected_size = manifest_obj.size_bytes
        if actual_size != expected_size:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    expected_size_bytes=expected_size,
                    actual_size_bytes=actual_size,
                    manifest=manifest_data,
                    signature_status="invalid",
                    error=(
                        f"Archive size mismatch: expected {expected_size} bytes, "
                        f"got {actual_size} bytes (possible truncation or tampering)."
                    ),
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 7. Compute and compare SHA-256
        computed_sha256 = compute_sha256(clean_backup_path)
        expected_sha256 = str(manifest_obj.sha256).strip().lower()
        if computed_sha256 != expected_sha256:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    expected_sha256=expected_sha256,
                    computed_sha256=computed_sha256,
                    expected_size_bytes=expected_size,
                    actual_size_bytes=actual_size,
                    manifest=manifest_data,
                    signature_status="invalid",
                    error=(
                        f"Cryptographic checksum mismatch: expected {expected_sha256}, "
                        f"computed {computed_sha256} (archive tampered or corrupted)."
                    ),
                    duration_ms=round(duration_ms, 2),
                )
            )

        # 8. Cryptographic manifest signature verification
        kid = sig_meta.get("key_id") if sig_meta else None
        alg = sig_meta.get("algorithm") if sig_meta else None
        ver = sig_meta.get("signature_version") if sig_meta else None

        if sig_meta is None:
            # Manifest is unsigned
            if is_strict:
                duration_ms = (time.perf_counter() - start_time) * 1000.0
                return self._record_verify_audit(
                    ManifestVerificationResult(
                        status="failed",
                        is_valid=False,
                        backup_path=clean_backup_path,
                        manifest_path=clean_manifest_path,
                        expected_sha256=expected_sha256,
                        computed_sha256=computed_sha256,
                        expected_size_bytes=expected_size,
                        actual_size_bytes=actual_size,
                        manifest=manifest_data,
                        signature_status="unsigned",
                        error="Strict signature policy violation: Backup manifest is unsigned.",
                        duration_ms=round(duration_ms, 2),
                    )
                )
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="verified",
                    is_valid=True,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    expected_sha256=expected_sha256,
                    computed_sha256=computed_sha256,
                    expected_size_bytes=expected_size,
                    actual_size_bytes=actual_size,
                    manifest=manifest_data,
                    signature_status="unsigned",
                    error=None,
                    duration_ms=round(duration_ms, 2),
                )
            )

        sig_valid, sig_stat, sig_err_msg = verify_manifest_signature(
            payload=payload,
            signature_meta=sig_meta,
            keyring=effective_keyring,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000.0

        if sig_valid:
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="verified",
                    is_valid=True,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    expected_sha256=expected_sha256,
                    computed_sha256=computed_sha256,
                    expected_size_bytes=expected_size,
                    actual_size_bytes=actual_size,
                    manifest=manifest_data,
                    signature_status="verified",
                    key_id=kid,
                    signature_algorithm=alg,
                    signature_version=ver,
                    error=None,
                    duration_ms=round(duration_ms, 2),
                )
            )

        # Signature check did not pass
        if sig_stat == "missing_key":
            err_text = (
                f"Strict signature policy violation: Verification key '{kid}' unavailable."
                if is_strict
                else f"Verification key '{kid}' is not available in verification keyring."
            )
            return self._record_verify_audit(
                ManifestVerificationResult(
                    status="failed" if is_strict else "unverified",
                    is_valid=False,
                    backup_path=clean_backup_path,
                    manifest_path=clean_manifest_path,
                    expected_sha256=expected_sha256,
                    computed_sha256=computed_sha256,
                    expected_size_bytes=expected_size,
                    actual_size_bytes=actual_size,
                    manifest=manifest_data,
                    signature_status="missing_key",
                    key_id=kid,
                    signature_algorithm=alg,
                    signature_version=ver,
                    signature_error=sig_err_msg,
                    error=err_text,
                    duration_ms=round(duration_ms, 2),
                )
            )

        # "invalid" or "unsupported_version"
        return self._record_verify_audit(
            ManifestVerificationResult(
                status="failed",
                is_valid=False,
                backup_path=clean_backup_path,
                manifest_path=clean_manifest_path,
                expected_sha256=expected_sha256,
                computed_sha256=computed_sha256,
                expected_size_bytes=expected_size,
                actual_size_bytes=actual_size,
                manifest=manifest_data,
                signature_status=sig_stat,
                key_id=kid,
                signature_algorithm=alg,
                signature_version=ver,
                signature_error=sig_err_msg,
                error=f"Manifest signature verification failed: {sig_err_msg}",
                duration_ms=round(duration_ms, 2),
            )
        )
    def _resolve_source_path(self, override_path: str | pathlib.Path | None = None) -> str:
        """Resolve and validate the source database path."""
        target = str(override_path or self.db_path).strip()
        if not target:
            raise BackupError("Source database path must not be empty.")
        if target in (":memory:", "file::memory:?cache=shared"):
            raise BackupError(
                "Cannot perform file backup on in-memory SQLite database. "
                "Specify a persistent file path."
            )
        return os.path.abspath(target)

    def _resolve_dest_path(
        self,
        dest: str | pathlib.Path | None = None,
        tag: str | None = None,
    ) -> str:
        """Compute the destination backup file path."""
        now_str = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        clean_tag = f"_{tag.strip()}" if tag and tag.strip() else ""

        if dest is None:
            dest_dir = os.path.abspath(self.backup_dir)
            filename = f"backup{clean_tag}_{now_str}.db"
            return os.path.join(dest_dir, filename)

        dest_str = str(dest).strip()
        if os.path.isdir(dest_str) or dest_str.endswith(("/", "\\")):
            filename = f"backup{clean_tag}_{now_str}.db"
            return os.path.abspath(os.path.join(dest_str, filename))

        # Check if parent is a directory or path has file extension
        root, ext = os.path.splitext(dest_str)
        if ext.lower() in (".db", ".sqlite", ".sqlite3", ".bak"):
            return os.path.abspath(dest_str)

        # Treat as directory if no extension
        filename = f"backup{clean_tag}_{now_str}.db"
        return os.path.abspath(os.path.join(dest_str, filename))

    def create_backup(
        self,
        db_path: str | pathlib.Path | None = None,
        dest: str | pathlib.Path | None = None,
        tag: str | None = None,
        overwrite: bool = False,
    ) -> BackupMetadata:
        """Execute an online, crash-consistent SQLite backup with WAL support.

        Uses the SQLite Online Backup API to safely capture live committed pages
        without stopping the server or taking locks that block readers.
        """
        source_path = self._resolve_source_path(db_path)
        if not os.path.exists(source_path):
            raise BackupError(f"Source database file not found: {source_path}")

        dest_path = self._resolve_dest_path(dest=dest, tag=tag)
        dest_dir = os.path.dirname(dest_path)
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

        if os.path.exists(dest_path) and not overwrite:
            raise BackupOverwriteError(
                f"Destination backup file already exists: {dest_path}. "
                "Pass overwrite=True to allow replacement."
            )

        start_time = time.perf_counter()
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Connect to source and destination
        source_conn = sqlite3.connect(source_path, timeout=15.0)
        source_conn.execute("PRAGMA busy_timeout=5000;")

        dest_conn = sqlite3.connect(dest_path, timeout=15.0)
        dest_conn.execute("PRAGMA busy_timeout=5000;")

        try:
            # Perform online page copying via SQLite backup API
            source_conn.backup(dest_conn, pages=-1)
            dest_conn.commit()
        except Exception as exc:
            dest_conn.close()
            source_conn.close()
            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
            raise BackupError(f"Online backup failed during page copy: {exc}") from exc
        finally:
            dest_conn.close()
            source_conn.close()

        # Post-backup integrity and schema verification
        val_conn = sqlite3.connect(dest_path, timeout=10.0)
        try:
            val_conn.execute("PRAGMA busy_timeout=5000;")
            res = val_conn.execute("PRAGMA integrity_check;").fetchone()
            if not res or res[0] != "ok":
                val_conn.close()
                if os.path.exists(dest_path):
                    try:
                        os.remove(dest_path)
                    except OSError:
                        pass
                raise CorruptBackupError(f"Post-backup integrity check failed: {res}")

            cursor = val_conn.cursor()
            table_counts: dict[str, int] = {}
            for (tbl,) in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            ).fetchall():
                try:
                    cnt = cursor.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
                    table_counts[tbl] = cnt
                except Exception:
                    table_counts[tbl] = -1
        finally:
            val_conn.close()

        duration_ms = (time.perf_counter() - start_time) * 1000.0
        size_bytes = os.path.getsize(dest_path)

        # Generate cryptographic integrity manifest atomically
        try:
            manifest = self.create_manifest(
                backup_path=dest_path,
                tag=tag,
            )
        except Exception as exc:
            # Clean up destination file if manifest generation / signing fails
            if os.path.exists(dest_path):
                try:
                    os.remove(dest_path)
                except OSError:
                    pass
            m_path = self.get_manifest_path(dest_path)
            if os.path.exists(m_path):
                try:
                    os.remove(m_path)
                except OSError:
                    pass
            logger.error("Failed to generate or sign manifest for backup %s: %s", dest_path, exc)
            raise

        logger.info(
            "Database backup created successfully: %s (%d bytes, sha256=%s, %.1f ms)",
            dest_path,
            size_bytes,
            manifest.sha256,
            duration_ms,
        )

        sig_stat = "verified" if manifest.signature is not None else "unsigned"
        kid = manifest.signature.get("key_id") if manifest.signature else None

        return BackupMetadata(
            backup_path=dest_path,
            source_db_path=source_path,
            timestamp=now_iso,
            size_bytes=size_bytes,
            integrity="ok",
            table_counts=table_counts,
            duration_ms=round(duration_ms, 2),
            tag=tag,
            manifest_path=self.get_manifest_path(dest_path),
            sha256=manifest.sha256,
            manifest_status="verified",
            signature_status=sig_stat,
            key_id=kid,
        )

    def validate_backup(
        self,
        backup_path: str | pathlib.Path,
        deep: bool = True,
    ) -> BackupValidationResult:
        """Thoroughly inspect and validate an SQLite backup file.

        Verifies the SQLite file magic header, integrity_check, quick_check,
        foreign_key_check, and core table definitions.
        """
        clean_path = str(backup_path).strip()
        errors: list[str] = []

        if not clean_path or not os.path.exists(clean_path):
            return BackupValidationResult(
                is_valid=False,
                backup_path=clean_path,
                size_bytes=0,
                integrity_check="file_not_found",
                quick_check="file_not_found",
                foreign_key_check="file_not_found",
                errors=[f"Backup file does not exist: {clean_path}"],
            )

        if not os.path.isfile(clean_path):
            return BackupValidationResult(
                is_valid=False,
                backup_path=clean_path,
                size_bytes=0,
                integrity_check="not_a_file",
                quick_check="not_a_file",
                foreign_key_check="not_a_file",
                errors=[f"Path is not a regular file: {clean_path}"],
            )

        size_bytes = os.path.getsize(clean_path)
        if size_bytes < 100:
            return BackupValidationResult(
                is_valid=False,
                backup_path=clean_path,
                size_bytes=size_bytes,
                integrity_check="truncated_file",
                quick_check="truncated_file",
                foreign_key_check="truncated_file",
                errors=[f"Backup file is truncated ({size_bytes} bytes)."],
            )

        # Inspect SQLite header magic
        try:
            with open(clean_path, "rb") as fh:
                header = fh.read(16)
                if header != SQLITE_HEADER_MAGIC:
                    return BackupValidationResult(
                        is_valid=False,
                        backup_path=clean_path,
                        size_bytes=size_bytes,
                        integrity_check="invalid_header",
                        quick_check="invalid_header",
                        foreign_key_check="invalid_header",
                        errors=["File does not start with valid SQLite format 3 magic header."],
                    )
        except Exception as exc:
            return BackupValidationResult(
                is_valid=False,
                backup_path=clean_path,
                size_bytes=size_bytes,
                integrity_check="unreadable",
                quick_check="unreadable",
                foreign_key_check="unreadable",
                errors=[f"Unable to read file header: {exc}"],
            )

        integrity_res = "error"
        quick_res = "error"
        fk_res = "ok"
        tables: list[str] = []
        missing_tables: list[str] = []
        table_counts: dict[str, int] = {}
        total_records = 0

        conn = sqlite3.connect(clean_path, timeout=10.0)
        try:
            conn.execute("PRAGMA busy_timeout=5000;")

            # PRAGMA integrity_check
            chk = conn.execute("PRAGMA integrity_check;").fetchall()
            if chk and chk[0][0] == "ok":
                integrity_res = "ok"
            else:
                integrity_res = "; ".join(str(r[0]) for r in chk)
                errors.append(f"Integrity check failed: {integrity_res}")

            # PRAGMA quick_check
            qchk = conn.execute("PRAGMA quick_check;").fetchall()
            if qchk and qchk[0][0] == "ok":
                quick_res = "ok"
            else:
                quick_res = "; ".join(str(r[0]) for r in qchk)
                errors.append(f"Quick check failed: {quick_res}")

            # PRAGMA foreign_key_check
            if deep:
                fk_violations = conn.execute("PRAGMA foreign_key_check;").fetchall()
                if fk_violations:
                    fk_res = f"{len(fk_violations)} violation(s)"
                    errors.append(f"Foreign key violations detected: {fk_res}")
                else:
                    fk_res = "ok"

            # Table inventory inspection
            cursor = conn.cursor()
            for (tbl,) in cursor.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            ).fetchall():
                tables.append(tbl)
                try:
                    cnt = cursor.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
                    table_counts[tbl] = cnt
                    total_records += cnt
                except Exception as exc:
                    table_counts[tbl] = -1
                    errors.append(f"Error querying table {tbl}: {exc}")

            # Check expected tables
            present_set = set(tables)
            for exp in sorted(EXPECTED_TABLES):
                if exp not in present_set:
                    missing_tables.append(exp)

        except sqlite3.DatabaseError as exc:
            errors.append(f"SQLite database error during inspection: {exc}")
        finally:
            conn.close()

        # Verify cryptographic manifest if present
        manifest_res = self.verify_manifest(clean_path)
        manifest_status = manifest_res.status
        sha256_val = manifest_res.computed_sha256
        manifest_err = manifest_res.error
        sig_status = manifest_res.signature_status
        kid = manifest_res.key_id
        if manifest_status == "failed":
            errors.append(f"Manifest verification failed: {manifest_res.error}")

        is_valid = len(errors) == 0 and integrity_res == "ok" and quick_res == "ok"
        return BackupValidationResult(
            is_valid=is_valid,
            backup_path=clean_path,
            size_bytes=size_bytes,
            integrity_check=integrity_res,
            quick_check=quick_res,
            foreign_key_check=fk_res,
            tables=tables,
            missing_tables=missing_tables,
            table_counts=table_counts,
            total_records=total_records,
            errors=errors,
            manifest_status=manifest_status,
            sha256=sha256_val,
            manifest_path=manifest_res.manifest_path if manifest_status != "unverified" else None,
            manifest_error=manifest_err if manifest_status == "failed" else None,
            signature_status=sig_status,
            key_id=kid,
        )

    def restore_backup(
        self,
        backup_path: str | pathlib.Path,
        target_db_path: str | pathlib.Path | None = None,
        force: bool = False,
        safety_backup: bool = True,
        safety_backup_dest: str | pathlib.Path | None = None,
    ) -> RestoreResult:
        """Safely restore database from a verified backup archive.

        1. Validates backup integrity and structure before touching target.
        2. Inspects target database for active runs or locks.
        3. Creates a pre-restore safety backup of active database.
        4. Replaces target database pages atomically via SQLite backup API.
        5. Re-establishes WAL journal mode and runs post-restore integrity check.
        """
        start_time = time.perf_counter()
        clean_backup_path = os.path.abspath(str(backup_path).strip())
        target_path = self._resolve_source_path(target_db_path)

        # 1. Validate backup before restoration
        val = self.validate_backup(clean_backup_path, deep=True)
        if not val.is_valid:
            err_msg = f"Cannot restore: backup validation failed ({'; '.join(val.errors)})"
            logger.error(err_msg)
            raise CorruptBackupError(err_msg)

        safety_backup_path: str | None = None
        target_exists = os.path.exists(target_path)

        # 2. Check if target database is currently in active use
        if target_exists and not force:
            try:
                chk_conn = sqlite3.connect(target_path, timeout=3.0)
                chk_conn.execute("PRAGMA busy_timeout=3000;")

                # Attempt immediate lock acquisition to detect other writers
                try:
                    chk_conn.execute("BEGIN IMMEDIATE;")
                    chk_conn.rollback()
                except sqlite3.OperationalError as exc:
                    chk_conn.close()
                    raise DatabaseInUseError(
                        f"Target database is currently locked by another active process: {exc}. "
                        "Pass force=True to override."
                    ) from exc

                # Check for active running workflows
                has_active_runs = False
                has_active_leases = False
                cursor = chk_conn.cursor()

                tables = {
                    r[0]
                    for r in cursor.execute(
                        "SELECT name FROM sqlite_master WHERE type='table';"
                    ).fetchall()
                }

                if "runs" in tables:
                    running_cnt = cursor.execute(
                        "SELECT COUNT(*) FROM runs WHERE status IN ('running', 'in_progress');"
                    ).fetchone()[0]
                    if running_cnt > 0:
                        has_active_runs = True

                if "workflow_admissions" in tables:
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    active_leases_cnt = cursor.execute(
                        "SELECT COUNT(*) FROM workflow_admissions WHERE expires_at > ?;",
                        (now_iso,),
                    ).fetchone()[0]
                    if active_leases_cnt > 0:
                        has_active_leases = True

                chk_conn.close()

                if has_active_runs or has_active_leases:
                    raise DatabaseInUseError(
                        "Target database has active workflows or unexpired admission leases. "
                        "Restoring now could corrupt in-flight state. Pass force=True to override."
                    )
            except sqlite3.DatabaseError:
                # If target is corrupt or empty, proceed with restore
                pass

        # 3. Create pre-restore safety backup
        if target_exists and safety_backup:
            try:
                safety_meta = self.create_backup(
                    db_path=target_path,
                    dest=safety_backup_dest,
                    tag="pre_restore_safety",
                    overwrite=False,
                )
                safety_backup_path = safety_meta.backup_path
                logger.info("Pre-restore safety backup created: %s", safety_backup_path)
            except Exception as exc:
                if not force:
                    raise BackupError(
                        f"Failed to create pre-restore safety backup: {exc}. "
                        "Pass force=True to proceed without safety backup."
                    ) from exc

        # 4. Perform atomic restore using SQLite backup API
        target_dir = os.path.dirname(target_path)
        if target_dir:
            os.makedirs(target_dir, exist_ok=True)

        backup_conn = sqlite3.connect(clean_backup_path, timeout=15.0)
        backup_conn.execute("PRAGMA busy_timeout=5000;")

        target_conn = sqlite3.connect(target_path, timeout=15.0)
        target_conn.execute("PRAGMA busy_timeout=5000;")

        try:
            backup_conn.backup(target_conn, pages=-1)
            target_conn.commit()

            # Ensure restored database is in WAL mode and checkpointed
            target_conn.execute("PRAGMA journal_mode=WAL;")
            target_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
            target_conn.commit()
        except Exception as exc:
            target_conn.close()
            backup_conn.close()
            raise BackupError(f"Restore failed during page copy: {exc}") from exc
        finally:
            target_conn.close()
            backup_conn.close()

        # 5. Post-restore verification
        post_conn = sqlite3.connect(target_path, timeout=10.0)
        restored_tables: list[str] = []
        table_counts: dict[str, int] = {}
        total_records = 0
        integrity_status = "ok"

        try:
            post_conn.execute("PRAGMA busy_timeout=5000;")
            res = post_conn.execute("PRAGMA integrity_check;").fetchone()
            if not res or res[0] != "ok":
                integrity_status = str(res[0]) if res else "failed"
                raise CorruptBackupError(
                    f"Post-restore integrity check failed: {integrity_status}"
                )

            cursor = post_conn.cursor()
            for (tbl,) in cursor.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
            ).fetchall():
                restored_tables.append(tbl)
                cnt = cursor.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
                table_counts[tbl] = cnt
                total_records += cnt
        finally:
            post_conn.close()

        duration_ms = (time.perf_counter() - start_time) * 1000.0
        logger.info(
            "Database restored successfully into %s from %s (%.1f ms)",
            target_path,
            clean_backup_path,
            duration_ms,
        )

        return RestoreResult(
            success=True,
            target_db_path=target_path,
            backup_path=clean_backup_path,
            safety_backup_path=safety_backup_path,
            restored_tables=restored_tables,
            table_counts=table_counts,
            total_records=total_records,
            integrity=integrity_status,
            duration_ms=round(duration_ms, 2),
        )

    def list_backups(self, dest_dir: str | pathlib.Path | None = None) -> list[BackupMetadata]:
        """Enumerate and inspect all backup archives in the backup directory."""
        search_dir = os.path.abspath(str(dest_dir or self.backup_dir).strip())
        if not os.path.exists(search_dir) or not os.path.isdir(search_dir):
            return []

        entries = os.listdir(search_dir)
        backups: list[BackupMetadata] = []

        for entry in entries:
            ext = os.path.splitext(entry)[1].lower()
            if ext not in (".db", ".sqlite", ".sqlite3", ".bak"):
                continue

            full_path = os.path.join(search_dir, entry)
            if not os.path.isfile(full_path):
                continue

            try:
                # Check magic bytes to confirm valid SQLite archive
                with open(full_path, "rb") as fh:
                    if fh.read(16) != SQLITE_HEADER_MAGIC:
                        continue

                mtime = os.path.getmtime(full_path)
                mtime_dt = datetime.datetime.fromtimestamp(mtime, tz=datetime.timezone.utc)
                size_bytes = os.path.getsize(full_path)

                # Quick integrity and table scan
                conn = sqlite3.connect(full_path, timeout=5.0)
                table_counts: dict[str, int] = {}
                integrity_str = "ok"
                try:
                    chk = conn.execute("PRAGMA quick_check;").fetchone()
                    if not chk or chk[0] != "ok":
                        integrity_str = "corrupt"
                    else:
                        cursor = conn.cursor()
                        for (tbl,) in cursor.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                        ).fetchall():
                            cnt = cursor.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
                            table_counts[tbl] = cnt
                finally:
                    conn.close()

                base, _ = os.path.splitext(entry)
                parts = base.split("_")
                tag_val: str | None = None
                if (
                    len(parts) >= 3
                    and parts[-1].isdigit()
                    and len(parts[-1]) == 6
                    and parts[-2].isdigit()
                    and len(parts[-2]) == 8
                ):
                    tag_parts = parts[1:-2]
                    tag_val = "_".join(tag_parts) if tag_parts else None

                m_path = self.get_manifest_path(full_path)
                m_status = "unverified"
                m_sha = None
                m_sig_status = "unsigned"
                m_kid = None
                if os.path.exists(m_path):
                    try:
                        with open(m_path, encoding="utf-8") as mfh:
                            m_obj = json.load(mfh)
                            payload = (
                                m_obj.get("payload", m_obj)
                                if isinstance(m_obj, dict)
                                else {}
                            )
                            m_sha = payload.get("sha256")
                            sig_meta = (
                                m_obj.get("signature")
                                if isinstance(m_obj, dict)
                                else None
                            )
                            if sig_meta and isinstance(sig_meta, dict):
                                m_sig_status = "signed"
                                m_kid = sig_meta.get("key_id")
                            m_status = "verified"
                    except Exception:
                        m_status = "failed"

                backups.append(
                    BackupMetadata(
                        backup_path=full_path,
                        source_db_path=self.db_path,
                        timestamp=mtime_dt.isoformat(),
                        size_bytes=size_bytes,
                        integrity=integrity_str,
                        table_counts=table_counts,
                        tag=tag_val,
                        manifest_path=m_path if os.path.exists(m_path) else None,
                        sha256=m_sha,
                        manifest_status=m_status,
                        signature_status=m_sig_status,
                        key_id=m_kid,
                    )
                )
            except Exception:
                continue

        # Sort newest to oldest
        backups.sort(key=lambda b: b.timestamp, reverse=True)
        return backups

    def _clean_orphaned_manifests(self, dest_dir: str | pathlib.Path) -> None:
        """Remove .manifest.json files whose companion backup archive no longer exists."""
        try:
            target_dir = os.path.abspath(str(dest_dir))
            if not os.path.isdir(target_dir):
                return
            for fname in os.listdir(target_dir):
                if fname.endswith(".manifest.json"):
                    archive_name = fname[:-len(".manifest.json")]
                    archive_path = os.path.join(target_dir, archive_name)
                    if not os.path.exists(archive_path):
                        orphan_path = os.path.join(target_dir, fname)
                        try:
                            os.remove(orphan_path)
                            logger.info("Cleaned orphaned backup manifest: %s", orphan_path)
                        except OSError:
                            pass
        except Exception as exc:
            logger.debug("Failed scanning for orphaned manifests: %s", exc)

    def clean_backups(
        self,
        dest_dir: str | pathlib.Path | None = None,
        keep_count: int | None = None,
        keep_days: int | None = None,
        dry_run: bool = False,
    ) -> RetentionResult:
        """Enforce retention policy: remove expired backups while protecting latest."""
        target_dir = dest_dir if dest_dir is not None else self.backup_dir
        if not dry_run and os.path.isdir(target_dir):
            self._clean_orphaned_manifests(target_dir)

        all_backups = self.list_backups(dest_dir=dest_dir)
        if len(all_backups) <= 1:
            # Protect the single latest valid backup; never delete when <= 1 exists
            retained_paths = [b.backup_path for b in all_backups]
            return RetentionResult(
                retained=retained_paths,
                deleted=[],
                total_retained=len(retained_paths),
                total_deleted=0,
                dry_run=dry_run,
            )

        effective_count = (
            keep_count
            if keep_count is not None
            else getattr(self.settings, "backup_retention_count", 10)
        )
        effective_days = (
            keep_days
            if keep_days is not None
            else getattr(self.settings, "backup_retention_days", 30)
        )

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        cutoff_dt: datetime.datetime | None = None
        if effective_days is not None and effective_days > 0:
            cutoff_dt = now_dt - datetime.timedelta(days=effective_days)

        to_delete: list[str] = []
        to_retain: list[str] = []

        # Index 0 is the newest backup; ALWAYS retained
        to_retain.append(all_backups[0].backup_path)

        for idx, b in enumerate(all_backups[1:], start=1):
            is_expired = False
            # Count-based retention
            if effective_count is not None and effective_count > 0 and idx >= effective_count:
                is_expired = True

            # Age-based retention
            if cutoff_dt is not None:
                try:
                    b_dt = datetime.datetime.fromisoformat(b.timestamp)
                    if b_dt < cutoff_dt:
                        is_expired = True
                except Exception:
                    pass

            if is_expired:
                to_delete.append(b.backup_path)
            else:
                to_retain.append(b.backup_path)

        deleted: list[str] = []
        for path in to_delete:
            if not dry_run:
                try:
                    os.remove(path)
                    # Clean companion manifest file if present
                    manifest_file = self.get_manifest_path(path)
                    if os.path.exists(manifest_file):
                        try:
                            os.remove(manifest_file)
                        except OSError:
                            pass
                    # Clean companion WAL / SHM files if left over
                    for companion in (f"{path}-wal", f"{path}-shm"):
                        if os.path.exists(companion):
                            try:
                                os.remove(companion)
                            except OSError:
                                pass
                    deleted.append(path)
                    logger.info("Deleted expired backup: %s", path)
                except Exception as exc:
                    logger.warning("Failed to delete expired backup %s: %s", path, exc)
            else:
                deleted.append(path)

        # Clean orphaned manifest files in backup directory
        search_dir = os.path.abspath(str(dest_dir or self.backup_dir).strip())
        if os.path.isdir(search_dir) and not dry_run:
            try:
                for fname in os.listdir(search_dir):
                    if fname.endswith(".manifest.json"):
                        target_archive = fname[:-14]
                        if not os.path.isfile(os.path.join(search_dir, target_archive)):
                            try:
                                os.remove(os.path.join(search_dir, fname))
                            except OSError:
                                pass
            except Exception:
                pass

        return RetentionResult(
            retained=to_retain,
            deleted=deleted,
            total_retained=len(to_retain),
            total_deleted=len(deleted),
            dry_run=dry_run,
        )


    def run_restore_drill(
        self,
        backup_path: str | pathlib.Path | None = None,
        target_temp_dir: str | pathlib.Path | None = None,
        sample_records: bool = True,
    ) -> RestoreDrillResult:
        """Safely restore a backup into an isolated database and verify operational recovery.

        1. Selects existing backup (latest by default, or specific path).
        2. Validates backup existence and file magic bytes.
        3. Restores into a dedicated temporary SQLite database.
        4. Strictly guarantees live production database is never modified or targeted.
        5. Performs SQLite integrity, quick, and foreign key verification.
        6. Verifies schema tables and samples deserialization of persisted records.
        7. Safely cleans up temporary databases even if errors occur.
        """
        start_time = time.perf_counter()
        drill_id = f"drill_{uuid.uuid4().hex[:12]}"
        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        errors: list[str] = []

        # 1. Select and resolve target backup file
        if backup_path is None:
            backups = self.list_backups()
            if not backups:
                return RestoreDrillResult(
                    drill_id=drill_id,
                    success=False,
                    backup_path="",
                    errors=["No backup archives found to drill."],
                    cleaned_up=True,
                    timestamp=now_iso,
                )
            clean_backup_path = backups[0].backup_path
            backup_ts = backups[0].timestamp
        else:
            clean_backup_path = os.path.abspath(str(backup_path).strip())
            if not os.path.isfile(clean_backup_path):
                return RestoreDrillResult(
                    drill_id=drill_id,
                    success=False,
                    backup_path=clean_backup_path,
                    errors=[f"Backup file not found: {clean_backup_path}"],
                    cleaned_up=True,
                    timestamp=now_iso,
                )
            mtime = os.path.getmtime(clean_backup_path)
            backup_ts = datetime.datetime.fromtimestamp(
                mtime, tz=datetime.timezone.utc
            ).isoformat()

        backup_age_seconds = max(0.0, time.time() - os.path.getmtime(clean_backup_path))

        # 2. Check basic file size and magic bytes
        try:
            file_size = os.path.getsize(clean_backup_path)
            if file_size < 100:
                errors.append(
                    f"Backup archive is too small ({file_size} bytes) to be valid SQLite."
                )
            else:
                with open(clean_backup_path, "rb") as fh:
                    if fh.read(16) != SQLITE_HEADER_MAGIC:
                        errors.append(
                            "File does not start with valid SQLite format 3 magic header."
                        )
        except Exception as exc:
            errors.append(f"Unable to inspect backup archive header: {exc}")

        # Verify cryptographic manifest if present
        manifest_res = self.verify_manifest(clean_backup_path)
        if manifest_res.status == "failed":
            errors.append(f"Candidate backup manifest verification failed: {manifest_res.error}")

        # If archive header or manifest is invalid, return failed drill immediately
        if errors:
            duration_ms = (time.perf_counter() - start_time) * 1000.0
            integrity_stat = "manifest_failed" if manifest_res.status == "failed" else "header_invalid"
            return RestoreDrillResult(
                drill_id=drill_id,
                success=False,
                backup_path=clean_backup_path,
                backup_timestamp=backup_ts,
                backup_age_seconds=round(backup_age_seconds, 2),
                duration_ms=round(duration_ms, 2),
                integrity_check=integrity_stat,
                quick_check=integrity_stat,
                foreign_key_check=integrity_stat,
                errors=errors,
                cleaned_up=True,
                manifest_status=manifest_res.status,
                sha256=manifest_res.computed_sha256,
                signature_status=manifest_res.signature_status,
                key_id=manifest_res.key_id,
                timestamp=now_iso,
            )

        # 3. Setup isolated temporary directory and database path
        temp_dir_obj: tempfile.TemporaryDirectory[str] | None = None
        custom_dir = False
        if target_temp_dir is not None:
            work_dir = os.path.abspath(str(target_temp_dir).strip())
            os.makedirs(work_dir, exist_ok=True)
            custom_dir = True
        else:
            temp_dir_obj = tempfile.TemporaryDirectory(prefix="orch_drill_")
            work_dir = temp_dir_obj.name

        target_temp_path = os.path.join(work_dir, f"restore_drill_{drill_id}.db")

        # CRITICAL SAFETY CHECK: Strictly prevent live production database modification
        live_db_abs = os.path.abspath(self.db_path)
        drill_target_abs = os.path.abspath(target_temp_path)
        if drill_target_abs == live_db_abs:
            if temp_dir_obj:
                temp_dir_obj.cleanup()
            raise BackupError(
                "CRITICAL: Drill target matches live production database path! Aborting drill."
            )

        integrity_check = "error"
        quick_check = "error"
        foreign_key_check = "error"
        tables_verified: list[str] = []
        table_counts: dict[str, int] = {}
        records_verified = 0
        queries_verified = 0
        cleaned_up = False

        try:
            # 4. Atomic page restore into isolated temporary target
            backup_conn = sqlite3.connect(clean_backup_path, timeout=15.0)
            backup_conn.execute("PRAGMA busy_timeout=5000;")

            drill_conn = sqlite3.connect(target_temp_path, timeout=15.0)
            drill_conn.execute("PRAGMA busy_timeout=5000;")

            try:
                backup_conn.backup(drill_conn, pages=-1)
                drill_conn.commit()
                drill_conn.execute("PRAGMA journal_mode=WAL;")
                drill_conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
                drill_conn.commit()
            except Exception as exc:
                errors.append(f"Restore drill page copy failed: {exc}")
            finally:
                drill_conn.close()
                backup_conn.close()

            # 5. Recovery and schema verification on isolated target
            if not errors and os.path.exists(target_temp_path):
                verify_conn = sqlite3.connect(target_temp_path, timeout=10.0)
                try:
                    verify_conn.row_factory = sqlite3.Row
                    verify_conn.execute("PRAGMA busy_timeout=5000;")

                    # PRAGMA integrity_check
                    chk = verify_conn.execute("PRAGMA integrity_check;").fetchall()
                    if chk and chk[0][0] == "ok":
                        integrity_check = "ok"
                    else:
                        integrity_check = "; ".join(str(r[0]) for r in chk)
                        errors.append(f"Integrity check failed: {integrity_check}")

                    # PRAGMA quick_check
                    qchk = verify_conn.execute("PRAGMA quick_check;").fetchall()
                    if qchk and qchk[0][0] == "ok":
                        quick_check = "ok"
                    else:
                        quick_check = "; ".join(str(r[0]) for r in qchk)
                        errors.append(f"Quick check failed: {quick_check}")

                    # PRAGMA foreign_key_check
                    fk = verify_conn.execute("PRAGMA foreign_key_check;").fetchall()
                    if fk:
                        foreign_key_check = f"{len(fk)} violation(s)"
                        errors.append(f"Foreign key violations detected: {foreign_key_check}")
                    else:
                        foreign_key_check = "ok"

                    # Table inventory
                    cur = verify_conn.cursor()
                    found_tables = [
                        r[0]
                        for r in cur.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name NOT LIKE 'sqlite_%' ORDER BY name;"
                        ).fetchall()
                    ]
                    tables_verified = found_tables

                    for tbl in found_tables:
                        try:
                            cnt = cur.execute(f"SELECT COUNT(*) FROM {tbl};").fetchone()[0]
                            table_counts[tbl] = cnt
                        except Exception as exc:
                            table_counts[tbl] = -1
                            errors.append(f"Failed counting records in {tbl}: {exc}")

                    # Sample critical table records and verify JSON deserialization
                    if sample_records:
                        sample_queries = [
                            (
                                "runs",
                                "SELECT run_id, task, status FROM runs LIMIT 20;",
                                [],
                            ),
                            (
                                "checkpoints",
                                "SELECT run_id, step, state_json FROM checkpoints LIMIT 20;",
                                ["state_json"],
                            ),
                            (
                                "audit_events",
                                "SELECT event_id, event_type, metadata_json FROM audit_events LIMIT 20;",
                                ["metadata_json"],
                            ),
                            (
                                "outbox_events",
                                "SELECT event_id, event_type, payload FROM outbox_events LIMIT 20;",
                                ["payload"],
                            ),
                            (
                                "scheduler_leases",
                                "SELECT lease_name, owner_id, expires_at FROM scheduler_leases LIMIT 20;",
                                [],
                            ),
                            (
                                "workflow_admissions",
                                "SELECT run_id, owner_id, expires_at FROM workflow_admissions LIMIT 20;",
                                [],
                            ),
                        ]
                        for tbl_name, q_str, json_cols in sample_queries:
                            if tbl_name in found_tables:
                                try:
                                    queries_verified += 1
                                    rows = cur.execute(q_str).fetchall()
                                    for row in rows:
                                        records_verified += 1
                                        for col in json_cols:
                                            raw_val = row[col]
                                            if raw_val:
                                                try:
                                                    json.loads(raw_val)
                                                except Exception as jexc:
                                                    errors.append(
                                                        f"JSON deserialization failure in "
                                                        f"{tbl_name}.{col}: {jexc}"
                                                    )
                                except Exception as qexc:
                                    errors.append(
                                        f"Failed sampling records from {tbl_name}: {qexc}"
                                    )
                finally:
                    verify_conn.close()

        finally:
            # 6. Safe cleanup of isolated temporary files (Windows file locks must be released)
            if temp_dir_obj is not None:
                try:
                    temp_dir_obj.cleanup()
                    cleaned_up = True
                except Exception as c_exc:
                    logger.warning("Failed cleaning temporary drill directory: %s", c_exc)
            elif custom_dir:
                for suffix in ("", "-wal", "-shm"):
                    fpath = target_temp_path + suffix
                    if os.path.exists(fpath):
                        try:
                            os.remove(fpath)
                        except OSError:
                            pass
                cleaned_up = not os.path.exists(target_temp_path)

        duration_ms = (time.perf_counter() - start_time) * 1000.0
        success = (
            len(errors) == 0
            and integrity_check == "ok"
            and quick_check == "ok"
            and foreign_key_check == "ok"
        )

        return RestoreDrillResult(
            drill_id=drill_id,
            success=success,
            backup_path=clean_backup_path,
            backup_timestamp=backup_ts,
            backup_age_seconds=round(backup_age_seconds, 2),
            duration_ms=round(duration_ms, 2),
            target_temp_path=target_temp_path,
            integrity_check=integrity_check,
            quick_check=quick_check,
            foreign_key_check=foreign_key_check,
            tables_verified=tables_verified,
            records_verified=records_verified,
            queries_verified=queries_verified,
            table_counts=table_counts,
            errors=errors,
            cleaned_up=cleaned_up,
            manifest_status=manifest_res.status,
            sha256=manifest_res.computed_sha256,
            signature_status=manifest_res.signature_status,
            key_id=manifest_res.key_id,
            timestamp=now_iso,
        )


# Module-level convenience wrappers


def create_backup(
    db_path: str | pathlib.Path | None = None,
    dest: str | pathlib.Path | None = None,
    tag: str | None = None,
    overwrite: bool = False,
    settings: Settings | None = None,
) -> BackupMetadata:
    """Create an online crash-consistent SQLite database backup."""
    manager = BackupManager(settings=settings, db_path=db_path)
    return manager.create_backup(db_path=db_path, dest=dest, tag=tag, overwrite=overwrite)


def validate_backup(
    backup_path: str | pathlib.Path,
    deep: bool = True,
    settings: Settings | None = None,
) -> BackupValidationResult:
    """Inspect and validate integrity and schema of a database backup."""
    manager = BackupManager(settings=settings)
    return manager.validate_backup(backup_path=backup_path, deep=deep)


def restore_backup(
    backup_path: str | pathlib.Path,
    target_db_path: str | pathlib.Path | None = None,
    force: bool = False,
    safety_backup: bool = True,
    safety_backup_dest: str | pathlib.Path | None = None,
    settings: Settings | None = None,
) -> RestoreResult:
    """Safely restore a database from an verified backup archive."""
    manager = BackupManager(settings=settings, db_path=target_db_path)
    return manager.restore_backup(
        backup_path=backup_path,
        target_db_path=target_db_path,
        force=force,
        safety_backup=safety_backup,
        safety_backup_dest=safety_backup_dest,
    )


def list_backups(
    dest_dir: str | pathlib.Path | None = None,
    settings: Settings | None = None,
) -> list[BackupMetadata]:
    """Enumerate all available backups in the backup directory."""
    manager = BackupManager(settings=settings, backup_dir=dest_dir)
    return manager.list_backups(dest_dir=dest_dir)


def clean_backups(
    dest_dir: str | pathlib.Path | None = None,
    keep_count: int | None = None,
    keep_days: int | None = None,
    dry_run: bool = False,
    settings: Settings | None = None,
) -> RetentionResult:
    """Enforce backup retention policy by purging expired archives."""
    manager = BackupManager(settings=settings, backup_dir=dest_dir)
    return manager.clean_backups(
        dest_dir=dest_dir,
        keep_count=keep_count,
        keep_days=keep_days,
        dry_run=dry_run,
    )


def run_restore_drill(
    backup_path: str | pathlib.Path | None = None,
    target_temp_dir: str | pathlib.Path | None = None,
    sample_records: bool = True,
    settings: Settings | None = None,
) -> RestoreDrillResult:
    """Execute an automated, isolated database restore drill and recovery verification."""
    manager = BackupManager(settings=settings)
    return manager.run_restore_drill(
        backup_path=backup_path,
        target_temp_dir=target_temp_dir,
        sample_records=sample_records,
    )


def verify_manifest(
    backup_path: str | pathlib.Path,
    manifest_path: str | pathlib.Path | None = None,
    settings: Settings | None = None,
) -> ManifestVerificationResult:
    """Verify cryptographic SHA-256 integrity manifest for a database backup archive."""
    manager = BackupManager(settings=settings)
    return manager.verify_manifest(backup_path=backup_path, manifest_path=manifest_path)


def backup_cli(argv: list[str] | None = None) -> int:
    """CLI sub-command dispatcher for backup, restore, validation, and retention."""
    parser = argparse.ArgumentParser(
        prog="orchestrator backup",
        description="Database backup, restore, validation, and disaster recovery commands.",
    )
    subparsers = parser.add_subparsers(dest="backup_command", help="Backup operation to execute")

    # backup create
    p_create = subparsers.add_parser("create", help="Create a new online database backup.")
    p_create.add_argument("--dest", default=None, help="Destination directory or file path.")
    p_create.add_argument("--db-path", default=None, help="Source database path override.")
    p_create.add_argument("--tag", default=None, help="Optional label to include in backup filename.")
    p_create.add_argument("--overwrite", action="store_true", help="Allow overwriting existing destination.")
    p_create.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup list
    p_list = subparsers.add_parser("list", help="List available database backup archives.")
    p_list.add_argument("--dir", default=None, help="Directory to search for backups.")
    p_list.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup validate
    p_validate = subparsers.add_parser("validate", help="Validate an existing backup archive.")
    p_validate.add_argument("backup_path", help="Path to backup archive to validate.")
    p_validate.add_argument("--quick", action="store_true", help="Skip deep foreign key validation.")
    p_validate.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup verify
    p_verify = subparsers.add_parser(
        "verify",
        help="Verify cryptographic SHA-256 integrity manifest of a backup archive.",
    )
    p_verify.add_argument("backup_path", help="Path to backup archive to verify.")
    p_verify.add_argument("--manifest-path", default=None, help="Manifest file override.")
    p_verify.add_argument("--db-path", default=None, help="Source database path override.")
    p_verify.add_argument("--dir", default=None, help="Backup directory override.")
    p_verify.add_argument("--strict", action="store_true", help="Enforce strict signature policy.")
    p_verify.add_argument("--json", action="store_true", help="Emit JSON output.")

    p_keys = subparsers.add_parser(
        "keys", help="Inspect backup signing and verification key identifiers safely."
    )
    p_keys.add_argument("--db-path", default=None, help="Source database path override.")
    p_keys.add_argument("--dir", default=None, help="Backup directory override.")
    p_keys.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup restore
    p_restore = subparsers.add_parser("restore", help="Restore database from a backup archive.")
    p_restore.add_argument("backup_path", help="Path to backup archive to restore.")
    p_restore.add_argument("--target", default=None, help="Target database path override.")
    p_restore.add_argument("--force", action="store_true", help="Force restore despite active database runs.")
    p_restore.add_argument(
        "--no-safety-backup",
        action="store_true",
        help="Skip pre-restore safety backup creation.",
    )
    p_restore.add_argument("--safety-backup-dest", default=None, help="Safety backup destination directory.")
    p_restore.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup clean
    p_clean = subparsers.add_parser("clean", help="Purge expired backups according to retention policy.")
    p_clean.add_argument("--dir", default=None, help="Backup directory.")
    p_clean.add_argument("--keep-count", type=int, default=None, help="Maximum number of backups to retain.")
    p_clean.add_argument("--keep-days", type=int, default=None, help="Maximum age in days to retain.")
    p_clean.add_argument("--dry-run", action="store_true", help="Preview deletions without removing files.")
    p_clean.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup status
    p_status = subparsers.add_parser(
        "status",
        help="Inspect backup scheduler state and operational diagnostics.",
    )
    p_status.add_argument("--dir", default=None, help="Backup directory override.")
    p_status.add_argument("--db-path", default=None, help="Source database path override.")
    p_status.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup trigger
    p_trigger = subparsers.add_parser("trigger", help="Trigger an immediate backup execution cycle.")
    p_trigger.add_argument("--dest", default=None, help="Destination directory override.")
    p_trigger.add_argument("--db-path", default=None, help="Source database path override.")
    p_trigger.add_argument("--tag", default=None, help="Optional label to include in backup filename.")
    p_trigger.add_argument("--reason", default="CLI manual trigger", help="Auditable trigger reason.")
    p_trigger.add_argument("--json", action="store_true", help="Emit JSON output.")

    # backup drill
    p_drill = subparsers.add_parser(
        "drill",
        help="Execute or inspect automated database restore drills.",
    )
    drill_subparsers = p_drill.add_subparsers(
        dest="drill_action",
        help="Restore drill action to execute",
    )

    # drill run
    p_drill_run = drill_subparsers.add_parser("run", help="Run an isolated restore drill.")
    p_drill_run.add_argument(
        "--backup-path",
        default=None,
        help="Specific backup file to test (defaults to latest backup).",
    )
    p_drill_run.add_argument("--temp-dir", default=None, help="Temporary directory for drill.")
    p_drill_run.add_argument(
        "--no-sample",
        action="store_true",
        help="Skip record sampling and JSON verification.",
    )
    p_drill_run.add_argument("--db-path", default=None, help="Database path override.")
    p_drill_run.add_argument("--dir", default=None, help="Backup directory override.")
    p_drill_run.add_argument("--json", action="store_true", help="Emit JSON output.")

    # drill status
    p_drill_status = drill_subparsers.add_parser(
        "status",
        help="Inspect the latest database restore drill status.",
    )
    p_drill_status.add_argument("--db-path", default=None, help="Database path override.")
    p_drill_status.add_argument("--json", action="store_true", help="Emit JSON output.")

    # drill history
    p_drill_history = drill_subparsers.add_parser(
        "history",
        help="List historical restore drill verification runs.",
    )
    p_drill_history.add_argument("--limit", type=int, default=20, help="Maximum entries to return.")
    p_drill_history.add_argument("--db-path", default=None, help="Database path override.")
    p_drill_history.add_argument("--json", action="store_true", help="Emit JSON output.")

    args = parser.parse_args(argv)

    if not args.backup_command:
        parser.print_help()
        return 1

    try:
        if args.backup_command == "create":
            meta = create_backup(
                db_path=args.db_path,
                dest=args.dest,
                tag=args.tag,
                overwrite=args.overwrite,
            )
            if args.json:
                print(json.dumps(meta.to_dict(), indent=2))
            else:
                print("\n=== DATABASE BACKUP CREATED ===")
                print(f"Backup file:   {meta.backup_path}")
                print(f"Source:        {meta.source_db_path}")
                print(f"Size:          {meta.size_bytes:,} bytes")
                print(f"Integrity:     {meta.integrity}")
                print(f"Duration:      {meta.duration_ms:.1f} ms")
                print(f"Timestamp:     {meta.timestamp}")
                print(f"Tables backed: {len(meta.table_counts)}")
            return 0

        elif args.backup_command == "list":
            backups = list_backups(dest_dir=args.dir)
            if args.json:
                print(json.dumps([b.to_dict() for b in backups], indent=2))
            else:
                print(f"\n=== AVAILABLE DATABASE BACKUPS ({len(backups)}) ===")
                if not backups:
                    print("No backup archives found.")
                else:
                    for b in backups:
                        print(
                            f"[{b.timestamp}] {os.path.basename(b.backup_path)} "
                            f"({b.size_bytes:,} bytes, integrity: {b.integrity}, "
                            f"manifest: {b.manifest_status})"
                        )
            return 0

        elif args.backup_command == "validate":
            val = validate_backup(backup_path=args.backup_path, deep=not args.quick)
            if args.json:
                print(json.dumps(val.to_dict(), indent=2))
            else:
                status_str = "VALID" if val.is_valid else "INVALID"
                print(f"\n=== BACKUP VALIDATION: {status_str} ===")
                print(f"File:         {val.backup_path}")
                print(f"Size:         {val.size_bytes:,} bytes")
                print(f"Integrity:    {val.integrity_check}")
                print(f"Manifest:     {val.manifest_status} (sha256: {val.sha256 or 'none'})")
                print(f"Quick check:  {val.quick_check}")
                print(f"Foreign keys: {val.foreign_key_check}")
                print(f"Tables:       {len(val.tables)} present, {len(val.missing_tables)} missing")
                print(f"Records:      {val.total_records:,}")
                if val.errors:
                    print("Errors:")
                    for err in val.errors:
                        print(f"  - {err}")
            return 0

        elif args.backup_command == "verify":
            mgr = BackupManager(db_path=args.db_path, backup_dir=args.dir)
            res = mgr.verify_manifest(
                backup_path=args.backup_path,
                manifest_path=args.manifest_path,
                strict_signature=getattr(args, "strict", False),
            )
            if args.json:
                print(json.dumps(res.to_dict(), indent=2))
            else:
                status_header = res.status.upper()
                print(f"\n=== BACKUP INTEGRITY VERIFICATION: {status_header} ===")
                print(f"Archive:       {res.backup_path}")
                print(f"Manifest:      {res.manifest_path or 'None'}")
                print(f"Status:        {res.status}")
                print(f"Signature:     {res.signature_status}")
                if res.key_id:
                    print(f"Key ID:        {res.key_id}")
                if res.computed_sha256:
                    print(f"Computed SHA:  {res.computed_sha256}")
                if res.expected_sha256:
                    print(f"Expected SHA:  {res.expected_sha256}")
                if res.actual_size_bytes is not None:
                    print(f"Archive Size:  {res.actual_size_bytes:,} bytes")
                print(f"Duration:      {res.duration_ms:.1f} ms")
                if res.error:
                    print(f"Details:       {res.error}")
            return 0 if res.status in ("verified", "unverified") and res.is_valid else 1

        elif args.backup_command == "keys":
            mgr = BackupManager(db_path=args.db_path, backup_dir=args.dir)
            key_info = mgr.settings.get_safe_backup_keyring_info()
            if args.json:
                print(json.dumps(key_info, indent=2))
            else:
                print("\n=== BACKUP SIGNING & VERIFICATION KEYS ===")
                print(f"Signing enabled:    {key_info['signing_enabled']}")
                print(f"Active key ID:      {key_info['active_key_id'] or 'None'}")
                print(f"Verification keys:  {key_info['verification_keys_count']}")
                for kid in key_info['verification_key_ids']:
                    marker = " (active)" if kid == key_info['active_key_id'] else ""
                    print(f"  - {kid}{marker}")
                print(f"Strict required:    {key_info['strict_signature_required']}")
            return 0

        elif args.backup_command == "restore":
            res = restore_backup(
                backup_path=args.backup_path,
                target_db_path=args.target,
                force=args.force,
                safety_backup=not args.no_safety_backup,
                safety_backup_dest=args.safety_backup_dest,
            )
            if args.json:
                print(json.dumps(res.to_dict(), indent=2))
            else:
                print("\n=== DATABASE RESTORE COMPLETED ===")
                print(f"Restored into:    {res.target_db_path}")
                print(f"From archive:     {res.backup_path}")
                if res.safety_backup_path:
                    print(f"Safety backup:    {res.safety_backup_path}")
                print(f"Integrity check:  {res.integrity}")
                print(f"Restored tables:  {len(res.restored_tables)}")
                print(f"Total records:    {res.total_records:,}")
                print(f"Duration:         {res.duration_ms:.1f} ms")
            return 0

        elif args.backup_command == "clean":
            ret = clean_backups(
                dest_dir=args.dir,
                keep_count=args.keep_count,
                keep_days=args.keep_days,
                dry_run=args.dry_run,
            )
            if args.json:
                print(json.dumps(ret.to_dict(), indent=2))
            else:
                mode_str = "[DRY-RUN] " if ret.dry_run else ""
                print(f"\n=== {mode_str}BACKUP RETENTION CLEANUP ===")
                print(f"Retained backups: {ret.total_retained}")
                print(f"Deleted backups:  {ret.total_deleted}")
                if ret.deleted:
                    print("Removed:")
                    for d in ret.deleted:
                        print(f"  - {d}")
            return 0

        elif args.backup_command == "status":
            from .scheduler import DatabaseBackupScheduler, get_backup_scheduler
            from .store import CheckpointStore

            store = CheckpointStore(db_path=args.db_path) if args.db_path else None
            scheduler = (
                DatabaseBackupScheduler(store=store, backup_dir=args.dir)
                if (args.dir or args.db_path)
                else get_backup_scheduler(store=store)
            )
            status_data = scheduler.get_status()
            if args.json:
                print(json.dumps(status_data, indent=2))
            else:
                print("\n=== DATABASE BACKUP SCHEDULER STATUS ===")
                print(f"Enabled:          {status_data['enabled']}")
                print(f"Running:          {status_data['running']}")
                print(
                    f"Leadership:       {status_data['leadership_state']} "
                    f"(leader={status_data['is_leader']})"
                )
                print(f"Owner ID:         {status_data['owner_id']}")
                print(f"Fence Token:      {status_data['fence_token']}")
                print(f"Backup Dir:       {status_data['backup_dir']}")
                print(f"Interval:         {status_data['interval_seconds']}s")
                print(f"Last Success:     {status_data['last_success_at'] or 'None'}")
                print(f"Last Failure:     {status_data['last_failed_at'] or 'None'}")
                print(f"Next Scheduled:   {status_data['next_scheduled_run_at'] or 'None'}")
                print(f"Total Backups:    {status_data['total_backups_count']}")
                print(f"Failed Count:     {status_data['failure_count']}")
                if status_data.get("last_backup_path"):
                    print(
                        f"Latest Backup:    {status_data['last_backup_path']} "
                        f"({status_data['last_size_bytes']:,} bytes)"
                    )
            return 0

        elif args.backup_command == "trigger":
            from .scheduler import DatabaseBackupScheduler, get_backup_scheduler
            from .store import CheckpointStore

            store = CheckpointStore(db_path=args.db_path) if args.db_path else None
            scheduler = (
                DatabaseBackupScheduler(store=store, backup_dir=args.dest)
                if (args.dest or args.db_path)
                else get_backup_scheduler(store=store)
            )
            result = scheduler.trigger_backup(
                actor="cli",
                operator="cli_operator",
                reason=args.reason,
                tag=args.tag,
            )
            if args.json:
                print(json.dumps(result, indent=2))
            else:
                status_str = result.get("status", "").upper()
                print(f"\n=== DATABASE BACKUP TRIGGER: {status_str} ===")
                print(f"Message:      {result.get('message')}")
                if result.get("metadata"):
                    meta = result["metadata"]
                    print(f"Backup file:  {meta.get('backup_path')}")
                    print(f"Size:         {meta.get('size_bytes', 0):,} bytes")
                    print(f"Integrity:    {meta.get('integrity')}")
                    print(f"Duration:     {result.get('duration_ms', 0):.1f} ms")
                if result.get("error"):
                    print(f"Error:        {result.get('error')}")
            return 0 if result.get("status") == "success" else 1

        elif args.backup_command == "drill":
            if not getattr(args, "drill_action", None):
                p_drill.print_help()
                return 1

            if args.drill_action == "run":
                mgr = BackupManager(
                    db_path=args.db_path,
                    backup_dir=args.dir,
                )
                res = mgr.run_restore_drill(
                    backup_path=args.backup_path,
                    target_temp_dir=args.temp_dir,
                    sample_records=not args.no_sample,
                )
                from .store import CheckpointStore

                store = CheckpointStore(db_path=args.db_path)
                if store.enabled:
                    store.record_restore_drill(
                        drill_id=res.drill_id,
                        started_at=res.timestamp,
                        completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        status="success" if res.success else "failed",
                        backup_path=res.backup_path,
                        backup_timestamp=res.backup_timestamp,
                        backup_age_seconds=res.backup_age_seconds,
                        duration_ms=res.duration_ms,
                        tables_verified=len(res.tables_verified),
                        records_verified=res.records_verified,
                        queries_verified=res.queries_verified,
                        integrity_check=res.integrity_check,
                        quick_check=res.quick_check,
                        foreign_key_check=res.foreign_key_check,
                        error_text="; ".join(res.errors) if res.errors else None,
                        operator="cli_operator",
                        reason="CLI manual restore drill",
                    )
                if args.json:
                    print(json.dumps(res.to_dict(), indent=2))
                else:
                    status_str = "SUCCESS" if res.success else "FAILED"
                    print(f"\n=== DATABASE RESTORE DRILL: {status_str} ===")
                    print(f"Drill ID:          {res.drill_id}")
                    print(f"Backup archive:    {res.backup_path}")
                    print(f"Backup age:        {res.backup_age_seconds:.1f} s")
                    print(f"Integrity check:   {res.integrity_check}")
                    print(f"Quick check:       {res.quick_check}")
                    print(f"Foreign keys:      {res.foreign_key_check}")
                    print(f"Tables verified:   {len(res.tables_verified)}")
                    print(f"Records verified:  {res.records_verified}")
                    print(f"Queries verified:  {res.queries_verified}")
                    print(f"Duration:          {res.duration_ms:.1f} ms")
                    print(f"Cleaned up:        {res.cleaned_up}")
                    if res.errors:
                        print("Errors:")
                        for err in res.errors:
                            print(f"  - {err}")
                return 0 if res.success else 1

            elif args.drill_action == "status":
                from .store import CheckpointStore

                store = CheckpointStore(db_path=args.db_path)
                latest = store.get_latest_restore_drill()
                if args.json:
                    print(json.dumps(latest or {}, indent=2))
                else:
                    print("\n=== LATEST RESTORE DRILL STATUS ===")
                    if not latest:
                        print("No restore drills recorded yet.")
                    else:
                        print(f"Drill ID:        {latest.get('drill_id')}")
                        print(f"Status:          {latest.get('status')}")
                        print(f"Started At:      {latest.get('started_at')}")
                        print(f"Backup Archive:  {latest.get('backup_path')}")
                        print(f"Backup Age:      {latest.get('backup_age_seconds', 0.0):.1f} s")
                        print(f"Duration:        {latest.get('duration_ms', 0.0):.1f} ms")
                        print(f"Integrity:       {latest.get('integrity_check')}")
                        print(f"Tables Verified: {latest.get('tables_verified')}")
                        print(f"Records Tested:  {latest.get('records_verified')}")
                        if latest.get("error_text"):
                            print(f"Error:           {latest.get('error_text')}")
                return 0

            elif args.drill_action == "history":
                from .store import CheckpointStore

                store = CheckpointStore(db_path=args.db_path)
                drills = store.list_restore_drills(limit=args.limit)
                if args.json:
                    print(json.dumps(drills, indent=2))
                else:
                    print(f"\n=== RESTORE DRILL HISTORY ({len(drills)}) ===")
                    if not drills:
                        print("No restore drill history records found.")
                    else:
                        for d in drills:
                            b_name = os.path.basename(str(d.get('backup_path', '')))
                            print(
                                f"[{d.get('started_at')}] {d.get('drill_id')} - "
                                f"status={d.get('status')} ({d.get('duration_ms', 0.0):.1f}ms) "
                                f"backup={b_name}"
                            )
                return 0

    except Exception as exc:
        if hasattr(args, "json") and args.json:
            print(json.dumps({"error": str(exc)}, indent=2))
        else:
            print(f"Error: {exc}", file=os.sys.stderr)
        return 1

    return 0
