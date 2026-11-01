"""SQLite-based durable checkpoint store for workflow persistence and resumption."""

from __future__ import annotations

import datetime
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION: int = 1

_SENSITIVE_KEYS = re.compile(r"(?i)(password|secret|api[_-]?key|auth|token|credential)")


class CheckpointError(Exception):
    """Base exception for persistence and checkpoint errors."""


class AdmissionFencingError(CheckpointError):
    """Raised when a workflow admission lease has been fenced out, expired, or superseded."""


class SchedulerFencingError(CheckpointError):
    """Raised when a scheduler lease has been fenced out, expired, or superseded."""


class StaleDispatcherError(CheckpointError):
    """Raised when an outbox claim has expired, been stolen, or been superseded by another worker."""


class CheckpointConflictError(CheckpointError):
    """Raised when an operation or transition conflicts with immutable or terminal run state."""


class RecoveryError(CheckpointError):
    """Raised when a stuck workflow recovery action cannot be safely performed."""


class CheckpointNotFoundError(CheckpointError):
    """Raised when no checkpoint exists for a given run ID."""


class ToolExecutionNotFoundError(CheckpointNotFoundError):
    """Raised when an operation key is not found in tool_executions."""


class ToolReconciliationConflictError(CheckpointConflictError):
    """Raised when a tool reconciliation transition conflicts with current status."""


class ToolApprovalConflictError(CheckpointConflictError):
    """Raised when an approval decision or revocation conflicts with existing state."""


class ToolApprovalNotFoundError(CheckpointNotFoundError):
    """Raised when an operation key is not found in tool_approvals for revocation or retrieval."""


class CheckpointCorruptError(CheckpointError):
    """Raised when checkpoint data is malformed or unparseable."""


class CheckpointIncompatibleError(CheckpointError):
    """Raised when checkpoint schema version does not match supported version."""


def sanitize_state_for_persistence(obj: Any) -> Any:
    """Recursively sanitize state dictionaries to prevent secret leakage in SQLite."""
    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            if _SENSITIVE_KEYS.search(str(k)):
                clean[k] = "[REDACTED]"
            else:
                clean[k] = sanitize_state_for_persistence(v)
        return clean
    elif isinstance(obj, (list, tuple, set)):
        return [sanitize_state_for_persistence(item) for item in obj]
    elif isinstance(obj, str):
        cleaned = re.sub(r"sk-[a-zA-Z0-9_-]{16,}", "[REDACTED_KEY]", obj)
        cleaned = re.sub(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{16,}", "Bearer [REDACTED_TOKEN]", cleaned)
        cleaned = re.sub(
            r"(?i)(password|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", cleaned
        )
        return cleaned
    return obj


@dataclass
class CheckpointRecord:
    run_id: str
    step: int
    transition: str
    state: dict[str, Any]
    schema_version: int
    created_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_utc_iso(ts_str: str | None) -> datetime.datetime | None:
    """Safely parse an ISO 8601 UTC timestamp string into a timezone-aware datetime object."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    clean = ts_str.strip()
    if not clean:
        return None
    try:
        if clean.endswith("Z"):
            clean = clean[:-1] + "+00:00"
        dt = datetime.datetime.fromisoformat(clean)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt.astimezone(datetime.timezone.utc)
    except Exception:
        return None


def is_approval_expired(
    record: dict[str, Any] | None,
    current_time: datetime.datetime | None = None,
) -> bool:
    """Check whether a tool approval record is expired by status or elapsed timeout."""
    if not record:
        return False
    if str(record.get("decision") or "").strip().lower() == "expired":
        return True
    expires_at_str = record.get("expires_at")
    if not expires_at_str:
        return False
    exp_dt = parse_utc_iso(expires_at_str)
    if exp_dt is None:
        return False
    now_dt = current_time or datetime.datetime.now(datetime.timezone.utc)
    return now_dt >= exp_dt


class CheckpointStore:
    """Local SQLite-backed durable checkpoint store for orchestrator workflows."""

    _DB_LOCKS: dict[str, threading.RLock] = {}
    _DB_LOCKS_MUTEX = threading.Lock()
    _IDEMPOTENCY_CONDITIONS: dict[tuple[str, str], threading.Condition] = {}
    _IDEMPOTENCY_MUTEX = threading.Lock()

    @classmethod
    def _get_idempotency_condition(cls, client_scope: str, key: str) -> threading.Condition:
        pair = (str(client_scope), str(key))
        with cls._IDEMPOTENCY_MUTEX:
            if pair not in cls._IDEMPOTENCY_CONDITIONS:
                cls._IDEMPOTENCY_CONDITIONS[pair] = threading.Condition()
            return cls._IDEMPOTENCY_CONDITIONS[pair]

    @classmethod
    def _get_db_lock(cls, db_path: str) -> threading.RLock:
        norm = db_path if db_path in (":memory:", "file::memory:?cache=shared") else os.path.abspath(db_path)
        with cls._DB_LOCKS_MUTEX:
            if norm not in cls._DB_LOCKS:
                cls._DB_LOCKS[norm] = threading.RLock()
            return cls._DB_LOCKS[norm]

    def __init__(
        self,
        db_path: str | Path | None = None,
        enabled: bool = True,
    ) -> None:
        if db_path is None:
            from ..config import get_settings

            self.db_path = str(get_settings().checkpoint_db_path)
            self.enabled = bool(get_settings().checkpoint_enabled) and enabled
        else:
            self.db_path = str(db_path)
            self.enabled = enabled

        self._is_memory = self.db_path in (":memory:", "file::memory:?cache=shared")
        self._mem_conn: sqlite3.Connection | None = None

        if self.enabled:
            self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not self.enabled:
            raise CheckpointError("CheckpointStore is disabled.")
        if self._is_memory:
            if self._mem_conn is None:
                self._mem_conn = sqlite3.connect(self.db_path, check_same_thread=False)
                self._mem_conn.row_factory = sqlite3.Row
            return self._mem_conn

        # File-based database
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute("PRAGMA foreign_keys=ON;")
        except Exception:
            pass
        return conn

    def _init_db(self) -> None:
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runs (
                            run_id TEXT PRIMARY KEY,
                            task TEXT,
                            status TEXT,
                            latest_step INTEGER DEFAULT 0,
                            schema_version INTEGER,
                            created_at TEXT,
                            updated_at TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS checkpoints (
                            run_id TEXT,
                            step INTEGER,
                            transition TEXT,
                            state_json TEXT,
                            schema_version INTEGER,
                            created_at TEXT,
                            PRIMARY KEY (run_id, step)
                        );
                        """
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_checkpoints_run_id ON checkpoints(run_id);"
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS audit_events (
                            event_id TEXT PRIMARY KEY,
                            timestamp TEXT NOT NULL,
                            event_type TEXT NOT NULL,
                            run_id TEXT,
                            correlation_id TEXT,
                            actor TEXT,
                            outcome TEXT NOT NULL,
                            metadata_json TEXT,
                            created_at TEXT NOT NULL
                        );
                        """
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_audit_events_run_id ON audit_events(run_id);"
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_audit_events_timestamp ON audit_events(timestamp);"
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_audit_events_type ON audit_events(event_type);"
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS idempotency_keys (
                            key TEXT NOT NULL,
                            client_scope TEXT NOT NULL,
                            request_hash TEXT NOT NULL,
                            run_id TEXT NOT NULL,
                            status TEXT NOT NULL,
                            response_code INTEGER,
                            response_json TEXT,
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL,
                            PRIMARY KEY (client_scope, key)
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_idempotency_created_at
                        ON idempotency_keys(created_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS workflow_admissions (
                            run_id TEXT PRIMARY KEY,
                            owner_id TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'active',
                            acquired_at TEXT NOT NULL,
                            heartbeat_at TEXT NOT NULL,
                            expires_at TEXT NOT NULL DEFAULT '',
                            fence_token INTEGER NOT NULL DEFAULT 1
                        );
                        """
                    )
                    try:
                        conn.execute(
                            "ALTER TABLE workflow_admissions ADD COLUMN expires_at TEXT NOT NULL DEFAULT '';"
                        )
                    except Exception:
                        pass
                    try:
                        conn.execute(
                            "ALTER TABLE workflow_admissions "
                            "ADD COLUMN fence_token INTEGER NOT NULL DEFAULT 1;"
                        )
                    except Exception:
                        pass
                    try:
                        conn.execute(
                            "ALTER TABLE runs ADD COLUMN fence_token INTEGER NOT NULL DEFAULT 0;"
                        )
                    except Exception:
                        pass
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_admissions_expires
                        ON workflow_admissions(expires_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_admissions_heartbeat
                        ON workflow_admissions(heartbeat_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_workflow_admissions_owner
                        ON workflow_admissions(owner_id);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS tool_executions (
                            operation_key TEXT PRIMARY KEY,
                            run_id TEXT NOT NULL,
                            subtask_id TEXT NOT NULL,
                            tool_name TEXT NOT NULL,
                            fence_token INTEGER NOT NULL DEFAULT 1,
                            status TEXT NOT NULL,
                            args_json TEXT NOT NULL,
                            result_text TEXT,
                            error_text TEXT,
                            reconciled_by TEXT,
                            reconciled_at TEXT,
                            reconciliation_reason TEXT,
                            created_at TEXT NOT NULL,
                            updated_at TEXT NOT NULL
                        );
                        """
                    )
                    for col_def in (
                        "reconciled_by TEXT",
                        "reconciled_at TEXT",
                        "reconciliation_reason TEXT",
                    ):
                        try:
                            conn.execute(f"ALTER TABLE tool_executions ADD COLUMN {col_def};")
                        except sqlite3.OperationalError:
                            pass
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_tool_executions_run_id
                        ON tool_executions(run_id);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_tool_executions_tool_name
                        ON tool_executions(tool_name);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS tool_approvals (
                            operation_key TEXT PRIMARY KEY,
                            run_id TEXT NOT NULL,
                            tool_name TEXT NOT NULL,
                            approver TEXT NOT NULL,
                            decision TEXT NOT NULL,
                            reason TEXT,
                            created_at TEXT NOT NULL,
                            argument_fingerprint TEXT,
                            risk_level TEXT,
                            policy_hash TEXT,
                            revoked_by TEXT,
                            revocation_reason TEXT,
                            revoked_at TEXT,
                            recovered_by TEXT,
                            expires_at TEXT,
                            expired_at TEXT,
                            escalated_at TEXT,
                            escalation_reason TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_tool_approvals_run_id
                        ON tool_approvals(run_id);
                        """
                    )
                    cursor = conn.execute("PRAGMA table_info(tool_approvals);")
                    existing_cols = {row["name"] for row in cursor.fetchall()}
                    for col_name in [
                        "argument_fingerprint",
                        "risk_level",
                        "policy_hash",
                        "revoked_by",
                        "revocation_reason",
                        "revoked_at",
                        "recovered_by",
                        "expires_at",
                        "expired_at",
                        "escalated_at",
                        "escalation_reason",
                    ]:
                        if col_name not in existing_cols:
                            conn.execute(f"ALTER TABLE tool_approvals ADD COLUMN {col_name} TEXT;")
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_tool_approvals_expires_at
                        ON tool_approvals(expires_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS scheduler_leases (
                            lease_name TEXT PRIMARY KEY,
                            owner_id TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'active',
                            acquired_at TEXT NOT NULL,
                            heartbeat_at TEXT NOT NULL,
                            expires_at TEXT NOT NULL,
                            fence_token INTEGER NOT NULL DEFAULT 1
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_scheduler_leases_expires
                        ON scheduler_leases(expires_at);
                        """
                    )
                    cursor = conn.execute("PRAGMA table_info(scheduler_leases);")
                    existing_lease_cols = {row["name"] for row in cursor.fetchall()}
                    for col_name, col_type in [
                        ("status", "TEXT NOT NULL DEFAULT 'active'"),
                        ("acquired_at", "TEXT NOT NULL DEFAULT ''"),
                        ("heartbeat_at", "TEXT NOT NULL DEFAULT ''"),
                        ("expires_at", "TEXT NOT NULL DEFAULT ''"),
                        ("fence_token", "INTEGER NOT NULL DEFAULT 1"),
                    ]:
                        if col_name not in existing_lease_cols:
                            try:
                                conn.execute(
                                    f"ALTER TABLE scheduler_leases ADD COLUMN {col_name} {col_type};"
                                )
                            except Exception:
                                pass
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS outbox_events (
                            event_id TEXT PRIMARY KEY,
                            event_type TEXT NOT NULL,
                            payload TEXT NOT NULL,
                            created_at TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'pending',
                            attempt_count INTEGER NOT NULL DEFAULT 0,
                            next_retry_at TEXT NOT NULL,
                            last_error TEXT,
                            claimed_by TEXT,
                            claimed_at TEXT,
                            claim_expires_at TEXT,
                            delivered_at TEXT,
                            idempotency_key TEXT UNIQUE,
                            fence_token INTEGER NOT NULL DEFAULT 1
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_outbox_status_retry
                        ON outbox_events(status, next_retry_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_outbox_claim_expires
                        ON outbox_events(claim_expires_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_outbox_created_at
                        ON outbox_events(created_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS outbox_delivery_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            event_id TEXT NOT NULL,
                            attempt_number INTEGER NOT NULL DEFAULT 0,
                            action TEXT NOT NULL,
                            status TEXT NOT NULL,
                            error_message TEXT,
                            claimed_by TEXT,
                            recorded_at TEXT NOT NULL,
                            operator TEXT,
                            reason TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_outbox_history_event
                        ON outbox_delivery_history(event_id, recorded_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS webhook_replay_records (
                            event_id TEXT PRIMARY KEY,
                            timestamp INTEGER NOT NULL,
                            expires_at REAL NOT NULL,
                            recorded_at TEXT NOT NULL
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_webhook_replay_expires
                        ON webhook_replay_records(expires_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS webhook_replay_cleanup_log (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            cleaned_at TEXT NOT NULL,
                            deleted_count INTEGER NOT NULL,
                            duration_ms REAL NOT NULL,
                            trigger_type TEXT NOT NULL,
                            operator TEXT,
                            reason TEXT
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_webhook_replay_cleanup_cleaned
                        ON webhook_replay_cleanup_log(cleaned_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS celery_worker_heartbeats (
                            worker_id TEXT PRIMARY KEY,
                            status TEXT NOT NULL DEFAULT 'active',
                            registered_at TEXT NOT NULL,
                            last_heartbeat_at TEXT NOT NULL,
                            active_tasks INTEGER NOT NULL DEFAULT 0,
                            queues TEXT NOT NULL DEFAULT 'orchestrator_workflows',
                            last_completed_at TEXT,
                            last_failure_at TEXT,
                            last_failure_reason TEXT,
                            metadata_json TEXT NOT NULL DEFAULT '{}'
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_celery_worker_heartbeats_last_hb
                        ON celery_worker_heartbeats(last_heartbeat_at);
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_celery_worker_heartbeats_status
                        ON celery_worker_heartbeats(status);
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS database_restore_drills (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            drill_id TEXT NOT NULL UNIQUE,
                            started_at TEXT NOT NULL,
                            completed_at TEXT,
                            status TEXT NOT NULL,
                            backup_path TEXT NOT NULL,
                            backup_timestamp TEXT,
                            backup_age_seconds REAL,
                            duration_ms REAL,
                            tables_verified INTEGER DEFAULT 0,
                            records_verified INTEGER DEFAULT 0,
                            queries_verified INTEGER DEFAULT 0,
                            integrity_check TEXT,
                            quick_check TEXT,
                            foreign_key_check TEXT,
                            error_text TEXT,
                            operator TEXT,
                            reason TEXT,
                            fence_token INTEGER
                        );
                        """
                    )
                    conn.execute(
                        """
                        CREATE INDEX IF NOT EXISTS idx_database_restore_drills_started
                        ON database_restore_drills(started_at);
                        """
                    )
                    cursor = conn.execute("PRAGMA table_info(outbox_events);")
                    existing_outbox_cols = {row["name"] for row in cursor.fetchall()}
                    for col_name, col_type in [
                        ("status", "TEXT NOT NULL DEFAULT 'pending'"),
                        ("attempt_count", "INTEGER NOT NULL DEFAULT 0"),
                        ("next_retry_at", "TEXT NOT NULL DEFAULT ''"),
                        ("last_error", "TEXT"),
                        ("claimed_by", "TEXT"),
                        ("claimed_at", "TEXT"),
                        ("claim_expires_at", "TEXT"),
                        ("delivered_at", "TEXT"),
                        ("idempotency_key", "TEXT"),
                        ("fence_token", "INTEGER NOT NULL DEFAULT 1"),
                        ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
                        ("last_recovery_reason", "TEXT"),
                        ("last_recovery_by", "TEXT"),
                        ("last_recovery_at", "TEXT"),
                    ]:
                        if col_name not in existing_outbox_cols:
                            try:
                                conn.execute(
                                    f"ALTER TABLE outbox_events ADD COLUMN {col_name} {col_type};"
                                )
                            except Exception:
                                pass
            except Exception as exc:
                logger.error("Failed to initialize checkpoint database at %s: %s", self.db_path, exc)
                raise CheckpointError(f"Database initialization failed: {exc}") from exc
            finally:
                if not self._is_memory:
                    conn.close()

    def save_checkpoint(
        self,
        run_id: str,
        state: dict[str, Any],
        transition: str = "state_update",
        owner_id: str | None = None,
        fence_token: int | None = None,
        privileged: bool = False,
    ) -> int:
        """Save a durable state checkpoint at an execution boundary.

        Enforces atomic admission lease ownership, fencing token validity, and valid
        terminal lifecycle state transitions inside a BEGIN IMMEDIATE transaction.
        Returns the recorded step number.
        """
        if not self.enabled:
            return 0

        clean_run_id = str(run_id).strip()
        if not clean_run_id:
            raise CheckpointError("run_id must not be empty")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        sanitized_state = sanitize_state_for_persistence(state)
        task_text = str(sanitized_state.get("task", "") or "")
        current_status = str(sanitized_state.get("status", "pending") or "pending")

        try:
            state_json = json.dumps(sanitized_state, default=str)
        except Exception as exc:
            raise CheckpointError(f"Failed to serialize state to JSON for run {clean_run_id}: {exc}") from exc

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            max_retries = 25
            for attempt in range(max_retries):
                try:
                    conn.isolation_level = None
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                            continue
                        raise CheckpointError(
                            f"Database locked while saving checkpoint for run {clean_run_id}: {exc}"
                        ) from exc

                    try:
                        # 0. Fencing validation
                        clean_owner = str(owner_id or "").strip()
                        req_token = int(fence_token) if fence_token is not None else None

                        cursor = conn.execute(
                            "SELECT fence_token, status, task, latest_step FROM runs WHERE run_id = ?;",
                            (clean_run_id,),
                        )
                        run_row = cursor.fetchone()

                        cursor = conn.execute(
                            """
                            SELECT owner_id, fence_token, expires_at
                            FROM workflow_admissions WHERE run_id = ?;
                            """,
                            (clean_run_id,),
                        )
                        adm_row = cursor.fetchone()

                        if clean_owner or req_token is not None:
                            if run_row and req_token is not None:
                                db_fence = int(run_row["fence_token"] or 0)
                                if db_fence > req_token:
                                    raise AdmissionFencingError(
                                        f"Admission fencing conflict for run '{clean_run_id}': "
                                        f"caller generation {req_token} is superseded by "
                                        f"database generation {db_fence}."
                                    )

                            if adm_row:
                                adm_owner = str(adm_row["owner_id"] or "").strip()
                                adm_token = int(adm_row["fence_token"] or 1)
                                adm_expires = str(adm_row["expires_at"] or "").strip()

                                if clean_owner and adm_owner != clean_owner:
                                    raise AdmissionFencingError(
                                        f"Admission fencing conflict for run '{clean_run_id}': "
                                        f"active lease is owned by '{adm_owner}', caller is '{clean_owner}'."
                                    )
                                if req_token is not None and adm_token > req_token:
                                    raise AdmissionFencingError(
                                        f"Admission fencing conflict for run '{clean_run_id}': "
                                        f"caller generation {req_token} is superseded by "
                                        f"admission generation {adm_token}."
                                    )
                                if adm_expires and adm_expires < now:
                                    raise AdmissionFencingError(
                                        f"Admission fencing conflict for run '{clean_run_id}': "
                                        f"lease expired at {adm_expires} (current time {now})."
                                    )
                            else:
                                raise AdmissionFencingError(
                                    f"Admission fencing conflict for run '{clean_run_id}': "
                                    f"no active admission lease found in database."
                                )
                        elif not privileged:
                            if adm_row:
                                raise AdmissionFencingError(
                                    f"Admission fencing conflict for run '{clean_run_id}': "
                                    f"active lease is owned by '{adm_row['owner_id']}', "
                                    f"but caller provided no admission credentials."
                                )

                        # 1. State transition validation
                        if run_row:
                            db_status = str(run_row["status"] or "")
                            db_fence = int(run_row["fence_token"] or 0)

                            if db_status in ("completed", "approved", "edited"):
                                if current_status != db_status:
                                    raise AdmissionFencingError(
                                        f"Invalid state transition for run '{clean_run_id}': "
                                        f"run has already reached terminal status '{db_status}' "
                                        f"and cannot transition to '{current_status}'."
                                    )
                            elif db_status in ("failed", "cancelled", "timed_out", "rejected"):
                                if current_status != db_status:
                                    is_valid_resume = (
                                        (
                                            (req_token is None and clean_owner == "")
                                            or (
                                                req_token is not None
                                                and req_token >= db_fence
                                                and adm_row is not None
                                                and int(adm_row["fence_token"] or 0) == req_token
                                            )
                                        )
                                        and current_status in ("running", "pending", "in_progress")
                                        and transition == "resume"
                                    )
                                    if not is_valid_resume and not privileged:
                                        raise AdmissionFencingError(
                                            f"Invalid state transition for run '{clean_run_id}': "
                                            f"run is in terminal status '{db_status}' "
                                            f"(generation {db_fence}); caller generation {req_token} "
                                            f"cannot transition it to '{current_status}'."
                                        )

                        # 2. Get next step number
                        cursor = conn.execute(
                            "SELECT COALESCE(MAX(step), 0) + 1 FROM checkpoints WHERE run_id = ?",
                            (clean_run_id,),
                        )
                        next_step = cursor.fetchone()[0]

                        # 3. Insert checkpoint record
                        conn.execute(
                            """
                            INSERT INTO checkpoints (
                                run_id, step, transition, state_json, schema_version, created_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?);
                            """,
                            (clean_run_id, next_step, transition, state_json, SCHEMA_VERSION, now),
                        )

                        eff_token = req_token if req_token is not None else (
                            int(run_row["fence_token"] or 0) if run_row else 0
                        )

                        # 4. Upsert runs summary
                        if run_row:
                            conn.execute(
                                """
                                UPDATE runs
                                SET task = CASE WHEN ? != '' THEN ? ELSE task END,
                                    status = ?,
                                    latest_step = ?,
                                    fence_token = MAX(fence_token, ?),
                                    updated_at = ?
                                WHERE run_id = ?;
                                """,
                                (
                                    task_text,
                                    task_text,
                                    current_status,
                                    next_step,
                                    eff_token,
                                    now,
                                    clean_run_id,
                                ),
                            )
                        else:
                            conn.execute(
                                """
                                INSERT INTO runs (
                                    run_id, task, status, latest_step,
                                    schema_version, created_at, updated_at, fence_token
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                                """,
                                (
                                    clean_run_id,
                                    task_text,
                                    current_status,
                                    next_step,
                                    SCHEMA_VERSION,
                                    now,
                                    now,
                                    eff_token,
                                ),
                            )

                        conn.execute("COMMIT;")
                        logger.debug(
                            "Saved checkpoint for run=%s step=%d transition=%s status=%s fence=%d",
                            clean_run_id,
                            next_step,
                            transition,
                            current_status,
                            eff_token,
                        )
                        return next_step
                    except Exception:
                        try:
                            conn.execute("ROLLBACK;")
                        except Exception:
                            pass
                        raise
                except AdmissionFencingError:
                    raise
                except CheckpointError:
                    raise
                except sqlite3.OperationalError as op_err:
                    is_lock_err = "locked" in str(op_err).lower() or "busy" in str(op_err).lower()
                    if is_lock_err and attempt < max_retries - 1:
                        time.sleep(0.01 * (attempt + 1))
                        continue
                    logger.error("Failed to save checkpoint for run %s: %s", clean_run_id, op_err)
                    err_msg = f"Checkpoint write failed for run {clean_run_id}: {op_err}"
                    raise CheckpointError(err_msg) from op_err
                except Exception as exc:
                    logger.error("Failed to save checkpoint for run %s: %s", clean_run_id, exc)
                    raise CheckpointError(f"Checkpoint write failed for run {clean_run_id}: {exc}") from exc
                finally:
                    conn.isolation_level = old_isolation
                    if not self._is_memory:
                        conn.close()
            return 0

    def load_latest_checkpoint(self, run_id: str) -> CheckpointRecord:
        """Load the most recent valid checkpoint for the given run_id."""
        if not self.enabled:
            raise CheckpointError("CheckpointStore is disabled.")

        clean_run_id = str(run_id).strip()
        if not clean_run_id:
            raise CheckpointNotFoundError("run_id must not be empty")

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, step, transition, state_json, schema_version, created_at
                    FROM checkpoints
                    WHERE run_id = ?
                    ORDER BY step DESC
                    LIMIT 1;
                    """,
                    (clean_run_id,),
                )
                row = cursor.fetchone()
                if not row:
                    raise CheckpointNotFoundError(f"No checkpoint found for run_id '{clean_run_id}'")

                return self._row_to_record(row)
            finally:
                if not self._is_memory:
                    conn.close()

    def load_checkpoint_step(self, run_id: str, step: int) -> CheckpointRecord:
        """Load a specific step's checkpoint for a run_id."""
        if not self.enabled:
            raise CheckpointError("CheckpointStore is disabled.")

        clean_run_id = str(run_id).strip()
        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, step, transition, state_json, schema_version, created_at
                    FROM checkpoints
                    WHERE run_id = ? AND step = ?;
                    """,
                    (clean_run_id, step),
                )
                row = cursor.fetchone()
                if not row:
                    raise CheckpointNotFoundError(
                        f"No checkpoint found for run_id '{clean_run_id}' at step {step}"
                    )

                return self._row_to_record(row)
            finally:
                if not self._is_memory:
                    conn.close()

    def list_checkpoints(self, run_id: str) -> list[CheckpointRecord]:
        """List all historical checkpoints for a run_id in step order."""
        if not self.enabled:
            return []

        clean_run_id = str(run_id).strip()
        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, step, transition, state_json, schema_version, created_at
                    FROM checkpoints
                    WHERE run_id = ?
                    ORDER BY step ASC;
                    """,
                    (clean_run_id,),
                )
                return [self._row_to_record(row) for row in cursor.fetchall()]
            finally:
                if not self._is_memory:
                    conn.close()

    def get_run_summary(self, run_id: str) -> dict[str, Any] | None:
        """Retrieve run metadata summary from the runs index table."""
        if not self.enabled:
            return None

        clean_run_id = str(run_id).strip()
        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, task, status, latest_step, schema_version,
                           created_at, updated_at, fence_token
                    FROM runs
                    WHERE run_id = ?;
                    """,
                    (clean_run_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "run_id": row["run_id"],
                    "task": row["task"],
                    "status": row["status"],
                    "latest_step": row["latest_step"],
                    "schema_version": row["schema_version"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "fence_token": int(row["fence_token"] or 0) if "fence_token" in row.keys() else 0,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def list_runs(self) -> list[dict[str, Any]]:
        """List all tracked runs ordered by latest update."""
        if not self.enabled:
            return []

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, task, status, latest_step, schema_version,
                           created_at, updated_at, fence_token
                    FROM runs
                    ORDER BY updated_at DESC;
                    """
                )
                return [
                    {
                        "run_id": row["run_id"],
                        "task": row["task"],
                        "status": row["status"],
                        "latest_step": row["latest_step"],
                        "schema_version": row["schema_version"],
                        "created_at": row["created_at"],
                        "updated_at": row["updated_at"],
                        "fence_token": int(row["fence_token"] or 0) if "fence_token" in row.keys() else 0,
                    }
                    for row in cursor.fetchall()
                ]
            finally:
                if not self._is_memory:
                    conn.close()

    def close(self) -> None:
        """Close persistent in-memory connection if active."""
        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            if self._mem_conn is not None:
                try:
                    self._mem_conn.close()
                except Exception:
                    pass
                self._mem_conn = None

    def find_interrupted_runs(self, lease_ttl_seconds: float = 30.0) -> list[str]:
        """Find all run IDs that were persisted as active/running without reaching terminal status,
        excluding those actively executing under a valid cross-process admission lease.
        """
        if not self.enabled:
            return []

        ttl = max(0.001, float(lease_ttl_seconds))
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        cutoff_iso = (now_dt - datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT r.run_id FROM runs r
                    LEFT JOIN workflow_admissions w ON r.run_id = w.run_id
                    WHERE r.status IN ('running', 'in_progress')
                      AND (w.run_id IS NULL OR w.expires_at < ? OR w.heartbeat_at < ?)
                    ORDER BY r.updated_at ASC;
                    """,
                    (now_iso, cutoff_iso),
                )
                return [row["run_id"] for row in cursor.fetchall()]
            finally:
                if not self._is_memory:
                    conn.close()

    def recover_run_as_interrupted(
        self,
        run_id: str,
        reason: str = "Process terminated while run was in progress",
        force: bool = False,
        owner_id: str | None = None,
    ) -> CheckpointRecord | None:
        """Recover an active/running run into an explicit, resumable 'interrupted' state.

        Guarantees:
        - Atomic validation and recovery inside a single BEGIN IMMEDIATE transaction.
        - Fences out any delayed previous worker by bumping runs.fence_token.
        - Shields active runs with valid lease heartbeats unless force=True or caller owns the lease.
        - Idempotent: if run is already terminal or interrupted, leaves status unchanged.
        - Clears workflow_admissions lease.
        """
        if not self.enabled:
            return None

        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return None

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        terminal_statuses = (
            "completed",
            "failed",
            "cancelled",
            "timed_out",
            "approved",
            "edited",
            "rejected",
        )

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            max_retries = 25
            for attempt in range(max_retries):
                try:
                    conn.isolation_level = None
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                            continue
                        raise CheckpointError(
                            f"Database locked while recovering run {clean_run_id}: {exc}"
                        ) from exc

                    try:
                        cursor = conn.execute(
                            """
                            SELECT run_id, task, status, latest_step, fence_token
                            FROM runs WHERE run_id = ?;
                            """,
                            (clean_run_id,),
                        )
                        run_row = cursor.fetchone()
                        if not run_row:
                            conn.execute("COMMIT;")
                            return None

                        curr_status = str(run_row["status"] or "")
                        db_fence = int(run_row["fence_token"] or 0)
                        latest_step = int(run_row["latest_step"] or 0)
                        task_text = str(run_row["task"] or "")

                        if curr_status in terminal_statuses or curr_status == "interrupted":
                            conn.execute(
                                "DELETE FROM workflow_admissions WHERE run_id = ?;",
                                (clean_run_id,),
                            )
                            conn.execute("COMMIT;")
                            return self.load_latest_checkpoint(clean_run_id)

                        cursor = conn.execute(
                            """
                            SELECT owner_id, fence_token, expires_at, heartbeat_at
                            FROM workflow_admissions WHERE run_id = ?;
                            """,
                            (clean_run_id,),
                        )
                        adm_row = cursor.fetchone()
                        if adm_row and not force:
                            adm_owner = str(adm_row["owner_id"] or "").strip()
                            caller_owner = str(owner_id or "").strip()
                            is_owner = bool(caller_owner and caller_owner == adm_owner)
                            adm_expires = str(adm_row["expires_at"] or "").strip()
                            if adm_expires != "" and adm_expires >= now and not is_owner:
                                # Shield live worker from premature recovery
                                conn.execute("ROLLBACK;")
                                return None

                        new_fence = db_fence + 1
                        conn.execute("DELETE FROM workflow_admissions WHERE run_id = ?;", (clean_run_id,))
                        conn.execute(
                            """
                            UPDATE tool_executions
                            SET status = 'uncertain',
                                error_text = 'Run interrupted before tool execution confirmed',
                                updated_at = ?
                            WHERE run_id = ? AND status = 'pending';
                            """,
                            (now, clean_run_id),
                        )

                        cursor = conn.execute(
                            """
                            SELECT state_json, step
                            FROM checkpoints WHERE run_id = ?
                            ORDER BY step DESC LIMIT 1;
                            """,
                            (clean_run_id,),
                        )
                        chk_row = cursor.fetchone()
                        if chk_row:
                            try:
                                latest_state = json.loads(chk_row["state_json"])
                            except Exception:
                                latest_state = {"run_id": clean_run_id, "task": task_text}
                            max_step = max(latest_step, int(chk_row["step"] or 0))
                        else:
                            latest_state = {"run_id": clean_run_id, "task": task_text}
                            max_step = latest_step

                        chk_status = latest_state.get("status")
                        if chk_status in terminal_statuses:
                            conn.execute(
                                """
                                UPDATE runs
                                SET status = ?, fence_token = ?, updated_at = ?
                                WHERE run_id = ?;
                                """,
                                (chk_status, new_fence, now, clean_run_id),
                            )
                            conn.execute("COMMIT;")
                            return self.load_latest_checkpoint(clean_run_id)

                        latest_state["status"] = "interrupted"
                        latest_state["interrupted_reason"] = reason

                        try:
                            from ..observability.tracing import create_trace_event

                            trace_events = latest_state.get("trace", [])
                            trace_events.append(
                                create_trace_event(
                                    node="workflow",
                                    event="interrupted",
                                    run_id=clean_run_id,
                                    reason=reason,
                                )
                            )
                            latest_state["trace"] = trace_events
                        except Exception:
                            pass

                        next_step = max_step + 1
                        state_json = json.dumps(
                            sanitize_state_for_persistence(latest_state),
                            default=str,
                        )

                        conn.execute(
                            """
                            INSERT INTO checkpoints (
                                run_id, step, transition, state_json, schema_version, created_at
                            )
                            VALUES (?, ?, 'interrupted', ?, ?, ?);
                            """,
                            (clean_run_id, next_step, state_json, SCHEMA_VERSION, now),
                        )
                        conn.execute(
                            """
                            UPDATE runs
                            SET status = 'interrupted',
                                latest_step = ?,
                                fence_token = ?,
                                updated_at = ?
                            WHERE run_id = ?;
                            """,
                            (next_step, new_fence, now, clean_run_id),
                        )
                        conn.execute("COMMIT;")
                        return self.load_latest_checkpoint(clean_run_id)
                    except Exception:
                        try:
                            conn.execute("ROLLBACK;")
                        except Exception:
                            pass
                        raise
                except CheckpointError:
                    raise
                except sqlite3.OperationalError as op_err:
                    is_lock_err = "locked" in str(op_err).lower() or "busy" in str(op_err).lower()
                    if is_lock_err and attempt < max_retries - 1:
                        time.sleep(0.01 * (attempt + 1))
                        continue
                    raise CheckpointError(
                        f"Database locked while recovering run {clean_run_id}: {op_err}"
                    ) from op_err
                except Exception as exc:
                    raise CheckpointError(
                        f"Failed to recover run {clean_run_id} as interrupted: {exc}"
                    ) from exc
                finally:
                    conn.isolation_level = old_isolation
                    if not self._is_memory:
                        conn.close()

    def cancel_run_administratively(
        self,
        run_id: str,
        reason: str = "User requested cancellation",
    ) -> CheckpointRecord:
        """Atomically transition a persistent workflow run to 'cancelled' from an administrative context.

        Guarantees:
        - Atomic validation and status update under BEGIN IMMEDIATE transaction.
        - Fences out any currently active or delayed worker by bumping fence_token.
        - Rejects cancellation if run has already reached terminal status (raising CheckpointConflictError).
        - Idempotent if run is already cancelled.
        - Releases admission reservation.
        """
        if not self.enabled:
            raise CheckpointError("CheckpointStore is disabled.")

        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise CheckpointNotFoundError("run_id must not be empty.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        terminal_statuses = (
            "completed",
            "failed",
            "timed_out",
            "approved",
            "edited",
            "rejected",
        )

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            max_retries = 25
            for attempt in range(max_retries):
                try:
                    conn.isolation_level = None
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                            continue
                        raise CheckpointError(
                            f"Database locked while cancelling run {clean_run_id}: {exc}"
                        ) from exc

                    try:
                        cursor = conn.execute(
                            """
                            SELECT run_id, task, status, latest_step, fence_token
                            FROM runs WHERE run_id = ?;
                            """,
                            (clean_run_id,),
                        )
                        run_row = cursor.fetchone()
                        if not run_row:
                            conn.execute("ROLLBACK;")
                            raise CheckpointNotFoundError(f"Run '{clean_run_id}' not found.")

                        curr_status = str(run_row["status"] or "")
                        db_fence = int(run_row["fence_token"] or 0)
                        latest_step = int(run_row["latest_step"] or 0)
                        task_text = str(run_row["task"] or "")

                        if curr_status == "cancelled":
                            conn.execute("COMMIT;")
                            return self.load_latest_checkpoint(clean_run_id)

                        if curr_status in terminal_statuses:
                            conn.execute("ROLLBACK;")
                            raise CheckpointConflictError(
                                f"Cannot cancel run '{clean_run_id}': "
                                f"run has already reached terminal status '{curr_status}'."
                            )

                        new_fence = db_fence + 1
                        conn.execute("DELETE FROM workflow_admissions WHERE run_id = ?;", (clean_run_id,))
                        conn.execute(
                            """
                            UPDATE tool_executions
                            SET status = 'uncertain',
                                error_text = 'Run cancelled before tool execution confirmed',
                                updated_at = ?
                            WHERE run_id = ? AND status = 'pending';
                            """,
                            (now, clean_run_id),
                        )

                        cursor = conn.execute(
                            """
                            SELECT state_json, step
                            FROM checkpoints WHERE run_id = ?
                            ORDER BY step DESC LIMIT 1;
                            """,
                            (clean_run_id,),
                        )
                        chk_row = cursor.fetchone()
                        if chk_row:
                            try:
                                state = json.loads(chk_row["state_json"])
                            except Exception:
                                state = {"run_id": clean_run_id, "task": task_text}
                            max_step = max(latest_step, int(chk_row["step"] or 0))
                        else:
                            state = {"run_id": clean_run_id, "task": task_text}
                            max_step = latest_step

                        chk_status = state.get("status")
                        if chk_status in terminal_statuses:
                            conn.execute(
                                """
                                UPDATE runs
                                SET status = ?, fence_token = ?, updated_at = ?
                                WHERE run_id = ?;
                                """,
                                (chk_status, new_fence, now, clean_run_id),
                            )
                            conn.execute("COMMIT;")
                            raise CheckpointConflictError(
                                f"Cannot cancel run '{clean_run_id}': "
                                f"run has already reached terminal status '{chk_status}'."
                            )

                        state["status"] = "cancelled"
                        state["cancellation_reason"] = reason

                        try:
                            from ..observability.tracing import create_trace_event

                            trace_events = state.get("trace", [])
                            trace_events.append(
                                create_trace_event(
                                    node="workflow",
                                    event="cancelled",
                                    run_id=clean_run_id,
                                    reason=reason,
                                )
                            )
                            state["trace"] = trace_events
                        except Exception:
                            pass

                        next_step = max_step + 1
                        state_json = json.dumps(
                            sanitize_state_for_persistence(state),
                            default=str,
                        )

                        conn.execute(
                            """
                            INSERT INTO checkpoints (
                                run_id, step, transition, state_json, schema_version, created_at
                            )
                            VALUES (?, ?, 'cancelled', ?, ?, ?);
                            """,
                            (clean_run_id, next_step, state_json, SCHEMA_VERSION, now),
                        )
                        conn.execute(
                            """
                            UPDATE runs
                            SET status = 'cancelled',
                                latest_step = ?,
                                fence_token = ?,
                                updated_at = ?
                            WHERE run_id = ?;
                            """,
                            (next_step, new_fence, now, clean_run_id),
                        )
                        conn.execute("COMMIT;")
                        return self.load_latest_checkpoint(clean_run_id)
                    except Exception:
                        try:
                            conn.execute("ROLLBACK;")
                        except Exception:
                            pass
                        raise
                except (CheckpointError, CheckpointConflictError):
                    raise
                except sqlite3.OperationalError as op_err:
                    is_lock_err = "locked" in str(op_err).lower() or "busy" in str(op_err).lower()
                    if is_lock_err and attempt < max_retries - 1:
                        time.sleep(0.01 * (attempt + 1))
                        continue
                    raise CheckpointError(
                        f"Database locked while cancelling run {clean_run_id}: {op_err}"
                    ) from op_err
                except Exception as exc:
                    raise CheckpointError(
                        f"Failed to cancel run {clean_run_id} administratively: {exc}"
                    ) from exc
                finally:
                    conn.isolation_level = old_isolation
                    if not self._is_memory:
                        conn.close()

    def recover_interrupted_runs(
        self,
        reason: str = "Process terminated while run was in progress",
        lease_ttl_seconds: float = 30.0,
    ) -> list[str]:
        """Scan and recover all in-flight runs left active by a previous process.

        Returns list of recovered run IDs.
        """
        if not self.enabled:
            return []

        interrupted_ids = self.find_interrupted_runs(lease_ttl_seconds=lease_ttl_seconds)
        recovered: list[str] = []
        for rid in interrupted_ids:
            rec = self.recover_run_as_interrupted(rid, reason=reason)
            if rec is not None:
                recovered.append(rid)
        return recovered

    def _row_to_record(self, row: sqlite3.Row) -> CheckpointRecord:
        schema_version = int(row["schema_version"])
        if schema_version > SCHEMA_VERSION:
            raise CheckpointIncompatibleError(
                f"Checkpoint schema version {schema_version} is newer than supported version {SCHEMA_VERSION}"
            )

        raw_json = row["state_json"]
        try:
            state_dict = json.loads(raw_json)
        except Exception as exc:
            raise CheckpointCorruptError(
                f"Corrupt checkpoint data for run '{row['run_id']}' at step {row['step']}: {exc}"
            ) from exc

        return CheckpointRecord(
            run_id=row["run_id"],
            step=row["step"],
            transition=row["transition"],
            state=state_dict,
            schema_version=schema_version,
            created_at=row["created_at"],
        )

    def check_health(self, timeout: float = 2.0) -> tuple[bool, str]:
        """Perform a safe, fast, non-blocking persistence health check.

        Returns:
            (is_healthy, status_message) tuple.
            Guaranteed never to raise unhandled exceptions or leak raw stack traces.
        """
        if not self.enabled:
            return True, "disabled"

        lock = self._get_db_lock(self.db_path)
        acquired = lock.acquire(timeout=timeout)
        if not acquired:
            logger.warning("Persistence health check timed out acquiring lock on %s", self.db_path)
            return False, "Database lock acquisition timeout"

        try:
            conn = self._get_connection()
            try:
                cursor = conn.execute("SELECT 1;")
                row = cursor.fetchone()
                if row and row[0] == 1:
                    return True, "healthy"
                return False, "Unexpected database probe response"
            finally:
                if not self._is_memory:
                    conn.close()
        except sqlite3.OperationalError as exc:
            logger.warning("Persistence health probe operational error: %s", exc)
            return False, "Database operational error"
        except sqlite3.DatabaseError as exc:
            logger.warning("Persistence health probe database error: %s", exc)
            return False, "Database corruption or format error"
        except Exception as exc:
            logger.warning("Persistence health probe unexpected error: %s", exc)
            return False, "Database connection failure"
        finally:
            lock.release()


    def get_status_counts(self, timeout: float = 2.0) -> dict[str, int]:
        """Aggregate counts of workflows by lifecycle status from the runs table.

        Returns a dictionary mapping status names to integer counts (e.g. {'completed': 3, 'running': 1}).
        Guaranteed non-blocking, exception-safe, and never holding lifecycle locks indefinitely.
        """
        counts: dict[str, int] = {}
        if not self.enabled:
            return counts

        lock = self._get_db_lock(self.db_path)
        acquired = lock.acquire(timeout=timeout)
        if not acquired:
            logger.warning("Could not acquire DB lock within %.1fs for status counts", timeout)
            return counts

        try:
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT status, COUNT(*) as cnt FROM runs GROUP BY status;"
                )
                for row in cursor.fetchall():
                    status_name = str(row["status"] or "unknown").strip()
                    counts[status_name] = int(row["cnt"])
            finally:
                if not self._is_memory:
                    conn.close()
        except Exception as exc:
            logger.warning("Failed to retrieve status counts: %s", exc)
        finally:
            lock.release()

        return counts

    def record_audit_event(
        self,
        event_id: str,
        timestamp: str,
        event_type: str,
        run_id: str | None,
        correlation_id: str | None,
        actor: str | None,
        outcome: str,
        metadata: dict[str, Any] | None = None,
        dedup: bool = False,
        conn: sqlite3.Connection | None = None,
        raise_on_error: bool = False,
    ) -> bool:
        """Persist a structured audit event to the audit_events table.

        Concurrency & Safety:
        - Thread-safe via process-wide _get_db_lock (when conn is None).
        - If an active transaction connection 'conn' is provided, executes within
          the caller's transaction/savepoint for atomic state and audit persistence.
        - Supports deduplication (e.g. for startup recovery, retries) when dedup=True.
        - When raise_on_error=True, re-raises errors to allow caller transaction rollback.
        """
        if not self.enabled:
            return False

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        clean_meta = dict(metadata or {})
        try:
            meta_json = json.dumps(clean_meta, default=str)
        except Exception:
            meta_json = "{}"

        def _insert_event(target_conn: Any) -> bool:
            if dedup:
                op_key = clean_meta.get("operation_key")
                if op_key:
                    query_sql = (
                        "SELECT 1 FROM audit_events WHERE event_type = ? AND outcome = ? "
                        "AND json_extract(metadata_json, '$.operation_key') = ?"
                    )
                    params: list[Any] = [event_type, outcome, str(op_key)]
                    if run_id:
                        query_sql += " AND run_id = ?"
                        params.append(run_id)
                    query_sql += " LIMIT 1;"
                    try:
                        cursor = target_conn.execute(query_sql, tuple(params))
                    except Exception:
                        fallback_sql = (
                            "SELECT 1 FROM audit_events WHERE event_type = ? AND outcome = ? "
                            "AND metadata_json LIKE ?"
                        )
                        fallback_params: list[Any] = [
                            event_type,
                            outcome,
                            f'%"operation_key": "{op_key}"%',
                        ]
                        if run_id:
                            fallback_sql += " AND run_id = ?"
                            fallback_params.append(run_id)
                        fallback_sql += " LIMIT 1;"
                        cursor = target_conn.execute(fallback_sql, tuple(fallback_params))
                    if cursor.fetchone():
                        return True
                elif run_id:
                    cursor = target_conn.execute(
                        "SELECT 1 FROM audit_events WHERE run_id = ? AND event_type = ? "
                        "AND outcome = ? LIMIT 1;",
                        (run_id, event_type, outcome),
                    )
                    if cursor.fetchone():
                        return True

            target_conn.execute(
                """
                INSERT OR IGNORE INTO audit_events (
                    event_id, timestamp, event_type, run_id, correlation_id,
                    actor, outcome, metadata_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    event_id,
                    timestamp,
                    event_type,
                    run_id,
                    correlation_id,
                    actor,
                    outcome,
                    meta_json,
                    now,
                ),
            )
            return True

        if conn is not None:
            try:
                return _insert_event(conn)
            except Exception as exc:
                logger.warning("Failed to persist audit event %s on conn: %s", event_id, exc)
                if raise_on_error:
                    raise
                return False

        with self._get_db_lock(self.db_path):
            managed_conn = self._get_connection()
            try:
                with managed_conn:
                    return _insert_event(managed_conn)
            except Exception as exc:
                logger.warning("Failed to persist audit event %s: %s", event_id, exc)
                if raise_on_error:
                    raise
                return False
            finally:
                if not self._is_memory:
                    managed_conn.close()

    def get_audit_events(
        self,
        run_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Query chronological audit events, bounded by limit and offset."""
        if not self.enabled:
            return []

        safe_limit = max(1, min(500, int(limit)))
        safe_offset = max(0, int(offset))

        query = "SELECT * FROM audit_events"
        params = []
        conditions = []

        if run_id:
            conditions.append("run_id = ?")
            params.append(str(run_id).strip())
        if event_type:
            conditions.append("event_type = ?")
            params.append(str(event_type).strip())

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp ASC, rowid ASC LIMIT ? OFFSET ?;"
        params.extend([safe_limit, safe_offset])

        events = []
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, params)
                for row in cursor.fetchall():
                    raw_meta = row["metadata_json"]
                    try:
                        meta = json.loads(raw_meta) if raw_meta else {}
                    except Exception:
                        meta = {}

                    events.append(
                        {
                            "event_id": row["event_id"],
                            "timestamp": row["timestamp"],
                            "event_type": row["event_type"],
                            "run_id": row["run_id"],
                            "correlation_id": row["correlation_id"],
                            "actor": row["actor"],
                            "outcome": row["outcome"],
                            "metadata": meta,
                        }
                    )
            except Exception as exc:
                logger.warning("Failed to query audit events: %s", exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

        return events

    def count_audit_events(self, run_id: str | None = None) -> int:
        """Return the count of recorded audit events for a run or in total."""
        if not self.enabled:
            return 0

        query = "SELECT COUNT(*) FROM audit_events"
        params = []
        if run_id:
            query += " WHERE run_id = ?"
            params.append(str(run_id).strip())

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, params)
                row = cursor.fetchone()
                return int(row[0]) if row else 0
            except Exception as exc:
                logger.warning("Failed to count audit events: %s", exc)
                return 0
            finally:
                if not self._is_memory:
                    conn.close()

    def purge_audit_events(
        self,
        max_age_days: int | None = None,
        before_timestamp: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Purge audit events older than cutoff timestamp under concurrency lock.

        Safety and Integrity Guarantees:
        - Only deletes rows from audit_events. Never touches checkpoints, runs, or other tables.
        - Deletion is permanent unless a separate backup exists.
        - Thread-safe via process-wide _get_db_lock.
        - Idempotent: repeated calls with same cutoff delete 0 rows and return success=True.
        - Non-blocking safe failure policy: catches errors, logs warning, returns success=False.
        """
        if not self.enabled:
            return {
                "success": False,
                "deleted_count": 0,
                "cutoff_timestamp": None,
                "max_age_days": max_age_days,
                "reason": "Persistence is disabled",
                "error": None,
            }

        cutoff: str | None = None
        if before_timestamp:
            clean_ts = str(before_timestamp).strip()
            try:
                parsed = datetime.datetime.fromisoformat(clean_ts.replace("Z", "+00:00"))
                cutoff = parsed.astimezone(datetime.timezone.utc).isoformat()
            except Exception:
                return {
                    "success": False,
                    "deleted_count": 0,
                    "cutoff_timestamp": None,
                    "max_age_days": max_age_days,
                    "reason": None,
                    "error": f"Invalid before_timestamp format: {before_timestamp!r}",
                }
        elif max_age_days is not None:
            if max_age_days < 0:
                return {
                    "success": False,
                    "deleted_count": 0,
                    "cutoff_timestamp": None,
                    "max_age_days": max_age_days,
                    "reason": None,
                    "error": "max_age_days must be non-negative",
                }
            if max_age_days == 0:
                return {
                    "success": True,
                    "deleted_count": 0,
                    "cutoff_timestamp": None,
                    "max_age_days": 0,
                    "reason": "Retention disabled (max_age_days=0)",
                    "error": None,
                }
            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = (now - datetime.timedelta(days=max_age_days)).isoformat()
        else:
            from ..config import get_settings

            settings = get_settings()
            cfg_days = getattr(settings, "audit_retention_days", 30)
            if cfg_days <= 0:
                return {
                    "success": True,
                    "deleted_count": 0,
                    "cutoff_timestamp": None,
                    "max_age_days": cfg_days,
                    "reason": "Retention disabled by configuration",
                    "error": None,
                }
            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = (now - datetime.timedelta(days=cfg_days)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = None
            try:
                conn = self._get_connection()
                with conn:
                    if limit is not None and limit > 0:
                        cursor = conn.execute(
                            """
                            DELETE FROM audit_events
                            WHERE rowid IN (
                                SELECT rowid FROM audit_events
                                WHERE timestamp < ?
                                ORDER BY timestamp ASC
                                LIMIT ?
                            );
                            """,
                            (cutoff, int(limit)),
                        )
                    else:
                        cursor = conn.execute(
                            "DELETE FROM audit_events WHERE timestamp < ?;",
                            (cutoff,),
                        )
                    deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0

                return {
                    "success": True,
                    "deleted_count": deleted_count,
                    "cutoff_timestamp": cutoff,
                    "max_age_days": max_age_days,
                    "reason": None,
                    "error": None,
                }
            except Exception as exc:
                logger.warning("Failed to purge audit events: %s", exc)
                return {
                    "success": False,
                    "deleted_count": 0,
                    "cutoff_timestamp": cutoff,
                    "max_age_days": max_age_days,
                    "reason": "Database error",
                    "error": str(exc),
                }
            finally:
                if conn is not None and not self._is_memory:
                    conn.close()

    def export_audit_events(
        self,
        run_id: str | None = None,
        event_type: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Retrieve audit events for export with bounded pagination and optional time range filters."""
        if not self.enabled:
            return []

        from ..config import get_settings

        max_limit = getattr(get_settings(), "audit_export_max_limit", 1000)
        safe_limit = max(1, min(max_limit, int(limit)))
        safe_offset = max(0, int(offset))

        query = "SELECT * FROM audit_events"
        params = []
        conditions = []

        if run_id:
            conditions.append("run_id = ?")
            params.append(str(run_id).strip())
        if event_type:
            conditions.append("event_type = ?")
            params.append(str(event_type).strip())
        if start_time:
            conditions.append("timestamp >= ?")
            params.append(str(start_time).strip())
        if end_time:
            conditions.append("timestamp <= ?")
            params.append(str(end_time).strip())

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY timestamp ASC, rowid ASC LIMIT ? OFFSET ?;"
        params.extend([safe_limit, safe_offset])

        events = []
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, params)
                for row in cursor.fetchall():
                    raw_meta = row["metadata_json"]
                    try:
                        meta = json.loads(raw_meta) if raw_meta else {}
                    except Exception:
                        meta = {}

                    events.append(
                        {
                            "event_id": row["event_id"],
                            "timestamp": row["timestamp"],
                            "event_type": row["event_type"],
                            "run_id": row["run_id"],
                            "correlation_id": row["correlation_id"],
                            "actor": row["actor"],
                            "outcome": row["outcome"],
                            "metadata": meta,
                        }
                    )
            except Exception as exc:
                logger.warning("Failed to export audit events: %s", exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

        return events

    def get_idempotency_record(
        self, client_scope: str | None = None, key: str = ""
    ) -> dict[str, Any] | None:
        """Retrieve an idempotency record for a client scope and key, or key alone."""
        if not self.enabled:
            return None
        clean_key = str(key or "").strip()
        if not clean_key:
            return None
        clean_scope = str(client_scope or "").strip() if client_scope is not None else None

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                if clean_scope is not None:
                    cursor = conn.execute(
                        """
                        SELECT key, client_scope, request_hash, run_id, status, response_code,
                               response_json, created_at, updated_at
                        FROM idempotency_keys
                        WHERE client_scope = ? AND key = ?;
                        """,
                        (clean_scope, clean_key),
                    )
                else:
                    cursor = conn.execute(
                        """
                        SELECT key, client_scope, request_hash, run_id, status, response_code,
                               response_json, created_at, updated_at
                        FROM idempotency_keys
                        WHERE key = ?
                        ORDER BY updated_at DESC
                        LIMIT 1;
                        """,
                        (clean_key,),
                    )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "key": row["key"],
                    "client_scope": row["client_scope"],
                    "request_hash": row["request_hash"],
                    "run_id": row["run_id"],
                    "status": row["status"],
                    "response_code": row["response_code"],
                    "response_json": row["response_json"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            except Exception as exc:
                logger.warning("Failed to retrieve idempotency record: %s", exc)
                return None
            finally:
                if not self._is_memory:
                    conn.close()

    def acquire_idempotency_key(
        self,
        client_scope: str,
        key: str,
        request_hash: str,
        tentative_run_id: str,
    ) -> tuple[str, dict[str, Any] | None]:
        """Atomically acquire an idempotency key.

        Returns a tuple (status, record):
        - ("acquired", None): Caller claimed the key and must proceed with workflow creation.
        - ("completed", record): Key was previously completed; return stored response.
        - ("in_progress", record): Another worker is actively creating this run; caller must wait.
        - ("conflict", record): Key was previously used with a different request payload.
        """
        if not self.enabled:
            return "acquired", None

        clean_scope = str(client_scope or "").strip()
        clean_key = str(key or "").strip()
        clean_hash = str(request_hash or "").strip()
        clean_run_id = str(tentative_run_id or "").strip()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cursor = conn.execute(
                        """
                        SELECT key, client_scope, request_hash, run_id, status, response_code,
                               response_json, created_at, updated_at
                        FROM idempotency_keys
                        WHERE client_scope = ? AND key = ?;
                        """,
                        (clean_scope, clean_key),
                    )
                    row = cursor.fetchone()
                    if row:
                        rec = {
                            "key": row["key"],
                            "client_scope": row["client_scope"],
                            "request_hash": row["request_hash"],
                            "run_id": row["run_id"],
                            "status": row["status"],
                            "response_code": row["response_code"],
                            "response_json": row["response_json"],
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                        if rec["request_hash"] != clean_hash:
                            return "conflict", rec
                        if rec["status"] == "completed":
                            return "completed", rec
                        if rec["status"] == "in_progress":
                            return "in_progress", rec
                        if rec["status"] == "failed":
                            # Prior attempt failed; allow safe retry by resetting to in_progress
                            conn.execute(
                                """
                                UPDATE idempotency_keys
                                SET status = 'in_progress', run_id = ?, updated_at = ?
                                WHERE client_scope = ? AND key = ?;
                                """,
                                (clean_run_id, now, clean_scope, clean_key),
                            )
                            return "acquired", None

                    # No existing record: atomic insertion of in_progress record
                    conn.execute(
                        """
                        INSERT INTO idempotency_keys (
                            key, client_scope, request_hash, run_id, status, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, 'in_progress', ?, ?);
                        """,
                        (clean_key, clean_scope, clean_hash, clean_run_id, now, now),
                    )
                    return "acquired", None
            except sqlite3.IntegrityError:
                # Concurrent race condition: another thread inserted first
                with conn:
                    cursor = conn.execute(
                        """
                        SELECT key, client_scope, request_hash, run_id, status, response_code,
                               response_json, created_at, updated_at
                        FROM idempotency_keys
                        WHERE client_scope = ? AND key = ?;
                        """,
                        (clean_scope, clean_key),
                    )
                    row = cursor.fetchone()
                    if row:
                        rec = {
                            "key": row["key"],
                            "client_scope": row["client_scope"],
                            "request_hash": row["request_hash"],
                            "run_id": row["run_id"],
                            "status": row["status"],
                            "response_code": row["response_code"],
                            "response_json": row["response_json"],
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                        if rec["request_hash"] != clean_hash:
                            return "conflict", rec
                        if rec["status"] == "completed":
                            return "completed", rec
                        return "in_progress", rec
                return "in_progress", None
            except Exception as exc:
                logger.error("Failed to acquire idempotency key: %s", exc)
                return "acquired", None
            finally:
                if not self._is_memory:
                    conn.close()

    def wait_for_idempotency_completion(
        self,
        client_scope: str,
        key: str,
        timeout: float = 30.0,
    ) -> dict[str, Any] | None:
        """Wait for an in-progress idempotency key to reach a terminal state."""
        deadline = time.monotonic() + max(0.1, timeout)
        cond = self._get_idempotency_condition(client_scope, key)

        while time.monotonic() < deadline:
            rec = self.get_idempotency_record(client_scope, key)
            if rec and rec.get("status") in ("completed", "failed"):
                return rec
            with cond:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                cond.wait(timeout=min(0.2, remaining))

        return self.get_idempotency_record(client_scope, key)

    def finalize_idempotency_key(
        self,
        client_scope: str,
        key: str,
        response_code: int,
        response_json: str,
    ) -> bool:
        """Finalize an in-progress idempotency key upon successful run creation."""
        if not self.enabled:
            return False

        clean_scope = str(client_scope or "").strip()
        clean_key = str(key or "").strip()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        UPDATE idempotency_keys
                        SET status = 'completed', response_code = ?, response_json = ?, updated_at = ?
                        WHERE client_scope = ? AND key = ?;
                        """,
                        (int(response_code), str(response_json), now, clean_scope, clean_key),
                    )
            except Exception as exc:
                logger.error("Failed to finalize idempotency key: %s", exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

        cond = self._get_idempotency_condition(clean_scope, clean_key)
        with cond:
            cond.notify_all()
        return True

    def fail_idempotency_key(
        self,
        client_scope: str,
        key: str,
        error_detail: str | None = None,
    ) -> bool:
        """Mark an in-progress idempotency key as failed so retries can safely re-attempt creation."""
        if not self.enabled:
            return False

        clean_scope = str(client_scope or "").strip()
        clean_key = str(key or "").strip()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        UPDATE idempotency_keys
                        SET status = 'failed', updated_at = ?
                        WHERE client_scope = ? AND key = ?;
                        """,
                        (now, clean_scope, clean_key),
                    )
            except Exception as exc:
                logger.error("Failed to mark idempotency key as failed: %s", exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

        cond = self._get_idempotency_condition(clean_scope, clean_key)
        with cond:
            cond.notify_all()
        return True

    def recover_in_progress_idempotency_keys(self) -> int:
        """Recover lingering in-progress idempotency keys during application startup."""
        if not self.enabled:
            return 0

        recovered_count = 0
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cursor = conn.execute(
                        """
                        SELECT key, client_scope, run_id
                        FROM idempotency_keys
                        WHERE status = 'in_progress';
                        """
                    )
                    rows = cursor.fetchall()
                    for r in rows:
                        run_id = r["run_id"]
                        client_scope = r["client_scope"]
                        key = r["key"]

                        summary = self.get_run_summary(run_id)
                        if summary:
                            from ..api.models import RunResponse

                            resp = RunResponse(
                                run_id=run_id,
                                task=summary.get("task", ""),
                                status=summary.get("status", "interrupted"),
                                created_at=summary.get("created_at"),
                                updated_at=summary.get("updated_at") or now,
                            )
                            conn.execute(
                                """
                                UPDATE idempotency_keys
                                SET status = 'completed', response_code = 201,
                                    response_json = ?, updated_at = ?
                                WHERE client_scope = ? AND key = ?;
                                """,
                                (resp.model_dump_json(), now, client_scope, key),
                            )
                        else:
                            # Run creation was incomplete before restart; allow retry
                            conn.execute(
                                """
                                UPDATE idempotency_keys
                                SET status = 'failed', updated_at = ?
                                WHERE client_scope = ? AND key = ?;
                                """,
                                (now, client_scope, key),
                            )
                        recovered_count += 1
            except Exception as exc:
                logger.warning("Failed during startup idempotency key recovery: %s", exc)
            finally:
                if not self._is_memory:
                    conn.close()

        return recovered_count

    def purge_idempotency_keys(
        self,
        max_age_hours: int | None = None,
        before_timestamp: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Purge idempotency keys older than retention cutoff under concurrency lock."""
        if not self.enabled:
            return {
                "success": False,
                "deleted_count": 0,
                "cutoff_timestamp": None,
                "reason": "Persistence is disabled",
            }

        cutoff: str | None = None
        if before_timestamp:
            try:
                parsed = datetime.datetime.fromisoformat(before_timestamp.replace("Z", "+00:00"))
                cutoff = parsed.astimezone(datetime.timezone.utc).isoformat()
            except Exception as exc:
                return {
                    "success": False,
                    "deleted_count": 0,
                    "cutoff_timestamp": None,
                    "error": f"Invalid before_timestamp: {exc}",
                }
        else:
            from ..config import get_settings

            settings = get_settings()
            hours = (
                max_age_hours
                if max_age_hours is not None
                else getattr(settings, "idempotency_retention_hours", 24)
            )
            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = (now - datetime.timedelta(hours=hours)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = None
            try:
                conn = self._get_connection()
                with conn:
                    if limit is not None and limit > 0:
                        cursor = conn.execute(
                            """
                            DELETE FROM idempotency_keys
                            WHERE rowid IN (
                                SELECT rowid FROM idempotency_keys
                                WHERE created_at < ?
                                ORDER BY created_at ASC
                                LIMIT ?
                            );
                            """,
                            (cutoff, int(limit)),
                        )
                    else:
                        cursor = conn.execute(
                            "DELETE FROM idempotency_keys WHERE created_at < ?;",
                            (cutoff,),
                        )
                    deleted_count = cursor.rowcount if cursor.rowcount >= 0 else 0

                return {
                    "success": True,
                    "deleted_count": deleted_count,
                    "cutoff_timestamp": cutoff,
                    "error": None,
                }
            except Exception as exc:
                logger.warning("Failed to purge idempotency keys: %s", exc)
                return {
                    "success": False,
                    "deleted_count": 0,
                    "cutoff_timestamp": cutoff,
                    "error": str(exc),
                }
            finally:
                if conn is not None and not self._is_memory:
                    conn.close()

    def acquire_workflow_admission(
        self,
        run_id: str,
        owner_id: str,
        max_active_runs: int,
        lease_ttl_seconds: float = 30.0,
    ) -> bool:
        """Atomically reserve a global workflow admission slot in the SQLite database.

        Under an immediate SQLite transaction, cleans expired leases, checks if global
        admissions are below max_active_runs, and inserts the reservation.

        Returns True if admitted, False if server capacity limit reached.
        """
        if not self.enabled:
            return True

        clean_run_id = str(run_id or "").strip()
        clean_owner_id = str(owner_id or "").strip()
        if not clean_run_id or not clean_owner_id:
            return False

        max_runs = max(1, int(max_active_runs))
        ttl = max(0.001, float(lease_ttl_seconds))
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        cutoff_dt = now_dt - datetime.timedelta(seconds=ttl)
        cutoff_iso = cutoff_dt.isoformat()
        expires_dt = now_dt + datetime.timedelta(seconds=ttl)
        expires_iso = expires_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            try:
                conn.isolation_level = None
                max_retries = 25
                for attempt in range(max_retries):
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                        break
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                        else:
                            raise CheckpointError(
                                f"Database locked while acquiring workflow admission: {exc}"
                            ) from exc

                try:
                    # 1. Clean expired admissions older than cutoff / past expiry
                    conn.execute(
                        "DELETE FROM workflow_admissions WHERE expires_at < ? OR heartbeat_at < ?;",
                        (now_iso, cutoff_iso),
                    )

                    # 2. Check if already admitted
                    cursor = conn.execute(
                        "SELECT owner_id, fence_token FROM workflow_admissions WHERE run_id = ?;",
                        (clean_run_id,),
                    )
                    existing = cursor.fetchone()
                    if existing:
                        if existing["owner_id"] == clean_owner_id:
                            # Re-entrant: refresh heartbeat and expiry, keep existing fence_token
                            conn.execute(
                                "UPDATE workflow_admissions "
                                "SET heartbeat_at = ?, expires_at = ? WHERE run_id = ?;",
                                (now_iso, expires_iso, clean_run_id),
                            )
                            conn.execute("COMMIT;")
                            return True
                        else:
                            # Already active under another owner
                            conn.execute("ROLLBACK;")
                            return False

                    # 3. Determine next fence token monotonically from runs table
                    cursor = conn.execute(
                        "SELECT COALESCE(fence_token, 0) AS max_token FROM runs WHERE run_id = ?;",
                        (clean_run_id,),
                    )
                    run_row = cursor.fetchone()
                    prev_token = int(run_row["max_token"]) if run_row else 0
                    new_token = prev_token + 1

                    # 4. Count currently active admissions across all processes
                    cursor = conn.execute("SELECT COUNT(*) AS cnt FROM workflow_admissions;")
                    row = cursor.fetchone()
                    active_count = int(row["cnt"]) if row else 0

                    if active_count >= max_runs:
                        conn.execute("ROLLBACK;")
                        return False

                    # 5. Insert reservation with monotonic fence token
                    conn.execute(
                        """
                        INSERT INTO workflow_admissions (
                            run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                        )
                        VALUES (?, ?, 'active', ?, ?, ?, ?);
                        """,
                        (clean_run_id, clean_owner_id, now_iso, now_iso, expires_iso, new_token),
                    )
                    conn.execute(
                        "UPDATE runs SET fence_token = ? WHERE run_id = ?;",
                        (new_token, clean_run_id),
                    )
                    conn.execute("COMMIT;")
                    return True
                except Exception:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    raise
            finally:
                conn.isolation_level = old_isolation
                if not self._is_memory:
                    conn.close()

    def release_workflow_admission(
        self,
        run_id: str,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> bool:
        """Release a workflow admission slot conditionally upon completion, failure, or cancellation."""
        if not self.enabled:
            return True

        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return False

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    if owner_id is not None and fence_token is not None:
                        cursor = conn.execute(
                            "DELETE FROM workflow_admissions "
                            "WHERE run_id = ? AND owner_id = ? AND fence_token = ?;",
                            (clean_run_id, str(owner_id).strip(), int(fence_token)),
                        )
                    elif owner_id is not None:
                        cursor = conn.execute(
                            "DELETE FROM workflow_admissions WHERE run_id = ? AND owner_id = ?;",
                            (clean_run_id, str(owner_id).strip()),
                        )
                    else:
                        cursor = conn.execute(
                            "DELETE FROM workflow_admissions WHERE run_id = ?;",
                            (clean_run_id,),
                        )
                    return cursor.rowcount > 0
            except Exception as exc:
                logger.warning("Error releasing admission for run %s: %s", clean_run_id, exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def heartbeat_workflow_admissions(
        self,
        owner_id: str,
        run_ids: list[str] | dict[str, int] | None = None,
        lease_ttl_seconds: float = 30.0,
    ) -> int:
        """Renew active admission leases and expiration timestamps for the given owner_id."""
        if not self.enabled:
            return 0

        clean_owner = str(owner_id or "").strip()
        if not clean_owner:
            return 0

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        ttl = max(0.001, float(lease_ttl_seconds))
        expires_iso = (now_dt + datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    if isinstance(run_ids, dict):
                        updated_count = 0
                        for r_id, tok in run_ids.items():
                            cur = conn.execute(
                                """
                                UPDATE workflow_admissions
                                SET heartbeat_at = ?, expires_at = ?
                                WHERE owner_id = ? AND run_id = ? AND fence_token = ?;
                                """,
                                (now_iso, expires_iso, clean_owner, str(r_id).strip(), int(tok)),
                            )
                            if cur.rowcount > 0:
                                updated_count += cur.rowcount
                        return updated_count
                    elif run_ids is not None:
                        clean_ids = [str(r).strip() for r in run_ids if str(r).strip()]
                        if not clean_ids:
                            return 0
                        placeholders = ",".join("?" for _ in clean_ids)
                        cursor = conn.execute(
                            f"UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? "
                            f"WHERE owner_id = ? AND run_id IN ({placeholders});",
                            [now_iso, expires_iso, clean_owner, *clean_ids],
                        )
                        return cursor.rowcount if cursor.rowcount >= 0 else 0
                    else:
                        cursor = conn.execute(
                            "UPDATE workflow_admissions SET heartbeat_at = ?, expires_at = ? "
                            "WHERE owner_id = ?;",
                            (now_iso, expires_iso, clean_owner),
                        )
                        return cursor.rowcount if cursor.rowcount >= 0 else 0
            except Exception as exc:
                logger.debug("Heartbeat update failed for owner %s: %s", clean_owner, exc)
                return 0
            finally:
                if not self._is_memory:
                    conn.close()

    def heartbeat_active_runs(
        self,
        owner_id: str,
        active_tokens: dict[str, int],
        lease_ttl_seconds: float = 30.0,
    ) -> tuple[int, set[str], set[str]]:
        """Renew active admission leases conditionally by owner_id and fence_token.

        Returns (updated_count, renewed_ids, failed_ids).
        """
        if not self.enabled:
            return 0, set(), set()

        clean_owner = str(owner_id or "").strip()
        if not clean_owner or not active_tokens:
            return 0, set(), set()

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        ttl = max(0.001, float(lease_ttl_seconds))
        expires_iso = (now_dt + datetime.timedelta(seconds=ttl)).isoformat()

        renewed_ids: set[str] = set()
        failed_ids: set[str] = set()
        updated_count = 0

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    for r_id, token in active_tokens.items():
                        c_id = str(r_id).strip()
                        if not c_id:
                            continue
                        cur = conn.execute(
                            """
                            UPDATE workflow_admissions
                            SET heartbeat_at = ?, expires_at = ?
                            WHERE owner_id = ? AND run_id = ? AND fence_token = ?;
                            """,
                            (now_iso, expires_iso, clean_owner, c_id, int(token)),
                        )
                        if cur.rowcount > 0:
                            updated_count += cur.rowcount
                            renewed_ids.add(c_id)
                        else:
                            failed_ids.add(c_id)
                return updated_count, renewed_ids, failed_ids
            finally:
                if not self._is_memory:
                    conn.close()

    def get_workflow_admission(self, run_id: str) -> dict[str, Any] | None:
        """Return the active admission record for run_id or None if not admitted."""
        if not self.enabled:
            return None

        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            return None

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    FROM workflow_admissions
                    WHERE run_id = ?;
                    """,
                    (clean_run_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "run_id": row["run_id"],
                    "owner_id": row["owner_id"],
                    "status": row["status"],
                    "acquired_at": row["acquired_at"],
                    "heartbeat_at": row["heartbeat_at"],
                    "expires_at": row["expires_at"],
                    "fence_token": int(row["fence_token"]),
                }
            except Exception as exc:
                logger.warning("Failed to get admission for run %s: %s", clean_run_id, exc)
                return None
            finally:
                if not self._is_memory:
                    conn.close()

    def validate_admission_ownership(
        self,
        run_id: str,
        owner_id: str,
        fence_token: int | None = None,
    ) -> bool:
        """Check whether the given owner and fence token currently hold a valid, unexpired admission lease."""
        if not self.enabled:
            return True

        clean_run_id = str(run_id or "").strip()
        clean_owner = str(owner_id or "").strip()
        if not clean_run_id or not clean_owner:
            return False

        now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT owner_id, fence_token, expires_at FROM workflow_admissions WHERE run_id = ?;",
                    (clean_run_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return False
                if str(row["owner_id"] or "").strip() != clean_owner:
                    return False
                if fence_token is not None and int(row["fence_token"] or 1) != int(fence_token):
                    return False
                if str(row["expires_at"] or "") < now_iso:
                    return False

                cursor = conn.execute(
                    "SELECT fence_token, status FROM runs WHERE run_id = ?;",
                    (clean_run_id,),
                )
                run_row = cursor.fetchone()
                if run_row:
                    if fence_token is not None and int(run_row["fence_token"] or 0) > int(fence_token):
                        return False
                    run_status = str(run_row["status"] or "").lower()
                    if run_status in ("completed", "cancelled", "interrupted"):
                        return False

                return True
            except Exception as exc:
                logger.warning("Error checking admission ownership for %s: %s", clean_run_id, exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def count_active_admissions(
        self,
        lease_ttl_seconds: float = 30.0,
    ) -> int:
        """Count active, non-expired workflow admissions across all processes."""
        if not self.enabled:
            return 0

        ttl = max(0.001, float(lease_ttl_seconds))
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        cutoff_iso = (now_dt - datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM workflow_admissions "
                    "WHERE expires_at >= ? AND heartbeat_at >= ?;",
                    (now_iso, cutoff_iso),
                )
                row = cursor.fetchone()
                return int(row["cnt"]) if row else 0
            except Exception as exc:
                logger.warning("Failed to count active admissions: %s", exc)
                return 0
            finally:
                if not self._is_memory:
                    conn.close()

    def list_active_admissions(
        self,
        lease_ttl_seconds: float = 30.0,
    ) -> list[dict[str, Any]]:
        """List active, non-expired admissions across all processes."""
        if not self.enabled:
            return []

        ttl = max(0.001, float(lease_ttl_seconds))
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        cutoff_iso = (now_dt - datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT run_id, owner_id, status, acquired_at, heartbeat_at, expires_at, fence_token
                    FROM workflow_admissions
                    WHERE expires_at >= ? AND heartbeat_at >= ?
                    ORDER BY acquired_at ASC;
                    """,
                    (now_iso, cutoff_iso),
                )
                return [
                    {
                        "run_id": row["run_id"],
                        "owner_id": row["owner_id"],
                        "status": row["status"],
                        "acquired_at": row["acquired_at"],
                        "heartbeat_at": row["heartbeat_at"],
                        "expires_at": row["expires_at"],
                        "fence_token": int(row["fence_token"] if "fence_token" in row.keys() else 1),
                    }
                    for row in cursor.fetchall()
                ]
            except Exception as exc:
                logger.warning("Failed to list active admissions: %s", exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

    def clean_stale_admissions(
        self,
        lease_ttl_seconds: float = 30.0,
    ) -> list[str]:
        """Delete abandoned admissions whose heartbeats have exceeded lease TTL and return their run IDs."""
        if not self.enabled:
            return []

        ttl = max(0.001, float(lease_ttl_seconds))
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()
        cutoff_iso = (now_dt - datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cursor = conn.execute(
                        "SELECT run_id FROM workflow_admissions WHERE expires_at < ? OR heartbeat_at < ?;",
                        (now_iso, cutoff_iso),
                    )
                    stale_ids = [row["run_id"] for row in cursor.fetchall()]
                    if stale_ids:
                        conn.execute(
                            "DELETE FROM workflow_admissions WHERE expires_at < ? OR heartbeat_at < ?;",
                            (now_iso, cutoff_iso),
                        )
                    return stale_ids
            except Exception as exc:
                logger.warning("Failed to clean stale admissions: %s", exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

    def get_tool_execution(self, operation_key: str) -> dict[str, Any] | None:
        """Retrieve recorded tool execution details by operation key."""
        if not self.enabled:
            return None

        clean_op = str(operation_key or "").strip()
        if not clean_op:
            return None

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, subtask_id, tool_name,
                           fence_token, status, args_json, result_text,
                           error_text, reconciled_by, reconciled_at,
                           reconciliation_reason, created_at, updated_at
                    FROM tool_executions
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "operation_key": row["operation_key"],
                    "run_id": row["run_id"],
                    "subtask_id": row["subtask_id"],
                    "tool_name": row["tool_name"],
                    "fence_token": int(row["fence_token"] or 1),
                    "status": row["status"],
                    "args_json": row["args_json"],
                    "result_text": row["result_text"],
                    "error_text": row["error_text"],
                    "reconciled_by": row["reconciled_by"],
                    "reconciled_at": row["reconciled_at"],
                    "reconciliation_reason": row["reconciliation_reason"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            except Exception as exc:
                logger.warning("Error fetching tool execution for key %s: %s", clean_op, exc)
                return None
            finally:
                if not self._is_memory:
                    conn.close()

    def record_tool_dispatch(
        self,
        operation_key: str,
        run_id: str,
        subtask_id: str,
        tool_name: str,
        fence_token: int = 1,
        args: dict[str, Any] | None = None,
    ) -> None:
        """Record the initial dispatch of a tool execution with status 'pending'."""
        if not self.enabled:
            return

        clean_op = str(operation_key or "").strip()
        clean_run = str(run_id or "").strip()
        clean_sub = str(subtask_id or "").strip()
        clean_tool = str(tool_name or "").strip()
        if not clean_op:
            return

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            args_json = json.dumps(args or {}, sort_keys=True, default=str)
        except Exception:
            args_json = "{}"

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    INSERT INTO tool_executions (
                        operation_key, run_id, subtask_id, tool_name, fence_token,
                        status, args_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    ON CONFLICT(operation_key) DO UPDATE SET
                        status = CASE WHEN tool_executions.status = 'completed'
                                      THEN 'completed' ELSE 'pending' END,
                        fence_token = excluded.fence_token,
                        updated_at = excluded.updated_at
                    WHERE tool_executions.status != 'completed';
                    """,
                    (clean_op, clean_run, clean_sub, clean_tool, int(fence_token), args_json, now, now),
                )
                conn.commit()
            except Exception as exc:
                logger.warning("Error recording tool dispatch for key %s: %s", clean_op, exc)
            finally:
                if not self._is_memory:
                    conn.close()

    def record_tool_completion(
        self,
        operation_key: str,
        result_text: str,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> None:
        """Record successful tool execution completion, enforcing admission ownership if owner_id given."""
        if not self.enabled:
            return

        clean_op = str(operation_key or "").strip()
        if not clean_op:
            return

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    "SELECT run_id FROM tool_executions WHERE operation_key = ?;",
                    (clean_op,),
                )
                row = cursor.fetchone()
                if row and owner_id:
                    run_id = row["run_id"]
                    if not self.validate_admission_ownership(run_id, owner_id, fence_token):
                        raise AdmissionFencingError(
                            f"Worker '{owner_id}' lost admission ownership for run '{run_id}'"
                        )

                conn.execute(
                    """
                    UPDATE tool_executions
                    SET status = 'completed', result_text = ?, updated_at = ?
                    WHERE operation_key = ?;
                    """,
                    (str(result_text), now, clean_op),
                )
                conn.commit()
            finally:
                if not self._is_memory:
                    conn.close()

    def record_tool_failure(
        self,
        operation_key: str,
        error_text: str,
        uncertain: bool = False,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> None:
        """Record tool execution failure, marking status as 'uncertain' or 'failed'."""
        if not self.enabled:
            return

        clean_op = str(operation_key or "").strip()
        if not clean_op:
            return

        status = "uncertain" if uncertain else "failed"
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                conn.execute(
                    """
                    UPDATE tool_executions
                    SET status = ?, error_text = ?, updated_at = ?
                    WHERE operation_key = ?;
                    """,
                    (status, str(error_text), now, clean_op),
                )
                conn.commit()
            except Exception as exc:
                logger.warning("Error recording tool failure for key %s: %s", clean_op, exc)
            finally:
                if not self._is_memory:
                    conn.close()

    def list_tool_executions(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List all tool executions recorded for a given workflow run, with optional filtering."""
        if not self.enabled:
            return []

        clean_run = str(run_id or "").strip()
        if not clean_run:
            return []

        query = """
            SELECT operation_key, run_id, subtask_id, tool_name,
                   fence_token, status, args_json, result_text,
                   error_text, reconciled_by, reconciled_at,
                   reconciliation_reason, created_at, updated_at
            FROM tool_executions
            WHERE run_id = ?
        """
        params: list[Any] = [clean_run]
        if status:
            query += " AND status = ?"
            params.append(status.strip().lower())
        if tool_name:
            query += " AND tool_name = ?"
            params.append(tool_name.strip())
        query += " ORDER BY created_at ASC;"

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, tuple(params))
                results = []
                for row in cursor.fetchall():
                    results.append(
                        {
                            "operation_key": row["operation_key"],
                            "run_id": row["run_id"],
                            "subtask_id": row["subtask_id"],
                            "tool_name": row["tool_name"],
                            "fence_token": int(row["fence_token"] or 1),
                            "status": row["status"],
                            "args_json": row["args_json"],
                            "result_text": row["result_text"],
                            "error_text": row["error_text"],
                            "reconciled_by": row["reconciled_by"],
                            "reconciled_at": row["reconciled_at"],
                            "reconciliation_reason": row["reconciliation_reason"],
                            "created_at": row["created_at"],
                            "updated_at": row["updated_at"],
                        }
                    )
                return results
            except Exception as exc:
                logger.warning("Error listing tool executions for run %s: %s", clean_run, exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

    def reconcile_tool_execution(
        self,
        run_id: str,
        operation_key: str,
        outcome: str,
        reason: str,
        result_text: str | None = None,
        error_text: str | None = None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        """Atomically reconcile an uncertain tool execution to completed or failed."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_outcome = str(outcome or "").strip().lower()
        clean_reason = str(reason or "").strip()
        clean_actor = str(actor or "operator").strip()

        if clean_outcome in ("success", "complete"):
            clean_outcome = "completed"
        elif clean_outcome in ("failure", "fail"):
            clean_outcome = "failed"

        if clean_outcome not in ("completed", "failed"):
            raise ValueError(
                f"Invalid reconciliation outcome '{outcome}'. Must be 'completed' or 'failed'."
            )

        if not clean_reason:
            raise ValueError("Reconciliation reason is required and cannot be empty.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                if not self._is_memory:
                    conn.execute("BEGIN IMMEDIATE;")

                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, subtask_id, tool_name,
                           fence_token, status, args_json, result_text,
                           error_text, created_at, updated_at,
                           reconciled_by, reconciled_at, reconciliation_reason
                    FROM tool_executions
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                row = cursor.fetchone()
                if not row:
                    raise ToolExecutionNotFoundError(
                        f"Tool execution '{clean_op}' not found."
                    )

                if row["run_id"] != clean_run:
                    raise ValueError(
                        f"Tool execution '{clean_op}' belongs to run '{row['run_id']}', not '{clean_run}'."
                    )

                current_status = row["status"]
                subtask_id = row["subtask_id"]
                tool_name = row["tool_name"]

                # Case 1: Idempotent repeat request
                if current_status == clean_outcome:
                    if not self._is_memory:
                        conn.execute("COMMIT;")
                    return {
                        "success": True,
                        "idempotent": True,
                        "operation_key": clean_op,
                        "run_id": clean_run,
                        "subtask_id": subtask_id,
                        "tool_name": tool_name,
                        "previous_status": current_status,
                        "status": current_status,
                        "reconciled_by": row["reconciled_by"] or clean_actor,
                        "reconciled_at": row["reconciled_at"] or row["updated_at"],
                        "reconciliation_reason": row["reconciliation_reason"] or clean_reason,
                    }

                # Case 2: In-flight execution cannot be reconciled
                if current_status == "pending":
                    raise ToolReconciliationConflictError(
                        f"Cannot reconcile tool execution '{clean_op}' while in-flight (status='pending')."
                    )

                # Case 3: Conflicting reconciliation from terminal completed or failed
                if current_status in ("completed", "failed"):
                    raise ToolReconciliationConflictError(
                        f"Cannot reconcile tool execution '{clean_op}' currently in "
                        f"status '{current_status}' to conflicting outcome '{clean_outcome}'."
                    )

                # Case 4: Valid transition from uncertain
                if current_status != "uncertain":
                    raise ToolReconciliationConflictError(
                        f"Invalid transition for tool execution '{clean_op}': "
                        f"status '{current_status}' cannot be reconciled."
                    )

                # Prepare updated text
                final_result = (
                    str(result_text) if result_text is not None
                    else (row["result_text"] or f"Reconciled to completed: {clean_reason}")
                ) if clean_outcome == "completed" else row["result_text"]

                final_error = (
                    str(error_text) if error_text is not None
                    else f"Reconciled to failed: {clean_reason}"
                ) if clean_outcome == "failed" else row["error_text"]

                up_cur = conn.execute(
                    """
                    UPDATE tool_executions
                    SET status = ?,
                        result_text = ?,
                        error_text = ?,
                        reconciled_by = ?,
                        reconciled_at = ?,
                        reconciliation_reason = ?,
                        updated_at = ?
                    WHERE operation_key = ? AND status = 'uncertain';
                    """,
                    (
                        clean_outcome,
                        final_result,
                        final_error,
                        clean_actor,
                        now,
                        clean_reason,
                        now,
                        clean_op,
                    ),
                )
                if up_cur.rowcount == 0:
                    raise ToolReconciliationConflictError(
                        f"Concurrent modification detected for tool execution '{clean_op}'."
                    )

                if not self._is_memory:
                    conn.execute("COMMIT;")

                return {
                    "success": True,
                    "idempotent": False,
                    "operation_key": clean_op,
                    "run_id": clean_run,
                    "subtask_id": subtask_id,
                    "tool_name": tool_name,
                    "previous_status": current_status,
                    "status": clean_outcome,
                    "reconciled_by": clean_actor,
                    "reconciled_at": now,
                    "reconciliation_reason": clean_reason,
                }
            except Exception:
                if not self._is_memory:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def recover_uncertain_tool_executions(self, run_id: str) -> int:
        """Mark any in-flight pending tool executions as uncertain during run recovery."""
        if not self.enabled:
            return 0

        clean_run = str(run_id or "").strip()
        if not clean_run:
            return 0

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    UPDATE tool_executions
                    SET status = 'uncertain',
                        error_text = 'Run interrupted before tool execution confirmed',
                        updated_at = ?
                    WHERE run_id = ? AND status = 'pending';
                    """,
                    (now, clean_run),
                )
                conn.commit()
                return cursor.rowcount
            except Exception as exc:
                logger.warning("Error recovering uncertain tool executions for run %s: %s", clean_run, exc)
                return 0
            finally:
                if not self._is_memory:
                    conn.close()

    def record_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        approver: str,
        decision: str,
        reason: str = "",
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        """Record an explicit human or operator approval decision for a tool execution."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_approver = str(approver or "").strip()
        clean_decision = str(decision or "").strip().lower()
        clean_reason = str(reason or "").strip()
        clean_fp = str(argument_fingerprint or "").strip() if argument_fingerprint else None
        clean_risk = str(risk_level or "").strip().lower() if risk_level else None
        clean_phash = str(policy_hash or "").strip() if policy_hash else None
        clean_expires_at = str(expires_at or "").strip() if expires_at else None

        if clean_decision not in ("approved", "rejected"):
            raise ValueError(
                f"Invalid approval decision '{decision}'. Must be 'approved' or 'rejected'."
            )
        if not clean_approver:
            raise ValueError("Approver identity is required.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()

                if existing:
                    existing_decision = str(existing["decision"] or "").lower()
                    if existing_decision == clean_decision:
                        return {
                            "operation_key": existing["operation_key"],
                            "run_id": existing["run_id"],
                            "tool_name": existing["tool_name"],
                            "approver": existing["approver"],
                            "decision": existing["decision"],
                            "reason": existing["reason"] or "",
                            "created_at": existing["created_at"],
                            "argument_fingerprint": existing["argument_fingerprint"],
                            "risk_level": existing["risk_level"],
                            "policy_hash": existing["policy_hash"],
                            "revoked_by": existing["revoked_by"],
                            "revocation_reason": existing["revocation_reason"],
                            "revoked_at": existing["revoked_at"],
                            "recovered_by": existing["recovered_by"],
                            "expires_at": existing["expires_at"],
                            "expired_at": existing["expired_at"],
                            "escalated_at": existing["escalated_at"],
                            "escalation_reason": existing["escalation_reason"],
                            "idempotent": True,
                        }

                    if existing_decision == "revoked":
                        rev_by = existing["revoked_by"] or "operator"
                        raise ToolApprovalConflictError(
                            f"Operation '{clean_op}' has been revoked by '{rev_by}' "
                            f"and cannot be approved or rejected."
                        )

                    if existing_decision == "expired":
                        raise ToolApprovalConflictError(
                            f"Operation '{clean_op}' has expired and cannot be approved or rejected. "
                            "Operational recovery is required to reset to pending before re-approval."
                        )

                    if existing_decision == "pending" and is_approval_expired(dict(existing)):
                        conn.execute(
                            """
                            UPDATE tool_approvals
                            SET decision = 'expired',
                                expired_at = ?
                            WHERE operation_key = ?;
                            """,
                            (now, clean_op),
                        )
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_APPROVAL_EXPIRED,
                            outcome="expired",
                            run_id=existing["run_id"],
                            actor=clean_approver,
                            metadata={
                                "tool_name": existing["tool_name"],
                                "operation_key": clean_op,
                                "reason": f"Expired inline at {existing['expires_at']}",
                                "expires_at": existing["expires_at"],
                                "expired_at": now,
                            },
                            store=self,
                            dedup=True,
                            conn=conn,
                            raise_on_error=True,
                        )
                        self.insert_outbox_event(
                            event_type="tool.approval_expired",
                            payload={
                                "operation_key": clean_op,
                                "run_id": existing["run_id"],
                                "tool_name": existing["tool_name"],
                                "reason": f"Expired inline at {existing['expires_at']}",
                                "expires_at": existing["expires_at"],
                                "expired_at": now,
                            },
                            idempotency_key=(
                                f"outbox:tool.approval_expired:{clean_op}:{existing['expires_at']}"
                            ),
                            conn=conn,
                            raise_on_error=True,
                        )
                        conn.commit()
                        raise ToolApprovalConflictError(
                            f"Operation '{clean_op}' approval window has expired "
                            f"(expired at: {existing['expires_at']}) and cannot be approved. "
                            "Operational recovery is required to reset to pending before re-approval."
                        )

                    if existing_decision == "pending":
                        final_expires_at = clean_expires_at or existing["expires_at"]
                        conn.execute(
                            """
                            UPDATE tool_approvals
                            SET approver = ?,
                                decision = ?,
                                reason = ?,
                                created_at = ?,
                                argument_fingerprint = COALESCE(?, argument_fingerprint),
                                risk_level = COALESCE(?, risk_level),
                                policy_hash = COALESCE(?, policy_hash),
                                expires_at = COALESCE(?, expires_at)
                            WHERE operation_key = ?;
                            """,
                            (
                                clean_approver,
                                clean_decision,
                                clean_reason,
                                now,
                                clean_fp,
                                clean_risk,
                                clean_phash,
                                final_expires_at,
                                clean_op,
                            ),
                        )
                        from ..observability.audit import AuditEventType, record_audit_event

                        event_type = (
                            AuditEventType.TOOL_APPROVED
                            if clean_decision == "approved"
                            else AuditEventType.TOOL_POLICY_DENIED
                        )
                        audit_ev = record_audit_event(
                            event_type=event_type,
                            outcome=clean_decision,
                            run_id=existing["run_id"],
                            actor=clean_approver,
                            metadata={
                                "operation_key": clean_op,
                                "tool_name": clean_tool or existing["tool_name"],
                                "decision": clean_decision,
                                "reason": clean_reason,
                                "idempotent": False,
                                "argument_fingerprint": clean_fp or existing["argument_fingerprint"],
                                "risk_level": clean_risk or existing["risk_level"],
                            },
                            store=self,
                            dedup=True,
                            conn=conn,
                            raise_on_error=True,
                        )
                        self.insert_outbox_event(
                            event_type=(
                                "tool.approval_granted"
                                if clean_decision == "approved"
                                else "tool.approval_rejected"
                            ),
                            payload={
                                "operation_key": clean_op,
                                "run_id": existing["run_id"],
                                "tool_name": clean_tool or existing["tool_name"],
                                "decision": clean_decision,
                                "approver": clean_approver,
                                "reason": clean_reason,
                                "argument_fingerprint": clean_fp or existing["argument_fingerprint"],
                                "risk_level": clean_risk or existing["risk_level"],
                            },
                            idempotency_key=(
                                f"outbox:tool.approval_{clean_decision}:{clean_op}:{clean_approver}"
                            ),
                            conn=conn,
                            raise_on_error=True,
                        )
                        conn.commit()
                        return {
                            "operation_key": clean_op,
                            "run_id": existing["run_id"],
                            "tool_name": clean_tool or existing["tool_name"],
                            "approver": clean_approver,
                            "decision": clean_decision,
                            "reason": clean_reason,
                            "created_at": now,
                            "argument_fingerprint": clean_fp or existing["argument_fingerprint"],
                            "risk_level": clean_risk or existing["risk_level"],
                            "policy_hash": clean_phash or existing["policy_hash"],
                            "revoked_by": None,
                            "revocation_reason": None,
                            "revoked_at": None,
                            "recovered_by": existing["recovered_by"],
                            "expires_at": final_expires_at,
                            "expired_at": None,
                            "escalated_at": existing["escalated_at"],
                            "escalation_reason": existing["escalation_reason"],
                            "idempotent": False,
                            "audit_event_id": audit_ev.event_id if audit_ev else None,
                        }

                    raise ToolApprovalConflictError(
                        f"Conflicting approval decision for operation '{clean_op}': "
                        f"existing decision is '{existing_decision}', cannot change to '{clean_decision}'."
                    )

                conn.execute(
                    """
                    INSERT INTO tool_approvals (
                        operation_key, run_id, tool_name, approver, decision, reason, created_at,
                        argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                        revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL);
                    """,
                    (
                        clean_op,
                        clean_run,
                        clean_tool,
                        clean_approver,
                        clean_decision,
                        clean_reason,
                        now,
                        clean_fp,
                        clean_risk,
                        clean_phash,
                        clean_expires_at,
                    ),
                )
                from ..observability.audit import AuditEventType, record_audit_event

                event_type = (
                    AuditEventType.TOOL_APPROVED
                    if clean_decision == "approved"
                    else AuditEventType.TOOL_POLICY_DENIED
                )
                audit_ev = record_audit_event(
                    event_type=event_type,
                    outcome=clean_decision,
                    run_id=clean_run,
                    actor=clean_approver,
                    metadata={
                        "operation_key": clean_op,
                        "tool_name": clean_tool,
                        "decision": clean_decision,
                        "reason": clean_reason,
                        "idempotent": False,
                        "argument_fingerprint": clean_fp,
                        "risk_level": clean_risk,
                    },
                    store=self,
                    dedup=True,
                    conn=conn,
                    raise_on_error=True,
                )
                self.insert_outbox_event(
                    event_type=(
                        "tool.approval_granted"
                        if clean_decision == "approved"
                        else "tool.approval_rejected"
                    ),
                    payload={
                        "operation_key": clean_op,
                        "run_id": clean_run,
                        "tool_name": clean_tool,
                        "decision": clean_decision,
                        "approver": clean_approver,
                        "reason": clean_reason,
                        "argument_fingerprint": clean_fp,
                        "risk_level": clean_risk,
                    },
                    idempotency_key=f"outbox:tool.approval_{clean_decision}:{clean_op}:{clean_approver}",
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()
                return {
                    "operation_key": clean_op,
                    "run_id": clean_run,
                    "tool_name": clean_tool,
                    "approver": clean_approver,
                    "decision": clean_decision,
                    "reason": clean_reason,
                    "created_at": now,
                    "argument_fingerprint": clean_fp,
                    "risk_level": clean_risk,
                    "policy_hash": clean_phash,
                    "revoked_by": None,
                    "revocation_reason": None,
                    "revoked_at": None,
                    "recovered_by": None,
                    "expires_at": clean_expires_at,
                    "expired_at": None,
                    "escalated_at": None,
                    "escalation_reason": None,
                    "idempotent": False,
                    "audit_event_id": audit_ev.event_id if audit_ev else None,
                }
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def revoke_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        revoked_by: str,
        reason: str = "",
    ) -> dict[str, Any]:
        """Revoke a previously recorded tool approval decision."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_revoked_by = str(revoked_by or "").strip()
        clean_reason = str(reason or "").strip()

        if not clean_op:
            raise ValueError("operation_key must not be empty.")
        if not clean_revoked_by:
            raise ValueError("revoked_by identity is required.")
        if not clean_reason:
            raise ValueError("revocation reason is required.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()

                if not existing:
                    raise ToolApprovalNotFoundError(
                        f"No approval record found for operation key '{clean_op}' to revoke."
                    )

                if clean_run and existing["run_id"] != clean_run:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' does not belong to run '{clean_run}'."
                    )
                if clean_tool and existing["tool_name"] != clean_tool:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' belongs to tool '{existing['tool_name']}', "
                        f"not '{clean_tool}'."
                    )

                existing_decision = str(existing["decision"] or "").lower()
                if existing_decision == "revoked":
                    return {
                        "operation_key": existing["operation_key"],
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "approver": existing["approver"],
                        "decision": "revoked",
                        "reason": existing["reason"] or "",
                        "created_at": existing["created_at"],
                        "argument_fingerprint": existing["argument_fingerprint"],
                        "risk_level": existing["risk_level"],
                        "policy_hash": existing["policy_hash"],
                        "revoked_by": existing["revoked_by"],
                        "revocation_reason": existing["revocation_reason"],
                        "revoked_at": existing["revoked_at"],
                        "idempotent": True,
                    }

                if existing_decision == "rejected":
                    raise ToolApprovalConflictError(
                        f"Cannot revoke operation '{clean_op}': operation is rejected, not approved."
                    )

                conn.execute(
                    """
                    UPDATE tool_approvals
                    SET decision = 'revoked',
                        revoked_by = ?,
                        revocation_reason = ?,
                        revoked_at = ?
                    WHERE operation_key = ?;
                    """,
                    (clean_revoked_by, clean_reason, now, clean_op),
                )
                from ..observability.audit import AuditEventType, record_audit_event

                audit_ev = record_audit_event(
                    event_type=AuditEventType.TOOL_APPROVAL_REVOKED,
                    outcome="revoked",
                    run_id=existing["run_id"],
                    actor=clean_revoked_by,
                    metadata={
                        "operation_key": clean_op,
                        "tool_name": clean_tool or existing["tool_name"],
                        "reason": clean_reason,
                        "idempotent": False,
                        "previous_decision": existing_decision,
                    },
                    store=self,
                    dedup=True,
                    conn=conn,
                    raise_on_error=True,
                )
                self.insert_outbox_event(
                    event_type="tool.approval_revoked",
                    payload={
                        "operation_key": clean_op,
                        "run_id": existing["run_id"],
                        "tool_name": clean_tool or existing["tool_name"],
                        "revoked_by": clean_revoked_by,
                        "reason": clean_reason,
                        "revoked_at": now,
                        "previous_decision": existing_decision,
                    },
                    idempotency_key=f"outbox:tool.approval_revoked:{clean_op}:{clean_revoked_by}",
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()
                return {
                    "operation_key": existing["operation_key"],
                    "run_id": existing["run_id"],
                    "tool_name": existing["tool_name"],
                    "approver": existing["approver"],
                    "decision": "revoked",
                    "reason": existing["reason"] or "",
                    "created_at": existing["created_at"],
                    "argument_fingerprint": existing["argument_fingerprint"],
                    "risk_level": existing["risk_level"],
                    "policy_hash": existing["policy_hash"],
                    "revoked_by": clean_revoked_by,
                    "revocation_reason": clean_reason,
                    "revoked_at": now,
                    "idempotent": False,
                    "audit_event_id": audit_ev.event_id if audit_ev else None,
                }
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def get_tool_approval(
        self,
        operation_key_or_run_id: str,
        operation_key: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve an approval decision by operation key, optionally accepting run_id as first arg."""
        if not self.enabled:
            return None

        if operation_key is not None:
            clean_op = str(operation_key or "").strip()
        else:
            clean_op = str(operation_key_or_run_id or "").strip()
        if not clean_op:
            return None

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "operation_key": row["operation_key"],
                    "run_id": row["run_id"],
                    "tool_name": row["tool_name"],
                    "approver": row["approver"],
                    "decision": row["decision"],
                    "reason": row["reason"] or "",
                    "created_at": row["created_at"],
                    "argument_fingerprint": row["argument_fingerprint"],
                    "risk_level": row["risk_level"],
                    "policy_hash": row["policy_hash"],
                    "revoked_by": row["revoked_by"],
                    "revocation_reason": row["revocation_reason"],
                    "revoked_at": row["revoked_at"],
                    "recovered_by": row["recovered_by"] if "recovered_by" in row.keys() else None,
                    "expires_at": row["expires_at"] if "expires_at" in row.keys() else None,
                    "expired_at": row["expired_at"] if "expired_at" in row.keys() else None,
                    "escalated_at": row["escalated_at"] if "escalated_at" in row.keys() else None,
                    "escalation_reason": (
                        row["escalation_reason"] if "escalation_reason" in row.keys() else None
                    ),
                }
            except Exception as exc:
                logger.warning("Error fetching tool approval for key %s: %s", clean_op, exc)
                return None
            finally:
                if not self._is_memory:
                    conn.close()

    def list_tool_approvals(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List all tool approvals recorded for a given workflow run, optionally filtered."""
        if not self.enabled:
            return []

        clean_run = str(run_id or "").strip()
        if not clean_run:
            return []

        clean_status = str(status or "").strip().lower() if status else None
        clean_tool = str(tool_name or "").strip() if tool_name else None

        query = """
            SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                   argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                   revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
            FROM tool_approvals
            WHERE run_id = ?
        """
        params: list[Any] = [clean_run]
        if clean_status:
            query += " AND LOWER(decision) = ?"
            params.append(clean_status)
        if clean_tool:
            query += " AND tool_name = ?"
            params.append(clean_tool)

        query += " ORDER BY created_at ASC;"

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, tuple(params))
                results = []
                for row in cursor.fetchall():
                    results.append(
                        {
                            "operation_key": row["operation_key"],
                            "run_id": row["run_id"],
                            "tool_name": row["tool_name"],
                            "approver": row["approver"],
                            "decision": row["decision"],
                            "reason": row["reason"] or "",
                            "created_at": row["created_at"],
                            "argument_fingerprint": row["argument_fingerprint"],
                            "risk_level": row["risk_level"],
                            "policy_hash": row["policy_hash"],
                            "revoked_by": row["revoked_by"],
                            "revocation_reason": row["revocation_reason"],
                            "revoked_at": row["revoked_at"],
                            "recovered_by": (
                                row["recovered_by"] if "recovered_by" in row.keys() else None
                            ),
                            "expires_at": (
                                row["expires_at"] if "expires_at" in row.keys() else None
                            ),
                            "expired_at": (
                                row["expired_at"] if "expired_at" in row.keys() else None
                            ),
                            "escalated_at": (
                                row["escalated_at"] if "escalated_at" in row.keys() else None
                            ),
                            "escalation_reason": (
                                row["escalation_reason"]
                                if "escalation_reason" in row.keys()
                                else None
                            ),
                        }
                    )
                return results
            except Exception as exc:
                logger.warning("Error listing tool approvals for run %s: %s", clean_run, exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

    def record_tool_approval_request(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        reason: str = "",
        expires_at: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Record a pending tool approval request if no prior decision exists."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_fp = str(argument_fingerprint or "").strip() if argument_fingerprint else None
        clean_risk = str(risk_level or "").strip() if risk_level else None
        clean_phash = str(policy_hash or "").strip() if policy_hash else None
        clean_reason = str(reason or "").strip() or "Pending human approval"

        if not clean_run:
            raise ValueError("run_id must not be empty.")
        if not clean_op:
            raise ValueError("operation_key must not be empty.")
        if not clean_tool:
            raise ValueError("tool_name must not be empty.")

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now = now_dt.isoformat()

        if expires_at:
            clean_expires_at = str(expires_at).strip()
        else:
            if timeout_seconds is None:
                try:
                    from ..config import get_settings

                    timeout_seconds = get_settings().tool_approval_timeout_seconds
                except Exception:
                    timeout_seconds = 3600.0
            exp_dt = now_dt + datetime.timedelta(seconds=float(timeout_seconds))
            clean_expires_at = exp_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()
                if existing:
                    return {
                        "operation_key": existing["operation_key"],
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "approver": existing["approver"],
                        "decision": existing["decision"],
                        "reason": existing["reason"] or "",
                        "created_at": existing["created_at"],
                        "argument_fingerprint": existing["argument_fingerprint"],
                        "risk_level": existing["risk_level"],
                        "policy_hash": existing["policy_hash"],
                        "revoked_by": existing["revoked_by"],
                        "revocation_reason": existing["revocation_reason"],
                        "revoked_at": existing["revoked_at"],
                        "recovered_by": existing["recovered_by"],
                        "expires_at": existing["expires_at"],
                        "expired_at": existing["expired_at"],
                        "escalated_at": existing["escalated_at"],
                        "escalation_reason": existing["escalation_reason"],
                        "idempotent": True,
                    }

                conn.execute(
                    """
                    INSERT INTO tool_approvals (
                        operation_key, run_id, tool_name, approver, decision, reason, created_at,
                        argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                        revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    ) VALUES (
                        ?, ?, ?, '', 'pending', ?, ?, ?, ?, ?,
                        NULL, NULL, NULL, NULL, ?, NULL, NULL, NULL
                    );
                    """,
                    (
                        clean_op,
                        clean_run,
                        clean_tool,
                        clean_reason,
                        now,
                        clean_fp,
                        clean_risk,
                        clean_phash,
                        clean_expires_at,
                    ),
                )
                self.insert_outbox_event(
                    event_type="tool.approval_requested",
                    payload={
                        "operation_key": clean_op,
                        "run_id": clean_run,
                        "tool_name": clean_tool,
                        "reason": clean_reason,
                        "expires_at": clean_expires_at,
                        "argument_fingerprint": clean_fp,
                        "risk_level": clean_risk,
                    },
                    idempotency_key=f"outbox:tool.approval_requested:{clean_op}",
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()
                return {
                    "operation_key": clean_op,
                    "run_id": clean_run,
                    "tool_name": clean_tool,
                    "approver": "",
                    "decision": "pending",
                    "reason": clean_reason,
                    "created_at": now,
                    "argument_fingerprint": clean_fp,
                    "risk_level": clean_risk,
                    "policy_hash": clean_phash,
                    "revoked_by": None,
                    "revocation_reason": None,
                    "revoked_at": None,
                    "recovered_by": None,
                    "expires_at": clean_expires_at,
                    "expired_at": None,
                    "escalated_at": None,
                    "escalation_reason": None,
                    "idempotent": False,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def recover_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        recovered_by: str,
        reason: str,
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        expires_at: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Reset a blocked, stale, rejected, revoked, or expired approval back to 'pending'.

        Does NOT automatically approve or redispatch the operation.
        Requires explicit human review and approval before execution can proceed.
        """
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_recovered_by = str(recovered_by or "").strip()
        clean_reason = str(reason or "").strip()
        clean_fp = str(argument_fingerprint or "").strip() if argument_fingerprint else None
        clean_risk = str(risk_level or "").strip() if risk_level else None
        clean_phash = str(policy_hash or "").strip() if policy_hash else None

        if not clean_run:
            raise ValueError("run_id must not be empty.")
        if not clean_op:
            raise ValueError("operation_key must not be empty.")
        if not clean_tool:
            raise ValueError("tool_name must not be empty.")
        if not clean_recovered_by:
            raise ValueError("recovered_by operator identity is required.")
        if not clean_reason:
            raise ValueError("recovery reason is required and cannot be empty.")

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now = now_dt.isoformat()
        rec_reason = f"Operational recovery by {clean_recovered_by}: {clean_reason}"

        if expires_at:
            clean_expires_at = str(expires_at).strip()
        else:
            if timeout_seconds is None:
                try:
                    from ..config import get_settings

                    timeout_seconds = get_settings().tool_approval_timeout_seconds
                except Exception:
                    timeout_seconds = 3600.0
            exp_dt = now_dt + datetime.timedelta(seconds=float(timeout_seconds))
            clean_expires_at = exp_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()

                if existing:
                    if clean_run and existing["run_id"] != clean_run:
                        raise ToolApprovalNotFoundError(
                            f"Approval '{clean_op}' does not belong to run '{clean_run}'."
                        )

                    conn.execute(
                        """
                        UPDATE tool_approvals
                        SET approver = '',
                            decision = 'pending',
                            reason = ?,
                            created_at = ?,
                            argument_fingerprint = COALESCE(?, argument_fingerprint),
                            risk_level = COALESCE(?, risk_level),
                            policy_hash = COALESCE(?, policy_hash),
                            revoked_by = NULL,
                            revocation_reason = NULL,
                            revoked_at = NULL,
                            recovered_by = ?,
                            expires_at = ?,
                            expired_at = NULL,
                            escalated_at = NULL,
                            escalation_reason = NULL
                        WHERE operation_key = ?;
                        """,
                        (
                            rec_reason,
                            now,
                            clean_fp,
                            clean_risk,
                            clean_phash,
                            clean_recovered_by,
                            clean_expires_at,
                            clean_op,
                        ),
                    )
                    from ..observability.audit import AuditEventType, record_audit_event

                    audit_ev = record_audit_event(
                        event_type=AuditEventType.TOOL_APPROVAL_RECOVERED,
                        outcome="recovered",
                        run_id=existing["run_id"],
                        actor=clean_recovered_by,
                        metadata={
                            "operation_key": clean_op,
                            "tool_name": clean_tool or existing["tool_name"],
                            "reason": clean_reason,
                            "new_decision": "pending",
                            "previous_decision": existing["decision"],
                        },
                        store=self,
                        dedup=True,
                        conn=conn,
                        raise_on_error=True,
                    )
                    self.insert_outbox_event(
                        event_type="tool.approval_recovered",
                        payload={
                            "operation_key": clean_op,
                            "run_id": existing["run_id"],
                            "tool_name": clean_tool or existing["tool_name"],
                            "recovered_by": clean_recovered_by,
                            "reason": clean_reason,
                            "recovered_at": now,
                            "new_expires_at": clean_expires_at,
                            "previous_decision": existing["decision"],
                        },
                        idempotency_key=(
                            f"outbox:tool.approval_recovered:{clean_op}:{clean_recovered_by}:{now}"
                        ),
                        conn=conn,
                        raise_on_error=True,
                    )
                    conn.commit()
                    return {
                        "operation_key": clean_op,
                        "run_id": existing["run_id"],
                        "tool_name": clean_tool or existing["tool_name"],
                        "approver": "",
                        "decision": "pending",
                        "reason": rec_reason,
                        "created_at": now,
                        "argument_fingerprint": clean_fp or existing["argument_fingerprint"],
                        "risk_level": clean_risk or existing["risk_level"],
                        "policy_hash": clean_phash or existing["policy_hash"],
                        "recovered_by": clean_recovered_by,
                        "recovery_reason": clean_reason,
                        "revoked_by": None,
                        "revocation_reason": None,
                        "revoked_at": None,
                        "expires_at": clean_expires_at,
                        "expired_at": None,
                        "escalated_at": None,
                        "escalation_reason": None,
                        "idempotent": False,
                        "audit_event_id": audit_ev.event_id if audit_ev else None,
                    }

                # If no prior record existed, insert as pending
                conn.execute(
                    """
                    INSERT INTO tool_approvals (
                        operation_key, run_id, tool_name, approver, decision, reason, created_at,
                        argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                        revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    ) VALUES (
                        ?, ?, ?, '', 'pending', ?, ?, ?, ?, ?,
                        NULL, NULL, NULL, ?, ?, NULL, NULL, NULL
                    );
                    """,
                    (
                        clean_op,
                        clean_run,
                        clean_tool,
                        rec_reason,
                        now,
                        clean_fp,
                        clean_risk,
                        clean_phash,
                        clean_recovered_by,
                        clean_expires_at,
                    ),
                )
                from ..observability.audit import AuditEventType, record_audit_event

                audit_ev = record_audit_event(
                    event_type=AuditEventType.TOOL_APPROVAL_RECOVERED,
                    outcome="recovered",
                    run_id=clean_run,
                    actor=clean_recovered_by,
                    metadata={
                        "operation_key": clean_op,
                        "tool_name": clean_tool,
                        "reason": clean_reason,
                        "new_decision": "pending",
                    },
                    store=self,
                    dedup=True,
                    conn=conn,
                    raise_on_error=True,
                )
                self.insert_outbox_event(
                    event_type="tool.approval_recovered",
                    payload={
                        "operation_key": clean_op,
                        "run_id": clean_run,
                        "tool_name": clean_tool,
                        "recovered_by": clean_recovered_by,
                        "reason": clean_reason,
                        "recovered_at": now,
                        "new_expires_at": clean_expires_at,
                    },
                    idempotency_key=(
                        f"outbox:tool.approval_recovered:{clean_op}:{clean_recovered_by}:{now}"
                    ),
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()
                return {
                    "operation_key": clean_op,
                    "run_id": clean_run,
                    "tool_name": clean_tool,
                    "approver": "",
                    "decision": "pending",
                    "reason": rec_reason,
                    "created_at": now,
                    "argument_fingerprint": clean_fp,
                    "risk_level": clean_risk,
                    "policy_hash": clean_phash,
                    "recovered_by": clean_recovered_by,
                    "recovery_reason": clean_reason,
                    "revoked_by": None,
                    "revocation_reason": None,
                    "revoked_at": None,
                    "expires_at": clean_expires_at,
                    "expired_at": None,
                    "escalated_at": None,
                    "escalation_reason": None,
                    "idempotent": False,
                    "audit_event_id": audit_ev.event_id if audit_ev else None,
                }
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def expire_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Approval expired due to timeout",
        force: bool = False,
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Mark an expired or timed-out tool approval record as 'expired'."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_reason = str(reason or "").strip() or "Approval expired due to timeout"
        clean_actor = str(actor or "system").strip()

        if not clean_op:
            raise ValueError("operation_key must not be empty.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                # Revalidate scheduler lease fencing if expected_fence_token provided
                if expected_fence_token is not None or expected_owner_id is not None:
                    is_valid = self.validate_scheduler_lease_ownership(
                        lease_name=lease_name or "tool_approval_scheduler",
                        owner_id=expected_owner_id or "",
                        fence_token=expected_fence_token or 0,
                        now_dt=datetime.datetime.now(datetime.timezone.utc),
                    )
                    if not is_valid:
                        raise SchedulerFencingError(
                            f"Scheduler lease fencing validation failed for owner '{expected_owner_id}' "
                            f"with token {expected_fence_token}. Worker has been superseded or lease expired."
                        )

                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()
                if not existing:
                    raise ToolApprovalNotFoundError(
                        f"Tool approval not found for operation key: {clean_op}"
                    )

                if clean_run and existing["run_id"] != clean_run:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' does not belong to run '{clean_run}'."
                    )
                if clean_tool and existing["tool_name"] != clean_tool:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' belongs to tool '{existing['tool_name']}', "
                        f"not '{clean_tool}'."
                    )

                existing_decision = str(existing["decision"] or "").lower()
                if existing_decision == "expired":
                    return {
                        "operation_key": existing["operation_key"],
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "approver": existing["approver"],
                        "decision": "expired",
                        "reason": existing["reason"] or "",
                        "created_at": existing["created_at"],
                        "argument_fingerprint": existing["argument_fingerprint"],
                        "risk_level": existing["risk_level"],
                        "policy_hash": existing["policy_hash"],
                        "revoked_by": existing["revoked_by"],
                        "revocation_reason": existing["revocation_reason"],
                        "revoked_at": existing["revoked_at"],
                        "recovered_by": existing["recovered_by"],
                        "expires_at": existing["expires_at"],
                        "expired_at": existing["expired_at"] or now,
                        "escalated_at": existing["escalated_at"],
                        "escalation_reason": existing["escalation_reason"],
                        "idempotent": True,
                    }

                if existing_decision == "revoked":
                    raise ToolApprovalConflictError(
                        f"Cannot expire operation '{clean_op}': operation is revoked, "
                        "not pending or approved."
                    )
                if existing_decision == "rejected":
                    raise ToolApprovalConflictError(
                        f"Cannot expire operation '{clean_op}': operation is rejected, "
                        "not pending or approved."
                    )

                if not force and not is_approval_expired(dict(existing)):
                    raise ToolApprovalConflictError(
                        f"Cannot expire operation '{clean_op}': approval window has not elapsed "
                        f"(scheduled expiration: '{existing['expires_at']}'). Use force=True to override."
                    )

                conn.execute(
                    """
                    UPDATE tool_approvals
                    SET decision = 'expired',
                        expired_at = ?,
                        reason = ?
                    WHERE operation_key = ?;
                    """,
                    (now, clean_reason, clean_op),
                )
                from ..observability.audit import AuditEventType, record_audit_event

                audit_ev = record_audit_event(
                    event_type=AuditEventType.TOOL_APPROVAL_EXPIRED,
                    outcome="expired",
                    run_id=existing["run_id"],
                    actor=clean_actor,
                    metadata={
                        "tool_name": existing["tool_name"],
                        "operation_key": clean_op,
                        "reason": clean_reason,
                        "expires_at": existing["expires_at"],
                        "expired_at": now,
                        "previous_decision": existing_decision,
                    },
                    store=self,
                    dedup=True,
                    conn=conn,
                    raise_on_error=True,
                )
                self.insert_outbox_event(
                    event_type="tool.approval_expired",
                    payload={
                        "operation_key": clean_op,
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "reason": clean_reason,
                        "expires_at": existing["expires_at"],
                        "expired_at": now,
                    },
                    idempotency_key=(
                        f"outbox:tool.approval_expired:{clean_op}:{existing['expires_at'] or now}"
                    ),
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()

                return {
                    "operation_key": existing["operation_key"],
                    "run_id": existing["run_id"],
                    "tool_name": existing["tool_name"],
                    "approver": existing["approver"],
                    "decision": "expired",
                    "reason": clean_reason,
                    "created_at": existing["created_at"],
                    "argument_fingerprint": existing["argument_fingerprint"],
                    "risk_level": existing["risk_level"],
                    "policy_hash": existing["policy_hash"],
                    "revoked_by": existing["revoked_by"],
                    "revocation_reason": existing["revocation_reason"],
                    "revoked_at": existing["revoked_at"],
                    "recovered_by": existing["recovered_by"],
                    "expires_at": existing["expires_at"],
                    "expired_at": now,
                    "escalated_at": existing["escalated_at"],
                    "escalation_reason": existing["escalation_reason"],
                    "idempotent": False,
                    "audit_event_id": audit_ev.event_id if audit_ev else None,
                }
            except SchedulerFencingError:
                raise
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def escalate_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Pending approval overdue",
        escalation_level: str = "manager_alert",
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Record an escalation event for an overdue tool approval."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_run = str(run_id or "").strip()
        clean_op = str(operation_key or "").strip()
        clean_tool = str(tool_name or "").strip()
        clean_reason = str(reason or "").strip() or "Pending approval overdue"
        clean_level = str(escalation_level or "manager_alert").strip()
        clean_actor = str(actor or "system").strip()

        if not clean_op:
            raise ValueError("operation_key must not be empty.")

        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                # Revalidate scheduler lease fencing if expected_fence_token provided
                if expected_fence_token is not None or expected_owner_id is not None:
                    is_valid = self.validate_scheduler_lease_ownership(
                        lease_name=lease_name or "tool_approval_scheduler",
                        owner_id=expected_owner_id or "",
                        fence_token=expected_fence_token or 0,
                        now_dt=datetime.datetime.now(datetime.timezone.utc),
                    )
                    if not is_valid:
                        raise SchedulerFencingError(
                            f"Scheduler lease fencing validation failed for owner '{expected_owner_id}' "
                            f"with token {expected_fence_token}. Worker has been superseded or lease expired."
                        )

                cursor = conn.execute(
                    """
                    SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                           argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                           revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
                    FROM tool_approvals
                    WHERE operation_key = ?;
                    """,
                    (clean_op,),
                )
                existing = cursor.fetchone()
                if not existing:
                    raise ToolApprovalNotFoundError(
                        f"Tool approval not found for operation key: {clean_op}"
                    )

                if clean_run and existing["run_id"] != clean_run:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' does not belong to run '{clean_run}'."
                    )
                if clean_tool and existing["tool_name"] != clean_tool:
                    raise ToolApprovalNotFoundError(
                        f"Approval '{clean_op}' belongs to tool '{existing['tool_name']}', "
                        f"not '{clean_tool}'."
                    )

                if existing["escalated_at"]:
                    return {
                        "operation_key": existing["operation_key"],
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "decision": existing["decision"],
                        "escalated_at": existing["escalated_at"],
                        "escalation_reason": existing["escalation_reason"] or clean_reason,
                        "escalation_level": clean_level,
                        "idempotent": True,
                    }

                conn.execute(
                    """
                    UPDATE tool_approvals
                    SET escalated_at = ?,
                        escalation_reason = ?
                    WHERE operation_key = ?;
                    """,
                    (now, clean_reason, clean_op),
                )
                from ..observability.audit import AuditEventType, record_audit_event

                audit_ev = record_audit_event(
                    event_type=AuditEventType.TOOL_APPROVAL_ESCALATED,
                    outcome="escalated",
                    run_id=existing["run_id"],
                    actor=clean_actor,
                    metadata={
                        "tool_name": existing["tool_name"],
                        "operation_key": clean_op,
                        "reason": clean_reason,
                        "escalation_level": clean_level,
                        "expires_at": existing["expires_at"],
                        "decision": existing["decision"],
                    },
                    store=self,
                    dedup=True,
                    conn=conn,
                    raise_on_error=True,
                )
                self.insert_outbox_event(
                    event_type="tool.approval_escalated",
                    payload={
                        "operation_key": clean_op,
                        "run_id": existing["run_id"],
                        "tool_name": existing["tool_name"],
                        "reason": clean_reason,
                        "escalation_level": clean_level,
                        "expires_at": existing["expires_at"],
                        "escalated_at": now,
                    },
                    idempotency_key=f"outbox:tool.approval_escalated:{clean_op}",
                    conn=conn,
                    raise_on_error=True,
                )
                conn.commit()

                return {
                    "operation_key": existing["operation_key"],
                    "run_id": existing["run_id"],
                    "tool_name": existing["tool_name"],
                    "decision": existing["decision"],
                    "escalated_at": now,
                    "escalation_reason": clean_reason,
                    "escalation_level": clean_level,
                    "idempotent": False,
                    "audit_event_id": audit_ev.event_id if audit_ev else None,
                }
            except SchedulerFencingError:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_batch_scan;")
                    conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                except Exception:
                    pass
                raise
            except Exception:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_batch_scan;")
                    conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                except Exception:
                    pass
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def check_and_expire_tool_approvals(
        self,
        run_id: str | None = None,
        before_timestamp: str | None = None,
        auto_escalate: bool = True,
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Scan and transition overdue pending or active approvals to 'expired'."""
        if not self.enabled:
            return {
                "scanned_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "expired_count": 0,
                "escalated_count": 0,
                "expired_operation_keys": [],
                "escalated_operation_keys": [],
                "failed_count": 0,
                "failed_operation_keys": [],
                "partial_failures": [],
                "records": [],
            }

        clean_run = str(run_id or "").strip() if run_id else None
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        cutoff_str = before_timestamp.strip() if before_timestamp else now_dt.isoformat()
        now_str = now_dt.isoformat()

        query = """
            SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                   argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                   revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
            FROM tool_approvals
            WHERE decision IN ('pending', 'approved')
              AND expires_at IS NOT NULL
              AND expires_at <= ?
        """
        params: list[Any] = [cutoff_str]
        if clean_run:
            query += " AND run_id = ?"
            params.append(clean_run)

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            try:
                conn.isolation_level = None
                max_retries = 25
                for attempt in range(max_retries):
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                        break
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                        else:
                            raise CheckpointError(
                                f"Database locked while scanning expired approvals: {exc}"
                            ) from exc

                # 1. Upfront lease fencing validation if expected_fence_token provided
                if expected_fence_token is not None or expected_owner_id is not None:
                    is_valid = self.validate_scheduler_lease_ownership(
                        lease_name=lease_name or "tool_approval_scheduler",
                        owner_id=expected_owner_id or "",
                        fence_token=expected_fence_token or 0,
                        now_dt=now_dt,
                    )
                    if not is_valid:
                        conn.rollback()
                        raise SchedulerFencingError(
                            f"Scheduler lease fencing validation failed for owner '{expected_owner_id}' "
                            f"with token {expected_fence_token}. Worker has been superseded or lease expired."
                        )

                cursor = conn.execute(query, tuple(params))
                rows = cursor.fetchall()
                expired_records: list[dict[str, Any]] = []
                failed_records: list[dict[str, Any]] = []

                conn.execute("SAVEPOINT sp_batch_scan;")

                for idx, row in enumerate(rows):
                    op_key = row["operation_key"]
                    r_id = row["run_id"]
                    t_name = row["tool_name"]
                    exp_reason = (
                        f"Approval expired automatically on timeout (cutoff: {cutoff_str})"
                    )

                    esc_at = row["escalated_at"]
                    esc_reason = row["escalation_reason"]
                    if auto_escalate and not esc_at:
                        esc_at = now_str
                        esc_reason = "Expired on timeout"

                    sp_name = f"sp_expire_{idx}"
                    try:
                        conn.execute(f"SAVEPOINT {sp_name};")
                        conn.execute(
                            """
                            UPDATE tool_approvals
                            SET decision = 'expired',
                                expired_at = ?,
                                reason = ?,
                                escalated_at = COALESCE(escalated_at, ?),
                                escalation_reason = COALESCE(escalation_reason, ?)
                            WHERE operation_key = ?;
                            """,
                            (now_str, exp_reason, esc_at, esc_reason, op_key),
                        )
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_APPROVAL_EXPIRED,
                            outcome="expired",
                            run_id=r_id,
                            actor=actor,
                            metadata={
                                "tool_name": t_name,
                                "operation_key": op_key,
                                "reason": exp_reason,
                                "expires_at": row["expires_at"],
                                "expired_at": now_str,
                            },
                            store=self,
                            dedup=True,
                            conn=conn,
                            raise_on_error=True,
                        )
                        self.insert_outbox_event(
                            event_type="tool.approval_expired",
                            payload={
                                "operation_key": op_key,
                                "run_id": r_id,
                                "tool_name": t_name,
                                "reason": exp_reason,
                                "expires_at": row["expires_at"],
                                "expired_at": now_str,
                            },
                            idempotency_key=f"outbox:tool.approval_expired:{op_key}:{row['expires_at']}",
                            conn=conn,
                            raise_on_error=True,
                        )
                        if auto_escalate and not row["escalated_at"]:
                            record_audit_event(
                                event_type=AuditEventType.TOOL_APPROVAL_ESCALATED,
                                outcome="escalated",
                                run_id=r_id,
                                actor=actor,
                                metadata={
                                    "tool_name": t_name,
                                    "operation_key": op_key,
                                    "reason": "Expired on timeout",
                                    "escalation_level": "automatic_expiry",
                                },
                                store=self,
                                dedup=True,
                                conn=conn,
                                raise_on_error=True,
                            )
                            self.insert_outbox_event(
                                event_type="tool.approval_escalated",
                                payload={
                                    "operation_key": op_key,
                                    "run_id": r_id,
                                    "tool_name": t_name,
                                    "reason": "Expired on timeout",
                                    "escalation_level": "automatic_expiry",
                                    "escalated_at": now_str,
                                },
                                idempotency_key=f"outbox:tool.approval_escalated:{op_key}",
                                conn=conn,
                                raise_on_error=True,
                            )
                        conn.execute(f"RELEASE SAVEPOINT {sp_name};")
                        expired_records.append(
                            {
                                "operation_key": op_key,
                                "run_id": r_id,
                                "tool_name": t_name,
                                "decision": "expired",
                                "reason": exp_reason,
                                "created_at": row["created_at"],
                                "expires_at": row["expires_at"],
                                "expired_at": now_str,
                                "escalated_at": esc_at,
                                "escalation_reason": esc_reason,
                                "idempotent": False,
                                "was_already_escalated": bool(row["escalated_at"]),
                            }
                        )
                    except Exception as item_err:
                        try:
                            conn.execute(f"ROLLBACK TO SAVEPOINT {sp_name};")
                            conn.execute(f"RELEASE SAVEPOINT {sp_name};")
                        except Exception:
                            pass
                        clean_err = sanitize_state_for_persistence(str(item_err))
                        failed_records.append(
                            {
                                "operation_key": op_key,
                                "run_id": r_id,
                                "tool_name": t_name,
                                "error": str(clean_err),
                            }
                        )
                        logger.warning(
                            "Failed to expire approval '%s' during batch scan: %s",
                            op_key,
                            clean_err,
                        )

                # 2. Before committing batch, re-validate fencing if expected_fence_token provided
                if expected_fence_token is not None or expected_owner_id is not None:
                    is_valid = self.validate_scheduler_lease_ownership(
                        lease_name=lease_name or "tool_approval_scheduler",
                        owner_id=expected_owner_id or "",
                        fence_token=expected_fence_token or 0,
                        now_dt=datetime.datetime.now(datetime.timezone.utc),
                    )
                    if not is_valid:
                        # Stale worker was superseded while batch was in flight!
                        # Roll back entire transaction so zero mutations are committed
                        try:
                            conn.execute("ROLLBACK TO SAVEPOINT sp_batch_scan;")
                            conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                        except Exception:
                            pass
                        conn.rollback()
                        raise SchedulerFencingError(
                            f"Scheduler lease fencing validation failed before commit for owner "
                            f"'{expected_owner_id}' with token {expected_fence_token}. "
                            f"Worker has been superseded or lease expired."
                        )

                try:
                    conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                except Exception:
                    pass
                conn.commit()

                return {
                    "scanned_at": now_str,
                    "expired_count": len(expired_records),
                    "escalated_count": len(expired_records) if auto_escalate else 0,
                    "expired_operation_keys": [r["operation_key"] for r in expired_records],
                    "escalated_operation_keys": [
                        r["operation_key"] for r in expired_records if r.get("escalated_at")
                    ],
                    "failed_count": len(failed_records),
                    "failed_operation_keys": [f["operation_key"] for f in failed_records],
                    "partial_failures": failed_records,
                    "records": expired_records,
                }
            except SchedulerFencingError:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_batch_scan;")
                    conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                except Exception:
                    pass
                raise
            except Exception:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_batch_scan;")
                    conn.execute("RELEASE SAVEPOINT sp_batch_scan;")
                except Exception:
                    pass
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                if self._is_memory:
                    try:
                        conn.isolation_level = old_isolation
                    except Exception:
                        pass
                else:
                    conn.close()

    def check_overdue_approvals(
        self,
        run_id: str | None = None,
        escalation_threshold_seconds: float | None = None,
        now_timestamp: str | None = None,
    ) -> list[dict[str, Any]]:
        """Identify pending tool approvals approaching expiry or overdue that need escalation."""
        if not self.enabled:
            return []

        clean_run = str(run_id or "").strip() if run_id else None
        if escalation_threshold_seconds is None:
            try:
                from ..config import get_settings

                escalation_threshold_seconds = (
                    get_settings().tool_approval_escalation_threshold_seconds
                )
            except Exception:
                escalation_threshold_seconds = 1800.0

        if now_timestamp:
            try:
                now_dt = datetime.datetime.fromisoformat(now_timestamp.replace("Z", "+00:00"))
            except Exception:
                now_dt = datetime.datetime.now(datetime.timezone.utc)
        else:
            now_dt = datetime.datetime.now(datetime.timezone.utc)
        threshold_dt = now_dt + datetime.timedelta(
            seconds=float(escalation_threshold_seconds)
        )
        threshold_str = threshold_dt.isoformat()
        created_cutoff_dt = now_dt - datetime.timedelta(
            seconds=float(escalation_threshold_seconds)
        )
        created_cutoff_str = created_cutoff_dt.isoformat()

        query = """
            SELECT operation_key, run_id, tool_name, approver, decision, reason, created_at,
                   argument_fingerprint, risk_level, policy_hash, revoked_by, revocation_reason,
                   revoked_at, recovered_by, expires_at, expired_at, escalated_at, escalation_reason
            FROM tool_approvals
            WHERE decision = 'pending'
              AND (
                  (expires_at IS NOT NULL AND expires_at <= ?)
                  OR (created_at <= ?)
              )
              AND escalated_at IS NULL
        """
        params: list[Any] = [threshold_str, created_cutoff_str]
        if clean_run:
            query += " AND run_id = ?"
            params.append(clean_run)

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, tuple(params))
                results = []
                for row in cursor.fetchall():
                    results.append(
                        {
                            "operation_key": row["operation_key"],
                            "run_id": row["run_id"],
                            "tool_name": row["tool_name"],
                            "decision": row["decision"],
                            "created_at": row["created_at"],
                            "expires_at": row["expires_at"],
                            "argument_fingerprint": row["argument_fingerprint"],
                            "risk_level": row["risk_level"],
                            "policy_hash": row["policy_hash"],
                        }
                    )
                return results
            finally:
                if not self._is_memory:
                    conn.close()

    def verify_approval_audit_consistency(
        self,
        operation_key: str,
    ) -> dict[str, Any]:
        """Inspect and report whether an approval record has a consistent audit trail in SQLite.

        Safely handles legacy data: does not invent or synthesize historical events
        with artificial timestamps.
        """
        if not self.enabled:
            return {"enabled": False, "consistent": True}

        clean_op = str(operation_key or "").strip()
        if not clean_op:
            return {"consistent": False, "error": "operation_key must not be empty"}

        approval = self.get_tool_approval(clean_op)
        if not approval:
            return {"exists": False, "consistent": True, "operation_key": clean_op}

        events = self.get_audit_events(run_id=approval.get("run_id"))
        matching = [
            e for e in events
            if clean_op in str(e.get("metadata") or "") or clean_op in str(e.get("metadata_json") or "")
        ]

        decision = str(approval.get("decision") or "").lower()
        has_audit = len(matching) > 0
        is_legacy = bool(not has_audit and decision in ("approved", "rejected", "expired", "revoked"))

        return {
            "operation_key": clean_op,
            "run_id": approval.get("run_id"),
            "tool_name": approval.get("tool_name"),
            "decision": decision,
            "exists": True,
            "has_audit_event": has_audit,
            "audit_event_count": len(matching),
            "is_legacy_uncommitted_audit": is_legacy,
            "consistent": not is_legacy,
        }

    def acquire_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        lease_ttl_seconds: float = 30.0,
        now_dt: datetime.datetime | None = None,
    ) -> tuple[bool, int, dict[str, Any] | None]:
        """Atomically acquire or take over a named cross-process scheduler lease.

        Returns (acquired, fence_token, lease_record).
        - If acquired=True, fence_token is the active generation token.
        - If acquired=False, fence_token is the current owner's generation token.
        """
        if not self.enabled:
            return False, 0, None

        clean_lease = str(lease_name or "tool_approval_scheduler").strip()
        clean_owner = str(owner_id or "").strip()
        if not clean_lease or not clean_owner:
            return False, 0, None

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()
        ttl = max(0.1, float(lease_ttl_seconds))
        expires_iso = (now + datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            try:
                conn.isolation_level = None
                max_retries = 25
                for attempt in range(max_retries):
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                        break
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                        else:
                            raise CheckpointError(
                                f"Database locked while acquiring scheduler lease '{clean_lease}': {exc}"
                            ) from exc

                try:
                    cursor = conn.execute(
                        """
                        SELECT lease_name, owner_id, status, acquired_at,
                               heartbeat_at, expires_at, fence_token
                        FROM scheduler_leases
                        WHERE lease_name = ?;
                        """,
                        (clean_lease,),
                    )
                    existing = cursor.fetchone()

                    if not existing:
                        conn.execute(
                            """
                            INSERT INTO scheduler_leases (
                                lease_name, owner_id, status, acquired_at,
                                heartbeat_at, expires_at, fence_token
                            ) VALUES (?, ?, 'active', ?, ?, ?, 1);
                            """,
                            (clean_lease, clean_owner, now_iso, now_iso, expires_iso),
                        )
                        conn.execute("COMMIT;")
                        return True, 1, {
                            "lease_name": clean_lease,
                            "owner_id": clean_owner,
                            "status": "active",
                            "acquired_at": now_iso,
                            "heartbeat_at": now_iso,
                            "expires_at": expires_iso,
                            "fence_token": 1,
                        }

                    curr_owner = str(existing["owner_id"])
                    curr_status = str(existing["status"])
                    curr_expires = str(existing["expires_at"])
                    curr_token = int(existing["fence_token"])

                    if curr_owner == clean_owner and curr_status == "active":
                        conn.execute(
                            """
                            UPDATE scheduler_leases
                            SET heartbeat_at = ?, expires_at = ?
                            WHERE lease_name = ? AND owner_id = ? AND fence_token = ?;
                            """,
                            (now_iso, expires_iso, clean_lease, clean_owner, curr_token),
                        )
                        conn.execute("COMMIT;")
                        return True, curr_token, {
                            "lease_name": clean_lease,
                            "owner_id": clean_owner,
                            "status": "active",
                            "acquired_at": existing["acquired_at"],
                            "heartbeat_at": now_iso,
                            "expires_at": expires_iso,
                            "fence_token": curr_token,
                        }

                    is_expired = curr_expires <= now_iso or curr_status != "active"
                    if is_expired:
                        new_token = curr_token + 1
                        conn.execute(
                            """
                            UPDATE scheduler_leases
                            SET owner_id = ?, status = 'active', acquired_at = ?, heartbeat_at = ?,
                                expires_at = ?, fence_token = ?
                            WHERE lease_name = ? AND fence_token = ?;
                            """,
                            (
                                clean_owner,
                                now_iso,
                                now_iso,
                                expires_iso,
                                new_token,
                                clean_lease,
                                curr_token,
                            ),
                        )
                        conn.execute("COMMIT;")
                        return True, new_token, {
                            "lease_name": clean_lease,
                            "owner_id": clean_owner,
                            "status": "active",
                            "acquired_at": now_iso,
                            "heartbeat_at": now_iso,
                            "expires_at": expires_iso,
                            "fence_token": new_token,
                        }

                    conn.execute("ROLLBACK;")
                    return False, curr_token, {
                        "lease_name": clean_lease,
                        "owner_id": curr_owner,
                        "status": curr_status,
                        "acquired_at": existing["acquired_at"],
                        "heartbeat_at": existing["heartbeat_at"],
                        "expires_at": curr_expires,
                        "fence_token": curr_token,
                    }
                except Exception:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    raise
            finally:
                conn.isolation_level = old_isolation
                if not self._is_memory:
                    conn.close()

    def renew_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int = 1,
        lease_ttl_seconds: float = 30.0,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Renew active scheduler lease conditionally by owner_id and fence_token."""
        if not self.enabled:
            return False

        clean_lease = str(lease_name or "tool_approval_scheduler").strip()
        clean_owner = str(owner_id or "").strip()
        if not clean_lease or not clean_owner or fence_token is None:
            return False

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()
        ttl = max(0.1, float(lease_ttl_seconds))
        expires_iso = (now + datetime.timedelta(seconds=ttl)).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.execute(
                        """
                        UPDATE scheduler_leases
                        SET heartbeat_at = ?, expires_at = ?
                        WHERE lease_name = ? AND owner_id = ? AND fence_token = ? AND status = 'active';
                        """,
                        (now_iso, expires_iso, clean_lease, clean_owner, int(fence_token)),
                    )
                    return cur.rowcount > 0
            except Exception as exc:
                logger.debug(
                    "Failed to renew scheduler lease '%s' for owner '%s': %s",
                    clean_lease,
                    clean_owner,
                    exc,
                )
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def release_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int | None = None,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Gracefully release leadership of a named scheduler lease."""
        if not self.enabled:
            return True

        clean_lease = str(lease_name or "tool_approval_scheduler").strip()
        clean_owner = str(owner_id or "").strip()
        if not clean_lease or not clean_owner:
            return False

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    if fence_token is not None:
                        cur = conn.execute(
                            """
                            UPDATE scheduler_leases
                            SET status = 'released', expires_at = ?
                            WHERE lease_name = ? AND owner_id = ? AND fence_token = ?;
                            """,
                            (now_iso, clean_lease, clean_owner, int(fence_token)),
                        )
                    else:
                        cur = conn.execute(
                            """
                            UPDATE scheduler_leases
                            SET status = 'released', expires_at = ?
                            WHERE lease_name = ? AND owner_id = ?;
                            """,
                            (now_iso, clean_lease, clean_owner),
                        )
                    return cur.rowcount > 0
            except Exception as exc:
                logger.debug(
                    "Failed to release scheduler lease '%s' for owner '%s': %s",
                    clean_lease,
                    clean_owner,
                    exc,
                )
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def validate_scheduler_lease_ownership(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int | None = None,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Check whether the given owner and fence token currently hold a valid, unexpired lease."""
        if not self.enabled:
            return True

        clean_lease = str(lease_name or "tool_approval_scheduler").strip()
        clean_owner = str(owner_id or "").strip()
        if not clean_lease or not clean_owner:
            return False

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT owner_id, status, expires_at, fence_token
                    FROM scheduler_leases
                    WHERE lease_name = ?;
                    """,
                    (clean_lease,),
                )
                row = cursor.fetchone()
                if not row:
                    return False
                if str(row["owner_id"]) != clean_owner:
                    return False
                if str(row["status"]) != "active":
                    return False
                if str(row["expires_at"]) <= now_iso:
                    return False
                if fence_token is not None and int(row["fence_token"]) != int(fence_token):
                    return False
                return True
            except Exception as exc:
                logger.debug("Failed to validate scheduler lease '%s': %s", clean_lease, exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def get_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
    ) -> dict[str, Any] | None:
        """Return the current scheduler lease record or None if absent."""
        if not self.enabled:
            return None

        clean_lease = str(lease_name or "tool_approval_scheduler").strip()
        if not clean_lease:
            return None

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT lease_name, owner_id, status, acquired_at,
                           heartbeat_at, expires_at, fence_token
                    FROM scheduler_leases
                    WHERE lease_name = ?;
                    """,
                    (clean_lease,),
                )
                row = cursor.fetchone()
                if not row:
                    return None
                return {
                    "lease_name": row["lease_name"],
                    "owner_id": row["owner_id"],
                    "status": row["status"],
                    "acquired_at": row["acquired_at"],
                    "heartbeat_at": row["heartbeat_at"],
                    "expires_at": row["expires_at"],
                    "fence_token": int(row["fence_token"]),
                }
            except Exception as exc:
                logger.debug("Failed to get scheduler lease '%s': %s", clean_lease, exc)
                return None
            finally:
                if not self._is_memory:
                    conn.close()

    def insert_outbox_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        created_at: str | None = None,
        conn: sqlite3.Connection | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any]:
        """Atomically insert a pending notification event into the outbox."""
        if not self.enabled:
            return {}
        try:
            from ..config import get_settings

            if not getattr(get_settings(), "outbox_enabled", True):
                return {}
        except Exception:
            pass

        clean_type = str(event_type or "").strip()
        if not clean_type:
            raise ValueError("event_type cannot be empty.")

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_str = created_at or now_dt.isoformat()
        clean_idemp = str(idempotency_key).strip() if idempotency_key else None
        event_id = f"outbox-{uuid.uuid4().hex}"

        safe_payload = sanitize_state_for_persistence(payload)
        payload_json = json.dumps(safe_payload, sort_keys=True)

        def _do_insert(c: sqlite3.Connection) -> dict[str, Any]:
            if clean_idemp:
                cursor = c.execute(
                    """
                    SELECT event_id, event_type, payload, created_at, status, attempt_count,
                           next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                           delivered_at, idempotency_key, fence_token
                    FROM outbox_events
                    WHERE idempotency_key = ?;
                    """,
                    (clean_idemp,),
                )
                existing = cursor.fetchone()
                if existing:
                    return {
                        "event_id": existing["event_id"],
                        "event_type": existing["event_type"],
                        "payload": json.loads(existing["payload"]) if existing["payload"] else {},
                        "created_at": existing["created_at"],
                        "status": existing["status"],
                        "attempt_count": existing["attempt_count"],
                        "next_retry_at": existing["next_retry_at"],
                        "last_error": existing["last_error"],
                        "claimed_by": existing["claimed_by"],
                        "claimed_at": existing["claimed_at"],
                        "claim_expires_at": existing["claim_expires_at"],
                        "delivered_at": existing["delivered_at"],
                        "idempotency_key": existing["idempotency_key"],
                        "fence_token": existing["fence_token"],
                        "idempotent": True,
                    }

            c.execute(
                """
                INSERT INTO outbox_events (
                    event_id, event_type, payload, created_at, status, attempt_count,
                    next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                    delivered_at, idempotency_key, fence_token
                ) VALUES (
                    ?, ?, ?, ?, 'pending', 0,
                    ?, NULL, NULL, NULL, NULL,
                    NULL, ?, 1
                );
                """,
                (
                    event_id,
                    clean_type,
                    payload_json,
                    now_str,
                    now_str,
                    clean_idemp,
                ),
            )
            c.execute(
                """
                INSERT INTO outbox_delivery_history (
                    event_id, attempt_number, action, status, error_message,
                    claimed_by, recorded_at, operator, reason
                ) VALUES (?, 0, 'created', 'pending', NULL, NULL, ?, NULL, NULL);
                """,
                (event_id, now_str),
            )
            return {
                "event_id": event_id,
                "event_type": clean_type,
                "payload": safe_payload,
                "created_at": now_str,
                "status": "pending",
                "attempt_count": 0,
                "next_retry_at": now_str,
                "last_error": None,
                "claimed_by": None,
                "claimed_at": None,
                "claim_expires_at": None,
                "delivered_at": None,
                "idempotency_key": clean_idemp,
                "fence_token": 1,
                "retry_count": 0,
                "last_recovery_reason": None,
                "last_recovery_by": None,
                "last_recovery_at": None,
                "idempotent": False,
            }

        if conn is not None:
            try:
                return _do_insert(conn)
            except Exception as exc:
                if raise_on_error:
                    raise
                logger.warning("Failed to insert outbox event on active connection: %s", exc)
                return {}
        else:
            with self._get_db_lock(self.db_path):
                c = self._get_connection()
                try:
                    c.execute("BEGIN IMMEDIATE;")
                    res = _do_insert(c)
                    c.commit()
                    return res
                except Exception as exc:
                    try:
                        c.rollback()
                    except Exception:
                        pass
                    if raise_on_error:
                        raise
                    logger.warning("Failed to insert outbox event: %s", exc)
                    return {}
                finally:
                    if not self._is_memory:
                        c.close()

    def claim_outbox_events(
        self,
        owner_id: str,
        batch_size: int = 50,
        claim_ttl_seconds: float = 30.0,
        event_types: list[str] | None = None,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        """Safely claim eligible pending or expired outbox events across processes."""
        if not self.enabled:
            return []

        clean_owner = str(owner_id or "").strip()
        if not clean_owner:
            raise ValueError("owner_id cannot be empty when claiming outbox events.")

        now_dt = (
            datetime.datetime.fromisoformat(now.replace("Z", "+00:00"))
            if now
            else datetime.datetime.now(datetime.timezone.utc)
        )
        now_str = now or now_dt.isoformat()
        claim_exp_dt = now_dt + datetime.timedelta(seconds=float(claim_ttl_seconds))
        claim_exp_str = claim_exp_dt.isoformat()
        clean_limit = max(1, min(int(batch_size), 1000))

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                type_filter = ""
                params: list[Any] = [now_str, now_str]
                if event_types:
                    placeholders = ", ".join(["?"] * len(event_types))
                    type_filter = f" AND event_type IN ({placeholders})"
                    params.extend(event_types)

                query = f"""
                    SELECT event_id, event_type, payload, created_at, status, attempt_count,
                           next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                           delivered_at, idempotency_key, fence_token
                    FROM outbox_events
                    WHERE ((status = 'pending' AND next_retry_at <= ?)
                       OR (status = 'claimed' AND claim_expires_at <= ?)){type_filter}
                    ORDER BY next_retry_at ASC, created_at ASC
                    LIMIT ?;
                """
                params.append(clean_limit)

                cursor = conn.execute(query, params)
                rows = cursor.fetchall()

                claimed: list[dict[str, Any]] = []
                for row in rows:
                    ev_id = row["event_id"]
                    old_fence = int(row["fence_token"])
                    new_fence = old_fence + 1
                    new_attempts = int(row["attempt_count"]) + 1

                    upd = conn.execute(
                        """
                        UPDATE outbox_events
                        SET status = 'claimed',
                            claimed_by = ?,
                            claimed_at = ?,
                            claim_expires_at = ?,
                            attempt_count = ?,
                            fence_token = ?
                        WHERE event_id = ? AND fence_token = ?;
                        """,
                        (
                            clean_owner,
                            now_str,
                            claim_exp_str,
                            new_attempts,
                            new_fence,
                            ev_id,
                            old_fence,
                        ),
                    )
                    if upd.rowcount > 0:
                        conn.execute(
                            """
                            INSERT INTO outbox_delivery_history (
                                event_id, attempt_number, action, status, error_message,
                                claimed_by, recorded_at, operator, reason
                            ) VALUES (?, ?, 'claim', 'claimed', NULL, ?, ?, NULL, NULL);
                            """,
                            (ev_id, new_attempts, clean_owner, now_str),
                        )
                        claimed.append(
                            {
                                "event_id": ev_id,
                                "event_type": row["event_type"],
                                "payload": json.loads(row["payload"]) if row["payload"] else {},
                                "created_at": row["created_at"],
                                "status": "claimed",
                                "attempt_count": new_attempts,
                                "next_retry_at": row["next_retry_at"],
                                "last_error": row["last_error"],
                                "claimed_by": clean_owner,
                                "claimed_at": now_str,
                                "claim_expires_at": claim_exp_str,
                                "delivered_at": row["delivered_at"],
                                "idempotency_key": row["idempotency_key"],
                                "fence_token": new_fence,
                            }
                        )

                conn.commit()
                return claimed
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                logger.error("Failed to claim outbox events: %s", exc)
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def update_outbox_event_delivery(
        self,
        event_id: str,
        claimed_by: str,
        fence_token: int,
        success: bool,
        error: str | None = None,
        retryable: bool = True,
        max_attempts: int = 5,
        backoff_base: float = 2.0,
        backoff_max: float = 300.0,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Update outbox event delivery status with fencing token validation."""
        if not self.enabled:
            return {}

        clean_id = str(event_id or "").strip()
        clean_owner = str(claimed_by or "").strip()
        now_dt = (
            datetime.datetime.fromisoformat(now.replace("Z", "+00:00"))
            if now
            else datetime.datetime.now(datetime.timezone.utc)
        )
        now_str = now or now_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                cursor = conn.execute(
                    """
                    SELECT event_id, event_type, payload, created_at, status, attempt_count,
                           next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                           delivered_at, idempotency_key, fence_token
                    FROM outbox_events
                    WHERE event_id = ?;
                    """,
                    (clean_id,),
                )
                row = cursor.fetchone()
                if not row:
                    raise CheckpointNotFoundError(f"Outbox event '{clean_id}' not found.")

                current_owner = row["claimed_by"]
                current_fence = int(row["fence_token"])

                if current_owner != clean_owner or current_fence != int(fence_token):
                    conn.rollback()
                    raise StaleDispatcherError(
                        f"Outbox event '{clean_id}' claim is stale or superseded. "
                        f"Expected owner={clean_owner}, token={fence_token}; "
                        f"found owner={current_owner}, token={current_fence}."
                    )

                if success:
                    new_status = "delivered"
                    delivered_at_val = now_str
                    sanitized_err = None
                    next_retry_val = row["next_retry_at"]
                else:
                    from ..graph.supervisor import sanitize_error

                    sanitized_err = sanitize_error(error or "Delivery failed")
                    delivered_at_val = None
                    curr_attempts = int(row["attempt_count"])

                    if not retryable or curr_attempts >= int(max_attempts):
                        new_status = "dead_letter"
                        next_retry_val = now_str
                    else:
                        new_status = "pending"
                        exp_backoff = min(
                            float(backoff_max),
                            float(backoff_base) * (2 ** max(0, curr_attempts - 1)),
                        )
                        retry_dt = now_dt + datetime.timedelta(seconds=exp_backoff)
                        next_retry_val = retry_dt.isoformat()

                conn.execute(
                    """
                    UPDATE outbox_events
                    SET status = ?,
                        delivered_at = ?,
                        last_error = ?,
                        next_retry_at = ?,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        claim_expires_at = NULL
                    WHERE event_id = ? AND fence_token = ?;
                    """,
                    (
                        new_status,
                        delivered_at_val,
                        sanitized_err,
                        next_retry_val,
                        clean_id,
                        current_fence,
                    ),
                )
                action_type = (
                    "delivered"
                    if success
                    else ("dead_letter" if new_status == "dead_letter" else "failed")
                )
                conn.execute(
                    """
                    INSERT INTO outbox_delivery_history (
                        event_id, attempt_number, action, status, error_message,
                        claimed_by, recorded_at, operator, reason
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL);
                    """,
                    (
                        clean_id,
                        int(row["attempt_count"]),
                        action_type,
                        new_status,
                        sanitized_err,
                        clean_owner,
                        now_str,
                    ),
                )
                conn.commit()

                return {
                    "event_id": clean_id,
                    "event_type": row["event_type"],
                    "payload": json.loads(row["payload"]) if row["payload"] else {},
                    "created_at": row["created_at"],
                    "status": new_status,
                    "attempt_count": row["attempt_count"],
                    "next_retry_at": next_retry_val,
                    "last_error": sanitized_err,
                    "delivered_at": delivered_at_val,
                    "idempotency_key": row["idempotency_key"],
                    "fence_token": current_fence,
                }
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if isinstance(exc, (StaleDispatcherError, CheckpointNotFoundError)):
                    raise
                logger.error("Failed to update outbox event delivery: %s", exc)
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def retry_outbox_event(
        self,
        event_id: str,
        reset_attempts: bool = False,
        reason: str = "Operator manual recovery",
        operator: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Manually reset a failed or dead_letter outbox event back to pending for retry."""
        if not self.enabled:
            raise RuntimeError("Persistence is disabled")

        clean_id = str(event_id or "").strip()
        clean_reason = str(reason or "").strip() or "Operator manual recovery"
        clean_operator = str(operator or "").strip() if operator else None
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_str = now_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                conn.execute("BEGIN IMMEDIATE;")
                cursor = conn.execute(
                    """
                    SELECT event_id, event_type, payload, created_at, status, attempt_count,
                           next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                           delivered_at, idempotency_key, fence_token, retry_count,
                           last_recovery_reason, last_recovery_by, last_recovery_at
                    FROM outbox_events
                    WHERE event_id = ?;
                    """,
                    (clean_id,),
                )
                row = cursor.fetchone()
                if not row:
                    raise CheckpointNotFoundError(f"Outbox event '{clean_id}' not found.")

                if row["status"] == "claimed" and not force:
                    exp_at = row["claim_expires_at"]
                    if exp_at and exp_at > now_str:
                        raise ValueError(
                            f"Cannot recover outbox event '{clean_id}': active claim held by "
                            f"'{row['claimed_by']}' until {exp_at}."
                        )

                new_attempts = 0 if reset_attempts else int(row["attempt_count"])
                new_fence = int(row["fence_token"]) + 1
                curr_retries = int(row["retry_count"] or 0)
                new_retries = curr_retries + 1
                last_err_val = None if reset_attempts else row["last_error"]

                conn.execute(
                    """
                    INSERT INTO outbox_delivery_history (
                        event_id, attempt_number, action, status, error_message,
                        claimed_by, recorded_at, operator, reason
                    ) VALUES (?, ?, 'retried', 'pending', ?, NULL, ?, ?, ?);
                    """,
                    (
                        clean_id,
                        int(row["attempt_count"]),
                        row["last_error"],
                        now_str,
                        clean_operator,
                        clean_reason,
                    ),
                )

                conn.execute(
                    """
                    UPDATE outbox_events
                    SET status = 'pending',
                        next_retry_at = ?,
                        attempt_count = ?,
                        last_error = ?,
                        claimed_by = NULL,
                        claimed_at = NULL,
                        claim_expires_at = NULL,
                        delivered_at = NULL,
                        fence_token = ?,
                        retry_count = ?,
                        last_recovery_reason = ?,
                        last_recovery_by = ?,
                        last_recovery_at = ?
                    WHERE event_id = ?;
                    """,
                    (
                        now_str,
                        new_attempts,
                        last_err_val,
                        new_fence,
                        new_retries,
                        clean_reason,
                        clean_operator,
                        now_str,
                        clean_id,
                    ),
                )

                from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

                payload_obj = json.loads(row["payload"]) if row["payload"] else {}
                run_id = payload_obj.get("run_id")

                record_audit_event(
                    event_type=AuditEventType.OUTBOX_RETRY_REQUESTED,
                    outcome=AuditOutcome.SUCCESS,
                    run_id=run_id,
                    actor=clean_operator or "operator",
                    metadata={
                        "event_id": clean_id,
                        "event_type": row["event_type"],
                        "reason": clean_reason,
                        "previous_status": row["status"],
                        "previous_attempt_count": row["attempt_count"],
                        "reset_attempts": reset_attempts,
                        "retry_count": new_retries,
                        "previous_error": row["last_error"],
                    },
                    store=self,
                    conn=conn,
                    raise_on_error=True,
                )

                conn.commit()

                return {
                    "event_id": clean_id,
                    "event_type": row["event_type"],
                    "payload": payload_obj,
                    "created_at": row["created_at"],
                    "status": "pending",
                    "attempt_count": new_attempts,
                    "next_retry_at": now_str,
                    "last_error": last_err_val,
                    "delivered_at": None,
                    "idempotency_key": row["idempotency_key"],
                    "fence_token": new_fence,
                    "retry_count": new_retries,
                    "last_recovery_reason": clean_reason,
                    "last_recovery_by": clean_operator,
                    "last_recovery_at": now_str,
                }
            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass
                if isinstance(exc, CheckpointNotFoundError):
                    raise
                logger.error("Failed to retry outbox event %s: %s", clean_id, exc)
                raise
            finally:
                if not self._is_memory:
                    conn.close()

    def recover_outbox_events(
        self,
        reason: str,
        event_ids: list[str] | None = None,
        status_filter: str | None = "dead_letter",
        operator: str | None = None,
        reset_attempts: bool = True,
        max_events: int = 50,
    ) -> dict[str, Any]:
        """Batch recover eligible outbox events with mandatory reason and audit logging."""
        if not self.enabled:
            return {
                "recovered_count": 0,
                "skipped_count": 0,
                "recovered_events": [],
                "reason": reason,
                "operator": operator or "operator",
            }

        clean_reason = str(reason or "").strip()
        if not clean_reason:
            raise ValueError("A non-empty recovery reason must be provided.")

        clean_limit = max(1, min(int(max_events), 500))
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                if event_ids:
                    placeholders = ", ".join(["?"] * len(event_ids))
                    query = f"""
                        SELECT event_id, status, claim_expires_at, claimed_by
                        FROM outbox_events
                        WHERE event_id IN ({placeholders})
                        LIMIT ?;
                    """
                    params = list(event_ids) + [clean_limit]
                else:
                    target_status = str(status_filter or "dead_letter").strip()
                    query = """
                        SELECT event_id, status, claim_expires_at, claimed_by
                        FROM outbox_events
                        WHERE status = ?
                        ORDER BY created_at ASC
                        LIMIT ?;
                    """
                    params = [target_status, clean_limit]

                cursor = conn.execute(query, params)
                candidates = cursor.fetchall()
            finally:
                if not self._is_memory:
                    conn.close()

        recovered_events: list[dict[str, Any]] = []
        skipped_count = 0

        for cand in candidates:
            ev_id = cand["event_id"]
            ev_status = cand["status"]
            exp_at = cand["claim_expires_at"]
            if ev_status == "claimed" and exp_at and exp_at > now_str:
                skipped_count += 1
                continue
            try:
                rec = self.retry_outbox_event(
                    event_id=ev_id,
                    reset_attempts=reset_attempts,
                    reason=clean_reason,
                    operator=operator,
                    force=False,
                )
                recovered_events.append(rec)
            except Exception as exc:
                logger.warning("Failed to recover event %s: %s", ev_id, exc)
                skipped_count += 1

        return {
            "recovered_count": len(recovered_events),
            "skipped_count": skipped_count,
            "recovered_events": recovered_events,
            "reason": clean_reason,
            "operator": operator or "operator",
        }

    def get_outbox_event_history(
        self,
        event_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Retrieve chronological delivery and recovery history for an outbox event."""
        if not self.enabled:
            return []

        clean_id = str(event_id or "").strip()
        clean_limit = max(1, min(int(limit), 500))

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT id, event_id, attempt_number, action, status,
                           error_message, claimed_by, recorded_at, operator, reason
                    FROM outbox_delivery_history
                    WHERE event_id = ?
                    ORDER BY id ASC
                    LIMIT ?;
                    """,
                    (clean_id, clean_limit),
                )
                from ..graph.supervisor import sanitize_error

                rows = cursor.fetchall()
                results: list[dict[str, Any]] = []
                for row in rows:
                    raw_err = row["error_message"]
                    results.append(
                        {
                            "id": row["id"],
                            "event_id": row["event_id"],
                            "attempt_number": row["attempt_number"],
                            "action": row["action"],
                            "status": row["status"],
                            "error_message": sanitize_error(raw_err) if raw_err else None,
                            "claimed_by": row["claimed_by"],
                            "recorded_at": row["recorded_at"],
                            "operator": row["operator"],
                            "reason": row["reason"],
                        }
                    )
                return results
            finally:
                if not self._is_memory:
                    conn.close()

    def get_outbox_events(
        self,
        status: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Query outbox events with optional status and event_type filters."""
        if not self.enabled:
            return []

        clean_limit = max(1, min(int(limit), 500))
        clean_offset = max(0, int(offset))
        clauses: list[str] = []
        params: list[Any] = []

        if status:
            clauses.append("status = ?")
            params.append(str(status).strip())
        if event_type:
            clauses.append("event_type = ?")
            params.append(str(event_type).strip())

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        query = f"""
            SELECT event_id, event_type, payload, created_at, status, attempt_count,
                   next_retry_at, last_error, claimed_by, claimed_at, claim_expires_at,
                   delivered_at, idempotency_key, fence_token, retry_count,
                   last_recovery_reason, last_recovery_by, last_recovery_at
            FROM outbox_events
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?;
        """
        params.extend([clean_limit, clean_offset])

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(query, params)
                rows = cursor.fetchall()
                results: list[dict[str, Any]] = []
                for row in rows:
                    results.append(
                        {
                            "event_id": row["event_id"],
                            "event_type": row["event_type"],
                            "payload": json.loads(row["payload"]) if row["payload"] else {},
                            "created_at": row["created_at"],
                            "status": row["status"],
                            "attempt_count": row["attempt_count"],
                            "next_retry_at": row["next_retry_at"],
                            "last_error": row["last_error"],
                            "claimed_by": row["claimed_by"],
                            "claimed_at": row["claimed_at"],
                            "claim_expires_at": row["claim_expires_at"],
                            "delivered_at": row["delivered_at"],
                            "idempotency_key": row["idempotency_key"],
                            "fence_token": row["fence_token"],
                            "retry_count": row["retry_count"] or 0,
                            "last_recovery_reason": row["last_recovery_reason"],
                            "last_recovery_by": row["last_recovery_by"],
                            "last_recovery_at": row["last_recovery_at"],
                        }
                    )
                return results
            finally:
                if not self._is_memory:
                    conn.close()

    def get_outbox_diagnostics(self, max_attempts: int = 5) -> dict[str, Any]:
        """Return diagnostic metrics, durable status counts, and latency statistics."""
        if not self.enabled:
            return {"enabled": False}

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_str = now_dt.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cursor = conn.execute(
                    """
                    SELECT status, COUNT(*) as cnt
                    FROM outbox_events
                    GROUP BY status;
                    """
                )
                counts: dict[str, int] = {
                    "pending": 0,
                    "claimed": 0,
                    "delivered": 0,
                    "dead_letter": 0,
                }
                total = 0
                for row in cursor.fetchall():
                    s = row["status"]
                    c = row["cnt"]
                    counts[s] = c
                    total += c

                cursor = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(attempt_count), 0) as total_attempts,
                        COALESCE(SUM(retry_count), 0) as total_retries,
                        COUNT(CASE WHEN attempt_count > 1 OR retry_count > 0 THEN 1 END) as retried_events,
                        COUNT(CASE WHEN attempt_count >= 2 THEN 1 END) as repeated_failures
                    FROM outbox_events;
                    """
                )
                retry_row = cursor.fetchone()
                retry_metrics = {
                    "total_attempts": retry_row["total_attempts"] if retry_row else 0,
                    "total_retries": retry_row["total_retries"] if retry_row else 0,
                    "retried_events_count": retry_row["retried_events"] if retry_row else 0,
                    "repeated_failures_count": retry_row["repeated_failures"] if retry_row else 0,
                }

                cursor = conn.execute(
                    """
                    SELECT MIN(created_at) as oldest_pending
                    FROM outbox_events
                    WHERE status = 'pending';
                    """
                )
                oldest_row = cursor.fetchone()
                oldest_pending = oldest_row["oldest_pending"] if oldest_row else None
                oldest_pending_age_sec: float | None = None
                if oldest_pending:
                    try:
                        clean_oldest = oldest_pending.replace("Z", "+00:00")
                        oldest_dt = datetime.datetime.fromisoformat(clean_oldest)
                        oldest_pending_age_sec = round(
                            max(0.0, (now_dt - oldest_dt).total_seconds()), 2
                        )
                    except Exception:
                        pass

                cursor = conn.execute("SELECT MAX(created_at) as newest FROM outbox_events;")
                newest_row = cursor.fetchone()
                newest = newest_row["newest"] if newest_row else None

                cursor = conn.execute(
                    """
                    SELECT created_at, delivered_at
                    FROM outbox_events
                    WHERE status = 'delivered' AND delivered_at IS NOT NULL
                    ORDER BY delivered_at DESC
                    LIMIT 200;
                    """
                )
                latencies: list[float] = []
                for row in cursor.fetchall():
                    c_at = row["created_at"]
                    d_at = row["delivered_at"]
                    if c_at and d_at:
                        try:
                            c_dt = datetime.datetime.fromisoformat(c_at.replace("Z", "+00:00"))
                            d_dt = datetime.datetime.fromisoformat(d_at.replace("Z", "+00:00"))
                            lat = (d_dt - c_dt).total_seconds()
                            if lat >= 0:
                                latencies.append(lat)
                        except Exception:
                            pass

                delivery_latency: dict[str, Any] = {
                    "avg_latency_seconds": None,
                    "min_latency_seconds": None,
                    "max_latency_seconds": None,
                    "p95_latency_seconds": None,
                    "delivered_samples_count": len(latencies),
                }
                if latencies:
                    latencies.sort()
                    p95_idx = int(0.95 * len(latencies))
                    delivery_latency["avg_latency_seconds"] = round(sum(latencies) / len(latencies), 3)
                    delivery_latency["min_latency_seconds"] = round(min(latencies), 3)
                    delivery_latency["max_latency_seconds"] = round(max(latencies), 3)
                    delivery_latency["p95_latency_seconds"] = round(
                        latencies[min(p95_idx, len(latencies) - 1)], 3
                    )

                cursor = conn.execute(
                    """
                    SELECT event_id, event_type, status, attempt_count, last_error,
                           created_at, next_retry_at, claimed_by
                    FROM outbox_events
                    WHERE last_error IS NOT NULL OR status = 'dead_letter'
                    ORDER BY created_at DESC
                    LIMIT 10;
                    """
                )
                from ..graph.supervisor import sanitize_error

                recent_failures: list[dict[str, Any]] = []
                for row in cursor.fetchall():
                    raw_err = row["last_error"]
                    recent_failures.append(
                        {
                            "event_id": row["event_id"],
                            "event_type": row["event_type"],
                            "status": row["status"],
                            "attempt_count": row["attempt_count"],
                            "last_error": sanitize_error(raw_err) if raw_err else None,
                            "created_at": row["created_at"],
                            "next_retry_at": row["next_retry_at"],
                            "claimed_by": row["claimed_by"],
                        }
                    )

                cursor = conn.execute(
                    """
                    SELECT event_id, claimed_by, claimed_at, claim_expires_at, fence_token
                    FROM outbox_events
                    WHERE status = 'claimed' AND claim_expires_at <= ?
                    ORDER BY claim_expires_at ASC
                    LIMIT 10;
                    """,
                    (now_str,),
                )
                expired_claims: list[dict[str, Any]] = []
                for row in cursor.fetchall():
                    expired_claims.append(
                        {
                            "event_id": row["event_id"],
                            "claimed_by": row["claimed_by"],
                            "claimed_at": row["claimed_at"],
                            "claim_expires_at": row["claim_expires_at"],
                            "fence_token": row["fence_token"],
                        }
                    )
                cursor = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM outbox_events
                    WHERE status = 'claimed' AND claim_expires_at <= ?;
                    """,
                    (now_str,),
                )
                exp_cnt_row = cursor.fetchone()
                expired_claims_count = exp_cnt_row["cnt"] if exp_cnt_row else 0

                threshold = max(1, max_attempts - 1)
                cursor = conn.execute(
                    """
                    SELECT event_id, event_type, status, attempt_count, next_retry_at
                    FROM outbox_events
                    WHERE status = 'pending' AND attempt_count >= ?
                    ORDER BY attempt_count DESC
                    LIMIT 10;
                    """,
                    (threshold,),
                )
                approaching_items = [
                    {
                        "event_id": r["event_id"],
                        "event_type": r["event_type"],
                        "status": r["status"],
                        "attempt_count": r["attempt_count"],
                        "next_retry_at": r["next_retry_at"],
                    }
                    for r in cursor.fetchall()
                ]

                cursor = conn.execute(
                    """
                    SELECT event_id, event_type, status, attempt_count, last_error
                    FROM outbox_events
                    WHERE status = 'dead_letter' OR attempt_count >= ?
                    ORDER BY attempt_count DESC
                    LIMIT 10;
                    """,
                    (max_attempts,),
                )
                exceeded_items = [
                    {
                        "event_id": r["event_id"],
                        "event_type": r["event_type"],
                        "status": r["status"],
                        "attempt_count": r["attempt_count"],
                        "last_error": sanitize_error(r["last_error"]) if r["last_error"] else None,
                    }
                    for r in cursor.fetchall()
                ]

                retry_limit_diagnostics = {
                    "max_configured_attempts": max_attempts,
                    "approaching_limit_count": len(approaching_items),
                    "exceeded_limit_count": len(exceeded_items),
                    "events_approaching_limit": approaching_items,
                    "events_exceeded_limit": exceeded_items,
                }

                return {
                    "enabled": True,
                    "total_events": total,
                    "counts": counts,
                    "oldest_pending_at": oldest_pending,
                    "oldest_pending_age_seconds": oldest_pending_age_sec,
                    "newest_event_at": newest,
                    "retry_metrics": retry_metrics,
                    "delivery_latency": delivery_latency,
                    "recent_failures": recent_failures,
                    "expired_claims_count": expired_claims_count,
                    "expired_claims": expired_claims,
                    "retry_limit_diagnostics": retry_limit_diagnostics,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def record_webhook_replay(
        self,
        event_id: str,
        timestamp: int,
        expires_at: float,
        recorded_at: str | None = None,
        now: float | None = None,
    ) -> bool:
        """Atomically record a verified webhook event ID.

        Returns True if the record was inserted or an expired record was replaced.
        Returns False if a valid, non-expired replay record already exists.
        """
        current_ts = now if now is not None else time.time()
        rec_str = recorded_at or datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.execute(
                        """
                        INSERT INTO webhook_replay_records (
                            event_id, timestamp, expires_at, recorded_at
                        )
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(event_id) DO UPDATE SET
                            timestamp = excluded.timestamp,
                            expires_at = excluded.expires_at,
                            recorded_at = excluded.recorded_at
                        WHERE webhook_replay_records.expires_at < ?;
                        """,
                        (event_id, timestamp, expires_at, rec_str, current_ts),
                    )
                    return cur.rowcount > 0
            finally:
                if not self._is_memory:
                    conn.close()

    def cleanup_expired_webhook_replays(
        self,
        now: float | None = None,
        limit: int = 500,
        trigger_type: str = "scheduled",
        operator: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Prune expired webhook replay records up to bounded limit and log maintenance."""
        current_ts = now if now is not None else time.time()
        clean_limit = max(1, min(int(limit), 10000))
        start_mono = time.monotonic()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.execute(
                        """
                        DELETE FROM webhook_replay_records
                        WHERE rowid IN (
                            SELECT rowid FROM webhook_replay_records
                            WHERE expires_at < ?
                            LIMIT ?
                        );
                        """,
                        (current_ts, clean_limit),
                    )
                    deleted = int(cur.rowcount)
                    duration_ms = round((time.monotonic() - start_mono) * 1000.0, 3)
                    now_iso = datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat()
                    conn.execute(
                        """
                        INSERT INTO webhook_replay_cleanup_log (
                            cleaned_at, deleted_count, duration_ms,
                            trigger_type, operator, reason
                        )
                        VALUES (?, ?, ?, ?, ?, ?);
                        """,
                        (
                            now_iso,
                            deleted,
                            duration_ms,
                            str(trigger_type or "scheduled").strip(),
                            str(operator or "system").strip(),
                            str(reason or "").strip(),
                        ),
                    )
                    return deleted
            finally:
                if not self._is_memory:
                    conn.close()

    def get_webhook_replay_diagnostics(
        self,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Derive bounded diagnostics for replay records and maintenance history."""
        current_ts = now if now is not None else time.time()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT
                        COUNT(*) AS total_count,
                        SUM(CASE WHEN expires_at >= ? THEN 1 ELSE 0 END) AS active_count,
                        SUM(CASE WHEN expires_at < ? THEN 1 ELSE 0 END) AS expired_count,
                        MIN(timestamp) AS min_ts,
                        MAX(timestamp) AS max_ts,
                        MIN(recorded_at) AS min_rec,
                        MAX(recorded_at) AS max_rec
                    FROM webhook_replay_records;
                    """,
                    (current_ts, current_ts),
                )
                row = cur.fetchone()
                total = int(row["total_count"] or 0)
                active = int(row["active_count"] or 0)
                expired = int(row["expired_count"] or 0)
                oldest_ts = int(row["min_ts"]) if row["min_ts"] is not None else None
                newest_ts = int(row["max_ts"]) if row["max_ts"] is not None else None
                oldest_rec = str(row["min_rec"]) if row["min_rec"] is not None else None
                newest_rec = str(row["max_rec"]) if row["max_rec"] is not None else None

                cur_log = conn.execute(
                    """
                    SELECT cleaned_at, deleted_count, duration_ms,
                           trigger_type, operator, reason
                    FROM webhook_replay_cleanup_log
                    ORDER BY id DESC
                    LIMIT 1;
                    """
                )
                last_row = cur_log.fetchone()
                last_cleanup = None
                if last_row:
                    last_cleanup = {
                        "cleaned_at": str(last_row["cleaned_at"]),
                        "deleted_count": int(last_row["deleted_count"]),
                        "duration_ms": float(last_row["duration_ms"]),
                        "trigger_type": str(last_row["trigger_type"]),
                        "operator": str(last_row["operator"]),
                        "reason": str(last_row["reason"]),
                    }

                return {
                    "enabled": True,
                    "storage_backend": "sqlite",
                    "total_records": total,
                    "active_records": active,
                    "expired_records": expired,
                    "oldest_timestamp": oldest_ts,
                    "newest_timestamp": newest_ts,
                    "oldest_recorded_at": oldest_rec,
                    "newest_recorded_at": newest_rec,
                    "last_cleanup": last_cleanup,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def get_webhook_replay_cleanup_history(
        self,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return chronological history of replay maintenance operations."""
        clean_limit = max(1, min(int(limit), 500))
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT id, cleaned_at, deleted_count, duration_ms,
                           trigger_type, operator, reason
                    FROM webhook_replay_cleanup_log
                    ORDER BY id DESC
                    LIMIT ?;
                    """,
                    (clean_limit,),
                )
                return [
                    {
                        "id": int(r["id"]),
                        "cleaned_at": str(r["cleaned_at"]),
                        "deleted_count": int(r["deleted_count"]),
                        "duration_ms": float(r["duration_ms"]),
                        "trigger_type": str(r["trigger_type"]),
                        "operator": str(r["operator"]),
                        "reason": str(r["reason"]),
                    }
                    for r in cur.fetchall()
                ]
            finally:
                if not self._is_memory:
                    conn.close()

    def record_restore_drill(
        self,
        drill_id: str,
        started_at: str,
        completed_at: str | None,
        status: str,
        backup_path: str,
        backup_timestamp: str | None = None,
        backup_age_seconds: float = 0.0,
        duration_ms: float = 0.0,
        tables_verified: int = 0,
        records_verified: int = 0,
        queries_verified: int = 0,
        integrity_check: str = "ok",
        quick_check: str = "ok",
        foreign_key_check: str = "ok",
        error_text: str | None = None,
        operator: str | None = None,
        reason: str | None = None,
        fence_token: int | None = None,
        retention_limit: int = 50,
    ) -> bool:
        """Persist a restore drill execution record and prune expired history."""
        if not self.enabled:
            return False

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        INSERT INTO database_restore_drills (
                            drill_id, started_at, completed_at, status, backup_path,
                            backup_timestamp, backup_age_seconds, duration_ms,
                            tables_verified, records_verified, queries_verified,
                            integrity_check, quick_check, foreign_key_check,
                            error_text, operator, reason, fence_token
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            str(drill_id),
                            str(started_at),
                            str(completed_at) if completed_at else None,
                            str(status),
                            str(backup_path),
                            str(backup_timestamp) if backup_timestamp else None,
                            float(backup_age_seconds),
                            float(duration_ms),
                            int(tables_verified),
                            int(records_verified),
                            int(queries_verified),
                            str(integrity_check),
                            str(quick_check),
                            str(foreign_key_check),
                            str(error_text) if error_text else None,
                            str(operator or "system"),
                            str(reason or ""),
                            int(fence_token) if fence_token is not None else None,
                        ),
                    )
                    if retention_limit > 0:
                        conn.execute(
                            """
                            DELETE FROM database_restore_drills
                            WHERE id NOT IN (
                                SELECT id FROM database_restore_drills
                                ORDER BY id DESC
                                LIMIT ?
                            );
                            """,
                            (int(retention_limit),),
                        )
                    return True
            except Exception as exc:
                logger.warning("Failed to record database restore drill: %s", exc)
                return False
            finally:
                if not self._is_memory:
                    conn.close()

    def list_restore_drills(
        self,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return chronological history of database restore drill operations."""
        if not self.enabled:
            return []

        clean_limit = max(1, min(int(limit), 500))
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT drill_id, started_at, completed_at, status, backup_path,
                           backup_timestamp, backup_age_seconds, duration_ms,
                           tables_verified, records_verified, queries_verified,
                           integrity_check, quick_check, foreign_key_check,
                           error_text, operator, reason, fence_token
                    FROM database_restore_drills
                    ORDER BY id DESC
                    LIMIT ?;
                    """,
                    (clean_limit,),
                )
                return [dict(r) for r in cur.fetchall()]
            except Exception as exc:
                logger.debug("Error listing restore drills: %s", exc)
                return []
            finally:
                if not self._is_memory:
                    conn.close()

    def get_latest_restore_drill(self) -> dict[str, Any] | None:
        """Return the most recent database restore drill record."""
        drills = self.list_restore_drills(limit=1)
        return drills[0] if drills else None

    def get_webhook_replay_count(
        self,
        active_only: bool = True,
        now: float | None = None,
    ) -> int:
        """Return the count of webhook replay records."""
        current_ts = now if now is not None else time.time()
        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                if active_only:
                    cur = conn.execute(
                        "SELECT COUNT(*) FROM webhook_replay_records WHERE expires_at >= ?;",
                        (current_ts,),
                    )
                else:
                    cur = conn.execute("SELECT COUNT(*) FROM webhook_replay_records;")
                row = cur.fetchone()
                return int(row[0]) if row else 0
            finally:
                if not self._is_memory:
                    conn.close()

    def record_worker_heartbeat(
        self,
        worker_id: str,
        status: str = "active",
        active_tasks: int = 0,
        queues: str = "orchestrator_workflows",
        last_completed_at: str | None = None,
        last_failure_at: str | None = None,
        last_failure_reason: str | None = None,
        metadata: dict[str, Any] | None = None,
        now_iso: str | None = None,
    ) -> dict[str, Any]:
        """Record or update worker heartbeat telemetry in the database."""
        if not self.enabled:
            return {}

        clean_worker_id = str(worker_id or "").strip()
        if not clean_worker_id:
            raise ValueError("worker_id must not be empty.")

        clean_status = str(status or "active").strip().lower()
        if clean_status not in ("active", "stale", "offline"):
            clean_status = "active"

        now_str = now_iso or datetime.datetime.now(datetime.timezone.utc).isoformat()
        clean_tasks = max(0, int(active_tasks))
        clean_queues = str(queues or "orchestrator_workflows").strip()

        sanitized_failure = None
        if last_failure_reason:
            sanitized_failure = _SENSITIVE_KEYS.sub("[REDACTED]", str(last_failure_reason))[:500]

        has_meta = 1 if metadata is not None else 0
        meta_dict = sanitize_state_for_persistence(metadata or {})
        try:
            meta_json = json.dumps(meta_dict, default=str)
        except Exception:
            meta_json = "{}"

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    conn.execute(
                        """
                        INSERT INTO celery_worker_heartbeats (
                            worker_id, status, registered_at, last_heartbeat_at,
                            active_tasks, queues, last_completed_at, last_failure_at,
                            last_failure_reason, metadata_json
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(worker_id) DO UPDATE SET
                            status = excluded.status,
                            last_heartbeat_at = excluded.last_heartbeat_at,
                            active_tasks = excluded.active_tasks,
                            queues = excluded.queues,
                            last_completed_at = COALESCE(
                                excluded.last_completed_at,
                                celery_worker_heartbeats.last_completed_at
                            ),
                            last_failure_at = COALESCE(
                                excluded.last_failure_at,
                                celery_worker_heartbeats.last_failure_at
                            ),
                            last_failure_reason = COALESCE(
                                excluded.last_failure_reason,
                                celery_worker_heartbeats.last_failure_reason
                            ),
                            metadata_json = CASE
                                WHEN ? = 1 THEN excluded.metadata_json
                                ELSE celery_worker_heartbeats.metadata_json
                            END;
                        """,
                        (
                            clean_worker_id,
                            clean_status,
                            now_str,
                            now_str,
                            clean_tasks,
                            clean_queues,
                            last_completed_at,
                            last_failure_at,
                            sanitized_failure,
                            meta_json,
                            has_meta,
                        ),
                    )
                return {
                    "worker_id": clean_worker_id,
                    "status": clean_status,
                    "last_heartbeat_at": now_str,
                    "active_tasks": clean_tasks,
                    "queues": clean_queues,
                    "last_completed_at": last_completed_at,
                    "last_failure_at": last_failure_at,
                    "last_failure_reason": sanitized_failure,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def get_worker_heartbeat(
        self,
        worker_id: str,
        stale_threshold_seconds: float = 60.0,
        now_dt: datetime.datetime | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve telemetry record for a specific Celery worker."""
        if not self.enabled:
            return None

        clean_worker_id = str(worker_id or "").strip()
        if not clean_worker_id:
            return None

        threshold = max(1.0, float(stale_threshold_seconds))
        now = now_dt or datetime.datetime.now(datetime.timezone.utc)

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT worker_id, status, registered_at, last_heartbeat_at,
                           active_tasks, queues, last_completed_at, last_failure_at,
                           last_failure_reason, metadata_json
                    FROM celery_worker_heartbeats
                    WHERE worker_id = ?;
                    """,
                    (clean_worker_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None

                hb_iso = str(row["last_heartbeat_at"] or "")
                age_seconds = 0.0
                try:
                    hb_dt = datetime.datetime.fromisoformat(hb_iso)
                    if hb_dt.tzinfo is None:
                        hb_dt = hb_dt.replace(tzinfo=datetime.timezone.utc)
                    age_seconds = max(0.0, (now - hb_dt).total_seconds())
                except Exception:
                    pass

                raw_status = str(row["status"] or "active").lower()
                is_stale = raw_status == "active" and age_seconds > threshold
                effective_status = "stale" if is_stale else raw_status

                try:
                    meta = json.loads(row["metadata_json"] or "{}")
                except Exception:
                    meta = {}

                return {
                    "worker_id": row["worker_id"],
                    "status": effective_status,
                    "registered_at": row["registered_at"],
                    "last_heartbeat_at": row["last_heartbeat_at"],
                    "heartbeat_age_seconds": round(age_seconds, 2),
                    "is_stale": is_stale,
                    "active_tasks": int(row["active_tasks"] or 0),
                    "queues": str(row["queues"] or "orchestrator_workflows"),
                    "last_completed_at": row["last_completed_at"],
                    "last_failure_at": row["last_failure_at"],
                    "last_failure_reason": row["last_failure_reason"],
                    "metadata": meta,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def list_worker_heartbeats(
        self,
        stale_threshold_seconds: float = 60.0,
        now_dt: datetime.datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List worker heartbeat records ordered by most recent heartbeat."""
        if not self.enabled:
            return []

        threshold = max(1.0, float(stale_threshold_seconds))
        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        clean_limit = max(1, min(int(limit), 1000))

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT worker_id, status, registered_at, last_heartbeat_at,
                           active_tasks, queues, last_completed_at, last_failure_at,
                           last_failure_reason, metadata_json
                    FROM celery_worker_heartbeats
                    ORDER BY last_heartbeat_at DESC
                    LIMIT ?;
                    """,
                    (clean_limit,),
                )
                results: list[dict[str, Any]] = []
                for row in cur.fetchall():
                    hb_iso = str(row["last_heartbeat_at"] or "")
                    age_seconds = 0.0
                    try:
                        hb_dt = datetime.datetime.fromisoformat(hb_iso)
                        if hb_dt.tzinfo is None:
                            hb_dt = hb_dt.replace(tzinfo=datetime.timezone.utc)
                        age_seconds = max(0.0, (now - hb_dt).total_seconds())
                    except Exception:
                        pass

                    raw_status = str(row["status"] or "active").lower()
                    is_stale = raw_status == "active" and age_seconds > threshold
                    effective_status = "stale" if is_stale else raw_status

                    try:
                        meta = json.loads(row["metadata_json"] or "{}")
                    except Exception:
                        meta = {}

                    results.append(
                        {
                            "worker_id": row["worker_id"],
                            "status": effective_status,
                            "registered_at": row["registered_at"],
                            "last_heartbeat_at": row["last_heartbeat_at"],
                            "heartbeat_age_seconds": round(age_seconds, 2),
                            "is_stale": is_stale,
                            "active_tasks": int(row["active_tasks"] or 0),
                            "queues": str(row["queues"] or "orchestrator_workflows"),
                            "last_completed_at": row["last_completed_at"],
                            "last_failure_at": row["last_failure_at"],
                            "last_failure_reason": row["last_failure_reason"],
                            "metadata": meta,
                        }
                    )
                return results
            finally:
                if not self._is_memory:
                    conn.close()

    def prune_worker_heartbeats(self, offline_retention_days: int = 7) -> int:
        """Delete stale or offline worker heartbeat records older than the retention period."""
        if not self.enabled:
            return 0

        days = max(1, int(offline_retention_days))
        cutoff = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
        ).isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            try:
                with conn:
                    cur = conn.execute(
                        """
                        DELETE FROM celery_worker_heartbeats
                        WHERE status = 'offline' AND last_heartbeat_at < ?;
                        """,
                        (cutoff,),
                    )
                    return int(cur.rowcount)
            finally:
                if not self._is_memory:
                    conn.close()

    def get_celery_queue_metrics(
        self,
        stale_threshold_seconds: float = 60.0,
        now_dt: datetime.datetime | None = None,
    ) -> dict[str, Any]:
        """Derive durable operational metrics for Celery task queuing and worker fleet."""
        if not self.enabled:
            return {
                "queued_count": 0,
                "running_count": 0,
                "waiting_approval_count": 0,
                "completed_count": 0,
                "failed_count": 0,
                "retry_count": 0,
                "oldest_queued_age_seconds": 0.0,
                "active_worker_count": 0,
                "stale_worker_count": 0,
                "offline_worker_count": 0,
                "total_worker_count": 0,
                "recent_failures": [],
            }

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        workers = self.list_worker_heartbeats(
            stale_threshold_seconds=stale_threshold_seconds, now_dt=now
        )
        active_workers = sum(1 for w in workers if w["status"] == "active")
        stale_workers = sum(1 for w in workers if w["status"] == "stale")
        offline_workers = sum(1 for w in workers if w["status"] == "offline")

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cur = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued_cnt,
                        SUM(CASE WHEN status IN ('running', 'in_progress') THEN 1 ELSE 0 END) AS running_cnt,
                        SUM(CASE WHEN status = 'waiting_approval' THEN 1 ELSE 0 END) AS waiting_approval_cnt,
                        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_cnt,
                        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_cnt
                    FROM runs;
                    """
                )
                row = cur.fetchone()
                queued_cnt = int(row["queued_cnt"] or 0) if row else 0
                running_cnt = int(row["running_cnt"] or 0) if row else 0
                waiting_approval_cnt = int(row["waiting_approval_cnt"] or 0) if row else 0
                completed_cnt = int(row["completed_cnt"] or 0) if row else 0
                failed_cnt = int(row["failed_cnt"] or 0) if row else 0

                cur_oldest = conn.execute(
                    "SELECT MIN(created_at) AS min_created FROM runs WHERE status = 'queued';"
                )
                oldest_row = cur_oldest.fetchone()
                oldest_queued_age = 0.0
                if oldest_row and oldest_row["min_created"]:
                    try:
                        c_dt = datetime.datetime.fromisoformat(str(oldest_row["min_created"]))
                        if c_dt.tzinfo is None:
                            c_dt = c_dt.replace(tzinfo=datetime.timezone.utc)
                        oldest_queued_age = max(0.0, (now - c_dt).total_seconds())
                    except Exception:
                        pass

                cur_retry = conn.execute(
                    "SELECT COUNT(*) AS retries FROM audit_events WHERE event_type = 'workflow.recovered';"
                )
                retry_row = cur_retry.fetchone()
                retry_count = int(retry_row["retries"] or 0) if retry_row else 0

                cur_failures = conn.execute(
                    """
                    SELECT r.run_id, r.task, r.updated_at, c.state_json
                    FROM runs r
                    LEFT JOIN checkpoints c ON r.run_id = c.run_id AND r.latest_step = c.step
                    WHERE r.status = 'failed'
                    ORDER BY r.updated_at DESC
                    LIMIT 10;
                    """
                )
                recent_failures: list[dict[str, Any]] = []
                for f_row in cur_failures.fetchall():
                    err_msg = ""
                    if f_row["state_json"]:
                        try:
                            s = json.loads(f_row["state_json"])
                            raw_err = s.get("error") or s.get("last_error") or ""
                            err_msg = _SENSITIVE_KEYS.sub("[REDACTED]", str(raw_err))[:300]
                        except Exception:
                            pass
                    recent_failures.append(
                        {
                            "run_id": f_row["run_id"],
                            "task": str(f_row["task"] or "")[:100],
                            "updated_at": f_row["updated_at"],
                            "error": err_msg or "Execution failed",
                        }
                    )

                return {
                    "queued_count": queued_cnt,
                    "running_count": running_cnt,
                    "waiting_approval_count": waiting_approval_cnt,
                    "completed_count": completed_cnt,
                    "failed_count": failed_cnt,
                    "retry_count": retry_count,
                    "oldest_queued_age_seconds": round(oldest_queued_age, 2),
                    "active_worker_count": active_workers,
                    "stale_worker_count": stale_workers,
                    "offline_worker_count": offline_workers,
                    "total_worker_count": len(workers),
                    "recent_failures": recent_failures,
                }
            finally:
                if not self._is_memory:
                    conn.close()

    def get_stuck_workflows(
        self,
        stuck_threshold_seconds: float = 300.0,
        stale_worker_threshold_seconds: float = 60.0,
        limit: int = 50,
        now_dt: datetime.datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Identify workflows stuck in queued or running states without forward progress."""
        if not self.enabled:
            return []

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()
        stuck_threshold = max(5.0, float(stuck_threshold_seconds))
        stale_threshold = max(1.0, float(stale_worker_threshold_seconds))
        clean_limit = max(1, min(int(limit), 500))

        with self._get_db_lock(self.db_path) if self._is_memory else nullcontext():
            conn = self._get_connection()
            try:
                cur_workers = conn.execute(
                    "SELECT worker_id, status, last_heartbeat_at FROM celery_worker_heartbeats;"
                )
                workers_map: dict[str, dict[str, Any]] = {}
                for w in cur_workers.fetchall():
                    hb_str = str(w["last_heartbeat_at"] or "")
                    w_age = 0.0
                    try:
                        w_dt = datetime.datetime.fromisoformat(hb_str)
                        if w_dt.tzinfo is None:
                            w_dt = w_dt.replace(tzinfo=datetime.timezone.utc)
                        w_age = max(0.0, (now - w_dt).total_seconds())
                    except Exception:
                        pass
                    w_status = str(w["status"] or "active").lower()
                    is_w_stale = w_status != "active" or w_age > stale_threshold
                    workers_map[w["worker_id"]] = {
                        "is_stale": is_w_stale,
                        "status": "stale" if is_w_stale else "active",
                    }

                cur_runs = conn.execute(
                    """
                    SELECT r.run_id, r.task, r.status, r.created_at, r.updated_at,
                           r.fence_token, a.owner_id, a.expires_at, a.fence_token AS adm_fence
                    FROM runs r
                    LEFT JOIN workflow_admissions a ON r.run_id = a.run_id
                    WHERE r.status IN ('queued', 'running', 'in_progress')
                    ORDER BY r.created_at ASC;
                    """
                )
                stuck_items: list[dict[str, Any]] = []
                for row in cur_runs.fetchall():
                    run_id = row["run_id"]
                    status = row["status"]
                    created_str = str(row["created_at"] or "")
                    age_seconds = 0.0
                    try:
                        c_dt = datetime.datetime.fromisoformat(created_str)
                        if c_dt.tzinfo is None:
                            c_dt = c_dt.replace(tzinfo=datetime.timezone.utc)
                        age_seconds = max(0.0, (now - c_dt).total_seconds())
                    except Exception:
                        pass

                    owner_id = row["owner_id"]
                    expires_str = str(row["expires_at"] or "")
                    fence_token = int(row["adm_fence"] or row["fence_token"] or 0)

                    stuck_reason = None
                    recovery_eligible = False

                    if status == "queued":
                        if age_seconds > stuck_threshold:
                            stuck_reason = "stuck_queued"
                            recovery_eligible = True
                    elif status in ("running", "in_progress"):
                        if not owner_id:
                            stuck_reason = "orphaned_lease"
                            recovery_eligible = True
                        else:
                            is_lease_expired = False
                            if expires_str:
                                is_lease_expired = expires_str < now_iso

                            worker_info = workers_map.get(owner_id)
                            worker_is_stale = worker_info["is_stale"] if worker_info else True

                            if is_lease_expired:
                                if worker_is_stale:
                                    stuck_reason = "expired_lease_dead_worker"
                                else:
                                    stuck_reason = "lease_expired"
                                recovery_eligible = True
                            else:
                                if age_seconds > stuck_threshold:
                                    stuck_reason = "deadline_exceeded"
                                    recovery_eligible = False

                    if stuck_reason is not None:
                        stuck_items.append(
                            {
                                "run_id": run_id,
                                "task": str(row["task"] or "")[:120],
                                "status": status,
                                "created_at": row["created_at"],
                                "updated_at": row["updated_at"],
                                "age_seconds": round(age_seconds, 2),
                                "owner_id": owner_id,
                                "fence_token": fence_token,
                                "stuck_reason": stuck_reason,
                                "recovery_eligible": recovery_eligible,
                            }
                        )
                        if len(stuck_items) >= clean_limit:
                            break

                return stuck_items
            finally:
                if not self._is_memory:
                    conn.close()

    def recover_stuck_workflow(
        self,
        run_id: str,
        operator: str,
        reason: str,
        action: str = "requeue",
        now_dt: datetime.datetime | None = None,
    ) -> dict[str, Any]:
        """Safely and idempotently recover an eligible stuck or interrupted workflow run."""
        if not self.enabled:
            raise RecoveryError("CheckpointStore is disabled.")

        clean_run_id = str(run_id or "").strip()
        if not clean_run_id:
            raise RecoveryError("run_id must not be empty.")

        clean_operator = str(operator or "").strip()
        if not clean_operator:
            raise RecoveryError("operator must not be empty.")

        clean_reason = str(reason or "").strip()
        if len(clean_reason) < 5:
            raise RecoveryError("Recovery reason must be at least 5 characters.")

        clean_action = str(action or "requeue").strip().lower()
        if clean_action not in ("requeue", "fail", "cancel"):
            raise RecoveryError(
                f"Invalid recovery action '{action}'. Must be 'requeue', 'fail', or 'cancel'."
            )

        now = now_dt or datetime.datetime.now(datetime.timezone.utc)
        now_iso = now.isoformat()

        with self._get_db_lock(self.db_path):
            conn = self._get_connection()
            old_isolation = conn.isolation_level
            try:
                conn.isolation_level = None
                max_retries = 25
                for attempt in range(max_retries):
                    try:
                        conn.execute("BEGIN IMMEDIATE;")
                        break
                    except sqlite3.OperationalError as exc:
                        is_busy = "locked" in str(exc).lower() or "busy" in str(exc).lower()
                        if is_busy and attempt < max_retries - 1:
                            time.sleep(0.01 * (attempt + 1))
                        else:
                            raise CheckpointError(
                                f"Database locked while recovering workflow {clean_run_id}: {exc}"
                            ) from exc

                try:
                    cur_run = conn.execute(
                        """
                        SELECT run_id, task, status, latest_step, schema_version, fence_token
                        FROM runs
                        WHERE run_id = ?;
                        """,
                        (clean_run_id,),
                    )
                    run_row = cur_run.fetchone()
                    if not run_row:
                        raise CheckpointNotFoundError(f"Run '{clean_run_id}' not found.")

                    prev_status = str(run_row["status"] or "unknown").lower()
                    if prev_status in ("completed", "cancelled"):
                        raise RecoveryError(
                            f"Cannot recover workflow '{clean_run_id}' in terminal status '{prev_status}'."
                        )

                    if prev_status == "waiting_approval" and clean_action == "requeue":
                        raise RecoveryError(
                            f"Workflow '{clean_run_id}' is waiting for human approval. "
                            "Cannot requeue without explicit approval decision."
                        )

                    cur_adm = conn.execute(
                        """
                        SELECT owner_id, expires_at, fence_token
                        FROM workflow_admissions
                        WHERE run_id = ?;
                        """,
                        (clean_run_id,),
                    )
                    adm_row = cur_adm.fetchone()
                    if adm_row:
                        exp_str = str(adm_row["expires_at"] or "")
                        if exp_str and exp_str >= now_iso:
                            raise RecoveryError(
                                f"Cannot recover workflow '{clean_run_id}': active unexpired "
                                f"admission lease held by owner '{adm_row['owner_id']}' until {exp_str}."
                            )

                    cur_tools = conn.execute(
                        """
                        SELECT COUNT(*) AS active_tools
                        FROM tool_executions
                        WHERE run_id = ? AND status IN ('running', 'in_flight', 'uncertain');
                        """,
                        (clean_run_id,),
                    )
                    tool_row = cur_tools.fetchone()
                    if (
                        tool_row
                        and int(tool_row["active_tools"] or 0) > 0
                        and clean_action == "requeue"
                    ):
                        raise RecoveryError(
                            f"Workflow '{clean_run_id}' has uncertain in-flight tool operations. "
                            "Reconciliation is required before requeuing."
                        )

                    run_fence = int(run_row["fence_token"] or 0)
                    adm_fence = int(adm_row["fence_token"] or 0) if adm_row else 0
                    new_fence = max(run_fence, adm_fence) + 1

                    conn.execute(
                        "DELETE FROM workflow_admissions WHERE run_id = ?;",
                        (clean_run_id,),
                    )

                    new_status = (
                        "queued"
                        if clean_action == "requeue"
                        else ("failed" if clean_action == "fail" else "cancelled")
                    )

                    cur_cp = conn.execute(
                        """
                        SELECT state_json FROM checkpoints
                        WHERE run_id = ?
                        ORDER BY step DESC LIMIT 1;
                        """,
                        (clean_run_id,),
                    )
                    cp_row = cur_cp.fetchone()
                    state: dict[str, Any] = {}
                    if cp_row and cp_row["state_json"]:
                        try:
                            state = json.loads(cp_row["state_json"])
                        except Exception:
                            state = {}

                    state["status"] = new_status
                    state["recovered_at"] = now_iso
                    state["recovered_by"] = clean_operator
                    state["recovery_reason"] = clean_reason
                    state["recovery_action"] = clean_action
                    state["fence_token"] = new_fence

                    if clean_action == "fail":
                        state["error"] = f"Workflow failed by operator {clean_operator}: {clean_reason}"
                    elif clean_action == "cancel":
                        state["cancelled_by"] = clean_operator

                    next_step = int(run_row["latest_step"] or 0) + 1
                    try:
                        state_json = json.dumps(state, default=str)
                    except Exception:
                        state_json = "{}"

                    conn.execute(
                        """
                        INSERT INTO checkpoints (
                            run_id, step, transition, state_json, schema_version, created_at
                        )
                        VALUES (?, ?, 'recover', ?, ?, ?);
                        """,
                        (clean_run_id, next_step, state_json, SCHEMA_VERSION, now_iso),
                    )

                    conn.execute(
                        """
                        UPDATE runs
                        SET status = ?, latest_step = ?, updated_at = ?, fence_token = ?
                        WHERE run_id = ?;
                        """,
                        (new_status, next_step, now_iso, new_fence, clean_run_id),
                    )

                    from ..observability.audit import AuditEventType

                    event_id = str(uuid.uuid4())
                    self.record_audit_event(
                        event_id=event_id,
                        timestamp=now_iso,
                        event_type=AuditEventType.WORKFLOW_RECOVERED,
                        run_id=clean_run_id,
                        correlation_id=None,
                        actor=clean_operator,
                        outcome="recovered",
                        metadata={
                            "reason": clean_reason,
                            "previous_status": prev_status,
                            "new_status": new_status,
                            "action": clean_action,
                            "fence_token": new_fence,
                        },
                        conn=conn,
                        raise_on_error=True,
                    )

                    conn.execute("COMMIT;")

                    return {
                        "run_id": clean_run_id,
                        "previous_status": prev_status,
                        "new_status": new_status,
                        "action": clean_action,
                        "fence_token": new_fence,
                        "recovered_by": clean_operator,
                        "reason": clean_reason,
                        "timestamp": now_iso,
                    }
                except Exception:
                    try:
                        conn.execute("ROLLBACK;")
                    except Exception:
                        pass
                    raise
            finally:
                conn.isolation_level = old_isolation
                if not self._is_memory:
                    conn.close()

    def with_fencing(
        self,
        owner_id: str,
        fence_token: int,
    ) -> FencedCheckpointStore:
        """Return a fenced view of this store bound to the given owner_id and fence_token."""
        return FencedCheckpointStore(self, owner_id=owner_id, fence_token=fence_token)


class FencedCheckpointStore:
    """Scoped wrapper around CheckpointStore that automatically binds owner_id
    and fence_token to mutations.
    """

    def __init__(
        self,
        store: CheckpointStore,
        owner_id: str,
        fence_token: int,
    ) -> None:
        self._store = store
        self._owner_id = str(owner_id or "").strip()
        self._fence_token = int(fence_token)

    @property
    def owner_id(self) -> str:
        """Return the bound worker owner ID."""
        return self._owner_id

    @property
    def fence_token(self) -> int:
        """Return the bound fencing generation token."""
        return self._fence_token

    @property
    def underlying_store(self) -> CheckpointStore:
        """Return the underlying CheckpointStore instance."""
        return self._store

    @property
    def enabled(self) -> bool:
        """Check whether underlying store persistence is enabled."""
        return bool(self._store.enabled)

    @property
    def db_path(self) -> str:
        """Return underlying database path."""
        return str(self._store.db_path)

    def save_checkpoint(
        self,
        run_id: str,
        state: dict[str, Any],
        transition: str = "state_update",
        owner_id: str | None = None,
        fence_token: int | None = None,
        privileged: bool = False,
    ) -> int:
        """Save durable checkpoint using bound owner_id and fence_token unless explicitly overridden."""
        return self._store.save_checkpoint(
            run_id,
            state,
            transition,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
            privileged=privileged,
        )

    def release_workflow_admission(
        self,
        run_id: str,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> bool:
        """Release admission using bound owner_id and fence_token unless explicitly overridden."""
        return self._store.release_workflow_admission(
            run_id,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
        )

    def validate_admission_ownership(
        self,
        run_id: str,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> bool:
        """Validate admission ownership using bound credentials."""
        return self._store.validate_admission_ownership(
            run_id,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
        )

    def get_tool_execution(self, operation_key: str) -> dict[str, Any] | None:
        """Retrieve tool execution details via underlying store."""
        return self._store.get_tool_execution(operation_key)

    def record_tool_dispatch(
        self,
        operation_key: str,
        run_id: str,
        subtask_id: str,
        tool_name: str,
        fence_token: int | None = None,
        args: dict[str, Any] | None = None,
    ) -> None:
        """Record tool dispatch binding this worker's fence token unless overridden."""
        return self._store.record_tool_dispatch(
            operation_key=operation_key,
            run_id=run_id,
            subtask_id=subtask_id,
            tool_name=tool_name,
            fence_token=fence_token if fence_token is not None else self._fence_token,
            args=args,
        )

    def record_tool_completion(
        self,
        operation_key: str,
        result_text: str,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> None:
        """Record tool completion enforcing this worker's owner_id and fence_token."""
        return self._store.record_tool_completion(
            operation_key=operation_key,
            result_text=result_text,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
        )

    def record_tool_failure(
        self,
        operation_key: str,
        error_text: str,
        uncertain: bool = False,
        owner_id: str | None = None,
        fence_token: int | None = None,
    ) -> None:
        """Record tool failure binding this worker's owner_id and fence_token."""
        return self._store.record_tool_failure(
            operation_key=operation_key,
            error_text=error_text,
            uncertain=uncertain,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
        )

    def list_tool_executions(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List tool executions for a run via underlying store."""
        return self._store.list_tool_executions(run_id, status=status, tool_name=tool_name)

    def reconcile_tool_execution(
        self,
        run_id: str,
        operation_key: str,
        outcome: str,
        reason: str,
        result_text: str | None = None,
        error_text: str | None = None,
        actor: str | None = None,
    ) -> dict[str, Any]:
        """Reconcile tool execution via underlying store."""
        return self._store.reconcile_tool_execution(
            run_id=run_id,
            operation_key=operation_key,
            outcome=outcome,
            reason=reason,
            result_text=result_text,
            error_text=error_text,
            actor=actor or self._owner_id,
        )

    def recover_uncertain_tool_executions(self, run_id: str) -> int:
        """Recover uncertain tool executions via underlying store."""
        return self._store.recover_uncertain_tool_executions(run_id)

    def record_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        approver: str,
        decision: str,
        reason: str = "",
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        """Record tool approval via underlying store."""
        return self._store.record_tool_approval(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            approver=approver,
            decision=decision,
            reason=reason,
            argument_fingerprint=argument_fingerprint,
            risk_level=risk_level,
            policy_hash=policy_hash,
            expires_at=expires_at,
        )

    def revoke_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        revoked_by: str,
        reason: str = "",
    ) -> dict[str, Any]:
        """Revoke tool approval via underlying store."""
        return self._store.revoke_tool_approval(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            revoked_by=revoked_by,
            reason=reason,
        )

    def get_tool_approval(
        self,
        operation_key_or_run_id: str,
        operation_key: str | None = None,
    ) -> dict[str, Any] | None:
        """Retrieve tool approval via underlying store."""
        return self._store.get_tool_approval(
            operation_key_or_run_id,
            operation_key=operation_key,
        )

    def list_tool_approvals(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> list[dict[str, Any]]:
        """List tool approvals via underlying store."""
        return self._store.list_tool_approvals(run_id, status=status, tool_name=tool_name)

    def record_tool_approval_request(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        reason: str = "",
        expires_at: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Record tool approval request via underlying store."""
        return self._store.record_tool_approval_request(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            argument_fingerprint=argument_fingerprint,
            risk_level=risk_level,
            policy_hash=policy_hash,
            reason=reason,
            expires_at=expires_at,
            timeout_seconds=timeout_seconds,
        )

    def recover_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        recovered_by: str,
        reason: str,
        argument_fingerprint: str | None = None,
        risk_level: str | None = None,
        policy_hash: str | None = None,
        expires_at: str | None = None,
        timeout_seconds: float | None = None,
    ) -> dict[str, Any]:
        """Recover tool approval via underlying store."""
        return self._store.recover_tool_approval(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            recovered_by=recovered_by,
            reason=reason,
            argument_fingerprint=argument_fingerprint,
            risk_level=risk_level,
            policy_hash=policy_hash,
            expires_at=expires_at,
            timeout_seconds=timeout_seconds,
        )

    def expire_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Approval expired due to timeout",
        force: bool = False,
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Expire tool approval via underlying store."""
        return self._store.expire_tool_approval(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            reason=reason,
            force=force,
            actor=actor or self._owner_id,
            lease_name=lease_name,
            expected_owner_id=expected_owner_id,
            expected_fence_token=expected_fence_token,
        )

    def escalate_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Pending approval overdue",
        escalation_level: str = "manager_alert",
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Escalate tool approval via underlying store."""
        return self._store.escalate_tool_approval(
            run_id=run_id,
            operation_key=operation_key,
            tool_name=tool_name,
            reason=reason,
            escalation_level=escalation_level,
            actor=actor or self._owner_id,
            lease_name=lease_name,
            expected_owner_id=expected_owner_id,
            expected_fence_token=expected_fence_token,
        )

    def check_and_expire_tool_approvals(
        self,
        run_id: str | None = None,
        before_timestamp: str | None = None,
        auto_escalate: bool = True,
        actor: str = "system",
        lease_name: str | None = None,
        expected_owner_id: str | None = None,
        expected_fence_token: int | None = None,
    ) -> dict[str, Any]:
        """Check and expire overdue tool approvals via underlying store."""
        return self._store.check_and_expire_tool_approvals(
            run_id=run_id,
            before_timestamp=before_timestamp,
            auto_escalate=auto_escalate,
            actor=actor or self._owner_id,
            lease_name=lease_name,
            expected_owner_id=expected_owner_id,
            expected_fence_token=expected_fence_token,
        )

    def check_overdue_approvals(
        self,
        run_id: str | None = None,
        escalation_threshold_seconds: float | None = None,
        now_timestamp: str | None = None,
    ) -> list[dict[str, Any]]:
        """Identify overdue tool approvals via underlying store."""
        return self._store.check_overdue_approvals(
            run_id=run_id,
            escalation_threshold_seconds=escalation_threshold_seconds,
            now_timestamp=now_timestamp,
        )

    def acquire_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        lease_ttl_seconds: float = 30.0,
        now_dt: datetime.datetime | None = None,
    ) -> tuple[bool, int, dict[str, Any] | None]:
        """Acquire scheduler lease via underlying store."""
        return self._store.acquire_scheduler_lease(
            lease_name=lease_name,
            owner_id=owner_id or self._owner_id,
            lease_ttl_seconds=lease_ttl_seconds,
            now_dt=now_dt,
        )

    def renew_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int = 1,
        lease_ttl_seconds: float = 30.0,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Renew scheduler lease via underlying store."""
        return self._store.renew_scheduler_lease(
            lease_name=lease_name,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token,
            lease_ttl_seconds=lease_ttl_seconds,
            now_dt=now_dt,
        )

    def release_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int | None = None,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Release scheduler lease via underlying store."""
        return self._store.release_scheduler_lease(
            lease_name=lease_name,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token,
            now_dt=now_dt,
        )

    def validate_scheduler_lease_ownership(
        self,
        lease_name: str = "tool_approval_scheduler",
        owner_id: str = "",
        fence_token: int | None = None,
        now_dt: datetime.datetime | None = None,
    ) -> bool:
        """Validate scheduler lease ownership via underlying store."""
        return self._store.validate_scheduler_lease_ownership(
            lease_name=lease_name,
            owner_id=owner_id or self._owner_id,
            fence_token=fence_token,
            now_dt=now_dt,
        )

    def get_scheduler_lease(
        self,
        lease_name: str = "tool_approval_scheduler",
    ) -> dict[str, Any] | None:
        """Get scheduler lease via underlying store."""
        return self._store.get_scheduler_lease(lease_name=lease_name)

    def record_audit_event(
        self,
        event_id: str,
        timestamp: str,
        event_type: str,
        run_id: str | None,
        correlation_id: str | None,
        actor: str | None,
        outcome: str,
        metadata: dict[str, Any] | None = None,
        dedup: bool = False,
        conn: sqlite3.Connection | None = None,
        raise_on_error: bool = False,
    ) -> bool:
        """Record audit event via underlying store."""
        return self._store.record_audit_event(
            event_id=event_id,
            timestamp=timestamp,
            event_type=event_type,
            run_id=run_id,
            correlation_id=correlation_id,
            actor=actor or self._owner_id,
            outcome=outcome,
            metadata=metadata,
            dedup=dedup,
            conn=conn,
            raise_on_error=raise_on_error,
        )

    def get_audit_events(
        self,
        run_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Query audit events via underlying store."""
        return self._store.get_audit_events(
            run_id=run_id,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    def verify_approval_audit_consistency(
        self,
        operation_key: str,
    ) -> dict[str, Any]:
        """Verify approval audit consistency via underlying store."""
        return self._store.verify_approval_audit_consistency(operation_key=operation_key)

    def insert_outbox_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        created_at: str | None = None,
        conn: sqlite3.Connection | None = None,
        raise_on_error: bool = True,
    ) -> dict[str, Any]:
        """Insert outbox event via underlying store."""
        return self._store.insert_outbox_event(
            event_type=event_type,
            payload=payload,
            idempotency_key=idempotency_key,
            created_at=created_at,
            conn=conn,
            raise_on_error=raise_on_error,
        )

    def claim_outbox_events(
        self,
        owner_id: str | None = None,
        batch_size: int = 50,
        claim_ttl_seconds: float = 30.0,
        event_types: list[str] | None = None,
        now: str | None = None,
    ) -> list[dict[str, Any]]:
        """Claim outbox events using bound owner_id unless overridden."""
        return self._store.claim_outbox_events(
            owner_id=owner_id or self._owner_id,
            batch_size=batch_size,
            claim_ttl_seconds=claim_ttl_seconds,
            event_types=event_types,
            now=now,
        )

    def update_outbox_event_delivery(
        self,
        event_id: str,
        claimed_by: str | None = None,
        fence_token: int | None = None,
        success: bool = True,
        error: str | None = None,
        retryable: bool = True,
        max_attempts: int = 5,
        backoff_base: float = 2.0,
        backoff_max: float = 300.0,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Update outbox delivery using bound credentials unless overridden."""
        return self._store.update_outbox_event_delivery(
            event_id=event_id,
            claimed_by=claimed_by or self._owner_id,
            fence_token=fence_token if fence_token is not None else self._fence_token,
            success=success,
            error=error,
            retryable=retryable,
            max_attempts=max_attempts,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            now=now,
        )

    def retry_outbox_event(
        self,
        event_id: str,
        reset_attempts: bool = False,
        reason: str = "Operator manual recovery",
        operator: str | None = None,
        force: bool = False,
    ) -> dict[str, Any]:
        """Retry outbox event via underlying store."""
        return self._store.retry_outbox_event(
            event_id=event_id,
            reset_attempts=reset_attempts,
            reason=reason,
            operator=operator or self._owner_id,
            force=force,
        )

    def recover_outbox_events(
        self,
        reason: str,
        event_ids: list[str] | None = None,
        status_filter: str | None = "dead_letter",
        operator: str | None = None,
        reset_attempts: bool = True,
        max_events: int = 50,
    ) -> dict[str, Any]:
        """Batch recover outbox events via underlying store."""
        return self._store.recover_outbox_events(
            reason=reason,
            event_ids=event_ids,
            status_filter=status_filter,
            operator=operator or self._owner_id,
            reset_attempts=reset_attempts,
            max_events=max_events,
        )

    def get_outbox_event_history(
        self,
        event_id: str,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Query event history via underlying store."""
        return self._store.get_outbox_event_history(event_id=event_id, limit=limit)

    def get_outbox_events(
        self,
        status: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Query outbox events via underlying store."""
        return self._store.get_outbox_events(
            status=status,
            event_type=event_type,
            limit=limit,
            offset=offset,
        )

    def get_outbox_diagnostics(self, max_attempts: int = 5) -> dict[str, Any]:
        """Get outbox diagnostics via underlying store."""
        return self._store.get_outbox_diagnostics(max_attempts=max_attempts)

    def record_webhook_replay(
        self,
        event_id: str,
        timestamp: int,
        expires_at: float,
        recorded_at: str | None = None,
        now: float | None = None,
    ) -> bool:
        """Record webhook replay via underlying store."""
        return self._store.record_webhook_replay(
            event_id=event_id,
            timestamp=timestamp,
            expires_at=expires_at,
            recorded_at=recorded_at,
            now=now,
        )

    def cleanup_expired_webhook_replays(
        self,
        now: float | None = None,
        limit: int = 500,
        trigger_type: str = "scheduled",
        operator: str | None = None,
        reason: str | None = None,
    ) -> int:
        """Prune expired webhook replay records via underlying store."""
        return self._store.cleanup_expired_webhook_replays(
            now=now,
            limit=limit,
            trigger_type=trigger_type,
            operator=operator,
            reason=reason,
        )

    def get_webhook_replay_count(
        self,
        active_only: bool = True,
        now: float | None = None,
    ) -> int:
        """Return count of webhook replay records via underlying store."""
        return self._store.get_webhook_replay_count(active_only=active_only, now=now)

    def get_webhook_replay_diagnostics(
        self,
        now: float | None = None,
    ) -> dict[str, Any]:
        """Derive bounded diagnostics via underlying store."""
        return self._store.get_webhook_replay_diagnostics(now=now)

    def get_webhook_replay_cleanup_history(
        self,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """Return chronological maintenance history via underlying store."""
        return self._store.get_webhook_replay_cleanup_history(limit=limit)

    def with_fencing(
        self,
        owner_id: str,
        fence_token: int,
    ) -> FencedCheckpointStore:
        """Return a newly bound fenced view."""
        return FencedCheckpointStore(self._store, owner_id=owner_id, fence_token=fence_token)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._store, name)
