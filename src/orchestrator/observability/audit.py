"""Structured, privacy-preserving audit logging for workflow lifecycle and security events.

Provides:
- Strongly-typed AuditEvent representations.
- Automatic sanitization of sensitive keys, tokens, credentials, and tracebacks.
- Bounded, queryable event persistence via SQLite.
- Safe failure policy: audit persistence failures never abort workflow execution.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any

logger = logging.getLogger("orchestrator.audit")

_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(?i)(password|secret|api[_-]?key|auth|token|credential|bearer|authorization)"
)


class AuditEventType:
    """Canonical event types for structured audit records."""

    # Workflow lifecycle
    WORKFLOW_CREATED = "workflow.created"
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_RESUMED = "workflow.resumed"
    WORKFLOW_CANCELLED = "workflow.cancelled"
    WORKFLOW_TIMED_OUT = "workflow.timed_out"
    WORKFLOW_INTERRUPTED = "workflow.interrupted"
    WORKFLOW_RECOVERED = "workflow.recovered"

    # Human-in-the-loop review
    HITL_ESCALATED = "hitl.escalated"
    HITL_APPROVED = "hitl.approved"
    HITL_REJECTED = "hitl.rejected"
    HITL_EDITED = "hitl.edited"

    # Security & protection
    SECURITY_AUTH_FAILURE = "security.auth_failure"
    SECURITY_RATE_LIMIT_EXCEEDED = "security.rate_limit_exceeded"

    # Tool execution & reconciliation
    TOOL_RECONCILED = "tool.reconciled"
    TOOL_POLICY_DENIED = "tool.policy_denied"
    TOOL_APPROVED = "tool.approved"
    TOOL_APPROVAL_REQUIRED = "tool.approval_required"
    TOOL_APPROVAL_REVOKED = "tool.approval_revoked"
    TOOL_APPROVER_DENIED = "tool.approver_denied"
    TOOL_APPROVAL_RECOVERED = "tool.approval_recovered"
    TOOL_APPROVAL_REQUESTED = "tool.approval_requested"
    TOOL_APPROVAL_EXPIRED = "tool.approval_expired"
    TOOL_APPROVAL_ESCALATED = "tool.approval_escalated"

    # Service lifecycle
    SERVICE_STARTUP = "service.startup"
    SERVICE_SHUTDOWN = "service.shutdown"

    # Transactional outbox & notification recovery
    OUTBOX_RETRY_REQUESTED = "outbox.retry_requested"
    OUTBOX_RECOVERED = "outbox.recovered"
    NOTIFICATION_REPLAY_CLEANUP = "notification.replay_cleanup"

    # Database backup lifecycle
    BACKUP_STARTED = "backup.started"
    BACKUP_COMPLETED = "backup.completed"
    BACKUP_FAILED = "backup.failed"

    # Restore drill lifecycle
    RESTORE_DRILL_STARTED = "restore_drill.started"
    RESTORE_DRILL_COMPLETED = "restore_drill.completed"
    RESTORE_DRILL_FAILED = "restore_drill.failed"

    # Backup integrity manifest & verification
    BACKUP_MANIFEST_CREATED = "backup.manifest_created"
    BACKUP_INTEGRITY_VERIFIED = "backup.integrity_verified"
    BACKUP_INTEGRITY_FAILED = "backup.integrity_failed"

    # Backup manifest cryptographic signing & signature verification
    BACKUP_MANIFEST_SIGNED = "backup.manifest_signed"
    BACKUP_SIGNATURE_VERIFIED = "backup.signature_verified"
    BACKUP_SIGNATURE_FAILED = "backup.signature_failed"


class AuditOutcome:
    """Canonical outcomes for audit events."""

    SUCCESS = "success"
    FAILURE = "failure"
    DENIED = "denied"
    ESCALATED = "escalated"
    INTERRUPTED = "interrupted"
    TIMEOUT = "timeout"


def sanitize_audit_metadata(obj: Any, depth: int = 0) -> Any:
    """Recursively sanitize metadata to guarantee zero secret or full-prompt leakage.

    Guarantees:
    - Redacts sensitive keys (passwords, tokens, api keys, auth headers).
    - Redacts Bearer tokens, OpenAI-style keys (sk-...), and secret query parameters.
    - Strips full tracebacks, retaining only the final sanitized exception summary.
    - Limits string lengths to prevent payload bloat.
    - Bounds recursion depth to prevent cyclic references.
    """
    if depth > 5:
        return "[MAX_DEPTH_REACHED]"

    if isinstance(obj, dict):
        clean_dict: dict[str, Any] = {}
        for k, v in obj.items():
            key_str = str(k)
            if _SENSITIVE_KEY_PATTERNS.search(key_str):
                clean_dict[key_str] = "[REDACTED]"
            elif key_str in ("task", "prompt") and isinstance(v, str):
                # Avoid recording full user task prompts; store count and short preview
                clean_dict["task_chars"] = len(v)
                preview = v[:80].strip()
                if len(v) > 80:
                    preview += "..."
                clean_dict["task_preview"] = re.sub(
                    r"(?i)(sk-[a-zA-Z0-9_-]{16,}|bearer\s+[a-zA-Z0-9_\-\.]+)",
                    "[REDACTED_KEY]",
                    preview,
                )
            else:
                clean_dict[key_str] = sanitize_audit_metadata(v, depth=depth + 1)
        return clean_dict
    elif isinstance(obj, (list, tuple, set)):
        return [sanitize_audit_metadata(item, depth=depth + 1) for item in list(obj)[:50]]
    elif isinstance(obj, str):
        val = obj.strip()
        if not val:
            return ""

        # Extract only the final line if value contains a Python traceback
        if "Traceback (most recent call last):" in val:
            lines = [line.strip() for line in val.splitlines() if line.strip()]
            val = lines[-1] if lines else "Internal execution error"

        # Redact known credential patterns
        val = re.sub(r"sk-[a-zA-Z0-9_-]{16,}", "[REDACTED_KEY]", val)
        val = re.sub(r"bearer\s+[a-zA-Z0-9_\-\.]{16,}", "Bearer [REDACTED_TOKEN]", val, flags=re.IGNORECASE)
        val = re.sub(
            r"(password|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+",
            r"\1=[REDACTED]",
            val,
            flags=re.IGNORECASE,
        )

        # Cap individual string lengths
        if len(val) > 300:
            val = val[:297] + "..."
        return val
    elif isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return str(obj)[:100]


def sanitize_tool_arguments(args_obj: Any) -> dict[str, Any]:
    """Sanitize tool input arguments to ensure secrets and credentials are never exposed."""
    if isinstance(args_obj, str):
        try:
            parsed = json.loads(args_obj)
        except Exception:
            return {"raw_preview": sanitize_audit_metadata(args_obj[:100])}
    elif isinstance(args_obj, dict):
        parsed = args_obj
    else:
        return {}

    sanitized = sanitize_audit_metadata(parsed)
    return sanitized if isinstance(sanitized, dict) else {"args": sanitized}


def sanitize_tool_execution(record: dict[str, Any]) -> dict[str, Any]:
    """Sanitize a raw tool execution record for safe API exposure."""
    raw_args = record.get("args_json", "{}")
    clean_args = sanitize_tool_arguments(raw_args)

    raw_error = record.get("error_text")
    clean_error = sanitize_audit_metadata(raw_error) if raw_error else None

    raw_result = record.get("result_text")
    clean_result = sanitize_audit_metadata(raw_result) if raw_result else None

    return {
        "operation_key": record.get("operation_key"),
        "run_id": record.get("run_id"),
        "subtask_id": record.get("subtask_id"),
        "tool_name": record.get("tool_name"),
        "fence_token": int(record.get("fence_token") or 1),
        "status": record.get("status"),
        "args": clean_args,
        "result_text": clean_result,
        "error_text": clean_error,
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "reconciled_by": record.get("reconciled_by"),
        "reconciled_at": record.get("reconciled_at"),
        "reconciliation_reason": record.get("reconciliation_reason"),
    }


def sanitize_tool_approval(record: dict[str, Any]) -> dict[str, Any]:
    """Sanitize a raw tool approval record for safe API exposure."""
    raw_approver = record.get("approver")
    clean_approver = sanitize_audit_metadata(raw_approver) if raw_approver else ""

    raw_reason = record.get("reason")
    clean_reason = sanitize_audit_metadata(raw_reason) if raw_reason else ""

    raw_rev_by = record.get("revoked_by")
    clean_rev_by = sanitize_audit_metadata(raw_rev_by) if raw_rev_by else None

    raw_rev_reason = record.get("revocation_reason")
    clean_rev_reason = sanitize_audit_metadata(raw_rev_reason) if raw_rev_reason else None

    raw_recovered_by = record.get("recovered_by")
    clean_recovered_by = sanitize_audit_metadata(raw_recovered_by) if raw_recovered_by else None

    raw_esc_reason = record.get("escalation_reason")
    clean_esc_reason = sanitize_audit_metadata(raw_esc_reason) if raw_esc_reason else None

    return {
        "operation_key": record.get("operation_key"),
        "run_id": record.get("run_id"),
        "tool_name": record.get("tool_name"),
        "approver": clean_approver,
        "decision": record.get("decision"),
        "reason": clean_reason,
        "created_at": record.get("created_at"),
        "argument_fingerprint": record.get("argument_fingerprint"),
        "risk_level": record.get("risk_level"),
        "policy_hash": record.get("policy_hash"),
        "revoked_by": clean_rev_by,
        "revocation_reason": clean_rev_reason,
        "revoked_at": record.get("revoked_at"),
        "recovered_by": clean_recovered_by,
        "expires_at": record.get("expires_at"),
        "expired_at": record.get("expired_at"),
        "escalated_at": record.get("escalated_at"),
        "escalation_reason": clean_esc_reason,
    }


@dataclass
class AuditEvent:
    """Strongly-typed, structured audit event record."""

    event_id: str
    timestamp: str
    event_type: str
    outcome: str
    run_id: str | None = None
    correlation_id: str | None = None
    actor: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def record_audit_event(
    event_type: str,
    outcome: str,
    run_id: str | None = None,
    correlation_id: str | None = None,
    actor: str | None = None,
    metadata: dict[str, Any] | None = None,
    store: Any = None,
    dedup: bool = False,
    conn: Any = None,
    raise_on_error: bool = False,
) -> AuditEvent:
    """Construct, sanitize, log, and persist a structured audit event.

    Safe Failure Policy:
    If SQLite persistence is disabled or encounters an operational error, the event
    is logged via standard structured logging and returned cleanly unless
    raise_on_error=True is set (e.g. for atomic approval lifecycle transactions).
    """
    event_id = f"aud-{uuid.uuid4().hex[:12]}"
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    clean_metadata = sanitize_audit_metadata(metadata or {})

    clean_run_id = str(run_id).strip() if run_id else None
    clean_corr_id = str(correlation_id).strip() if correlation_id else None
    clean_actor = str(actor).strip() if actor else None

    event = AuditEvent(
        event_id=event_id,
        timestamp=now,
        event_type=str(event_type),
        outcome=str(outcome),
        run_id=clean_run_id,
        correlation_id=clean_corr_id,
        actor=clean_actor,
        metadata=clean_metadata,
    )

    # Standard structured logging
    logger.info(
        "AUDIT_EVENT: %s",
        json.dumps(event.to_dict(), default=str),
    )

    # Durable persistence via CheckpointStore
    try:
        if store is None:
            from ..persistence.store import CheckpointStore

            store = CheckpointStore()

        if store and getattr(store, "enabled", False):
            ok = store.record_audit_event(
                event_id=event.event_id,
                timestamp=event.timestamp,
                event_type=event.event_type,
                run_id=event.run_id,
                correlation_id=event.correlation_id,
                actor=event.actor,
                outcome=event.outcome,
                metadata=event.metadata,
                dedup=dedup,
                conn=conn,
                raise_on_error=raise_on_error,
            )
            if raise_on_error and not ok:
                raise RuntimeError(
                    f"Failed to persist audit event '{event.event_id}' ({event.event_type})"
                )
    except Exception as exc:
        if raise_on_error:
            raise
        logger.warning(
            "Audit persistence encountered an error (safe failure policy applied): %s",
            exc,
        )

    return event


def serialize_audit_events_json(events: list[dict[str, Any]] | list[AuditEvent]) -> str:
    """Serialize audit event records to a formatted JSON array string."""
    raw = [e.to_dict() if isinstance(e, AuditEvent) else dict(e) for e in events]
    return json.dumps(raw, indent=2, default=str)


def serialize_audit_events_jsonl(events: list[dict[str, Any]] | list[AuditEvent]) -> str:
    """Serialize audit event records to a newline-delimited JSON (JSONL) string."""
    lines = []
    for e in events:
        item = e.to_dict() if isinstance(e, AuditEvent) else dict(e)
        lines.append(json.dumps(item, default=str))
    return "\n".join(lines) + ("\n" if lines else "")


def serialize_audit_events_csv(events: list[dict[str, Any]] | list[AuditEvent]) -> str:
    """Serialize audit event records to standard CSV with headers and JSON-encoded metadata."""
    import csv
    import io

    output = io.StringIO()
    fieldnames = [
        "event_id",
        "timestamp",
        "event_type",
        "outcome",
        "run_id",
        "correlation_id",
        "actor",
        "metadata",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for e in events:
        row = e.to_dict() if isinstance(e, AuditEvent) else dict(e)
        meta = row.get("metadata")
        if isinstance(meta, (dict, list)):
            row["metadata"] = json.dumps(meta, default=str)
        elif meta is None:
            row["metadata"] = "{}"
        writer.writerow(row)
    return output.getvalue()
