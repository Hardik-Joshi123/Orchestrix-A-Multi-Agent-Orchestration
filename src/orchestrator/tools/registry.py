"""A registry of tools available to specialists.

Each tool carries a name, JSON schema, and a simple token-bucket rate limit.
Register with the @tool decorator; specialists look tools up by name.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import inspect
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from ..persistence.store import AdmissionFencingError

logger = logging.getLogger(__name__)


class ToolError(Exception):
    """Base exception for all tool-related errors."""


class ToolNotFoundError(ToolError, KeyError):
    """Raised when an unknown tool name is invoked."""


class ToolValidationError(ToolError, ValueError, TypeError):
    """Raised when tool arguments fail schema validation."""


class ToolRateLimitError(ToolError, RuntimeError):
    """Raised when a tool's rate limit is exceeded."""


class ToolExecutionError(ToolError, RuntimeError):
    """Raised when a tool encounters an execution error."""


class ToolTimeoutError(ToolExecutionError, TimeoutError):
    """Raised when tool execution exceeds its timeout limit."""


class ToolApprovalError(ToolError, PermissionError):
    """Raised when an unapproved side-effecting or high-risk tool is invoked."""


class ToolPermissionDeniedError(ToolApprovalError):
    """Raised when an agent is not permitted to invoke a tool, or approval was rejected."""


class ToolApprovalRequiredError(ToolApprovalError):
    """Raised when a tool invocation requires human approval before execution."""


class ToolApprovalConflictError(ToolApprovalError):
    """Raised when an approval decision or revocation conflicts with existing state."""


class ToolApproverUnauthorizedError(ToolApprovalError):
    """Raised when an actor lacks authorization to approve or revoke a tool execution."""


class ToolApprovalExpiredError(ToolApprovalError):
    """Raised when an approval has expired and cannot authorize tool execution."""


class ToolUncertainOutcomeError(ToolExecutionError):
    """Raised when tool execution outcome is uncertain (e.g. timeout during mutating operation)."""


class ToolFencingError(ToolExecutionError, AdmissionFencingError):
    """Raised when tool execution or result persistence is rejected due to lost admission ownership."""


class ToolDuplicateExecutionError(ToolExecutionError):
    """Raised when a non-idempotent mutating tool is re-invoked with an in-flight operation key."""


class IdempotencyMode(str, Enum):
    """Idempotency classification for tools."""

    READ_ONLY = "read_only"
    MUTATING_IDEMPOTENT = "mutating_idempotent"
    MUTATING_NON_IDEMPOTENT = "mutating_non_idempotent"


class RiskLevel(str, Enum):
    """Risk classification for tool actions."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ToolApproval:
    """Approval metadata for a tool execution."""

    approver: str
    decision: str  # "approved", "rejected", "revoked", or "expired"
    reason: str = ""
    created_at: str | None = None
    argument_fingerprint: str | None = None
    risk_level: str | None = None
    policy_hash: str | None = None
    revoked_by: str | None = None
    revocation_reason: str | None = None
    revoked_at: str | None = None
    expires_at: str | None = None
    expired_at: str | None = None
    escalated_at: str | None = None
    escalation_reason: str | None = None


@dataclass
class ToolPolicy:
    """Policy governing tool execution permissions and risk-based approval requirements."""

    tool_name: str
    risk_level: RiskLevel = RiskLevel.LOW
    allowed_agents: list[str] = field(default_factory=lambda: ["*"])
    requires_approval: bool = False
    authorized_approvers: list[str] = field(default_factory=lambda: ["*"])
    approval_timeout_seconds: float | None = None


@dataclass
class ToolExecutionContext:
    """Context carried into tool execution for idempotency tracking, fencing, and cooperative cancellation."""

    run_id: str
    subtask_id: str
    fence_token: int = 1
    cancellation_token: Any | None = None
    store: Any | None = None
    owner_id: str | None = None
    operation_key: str | None = None
    agent_id: str | None = None
    approval: ToolApproval | None = None

    def __post_init__(self) -> None:
        if self.store is not None:
            if self.owner_id is None and hasattr(self.store, "owner_id"):
                self.owner_id = self.store.owner_id
            if (self.fence_token is None or self.fence_token == 1) and hasattr(self.store, "fence_token"):
                bound_fence = self.store.fence_token
                if bound_fence is not None:
                    self.fence_token = bound_fence


def derive_operation_key(
    run_id: str,
    subtask_id: str,
    tool_name: str,
    kwargs: dict[str, Any],
    fence_token: int | None = None,
) -> str:
    """Derive a deterministic, stable operation key for tool idempotency tracking."""
    clean_run = str(run_id or "").strip()
    clean_subtask = str(subtask_id or "").strip()
    clean_tool = str(tool_name or "").strip()
    filtered_args = {k: v for k, v in kwargs.items() if k not in ("context", "idempotency_key")}
    serialized = json.dumps(filtered_args, sort_keys=True, separators=(",", ":"), default=str)
    args_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    return f"toolop:{clean_run}:{clean_subtask}:{clean_tool}:{args_hash}"


def compute_argument_fingerprint(kwargs: dict[str, Any]) -> str:
    """Compute deterministic SHA-256 fingerprint for tool kwargs."""
    filtered_args = {k: v for k, v in kwargs.items() if k not in ("context", "idempotency_key")}
    serialized = json.dumps(filtered_args, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def compute_policy_hash(policy: ToolPolicy | None) -> str:
    """Compute deterministic SHA-256 hash for a ToolPolicy."""
    if policy is None:
        return ""
    data: dict[str, Any] = {
        "tool_name": policy.tool_name,
        "risk_level": (
            policy.risk_level.value
            if isinstance(policy.risk_level, RiskLevel)
            else str(policy.risk_level)
        ),
        "allowed_agents": sorted(policy.allowed_agents),
        "requires_approval": policy.requires_approval,
        "authorized_approvers": sorted(policy.authorized_approvers),
    }
    if getattr(policy, "approval_timeout_seconds", None) is not None:
        data["approval_timeout_seconds"] = float(policy.approval_timeout_seconds)
    serialized = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]


def verify_approver_authorization(
    approver: str | None,
    tool_name: str,
    policy: ToolPolicy | None = None,
) -> None:
    """Validate that approver identity is non-generic and authorized by tool and global policy."""
    if not approver or not isinstance(approver, str) or not approver.strip():
        raise ToolApproverUnauthorizedError("Approver identity must be provided")

    clean_approver = approver.strip()
    generic_identities = {
        "bearer-token",
        "authenticated-client",
        "unknown",
        "anonymous",
    }
    if clean_approver.lower() in generic_identities:
        raise ToolApproverUnauthorizedError(
            f"Approver identity '{clean_approver}' is generic or unverified; "
            "explicit authorized approver identity required"
        )

    # Tool policy authorization check
    if policy is not None and policy.authorized_approvers:
        if "*" not in policy.authorized_approvers and clean_approver not in policy.authorized_approvers:
            raise ToolApproverUnauthorizedError(
                f"Approver '{clean_approver}' is not authorized to approve tool '{tool_name}'. "
                f"Authorized approvers: {policy.authorized_approvers}"
            )

    # Global settings authorization check
    try:
        from ..config import get_settings

        settings = get_settings()
        allowed_str = getattr(settings, "tool_approval_allowed_approvers", "*") or "*"
        allowed_list = [a.strip() for a in allowed_str.split(",") if a.strip()]
        if allowed_list and "*" not in allowed_list and clean_approver not in allowed_list:
            raise ToolApproverUnauthorizedError(
                f"Approver '{clean_approver}' is not in globally configured allowed approvers: {allowed_list}"
            )
    except ToolApproverUnauthorizedError:
        raise
    except Exception:
        pass


def diagnose_tool_approval(
    run_id: str,
    operation_key: str,
    tool_name: str | None = None,
    arguments: dict[str, Any] | None = None,
    store: Any = None,
    current_agent_id: str | None = None,
    owner_id: str | None = None,
    fence_token: int | None = None,
    policy: ToolPolicy | None = None,
    cancellation_token: Any = None,
    admission_fence_token: int | None = None,
    active_lease: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Inspect and evaluate diagnostic status for a tool execution's approval lifecycle.

    Distinguishes:
    - missing_approval
    - pending_approval
    - argument_fingerprint_mismatch
    - policy_risk_mismatch
    - policy_hash_mismatch
    - revoked_approval
    - rejected_approval
    - unauthorized_approver
    - workflow_cancelled
    - admission_fencing_lost
    - valid_approved
    """
    clean_run = str(run_id or "").strip()
    clean_op = str(operation_key or "").strip()
    resolved_tool_name = str(tool_name or "").strip()

    # 1. Fetch approval record from store if available
    approval_rec: dict[str, Any] | None = None
    if store and clean_op and hasattr(store, "get_tool_approval"):
        try:
            approval_rec = store.get_tool_approval(clean_op)
        except Exception:
            approval_rec = None

    if not resolved_tool_name and approval_rec:
        resolved_tool_name = str(approval_rec.get("tool_name") or "")

    # If still no tool_name, attempt to extract from op_key format: toolop:run:subtask:tool:hash
    if not resolved_tool_name and clean_op.startswith("toolop:"):
        parts = clean_op.split(":")
        if len(parts) >= 4:
            resolved_tool_name = parts[3]

    # Resolve active policy
    if policy is None and resolved_tool_name:
        policy = REGISTRY.get_policy(resolved_tool_name)
    if policy is None and resolved_tool_name in REGISTRY:
        policy = REGISTRY._tools[resolved_tool_name].policy

    # Current risk level & policy hash
    current_risk: str | None = None
    current_phash: str | None = None
    authorized_approvers: list[str] = ["*"]
    if policy:
        current_risk = (
            policy.risk_level.value
            if isinstance(policy.risk_level, RiskLevel)
            else str(policy.risk_level)
        )
        current_phash = compute_policy_hash(policy)
        authorized_approvers = list(policy.authorized_approvers)

    # Current argument fingerprint
    current_fp: str | None = None
    if arguments is not None:
        current_fp = compute_argument_fingerprint(arguments)
    elif store and clean_op and hasattr(store, "get_tool_execution"):
        try:
            exec_rec = store.get_tool_execution(clean_op)
            if exec_rec and exec_rec.get("args_json"):
                args_data = json.loads(exec_rec["args_json"])
                current_fp = compute_argument_fingerprint(args_data)
        except Exception:
            pass

    # Audit events for this run and operation
    audit_events: list[dict[str, Any]] = []
    if store and clean_run and hasattr(store, "get_audit_events"):
        try:
            raw_evs = store.get_audit_events(run_id=clean_run)
            for ev in raw_evs:
                meta = ev.get("metadata") or {}
                if (
                    meta.get("operation_key") == clean_op
                    or (resolved_tool_name and meta.get("tool_name") == resolved_tool_name)
                    or ev.get("event_type", "").startswith("tool.approv")
                    or ev.get("event_type") == "tool.policy_denied"
                ):
                    audit_events.append(ev)
        except Exception:
            pass

    dec = str(approval_rec.get("decision") or "").strip().lower() if approval_rec else ""
    bound_fp = approval_rec.get("argument_fingerprint") if approval_rec else None
    bound_risk = approval_rec.get("risk_level") if approval_rec else None
    bound_phash = approval_rec.get("policy_hash") if approval_rec else None
    approver = approval_rec.get("approver") if approval_rec else None
    revoked_by = approval_rec.get("revoked_by") if approval_rec else None
    revocation_reason = approval_rec.get("revocation_reason") if approval_rec else None
    created_at = approval_rec.get("created_at") if approval_rec else None
    revoked_at = approval_rec.get("revoked_at") if approval_rec else None
    expires_at = approval_rec.get("expires_at") if approval_rec else None
    expired_at = approval_rec.get("expired_at") if approval_rec else None
    escalated_at = approval_rec.get("escalated_at") if approval_rec else None
    escalation_reason = approval_rec.get("escalation_reason") if approval_rec else None

    from ..persistence.store import is_approval_expired

    is_expired_flag = is_approval_expired(approval_rec) if approval_rec else False

    def _build_diag(
        *,
        state: str,
        diagnostic_code: str,
        message: str,
        is_blocked: bool,
        is_stale: bool,
        can_execute: bool,
        risk_level: str | None = None,
        approver_override: str | None = None,
        revoked_by_override: str | None = None,
        revocation_reason_override: str | None = None,
        revoked_at_override: str | None = None,
        expires_at_override: str | None = None,
        expired_at_override: str | None = None,
        is_expired_override: bool | None = None,
        escalated_at_override: str | None = None,
        escalation_reason_override: str | None = None,
    ) -> dict[str, Any]:
        return {
            "operation_key": clean_op,
            "run_id": clean_run,
            "tool_name": resolved_tool_name,
            "state": state,
            "status": state,
            "diagnostic_code": diagnostic_code,
            "message": message,
            "is_blocked": is_blocked,
            "is_stale": is_stale,
            "can_execute": can_execute,
            "can_proceed": can_execute,
            "risk_level": risk_level if risk_level is not None else (bound_risk or current_risk),
            "current_risk_level": current_risk,
            "bound_fingerprint": bound_fp,
            "current_fingerprint": current_fp,
            "bound_policy_hash": bound_phash,
            "current_policy_hash": current_phash,
            "approver": approver_override if approver_override is not None else approver,
            "authorized_approvers": authorized_approvers,
            "revoked_by": revoked_by_override if revoked_by_override is not None else revoked_by,
            "revocation_reason": (
                revocation_reason_override
                if revocation_reason_override is not None
                else revocation_reason
            ),
            "created_at": created_at,
            "revoked_at": revoked_at_override if revoked_at_override is not None else revoked_at,
            "expires_at": expires_at_override if expires_at_override is not None else expires_at,
            "expired_at": expired_at_override if expired_at_override is not None else expired_at,
            "is_expired": is_expired_override if is_expired_override is not None else is_expired_flag,
            "escalated_at": escalated_at_override if escalated_at_override is not None else escalated_at,
            "escalation_reason": (
                escalation_reason_override
                if escalation_reason_override is not None
                else escalation_reason
            ),
            "audit_event_count": len(audit_events),
            "relevant_audit_events": audit_events[-10:],
        }

    # 2. Check workflow cancellation
    if cancellation_token is not None and getattr(cancellation_token, "is_cancelled", False):
        reason = getattr(cancellation_token, "reason", None) or "aborted"
        return _build_diag(
            state="blocked",
            diagnostic_code="workflow_cancelled",
            message=f"Workflow run '{clean_run}' has been cancelled ({reason}); execution is blocked.",
            is_blocked=True,
            is_stale=False,
            can_execute=False,
        )

    if store and clean_run and hasattr(store, "get_run_summary"):
        try:
            summary = store.get_run_summary(clean_run)
            if summary and str(summary.get("status") or "").lower() == "cancelled":
                return _build_diag(
                    state="blocked",
                    diagnostic_code="workflow_cancelled",
                    message=f"Workflow run '{clean_run}' has been cancelled; execution is blocked.",
                    is_blocked=True,
                    is_stale=False,
                    can_execute=False,
                )
        except Exception:
            pass

    # 3. Check admission fencing
    effective_fence_token = (
        admission_fence_token if admission_fence_token is not None else fence_token
    )
    if active_lease is not None and effective_fence_token is not None:
        lease_fence = active_lease.get("fence_token")
        if lease_fence is not None and lease_fence != effective_fence_token:
            return _build_diag(
                state="blocked",
                diagnostic_code="admission_fencing_lost",
                message=(
                    f"Worker fence token ({effective_fence_token}) does not match active lease "
                    f"token ({lease_fence}) for run '{clean_run}'; execution is fenced."
                ),
                is_blocked=True,
                is_stale=False,
                can_execute=False,
            )

    if store and clean_run and owner_id and hasattr(store, "validate_admission_ownership"):
        try:
            is_owner = store.validate_admission_ownership(
                clean_run, owner_id, effective_fence_token or 1
            )
            if not is_owner:
                return _build_diag(
                    state="blocked",
                    diagnostic_code="admission_fencing_lost",
                    message=(
                        f"Worker '{owner_id}' does not hold valid admission ownership for run "
                        f"'{clean_run}' (fence token {effective_fence_token or 1}); execution is fenced."
                    ),
                    is_blocked=True,
                    is_stale=False,
                    can_execute=False,
                )
        except Exception:
            pass

    # 4. If no approval record exists
    if not approval_rec:
        return _build_diag(
            state="missing",
            diagnostic_code="missing_approval",
            message=(
                f"No approval record found for tool '{resolved_tool_name}' (operation: '{clean_op}'). "
                "Human approval is required before execution."
            ),
            is_blocked=True,
            is_stale=False,
            can_execute=False,
            risk_level=current_risk,
        )

    # 5. Check revoked state
    if dec == "revoked":
        return _build_diag(
            state="revoked",
            diagnostic_code="revoked_approval",
            message=(
                f"Approval for tool '{resolved_tool_name}' was revoked by '{revoked_by or 'operator'}': "
                f"{revocation_reason or 'No reason provided'}."
            ),
            is_blocked=True,
            is_stale=False,
            can_execute=False,
            risk_level=bound_risk or current_risk,
        )

    # 6. Check rejected state
    if dec == "rejected":
        rej_reason = approval_rec.get("reason") or "No justification provided"
        return _build_diag(
            state="rejected",
            diagnostic_code="rejected_approval",
            message=(
                f"Tool '{resolved_tool_name}' execution was explicitly rejected by "
                f"'{approver or 'operator'}': {rej_reason}."
            ),
            is_blocked=True,
            is_stale=False,
            can_execute=False,
            risk_level=bound_risk or current_risk,
            revoked_by_override=None,
            revocation_reason_override=None,
            revoked_at_override=None,
        )

    # 7. Check expired state
    if dec == "expired" or is_expired_flag:
        return _build_diag(
            state="expired",
            diagnostic_code="expired_approval",
            message=(
                f"Approval for tool '{resolved_tool_name}' has expired "
                f"(expires_at: {expires_at or 'unknown'}). Fresh human approval is required."
            ),
            is_blocked=True,
            is_stale=True,
            can_execute=False,
            risk_level=bound_risk or current_risk,
            is_expired_override=True,
        )

    # 8. Check pending state
    if dec == "pending":
        return _build_diag(
            state="pending",
            diagnostic_code="pending_approval",
            message=(
                f"Tool '{resolved_tool_name}' execution is pending human approval "
                f"(operation: '{clean_op}')."
            ),
            is_blocked=True,
            is_stale=False,
            can_execute=False,
            risk_level=bound_risk or current_risk,
            approver_override=None,
            revoked_by_override=None,
            revocation_reason_override=None,
            revoked_at_override=None,
        )

    # 8. Check approved state: verify validity and freshness
    if dec == "approved":
        # a. Verify approver authorization
        if approver:
            try:
                verify_approver_authorization(approver, resolved_tool_name, policy)
            except ToolApproverUnauthorizedError as unauth_err:
                return _build_diag(
                    state="approved",
                    diagnostic_code="unauthorized_approver",
                    message=(
                        f"Approver '{approver}' lacks authority to approve tool "
                        f"'{resolved_tool_name}': {unauth_err}."
                    ),
                    is_blocked=True,
                    is_stale=True,
                    can_execute=False,
                    risk_level=bound_risk or current_risk,
                    revoked_by_override=None,
                    revocation_reason_override=None,
                    revoked_at_override=None,
                )

        # b. Check argument fingerprint mismatch
        if bound_fp and current_fp and bound_fp != current_fp:
            return _build_diag(
                state="approved",
                diagnostic_code="argument_fingerprint_mismatch",
                message=(
                    f"Argument fingerprint mismatch for tool '{resolved_tool_name}': "
                    f"bound fingerprint '{bound_fp}' does not match current arguments '{current_fp}'. "
                    "New approval required."
                ),
                is_blocked=True,
                is_stale=True,
                can_execute=False,
                risk_level=bound_risk or current_risk,
                revoked_by_override=None,
                revocation_reason_override=None,
                revoked_at_override=None,
            )

        # c. Check policy risk mismatch
        if bound_risk and current_risk and bound_risk != current_risk:
            return _build_diag(
                state="approved",
                diagnostic_code="policy_risk_mismatch",
                message=(
                    f"Tool risk level changed from '{bound_risk}' to '{current_risk}' "
                    "after approval was recorded. New approval required."
                ),
                is_blocked=True,
                is_stale=True,
                can_execute=False,
                risk_level=bound_risk,
                revoked_by_override=None,
                revocation_reason_override=None,
                revoked_at_override=None,
            )

        # d. Check policy hash mismatch
        if bound_phash and current_phash and bound_phash != current_phash:
            return _build_diag(
                state="approved",
                diagnostic_code="policy_hash_mismatch",
                message=(
                    "Tool policy definition changed after approval was recorded. "
                    "New approval required."
                ),
                is_blocked=True,
                is_stale=True,
                can_execute=False,
                risk_level=bound_risk or current_risk,
                revoked_by_override=None,
                revocation_reason_override=None,
                revoked_at_override=None,
            )

        # e. Approved, fresh, matching, and authorized
        return _build_diag(
            state="approved",
            diagnostic_code="valid_approved",
            message=(
                f"Operation '{clean_op}' has valid, matching approval from '{approver}' "
                "and is authorized to execute."
            ),
            is_blocked=False,
            is_stale=False,
            can_execute=True,
            risk_level=bound_risk or current_risk,
            revoked_by_override=None,
            revocation_reason_override=None,
            revoked_at_override=None,
        )

    # Unknown decision fallback
    return _build_diag(
        state=dec or "unknown",
        diagnostic_code="unknown_state",
        message=f"Operation '{clean_op}' has unrecognized decision status '{dec}'.",
        is_blocked=True,
        is_stale=True,
        can_execute=False,
        risk_level=bound_risk or current_risk,
    )


def _check_type(name: str, key: str, val: Any, expected_type: str) -> None:
    if expected_type == "string":
        if not isinstance(val, str):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be string, got {type(val).__name__}"
            )
    elif expected_type in ("integer", "int"):
        if not isinstance(val, int) or isinstance(val, bool):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be integer, got {type(val).__name__}"
            )
    elif expected_type in ("number", "float"):
        if not isinstance(val, (int, float)) or isinstance(val, bool):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be number, got {type(val).__name__}"
            )
    elif expected_type in ("boolean", "bool"):
        if not isinstance(val, bool):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be boolean, got {type(val).__name__}"
            )
    elif expected_type in ("array", "list"):
        if not isinstance(val, list):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be list, got {type(val).__name__}"
            )
    elif expected_type in ("object", "dict"):
        if not isinstance(val, dict):
            raise ToolValidationError(
                f"Argument '{key}' for tool '{name}' must be dict, got {type(val).__name__}"
            )


def validate_schema(name: str, schema: dict, kwargs: dict) -> None:
    """Validate kwargs against a tool's JSON schema subset."""
    if not schema or not isinstance(schema, dict):
        return

    # Check required properties
    required = schema.get("required", [])
    for prop in required:
        if prop not in kwargs:
            raise ToolValidationError(f"Missing required argument '{prop}' for tool '{name}'")
        if kwargs[prop] is None:
            raise ToolValidationError(f"Argument '{prop}' cannot be None for tool '{name}'")

    properties = schema.get("properties", {})
    if properties:
        for key, val in kwargs.items():
            if key not in properties:
                if schema.get("additionalProperties") is False:
                    raise ToolValidationError(f"Unexpected argument '{key}' for tool '{name}'")
                continue

            expected_type = properties[key].get("type")
            if expected_type:
                _check_type(name, key, val, expected_type)


@dataclass
class _Bucket:
    rate_per_min: int
    allowance: float = field(init=False)
    last: float = field(default=0.0)

    def __post_init__(self) -> None:
        self.allowance = float(self.rate_per_min)

    def take(self, now: float) -> bool:
        if self.last == 0.0:
            self.last = now
        self.allowance += (now - self.last) * (self.rate_per_min / 60.0)
        self.last = now
        self.allowance = min(self.allowance, float(self.rate_per_min))
        if self.allowance < 1.0:
            return False
        self.allowance -= 1.0
        return True


@dataclass
class Tool:
    name: str
    fn: Callable[..., str]
    schema: dict
    rate_per_min: int = 60
    # Tools that can cause external effects (writes, payments, deploys, comms)
    # must be human-approved unless explicitly allow-listed (VA-02).
    side_effecting: bool = False
    approved: bool = False
    timeout_seconds: float | None = 30.0
    max_retries: int = 0
    idempotency_mode: IdempotencyMode | str = IdempotencyMode.READ_ONLY
    supports_idempotency_key: bool = False
    retry_on_uncertain: bool = False
    policy: ToolPolicy | None = None
    risk_level: RiskLevel | str = RiskLevel.LOW
    allowed_agents: list[str] = field(default_factory=lambda: ["*"])
    requires_approval: bool | None = None
    authorized_approvers: list[str] = field(default_factory=lambda: ["*"])
    _registry: Any = field(default=None, repr=False)
    _bucket: _Bucket = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._bucket = _Bucket(self.rate_per_min)
        if isinstance(self.idempotency_mode, str):
            try:
                self.idempotency_mode = IdempotencyMode(self.idempotency_mode)
            except ValueError:
                self.idempotency_mode = IdempotencyMode.READ_ONLY

        # Backward compatibility and safety defaults
        if self.side_effecting and self.idempotency_mode == IdempotencyMode.READ_ONLY:
            self.idempotency_mode = IdempotencyMode.MUTATING_NON_IDEMPOTENT
        elif self.idempotency_mode in (
            IdempotencyMode.MUTATING_IDEMPOTENT,
            IdempotencyMode.MUTATING_NON_IDEMPOTENT,
        ):
            self.side_effecting = True

        if self.idempotency_mode == IdempotencyMode.MUTATING_IDEMPOTENT:
            self.supports_idempotency_key = True

        if isinstance(self.risk_level, str):
            try:
                self.risk_level = RiskLevel(self.risk_level.lower())
            except ValueError:
                self.risk_level = RiskLevel.LOW

        if self.policy is not None:
            self.risk_level = self.policy.risk_level
            self.allowed_agents = list(self.policy.allowed_agents)
            self.requires_approval = self.policy.requires_approval
            self.authorized_approvers = list(self.policy.authorized_approvers)
        else:
            req_app = self.requires_approval
            if req_app is None:
                if self.risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL):
                    req_app = True
                else:
                    req_app = False
            self.requires_approval = req_app
            self.policy = ToolPolicy(
                tool_name=self.name,
                risk_level=self.risk_level,
                allowed_agents=list(self.allowed_agents),
                requires_approval=self.requires_approval,
                authorized_approvers=list(self.authorized_approvers),
            )

    def __call__(self, context: ToolExecutionContext | None = None, **kwargs) -> str:
        # Step 1: Argument Schema Validation
        validate_schema(self.name, self.schema, kwargs)

        # Step 2: Rate Limit Verification
        now = time.monotonic()
        if not self._bucket.take(now):
            raise ToolRateLimitError(f"rate limit exceeded for tool '{self.name}'")

        # Step 3: Effective Policy Resolution & Agent Permission Gate
        policy = None
        if self._registry is not None and hasattr(self._registry, "get_policy"):
            policy = self._registry.get_policy(self.name)
        if policy is None:
            policy = self.policy
        if policy is None:
            policy = ToolPolicy(
                tool_name=self.name,
                risk_level=(
                    self.risk_level
                    if isinstance(self.risk_level, RiskLevel)
                    else RiskLevel(self.risk_level)
                ),
                allowed_agents=list(self.allowed_agents),
                requires_approval=bool(self.requires_approval),
            )

        agent_id = context.agent_id if context else None
        if policy.allowed_agents and "*" not in policy.allowed_agents:
            if not agent_id or agent_id not in policy.allowed_agents:
                denial_reason = (
                    f"Agent '{agent_id or 'unspecified'}' is not authorized to invoke "
                    f"tool '{self.name}'. Allowed agents: {policy.allowed_agents}"
                )
                if context and context.store:
                    try:
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_POLICY_DENIED,
                            outcome="denied",
                            run_id=context.run_id,
                            actor=agent_id or "unspecified",
                            metadata={
                                "tool_name": self.name,
                                "reason": denial_reason,
                                "allowed_agents": policy.allowed_agents,
                                "risk_level": (
                                    policy.risk_level.value
                                    if isinstance(policy.risk_level, RiskLevel)
                                    else str(policy.risk_level)
                                ),
                            },
                            store=context.store,
                        )
                    except Exception:
                        pass
                raise ToolPermissionDeniedError(denial_reason)

        # Step 4: Operation Key Derivation
        op_key: str | None = None
        if context and context.run_id:
            op_key = context.operation_key or derive_operation_key(
                context.run_id,
                context.subtask_id,
                self.name,
                kwargs,
                context.fence_token,
            )

        # Step 5: Cancellation Gate (Pre-Execution)
        if context and context.cancellation_token and context.cancellation_token.is_cancelled:
            raise ToolExecutionError(
                f"Tool execution cancelled before dispatch: {context.cancellation_token.reason}"
            )

        # Step 6: Fencing Gate (Pre-Execution)
        if context and context.store and context.run_id and context.owner_id:
            is_valid = context.store.validate_admission_ownership(
                context.run_id, context.owner_id, context.fence_token
            )
            if not is_valid:
                raise ToolFencingError(
                    f"Worker '{context.owner_id}' does not hold valid admission ownership "
                    f"for run '{context.run_id}' with fence token {context.fence_token}"
                )

        # Step 7: Risk-Based Approval Verification
        needs_approval = (
            policy.requires_approval
            or policy.risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL)
        ) and not self.approved

        if needs_approval:
            approval_record: dict[str, Any] | None = None
            if context and context.store and op_key:
                approval_record = context.store.get_tool_approval(op_key)
            if approval_record is None and context and context.approval:
                approval_record = {
                    "decision": str(context.approval.decision).lower().strip(),
                    "approver": context.approval.approver,
                    "reason": context.approval.reason,
                    "argument_fingerprint": context.approval.argument_fingerprint,
                    "risk_level": context.approval.risk_level,
                    "policy_hash": context.approval.policy_hash,
                    "revoked_by": context.approval.revoked_by,
                    "revocation_reason": context.approval.revocation_reason,
                    "revoked_at": context.approval.revoked_at,
                    "expires_at": context.approval.expires_at,
                    "expired_at": context.approval.expired_at,
                    "escalated_at": context.approval.escalated_at,
                    "escalation_reason": context.approval.escalation_reason,
                }

            if approval_record is None:
                r_val = (
                    policy.risk_level.value
                    if isinstance(policy.risk_level, RiskLevel)
                    else str(policy.risk_level)
                )
                curr_fp = compute_argument_fingerprint(kwargs)
                curr_phash = compute_policy_hash(policy)

                has_req_fn = (
                    hasattr(context.store, "record_tool_approval_request")
                    if context.store
                    else False
                )
                if context and context.store and op_key and has_req_fn:
                    try:
                        context.store.record_tool_approval_request(
                            run_id=context.run_id,
                            operation_key=op_key,
                            tool_name=self.name,
                            argument_fingerprint=curr_fp,
                            risk_level=r_val,
                            policy_hash=curr_phash,
                            reason=f"Action requires approval (risk level: {r_val})",
                            timeout_seconds=getattr(policy, "approval_timeout_seconds", None),
                        )
                    except Exception:
                        pass

                if context and context.store:
                    try:
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_APPROVAL_REQUIRED,
                            outcome="approval_required",
                            run_id=context.run_id,
                            actor=agent_id or "unspecified",
                            metadata={
                                "tool_name": self.name,
                                "operation_key": op_key,
                                "risk_level": r_val,
                                "argument_fingerprint": curr_fp,
                            },
                            store=context.store,
                        )
                    except Exception:
                        pass
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' (risk level: {r_val}) "
                    f"requires human approval before execution (operation key: {op_key or 'none'})"
                )

            dec = str(approval_record.get("decision") or "").strip().lower()
            from ..persistence.store import is_approval_expired

            if dec == "expired" or is_approval_expired(approval_record):
                if (
                    dec != "expired"
                    and context
                    and context.store
                    and op_key
                    and hasattr(context.store, "expire_tool_approval")
                ):
                    try:
                        context.store.expire_tool_approval(
                            context.run_id,
                            op_key,
                            tool_name=self.name,
                            reason="Approval expired before tool invocation",
                        )
                    except Exception:
                        pass
                if context and context.store:
                    try:
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_APPROVAL_EXPIRED,
                            outcome="expired",
                            run_id=context.run_id,
                            actor=approval_record.get("approver") or "system",
                            metadata={
                                "tool_name": self.name,
                                "operation_key": op_key,
                                "expires_at": approval_record.get("expires_at"),
                                "expired_at": approval_record.get("expired_at"),
                            },
                            store=context.store,
                        )
                    except Exception:
                        pass
                exp_at = approval_record.get("expires_at")
                exp_detail = f"Approval for operation '{op_key}' expired"
                if exp_at:
                    exp_detail += f" at {exp_at}"
                raise ToolApprovalExpiredError(
                    f"{exp_detail} and cannot authorize tool '{self.name}' execution."
                )
            if dec == "pending":
                r_val = (
                    policy.risk_level.value
                    if isinstance(policy.risk_level, RiskLevel)
                    else str(policy.risk_level)
                )
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' (risk level: {r_val}) "
                    f"requires human approval before execution (operation key: {op_key or 'none'}). "
                    "Current approval status is 'pending'."
                )
            if dec == "revoked":
                rev_by = approval_record.get("revoked_by") or "operator"
                rev_reason = approval_record.get("revocation_reason") or "revoked"
                raise ToolPermissionDeniedError(
                    f"Tool execution '{op_key}' approval was revoked by '{rev_by}': {rev_reason}"
                )

            if dec == "rejected":
                rej_reason = (
                    f"Tool execution '{op_key}' for tool '{self.name}' was explicitly rejected "
                    f"by approver '{approval_record.get('approver')}': {approval_record.get('reason')}"
                )
                if context and context.store:
                    try:
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_POLICY_DENIED,
                            outcome="rejected",
                            run_id=context.run_id,
                            actor=approval_record.get("approver") or "operator",
                            metadata={
                                "tool_name": self.name,
                                "operation_key": op_key,
                                "approver": approval_record.get("approver"),
                                "reason": approval_record.get("reason"),
                                "risk_level": (
                                    policy.risk_level.value
                                    if isinstance(policy.risk_level, RiskLevel)
                                    else str(policy.risk_level)
                                ),
                            },
                            store=context.store,
                        )
                    except Exception:
                        pass
                raise ToolPermissionDeniedError(rej_reason)

            if dec != "approved":
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' requires explicit approval decision 'approved', got '{dec}'"
                )

            # Approver Authorization Check (Requirement 3)
            approver_name = approval_record.get("approver")
            try:
                verify_approver_authorization(approver_name, self.name, policy)
            except ToolApproverUnauthorizedError as unauth_err:
                if context and context.store:
                    try:
                        from ..observability.audit import AuditEventType, record_audit_event

                        record_audit_event(
                            event_type=AuditEventType.TOOL_APPROVER_DENIED,
                            outcome="denied",
                            run_id=context.run_id,
                            actor=approver_name or "unknown",
                            metadata={
                                "tool_name": self.name,
                                "operation_key": op_key,
                                "reason": str(unauth_err),
                            },
                            store=context.store,
                        )
                    except Exception:
                        pass
                raise

            # Argument Fingerprint Binding Check (Requirement 2)
            curr_fp = compute_argument_fingerprint(kwargs)
            bound_fp = approval_record.get("argument_fingerprint")
            if bound_fp and bound_fp != curr_fp:
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' approval argument fingerprint mismatch: expected "
                    f"'{bound_fp}', got '{curr_fp}'. Arguments have changed; new approval required."
                )

            # Policy and Risk Level Freshness Check (Requirement 2)
            bound_risk = approval_record.get("risk_level")
            curr_risk = (
                policy.risk_level.value
                if isinstance(policy.risk_level, RiskLevel)
                else str(policy.risk_level)
            )
            if bound_risk and bound_risk != curr_risk:
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' policy risk level changed from '{bound_risk}' to "
                    f"'{curr_risk}' after approval; new approval required."
                )

            bound_phash = approval_record.get("policy_hash")
            curr_phash = compute_policy_hash(policy)
            if bound_phash and bound_phash != curr_phash:
                raise ToolApprovalRequiredError(
                    f"Tool '{self.name}' policy definition changed after approval; "
                    "new approval required."
                )

            # Revalidate immediately before dispatch after approval (Requirement 4):
            # 1. Cancellation token
            if context and context.cancellation_token and context.cancellation_token.is_cancelled:
                raise ToolExecutionError(
                    f"Tool execution cancelled post-approval before dispatch: "
                    f"{context.cancellation_token.reason}"
                )
            # 2. Agent Authorization
            if policy.allowed_agents and "*" not in policy.allowed_agents:
                if not agent_id or agent_id not in policy.allowed_agents:
                    raise ToolPermissionDeniedError(
                        f"Agent '{agent_id or 'unspecified'}' is not authorized to dispatch "
                        f"approved tool '{self.name}'"
                    )
            # 3. Admission Ownership (Fencing)
            if context and context.store and context.run_id and context.owner_id:
                is_valid = context.store.validate_admission_ownership(
                    context.run_id, context.owner_id, context.fence_token
                )
                if not is_valid:
                    raise ToolFencingError(
                        f"Worker '{context.owner_id}' lost admission ownership post-approval for run "
                        f"'{context.run_id}' with fence token {context.fence_token}"
                    )

        # Step 8: Idempotency Record Check (Pre-Execution)
        if context and context.store and op_key and self.idempotency_mode != IdempotencyMode.READ_ONLY:
            prior = context.store.get_tool_execution(op_key)
            if prior:
                prior_status = prior.get("status")
                if prior_status == "completed":
                    logger.info(
                        "Tool '%s' returning recorded completed result for op %s", self.name, op_key
                    )
                    return str(prior.get("result_text") or "")
                if prior_status == "uncertain":
                    if not self.retry_on_uncertain:
                        raise ToolUncertainOutcomeError(
                            f"Tool '{self.name}' has uncertain prior execution ({op_key}) "
                            f"and cannot be safely re-executed: {prior.get('error_text')}"
                        )
                elif prior_status == "pending":
                    if self.idempotency_mode == IdempotencyMode.MUTATING_NON_IDEMPOTENT:
                        raise ToolDuplicateExecutionError(
                            f"Tool '{self.name}' has an in-flight non-idempotent execution ({op_key})"
                        )
                elif prior_status == "failed":
                    is_mutating_non_idem = (
                        self.idempotency_mode == IdempotencyMode.MUTATING_NON_IDEMPOTENT
                    )
                    if is_mutating_non_idem or prior.get("reconciled_by"):
                        err_detail = prior.get("error_text") or ""
                        rec_reason = prior.get("reconciliation_reason") or ""
                        summary_parts = [p for p in (err_detail, rec_reason) if p]
                        reason_summary = " - ".join(summary_parts) or "unknown error"
                        raise ToolExecutionError(
                            f"Tool '{self.name}' prior execution failed or was reconciled as "
                            f"failed ({op_key}): {reason_summary}"
                        )

        # Step 9: Dispatch Record (Pre-Execution)
        if context and context.store and op_key and self.idempotency_mode != IdempotencyMode.READ_ONLY:
            context.store.record_tool_dispatch(
                operation_key=op_key,
                run_id=context.run_id,
                subtask_id=context.subtask_id,
                tool_name=self.name,
                fence_token=context.fence_token,
                args=kwargs,
            )

        # Step 10: Downstream Execution with Timeout and Retries
        if self.idempotency_mode == IdempotencyMode.MUTATING_NON_IDEMPOTENT:
            effective_retries = 0
        else:
            effective_retries = max(0, self.max_retries)

        fn_kwargs = dict(kwargs)
        if self.supports_idempotency_key and op_key:
            sig_params = inspect.signature(self.fn).parameters
            accepts_var_kw = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig_params.values())
            if "idempotency_key" in sig_params or accepts_var_kw:
                fn_kwargs["idempotency_key"] = op_key

        attempts = 0
        result_str: str | None = None
        while True:
            attempts += 1
            if context and context.cancellation_token and context.cancellation_token.is_cancelled:
                raise ToolExecutionError(
                    f"Tool execution cancelled: {context.cancellation_token.reason}"
                )

            # Pre-dispatch revalidation (Requirement 5): Check for concurrent
            # revocation, rejection, or expiration
            if needs_approval and context and context.store and op_key:
                fresh_app = context.store.get_tool_approval(op_key)
                if fresh_app:
                    f_dec = str(fresh_app.get("decision") or "").strip().lower()
                    if f_dec == "revoked":
                        raise ToolPermissionDeniedError(
                            f"Tool execution '{op_key}' approval was revoked before dispatch: "
                            f"{fresh_app.get('revocation_reason') or 'revoked by approver'}"
                        )
                    if f_dec == "rejected":
                        raise ToolPermissionDeniedError(
                            f"Tool execution '{op_key}' approval was rejected before dispatch: "
                            f"{fresh_app.get('reason') or 'rejected'}"
                        )
                    from ..persistence.store import is_approval_expired

                    if f_dec == "expired" or is_approval_expired(fresh_app):
                        if (
                            f_dec != "expired"
                            and hasattr(context.store, "expire_tool_approval")
                        ):
                            try:
                                context.store.expire_tool_approval(
                                    context.run_id,
                                    op_key,
                                    tool_name=self.name,
                                    reason="Approval expired before tool dispatch",
                                )
                            except Exception:
                                pass
                        exp_at = fresh_app.get("expires_at")
                        raise ToolApprovalExpiredError(
                            f"Tool execution '{op_key}' approval expired (expires_at: {exp_at}) "
                            f"before dispatch and cannot authorize execution"
                        )

            try:
                if self.timeout_seconds and self.timeout_seconds > 0:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self.fn, **fn_kwargs)
                        result_str = future.result(timeout=self.timeout_seconds)
                else:
                    result_str = self.fn(**fn_kwargs)
                break
            except concurrent.futures.TimeoutError as te:
                if self.idempotency_mode in (
                    IdempotencyMode.MUTATING_IDEMPOTENT,
                    IdempotencyMode.MUTATING_NON_IDEMPOTENT,
                ):
                    if context and context.store and op_key:
                        try:
                            context.store.record_tool_failure(
                                op_key,
                                error_text=(
                                    f"Tool '{self.name}' timed out after "
                                    f"{self.timeout_seconds}s (uncertain outcome)"
                                ),
                                uncertain=True,
                                owner_id=context.owner_id,
                                fence_token=context.fence_token,
                            )
                        except Exception as rec_err:
                            logger.warning("Failed to record tool failure: %s", rec_err)
                    raise ToolUncertainOutcomeError(
                        f"Tool '{self.name}' timed out after {self.timeout_seconds}s with uncertain outcome"
                    ) from te

                if attempts > effective_retries:
                    if context and context.store and op_key:
                        try:
                            context.store.record_tool_failure(
                                op_key,
                                error_text=f"Tool '{self.name}' timed out after {self.timeout_seconds}s",
                                uncertain=False,
                                owner_id=context.owner_id,
                                fence_token=context.fence_token,
                            )
                        except Exception:
                            pass
                    raise ToolTimeoutError(
                        f"Tool '{self.name}' timed out after {self.timeout_seconds}s"
                    ) from te

            except Exception as exc:
                err_cls = type(exc).__name__.lower()
                is_uncertain_drop = any(
                    kw in err_cls for kw in ("connection", "remotedisconnected", "brokenpipe")
                )
                if is_uncertain_drop and self.idempotency_mode in (
                    IdempotencyMode.MUTATING_IDEMPOTENT,
                    IdempotencyMode.MUTATING_NON_IDEMPOTENT,
                ):
                    if context and context.store and op_key:
                        try:
                            context.store.record_tool_failure(
                                op_key,
                                error_text=(
                                    f"Tool '{self.name}' failed with {type(exc).__name__}: "
                                    f"{exc} (uncertain outcome)"
                                ),
                                uncertain=True,
                                owner_id=context.owner_id,
                                fence_token=context.fence_token,
                            )
                        except Exception:
                            pass
                    raise ToolUncertainOutcomeError(
                        f"Tool '{self.name}' failed with {type(exc).__name__} ({exc}) "
                        "causing uncertain outcome"
                    ) from exc

                if attempts > effective_retries:
                    if context and context.store and op_key:
                        try:
                            context.store.record_tool_failure(
                                op_key,
                                error_text=str(exc),
                                uncertain=False,
                                owner_id=context.owner_id,
                                fence_token=context.fence_token,
                            )
                        except Exception:
                            pass
                    raise

        # Step 11: Fencing Gate (Post-Execution, Pre-Commit)
        if context and context.store and context.run_id and context.owner_id:
            is_valid = context.store.validate_admission_ownership(
                context.run_id, context.owner_id, context.fence_token
            )
            if not is_valid:
                raise ToolFencingError(
                    f"Worker '{context.owner_id}' lost admission ownership for run '{context.run_id}' "
                    f"before recording completion of tool '{self.name}'"
                )

        # Step 12: Completion Record
        if context and context.store and op_key:
            try:
                context.store.record_tool_completion(
                    operation_key=op_key,
                    result_text=str(result_str),
                    owner_id=context.owner_id,
                    fence_token=context.fence_token,
                )
            except AdmissionFencingError as fence_err:
                raise ToolFencingError(
                    f"Worker '{context.owner_id}' lost admission ownership during completion: {fence_err}"
                ) from fence_err

        # Step 13: Return Result
        return str(result_str)


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}
        self._policies: dict[str, ToolPolicy] = {}

    def register(self, t: Tool) -> None:
        t._registry = self
        self._tools[t.name] = t
        if t.policy:
            self._policies[t.name] = t.policy

    def set_policy(self, policy: ToolPolicy) -> None:
        """Assign or update a tool permission policy."""
        self._policies[policy.tool_name] = policy
        if policy.tool_name in self._tools:
            t = self._tools[policy.tool_name]
            t.policy = policy
            t.risk_level = policy.risk_level
            t.allowed_agents = list(policy.allowed_agents)
            t.requires_approval = policy.requires_approval
            t.authorized_approvers = list(policy.authorized_approvers)

    def get_policy(self, name: str) -> ToolPolicy | None:
        """Get the configured policy for a tool, if any."""
        return self._policies.get(name)

    def call(self, name: str, context: ToolExecutionContext | None = None, **kwargs) -> str:
        """Invoke a tool by name, validating arguments, enforcing rate limits, and checking idempotency."""
        if name not in self._tools:
            raise ToolNotFoundError(f"unknown tool: {name}")
        return self._tools[name](context=context, **kwargs)

    def execute(
        self,
        name: str,
        enforce_approval: bool = True,
        context: ToolExecutionContext | None = None,
        **kwargs,
    ) -> str:
        """Invoke a tool with explicit HITL approval verification and idempotency tracking."""
        if name not in self._tools:
            raise ToolNotFoundError(f"unknown tool: {name}")
        t = self._tools[name]
        if enforce_approval and t.side_effecting and not t.approved:
            raise ToolApprovalError(
                f"Tool '{name}' is side-effecting and requires human approval before execution"
            )
        return t(context=context, **kwargs)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    def requires_approval(self, name: str) -> bool:
        """True if using this tool must be gated by human approval.

        Default-deny: an unknown tool, or a tool requiring approval/high risk,
        or a side-effecting tool that has not been explicitly allow-listed, requires approval.
        """
        t = self._tools.get(name)
        if t is None:
            return True
        policy = self.get_policy(name) or t.policy
        if policy:
            if policy.requires_approval or policy.risk_level in (RiskLevel.HIGH, RiskLevel.CRITICAL):
                return not t.approved
        return (t.side_effecting and not t.approved) or False

    def schemas(self) -> list[dict]:
        return [
            {
                "name": t.name,
                "schema": t.schema,
                "rate_per_min": t.rate_per_min,
                "side_effecting": t.side_effecting,
                "approved": t.approved,
                "idempotency_mode": str(
                    t.idempotency_mode.value
                    if isinstance(t.idempotency_mode, IdempotencyMode)
                    else t.idempotency_mode
                ),
                "supports_idempotency_key": t.supports_idempotency_key,
                "risk_level": (
                    t.risk_level.value
                    if isinstance(t.risk_level, RiskLevel)
                    else str(t.risk_level)
                ),
                "allowed_agents": list(t.allowed_agents),
                "requires_approval": t.requires_approval,
                "authorized_approvers": list(t.authorized_approvers),
            }
            for t in self._tools.values()
        ]


REGISTRY = ToolRegistry()


def tool(
    name: str,
    schema: dict,
    rate_per_min: int = 60,
    side_effecting: bool = False,
    approved: bool = False,
    timeout_seconds: float | None = 30.0,
    max_retries: int = 0,
    idempotency_mode: IdempotencyMode | str | None = None,
    supports_idempotency_key: bool = False,
    retry_on_uncertain: bool = False,
    policy: ToolPolicy | None = None,
    risk_level: RiskLevel | str = RiskLevel.LOW,
    allowed_agents: list[str] | None = None,
    requires_approval: bool | None = None,
    authorized_approvers: list[str] | None = None,
):
    """Decorator to register a function as a rate-limited tool."""

    def deco(fn: Callable[..., str]) -> Callable[..., str]:
        resolved_mode = idempotency_mode
        if resolved_mode is None:
            resolved_mode = (
                IdempotencyMode.MUTATING_NON_IDEMPOTENT if side_effecting else IdempotencyMode.READ_ONLY
            )
        REGISTRY.register(
            Tool(
                name=name,
                fn=fn,
                schema=schema,
                rate_per_min=rate_per_min,
                side_effecting=side_effecting,
                approved=approved,
                timeout_seconds=timeout_seconds,
                max_retries=max_retries,
                idempotency_mode=resolved_mode,
                supports_idempotency_key=supports_idempotency_key,
                retry_on_uncertain=retry_on_uncertain,
                policy=policy,
                risk_level=risk_level,
                allowed_agents=allowed_agents or ["*"],
                requires_approval=requires_approval,
                authorized_approvers=authorized_approvers or ["*"],
            )
        )
        return fn

    return deco


# ── built-in demo tools ──────────────────────────────────────────
@tool(
    name="web_search",
    schema={"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]},
    rate_per_min=30,
    side_effecting=False,
    timeout_seconds=10.0,
    max_retries=1,
)
def web_search(query: str) -> str:
    """Stub web search — replace with a real provider (Tavily, SerpAPI, etc.)."""
    return f"[web_search results for: {query}]"


@tool(
    name="calculator",
    schema={
        "type": "object",
        "properties": {"expression": {"type": "string"}},
        "required": ["expression"],
    },
    rate_per_min=120,
    side_effecting=False,
    timeout_seconds=5.0,
)
def calculator(expression: str) -> str:
    """Evaluate a basic arithmetic expression safely.

    Uses an AST walker rather than ``eval``: only +, -, *, /, %, and unary
    minus are permitted. Exponentiation (``**``) is deliberately rejected to
    avoid resource-exhaustion (e.g. ``9**9**9``).
    """
    return str(_safe_arith(expression))


def _safe_arith(expression: str):
    import ast

    _BIN = {
        ast.Add: lambda a, b: a + b,
        ast.Sub: lambda a, b: a - b,
        ast.Mult: lambda a, b: a * b,
        ast.Div: lambda a, b: a / b,
        ast.Mod: lambda a, b: a % b,
    }

    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            v = _eval(node.operand)
            return +v if isinstance(node.op, ast.UAdd) else -v
        if isinstance(node, ast.BinOp) and type(node.op) in _BIN:
            left = _eval(node.left)
            right = _eval(node.right)
            if type(node.op) in (ast.Div, ast.Mod) and right == 0:
                raise ValueError("division by zero")
            return _BIN[type(node.op)](left, right)
        raise ValueError("unsupported expression")

    try:
        tree = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError("invalid expression") from exc
    return _eval(tree)
