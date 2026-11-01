"""Pydantic request and response schemas for the orchestration HTTP API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    """Service health and readiness telemetry."""

    status: str = Field(default="healthy", description="Current service health state.")
    version: str = Field(description="Package version.")
    timestamp: str = Field(description="Current UTC timestamp.")
    checkpoint_backend: str = Field(default="sqlite", description="Durable checkpoint backend.")
    persistence_enabled: bool = Field(description="Whether checkpoint persistence is active.")
    active_llm_provider: str = Field(description="Configured active LLM provider.")
    active_runs_count: int = Field(default=0, description="Number of currently executing runs in-process.")
    auth_required: bool = Field(default=False, description="Whether API token authentication is enforced.")


class ReadyResponse(BaseModel):
    """Service readiness probe telemetry."""

    status: str = Field(description="Service readiness state ('ready' or 'not_ready').")
    timestamp: str = Field(description="Current UTC timestamp.")
    accepting_runs: bool = Field(
        description="Whether the service is currently accepting new workflow submissions.",
    )
    database: str = Field(
        description="Persistence backend health status ('healthy', 'unhealthy', or 'disabled').",
    )
    active_runs: int = Field(default=0, description="Number of currently executing runs in-process.")
    reason: str | None = Field(
        default=None,
        description="Human-readable explanation if service is not ready.",
    )



class CreateRunRequest(BaseModel):
    """Payload to initiate a new workflow run."""

    task: str = Field(description="The primary natural language task instruction.")
    plan: list[dict[str, Any]] | None = Field(
        default=None,
        description="Optional pre-determined plan of specialist subtasks.",
    )
    max_concurrency: int | None = Field(
        default=None,
        ge=1,
        description="Optional bound on concurrent specialist workers for this run.",
    )
    run_id: str | None = Field(
        default=None,
        description="Optional custom identifier for run correlation.",
    )
    background: bool = Field(
        default=False,
        description="Execute asynchronously in background if True; block until complete if False.",
    )
    timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        le=86400,
        description="Optional execution deadline budget in seconds.",
    )
    execution_backend: str | None = Field(
        default=None,
        description="Optional execution backend: 'direct', 'thread', or 'celery'.",
    )


class ResumeRunRequest(BaseModel):
    """Payload to resume an interrupted, failed, or resolved workflow."""

    max_concurrency: int | None = Field(
        default=None,
        ge=1,
        description="Optional bound on concurrent specialist workers upon resumption.",
    )
    background: bool = Field(
        default=False,
        description="Execute asynchronously in background if True; block until complete if False.",
    )
    timeout_seconds: float | None = Field(
        default=None,
        gt=0,
        le=86400,
        description="Optional execution deadline budget in seconds for resumed execution.",
    )
    execution_backend: str | None = Field(
        default=None,
        description="Optional execution backend: 'direct', 'thread', or 'celery'.",
    )


class CancelRunRequest(BaseModel):
    """Payload to request cooperative cancellation of an active workflow run."""

    reason: str = Field(
        default="User requested cancellation",
        max_length=500,
        description="Optional human-readable explanation for cancellation.",
    )


class RunReviewSummary(BaseModel):
    """Review and quality metrics for a workflow execution."""

    quality: float | None = None
    confidence: float | None = None
    escalated: bool = False
    escalation_reason: str | None = None


class HitlStatusSummary(BaseModel):
    """Human-in-the-loop escalation and review status."""

    escalated: bool = False
    escalation_reason: str | None = None
    escalated_subtask_id: str | None = None
    pending_review: bool = False
    approval_level: str | None = None
    resolution: dict[str, Any] | None = None


class RunResponse(BaseModel):
    """Detailed workflow execution and result representation."""

    run_id: str = Field(description="Unique run correlation identifier.")
    task: str = Field(description="Original task instruction.")
    status: str = Field(description="Current lifecycle status (running, completed, partial, failed, etc.).")
    answer: str | None = Field(default=None, description="Synthesized final answer or partial result.")
    completed_subtasks: list[str] = Field(default_factory=list, description="IDs of completed subtasks.")
    failed_subtasks: list[str] = Field(default_factory=list, description="IDs of failed subtasks.")
    blocked_subtasks: list[str] = Field(default_factory=list, description="IDs of blocked subtasks.")
    latest_step: int | None = Field(default=None, description="Most recent recorded checkpoint step.")
    review: RunReviewSummary | None = None
    hitl: HitlStatusSummary | None = None
    usage: dict[str, Any] | None = None
    created_at: str | None = None
    updated_at: str | None = None


class CheckpointMetadata(BaseModel):
    """Safe, metadata-only view of a durable execution checkpoint."""

    step: int = Field(description="Monotonically increasing step number.")
    transition: str = Field(description="Lifecycle transition name at checkpoint capture.")
    schema_version: int = Field(description="Checkpoint schema compatibility version.")
    created_at: str = Field(description="UTC timestamp when checkpoint was recorded.")


class CheckpointsListResponse(BaseModel):
    """Metadata collection of historical checkpoints for a run."""

    run_id: str
    checkpoints: list[CheckpointMetadata]
    total_steps: int


class EvaluationCaseSummary(BaseModel):
    """Summary of a versioned evaluation test case."""

    case_id: str
    name: str
    description: str
    tags: list[str] = Field(default_factory=list)
    expected_status: str


class EvaluationSummaryResponse(BaseModel):
    """Evaluation framework dataset and latest summary telemetry."""

    dataset_version: str
    total_cases: int
    cases: list[EvaluationCaseSummary]
    latest_summary: dict[str, Any] | None = None


class ErrorDetail(BaseModel):
    """Structured, sanitized error payload."""

    detail: str
    error_code: str | None = None


class ApplicationDiagnostics(BaseModel):
    """Application-level diagnostics metadata."""

    version: str = Field(description="Package version.")
    status: str = Field(description="Current application status ('healthy' or 'shutting_down').")
    is_shutting_down: bool = Field(description="Whether application is currently shutting down.")
    started_at: str | None = Field(default=None, description="UTC timestamp when RunManager was initialized.")
    auth_required: bool = Field(default=False, description="Whether API token authentication is enforced.")


class ExecutionDiagnostics(BaseModel):
    """Execution engine and concurrency metrics."""

    active_runs_count: int = Field(default=0, description="Number of currently executing runs in-process.")
    max_active_runs: int = Field(
        default=10,
        description="Configured maximum number of concurrently active workflow runs permitted in-process.",
    )
    available_capacity: int = Field(
        default=10,
        description="Remaining workflow admission slots available before backpressure rejection.",
    )
    lifecycle_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Authoritative counts of runs across all lifecycle states from persistence.",
    )
    specialist_concurrency: int = Field(description="Bound on concurrent specialist workers.")
    max_task_chars: int = Field(description="Maximum permissible characters for task prompt.")
    workflow_timeout_seconds: float | None = Field(
        default=None,
        description="Global workflow deadline budget in seconds, if configured.",
    )
    global_active_runs_count: int | None = Field(
        default=None,
        description="Global active workflow runs across all processes when persistence is enabled.",
    )
    global_available_capacity: int | None = Field(
        default=None,
        description="Global available admission capacity across all processes when persistence is enabled.",
    )
    owner_id: str | None = Field(
        default=None,
        description="Identifier of the worker process owning this execution context.",
    )
    backend: str = Field(
        default="direct",
        description="Workflow execution backend: 'direct', 'thread', or 'celery'.",
    )
    celery: dict[str, Any] | None = Field(
        default=None,
        description="Celery background execution diagnostics if configured.",
    )


class PersistenceDiagnostics(BaseModel):
    """Persistence backend diagnostics."""

    enabled: bool = Field(description="Whether durable checkpointing is enabled.")
    backend: str = Field(default="sqlite", description="Storage backend type.")
    status: str = Field(description="Database connectivity status ('healthy', 'unhealthy', or 'disabled').")
    db_name: str = Field(
        description="Database file name or target (sanitized without sensitive host directories).",
    )
    detail: str | None = Field(default=None, description="Additional status detail or probe error message.")


class RuntimeLimitsDiagnostics(BaseModel):
    """Runtime limits and configuration parameters."""

    max_active_runs: int = Field(
        default=10,
        description="Configured maximum active workflow runs permitted globally.",
    )
    admission_lease_ttl_seconds: float = Field(
        default=30.0,
        description="Configured lease TTL in seconds for cross-process workflow admissions.",
    )
    admission_heartbeat_interval_seconds: float = Field(
        default=5.0,
        description="Configured interval in seconds between admission heartbeat renewals.",
    )
    shutdown_grace_seconds: float = Field(
        description="Configured grace period for graceful shutdown in seconds.",
    )
    recovery_on_startup: bool = Field(description="Whether startup recovery for interrupted runs is enabled.")
    rate_limit_enabled: bool = Field(description="Whether request rate limiting is enabled.")
    rate_limit_requests: int = Field(description="Maximum requests permitted within the rate limit window.")
    rate_limit_window_seconds: float = Field(description="Rate limit evaluation window in seconds.")
    max_body_bytes: int = Field(description="Maximum allowed request payload size in bytes.")
    max_body_mb: float = Field(description="Maximum allowed request payload size in megabytes.")


class RecoveryDiagnostics(BaseModel):
    """Startup recovery summary."""

    status: str = Field(description="Recovery status ('completed', 'skipped', 'disabled', or 'pending').")
    recovered_count: int = Field(default=0, description="Number of interrupted runs recovered on startup.")
    recovered_run_ids: list[str] = Field(default_factory=list, description="IDs of recovered runs.")
    timestamp: str | None = Field(default=None, description="UTC timestamp of the recovery run.")


class ApprovalSchedulerDiagnostics(BaseModel):
    """Automated approval lifecycle scheduler status and metrics."""

    enabled: bool = Field(description="Whether the automated approval scheduler is enabled.")
    running: bool = Field(
        default=False,
        description="Whether the background scheduler loop is currently running.",
    )
    scan_interval_seconds: float = Field(
        description="Configured interval in seconds between automated scans.",
    )
    last_scan_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent scan attempt.",
    )
    last_successful_scan_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent successful scan.",
    )
    next_scheduled_scan_at: str | None = Field(
        default=None,
        description="Estimated UTC timestamp of the next automated scan.",
    )
    total_scans: int = Field(
        default=0,
        description="Total number of scan cycles executed by this scheduler.",
    )
    total_expired: int = Field(
        default=0,
        description="Total approvals transitioned to expired by this scheduler.",
    )
    total_escalated: int = Field(
        default=0,
        description="Total approvals escalated by this scheduler.",
    )
    consecutive_failures: int = Field(
        default=0,
        description="Count of consecutive failed scan attempts.",
    )
    last_scan_failed_count: int = Field(
        default=0,
        description="Count of approvals that encountered errors in the latest scan cycle.",
    )
    last_error: str | None = Field(
        default=None,
        description="Sanitized error summary from most recent failed scan.",
    )
    is_leader: bool = Field(
        default=False,
        description="Whether this worker process currently holds the scheduler lease.",
    )
    leadership_state: str = Field(
        default="standby",
        description="Current leadership status: leader, standby, lost, or disabled.",
    )
    owner_id: str | None = Field(
        default=None,
        description="Unique worker instance identifier of this scheduler.",
    )
    lease_owner_id: str | None = Field(
        default=None,
        description="Identifier of the worker currently holding the database lease.",
    )
    lease_expires_at: str | None = Field(
        default=None,
        description="UTC timestamp when the current scheduler lease expires.",
    )
    lease_renewed_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent lease heartbeat/renewal.",
    )
    fence_token: int | None = Field(
        default=None,
        description="Active monotonic fencing generation token of the scheduler lease.",
    )
    lease_ttl_seconds: float = Field(
        default=30.0,
        description="Configured cross-process scheduler lease TTL in seconds.",
    )
    heartbeat_interval_seconds: float = Field(
        default=5.0,
        description="Configured interval in seconds between lease heartbeats.",
    )


class BackupSchedulerDiagnostics(BaseModel):
    """Automated database backup scheduler status and operational metrics."""

    enabled: bool = Field(description="Whether the automated backup scheduler is enabled.")
    running: bool = Field(
        default=False,
        description="Whether the background scheduler loop is currently running.",
    )
    interval_seconds: float = Field(
        description="Configured interval in seconds between automated backup runs.",
    )
    backup_dir: str = Field(description="Configured directory storing database backup archives.")
    retention_count: int = Field(description="Maximum number of backup archives to retain.")
    retention_days: int = Field(description="Maximum age in days of backup archives to retain.")
    max_duration_seconds: float = Field(
        default=300.0,
        description="Maximum allowed duration in seconds for a backup run before timing out.",
    )
    max_retries: int = Field(
        default=3,
        description="Maximum retry attempts after a failed backup execution.",
    )
    retry_backoff_seconds: float = Field(
        default=5.0,
        description="Initial backoff in seconds between retry attempts.",
    )
    max_backoff_seconds: float = Field(
        default=60.0,
        description="Maximum capped backoff in seconds between retry attempts.",
    )
    is_leader: bool = Field(
        default=False,
        description="Whether this worker process currently holds the scheduler lease.",
    )
    leadership_state: str = Field(
        default="standby",
        description="Current leadership status: leader, standby, lost, stopped, or disabled.",
    )
    owner_id: str | None = Field(
        default=None,
        description="Unique identifier of this scheduler worker instance.",
    )
    fence_token: int | None = Field(
        default=None,
        description="Active monotonic fencing generation token of the scheduler lease.",
    )
    last_run_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent backup execution attempt.",
    )
    last_success_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent successful backup run.",
    )
    last_failed_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent failed backup run.",
    )
    next_scheduled_run_at: str | None = Field(
        default=None,
        description="Estimated UTC timestamp of the next automated backup run.",
    )
    last_duration_ms: float = Field(
        default=0.0,
        description="Execution duration in milliseconds of the last backup run.",
    )
    last_backup_path: str | None = Field(
        default=None,
        description="File path of the most recently created backup archive.",
    )
    last_size_bytes: int = Field(
        default=0,
        description="Size in bytes of the most recently created backup archive.",
    )
    last_integrity: str | None = Field(
        default=None,
        description="Integrity check status result of the most recently created backup.",
    )
    last_tables_count: int = Field(
        default=0,
        description="Number of tables included in the most recently created backup.",
    )
    total_backups_count: int = Field(
        default=0,
        description="Total number of backups successfully completed by this scheduler.",
    )
    failure_count: int = Field(
        default=0,
        description="Total number of backup attempts that encountered errors.",
    )
    consecutive_failures: int = Field(
        default=0,
        description="Count of consecutive failed backup execution attempts.",
    )
    last_error: str | None = Field(
        default=None,
        description="Sanitized error summary from the most recent failed backup run.",
    )
    restore_drill_enabled: bool | None = Field(
        default=None,
        description="Whether automated restore drills are enabled.",
    )
    restore_drill_interval_seconds: float | None = Field(
        default=None,
        description="Interval in seconds between automated restore drills.",
    )
    last_drill_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent restore drill attempt.",
    )
    last_drill_success_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent successful restore drill.",
    )
    last_drill_failed_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent failed restore drill.",
    )
    last_drill_status: str | None = Field(
        default=None,
        description="Status of the most recent restore drill: success, failed, or skipped.",
    )
    last_drill_duration_ms: float | None = Field(
        default=None,
        description="Duration in milliseconds of the most recent restore drill.",
    )
    total_drills_count: int | None = Field(
        default=None,
        description="Total number of restore drills executed.",
    )
    drill_failure_count: int | None = Field(
        default=None,
        description="Total number of failed restore drills.",
    )


class DiagnosticsResponse(BaseModel):
    """Comprehensive runtime diagnostics snapshot."""

    timestamp: str = Field(description="Current UTC timestamp.")
    application: ApplicationDiagnostics
    execution: ExecutionDiagnostics
    persistence: PersistenceDiagnostics
    limits: RuntimeLimitsDiagnostics
    recovery: RecoveryDiagnostics
    approval_scheduler: ApprovalSchedulerDiagnostics | None = Field(
        default=None,
        description="Automated approval lifecycle scheduler diagnostics.",
    )
    backup_scheduler: BackupSchedulerDiagnostics | None = Field(
        default=None,
        description="Automated database backup scheduler diagnostics.",
    )


class AuditEventModel(BaseModel):
    """Structured audit event representation."""

    event_id: str
    timestamp: str
    event_type: str
    outcome: str
    run_id: str | None = None
    correlation_id: str | None = None
    actor: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunAuditResponse(BaseModel):
    """Paginated audit events response for a workflow run or system query."""

    run_id: str | None = None
    total_events: int
    limit: int
    offset: int
    events: list[AuditEventModel]


class AuditCleanupRequest(BaseModel):
    """Request payload to trigger audit event cleanup."""

    max_age_days: int | None = Field(
        default=None,
        description="Remove events older than this number of days. If unset, uses configured retention.",
    )
    before_timestamp: str | None = Field(
        default=None,
        description="Remove events older than this ISO UTC timestamp.",
    )
    limit: int | None = Field(
        default=None,
        description="Optional maximum number of records to purge in this execution.",
    )


class AuditCleanupResponse(BaseModel):
    """Response payload summarizing the audit retention cleanup operation."""

    success: bool
    deleted_count: int
    cutoff_timestamp: str | None = None
    max_age_days: int | None = None
    reason: str | None = None
    error: str | None = None


class ToolExecutionModel(BaseModel):
    """Sanitized representation of a tool execution record."""

    operation_key: str = Field(description="Unique deterministic operation key.")
    run_id: str = Field(description="Workflow run identifier.")
    subtask_id: str = Field(description="Subtask identifier that invoked the tool.")
    tool_name: str = Field(description="Name of the invoked tool.")
    fence_token: int = Field(default=1, description="Worker fencing token at execution time.")
    status: str = Field(
        description="Execution lifecycle status ('pending', 'completed', 'failed', 'uncertain').",
    )
    args: dict[str, Any] = Field(
        default_factory=dict,
        description="Sanitized arguments with secrets and credentials redacted.",
    )
    result_text: str | None = Field(
        default=None,
        description="Sanitized result output if completed.",
    )
    error_text: str | None = Field(
        default=None,
        description="Sanitized error details if failed or uncertain.",
    )
    created_at: str = Field(description="UTC timestamp when execution was dispatched.")
    updated_at: str = Field(description="UTC timestamp of latest status update.")
    reconciled_by: str | None = Field(
        default=None,
        description="Actor identity who reconciled an uncertain execution.",
    )
    reconciled_at: str | None = Field(
        default=None,
        description="UTC timestamp when reconciled.",
    )
    reconciliation_reason: str | None = Field(
        default=None,
        description="Explanation provided for the reconciliation decision.",
    )


class RunToolExecutionsResponse(BaseModel):
    """List of tool executions for a workflow run."""

    run_id: str = Field(description="Workflow run identifier.")
    total_executions: int = Field(description="Total executions matching query.")
    executions: list[ToolExecutionModel] = Field(
        default_factory=list,
        description="Sanitized execution records.",
    )


class ReconcileToolExecutionRequest(BaseModel):
    """Request payload to reconcile an uncertain tool execution."""

    outcome: str = Field(
        description="Verified outcome: 'completed' or 'failed'.",
    )
    reason: str = Field(
        min_length=3,
        max_length=1000,
        description="Mandatory explanation of the reconciliation decision.",
    )
    result_text: str | None = Field(
        default=None,
        description="Optional result payload if verified completed.",
    )
    error_text: str | None = Field(
        default=None,
        description="Optional error details if verified failed.",
    )
    operator: str | None = Field(
        default=None,
        description="Optional operator username or identifier.",
    )


class ReconcileToolExecutionResponse(BaseModel):
    """Response payload for a tool execution reconciliation action."""

    success: bool = Field(description="Whether reconciliation succeeded.")
    operation_key: str = Field(description="Operation key of the reconciled execution.")
    run_id: str = Field(description="Workflow run identifier.")
    previous_status: str = Field(description="Status prior to reconciliation.")
    status: str = Field(description="Current status following reconciliation.")
    reconciled_by: str = Field(description="Actor who reconciled the execution.")
    reconciled_at: str = Field(description="UTC timestamp when reconciled.")
    reconciliation_reason: str = Field(description="Explanation recorded for the decision.")
    idempotent: bool = Field(
        default=False,
        description="Whether this request matched an existing reconciliation outcome without changing state.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the reconciliation.",
    )
    execution: ToolExecutionModel | None = Field(
        default=None,
        description="Updated sanitized tool execution record.",
    )


class ToolApprovalModel(BaseModel):
    """Model representing an approved, rejected, or revoked decision for a tool execution."""

    operation_key: str = Field(description="Operation key of the tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    approver: str = Field(description="Identity of the operator or human who decided.")
    decision: str = Field(description="Approval decision: 'approved', 'rejected', or 'revoked'.")
    reason: str = Field(default="", description="Explanation or justification for the decision.")
    created_at: str = Field(description="UTC timestamp when the decision was recorded.")
    argument_fingerprint: str | None = Field(
        default=None,
        description="SHA-256 fingerprint of the tool arguments bound to this approval.",
    )
    risk_level: str | None = Field(
        default=None,
        description="Tool risk level at the time of approval.",
    )
    policy_hash: str | None = Field(
        default=None,
        description="Hash of the policy configuration at the time of approval.",
    )
    revoked_by: str | None = Field(
        default=None,
        description="Operator who revoked the approval, if revoked.",
    )
    revocation_reason: str | None = Field(
        default=None,
        description="Reason recorded for revocation, if revoked.",
    )
    revoked_at: str | None = Field(
        default=None,
        description="UTC timestamp when revoked, if revoked.",
    )
    recovered_by: str | None = Field(
        default=None,
        description="Operator who recovered the approval, if recovered.",
    )
    expires_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval expires.",
    )
    expired_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval expired, if expired.",
    )
    is_expired: bool = Field(
        default=False,
        description="Whether approval has expired.",
    )
    escalated_at: str | None = Field(
        default=None,
        description="UTC timestamp when escalation was recorded.",
    )
    escalation_reason: str | None = Field(
        default=None,
        description="Reason for escalation, if escalated.",
    )


class ApproveToolExecutionRequest(BaseModel):
    """Request payload to approve or reject a tool execution."""

    tool_name: str = Field(min_length=1, description="Name of the tool being approved.")
    decision: str = Field(
        default="approved",
        description="Approval decision: 'approved' or 'rejected'.",
    )
    reason: str = Field(
        default="",
        max_length=1000,
        description="Optional justification or policy citation for the decision.",
    )
    approver: str | None = Field(
        default=None,
        description="Optional operator username or identifier.",
    )
    argument_fingerprint: str | None = Field(
        default=None,
        description="Optional explicit argument fingerprint to bind approval to exact arguments.",
    )
    arguments: dict[str, Any] | None = Field(
        default=None,
        description="Optional raw arguments from which the fingerprint is computed if not supplied.",
    )


class ApproveToolExecutionResponse(BaseModel):
    """Response payload for a tool approval decision."""

    success: bool = Field(description="Whether the approval was recorded successfully.")
    operation_key: str = Field(description="Operation key of the tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    approver: str = Field(description="Identity of the approver.")
    decision: str = Field(description="Recorded decision: 'approved' or 'rejected'.")
    reason: str = Field(default="", description="Recorded reason.")
    created_at: str = Field(description="UTC timestamp when recorded.")
    idempotent: bool = Field(
        default=False,
        description="Whether this request matched an existing identical approval without changing state.",
    )
    argument_fingerprint: str | None = Field(
        default=None,
        description="Argument fingerprint bound to this approval.",
    )
    risk_level: str | None = Field(
        default=None,
        description="Tool risk level when approved.",
    )
    policy_hash: str | None = Field(
        default=None,
        description="Policy hash when approved.",
    )
    expires_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval expires.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the approval.",
    )


class RevokeToolApprovalRequest(BaseModel):
    """Request payload to revoke an existing tool approval."""

    tool_name: str = Field(min_length=1, description="Name of the tool whose approval is being revoked.")
    reason: str = Field(
        default="",
        max_length=1000,
        description="Justification or explanation for the revocation.",
    )
    revoked_by: str | None = Field(
        default=None,
        description="Optional operator username or identifier performing the revocation.",
    )


class RevokeToolApprovalResponse(BaseModel):
    """Response payload for revoking a tool approval."""

    success: bool = Field(description="Whether the revocation was recorded successfully.")
    operation_key: str = Field(description="Operation key of the tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    revoked_by: str = Field(description="Identity of the operator who revoked the approval.")
    decision: str = Field(default="revoked", description="Updated decision status: 'revoked'.")
    reason: str = Field(default="", description="Recorded revocation reason.")
    revoked_at: str = Field(description="UTC timestamp when revoked.")
    idempotent: bool = Field(
        default=False,
        description="Whether this request matched an existing revocation without changing state.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the revocation.",
    )


class RunToolApprovalsResponse(BaseModel):
    """Response payload listing tool approvals for a workflow run."""

    run_id: str = Field(description="Workflow run identifier.")
    total_approvals: int = Field(description="Total count of approval records matching query.")
    total_count: int | None = Field(
        default=None,
        description="Convenience alias for total_approvals.",
    )
    approvals: list[ToolApprovalModel] = Field(description="List of sanitized approval records.")


class DiagnoseToolApprovalRequest(BaseModel):
    """Request payload to inspect diagnostics for a tool operation approval."""

    tool_name: str | None = Field(default=None, description="Optional name of the tool.")
    arguments: dict[str, Any] | None = Field(
        default=None,
        description="Optional current invocation arguments.",
    )


class ToolApprovalDiagnosticsResponse(BaseModel):
    """Comprehensive diagnostic information for an operation's approval state."""

    operation_key: str = Field(description="Operation key of the tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    state: str = Field(
        description="Current approval state: 'pending', 'approved', 'rejected', 'revoked', or 'missing'.",
    )
    status: str | None = Field(
        default=None,
        description="Convenience alias matching state.",
    )
    diagnostic_code: str = Field(
        description="Machine-readable diagnostic code explaining the exact approval status.",
    )
    message: str = Field(description="Human-readable, sanitized explanation of the diagnostic finding.")
    is_blocked: bool = Field(
        description="Whether execution is currently blocked due to approval requirements.",
    )
    is_stale: bool = Field(description="Whether an existing approval is invalidated by parameter changes.")
    can_execute: bool = Field(
        description="Whether the operation is authorized, fresh, matching, and safe to execute.",
    )
    can_proceed: bool | None = Field(
        default=None,
        description="Convenience alias matching can_execute.",
    )
    risk_level: str | None = Field(default=None, description="Risk level bound to the approval, if any.")
    current_risk_level: str | None = Field(
        default=None,
        description="Currently configured risk level for the tool.",
    )
    bound_fingerprint: str | None = Field(
        default=None,
        description="Argument fingerprint bound to the approval, if any.",
    )
    current_fingerprint: str | None = Field(
        default=None,
        description="Current argument fingerprint computed from provided arguments.",
    )
    bound_policy_hash: str | None = Field(
        default=None,
        description="Policy hash bound to the approval, if any.",
    )
    current_policy_hash: str | None = Field(
        default=None,
        description="Currently active policy hash for the tool.",
    )
    approver: str | None = Field(default=None, description="Approver identity if approved or rejected.")
    authorized_approvers: list[str] = Field(
        default_factory=list,
        description="List of authorized approvers for this tool.",
    )
    revoked_by: str | None = Field(default=None, description="Identity of revoker if revoked.")
    revocation_reason: str | None = Field(default=None, description="Reason for revocation if revoked.")
    created_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval/request was created.",
    )
    revoked_at: str | None = Field(default=None, description="UTC timestamp when revoked, if revoked.")
    audit_event_count: int = Field(
        default=0,
        description="Count of relevant audit events found for this operation.",
    )
    relevant_audit_events: list[AuditEventModel] = Field(
        default_factory=list,
        description="Sanitized audit events associated with this operation.",
    )
    expires_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval expires, if configured.",
    )
    expired_at: str | None = Field(
        default=None,
        description="UTC timestamp when approval expired, if expired.",
    )
    is_expired: bool = Field(
        default=False,
        description="Whether approval is currently expired.",
    )
    escalated_at: str | None = Field(
        default=None,
        description="UTC timestamp when escalation was recorded, if escalated.",
    )
    escalation_reason: str | None = Field(
        default=None,
        description="Recorded reason or note for escalation, if escalated.",
    )


class RecoverToolApprovalRequest(BaseModel):
    """Request payload to initiate operational recovery for a blocked or stale approval."""

    tool_name: str = Field(min_length=1, description="Name of the tool being recovered.")
    reason: str = Field(
        min_length=1,
        max_length=1000,
        description="Explicit justification for initiating operational recovery.",
    )
    operator: str | None = Field(default=None, description="Operator identity initiating recovery.")
    arguments: dict[str, Any] | None = Field(
        default=None,
        description="Optional raw arguments from which updated fingerprint is computed.",
    )


class RecoverToolApprovalResponse(BaseModel):
    """Response payload for an approval operational recovery action."""

    success: bool = Field(description="Whether the recovery action was recorded successfully.")
    operation_key: str = Field(description="Operation key of the recovered tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    decision: str = Field(
        default="pending",
        description="Reset decision status: always 'pending' to require explicit review.",
    )
    reason: str = Field(description="Recorded justification for the recovery action.")
    recovered_by: str = Field(description="Identity of the operator who initiated recovery.")
    recovered_at: str = Field(description="UTC timestamp when recovery was executed.")
    argument_fingerprint: str | None = Field(default=None, description="Updated argument fingerprint.")
    risk_level: str | None = Field(default=None, description="Tool risk level at time of recovery.")
    policy_hash: str | None = Field(default=None, description="Policy hash at time of recovery.")
    expires_at: str | None = Field(
        default=None,
        description="Fresh UTC timestamp when recovered approval will expire.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the recovery.",
    )


class ExpireToolApprovalRequest(BaseModel):
    """Request payload to manually expire a tool approval."""

    tool_name: str | None = Field(default=None, description="Optional name of the tool.")
    reason: str = Field(
        default="Approval expired by operator",
        max_length=1000,
        description="Reason or justification for marking approval as expired.",
    )
    actor: str | None = Field(default=None, description="Operator or system identity expiring approval.")
    force: bool = Field(
        default=False,
        description="Whether to force expiration even if not yet past expires_at.",
    )


class ExpireToolApprovalResponse(BaseModel):
    """Response payload for expiring a tool approval."""

    success: bool = Field(description="Whether the approval was marked as expired.")
    operation_key: str = Field(description="Operation key of the expired tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    decision: str = Field(default="expired", description="Approval decision status ('expired').")
    expired_at: str = Field(description="UTC timestamp when marked as expired.")
    reason: str = Field(description="Recorded reason for expiration.")
    idempotent: bool = Field(
        default=False,
        description="Whether this request matched an already expired approval without change.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the expiration.",
    )


class EscalateToolApprovalRequest(BaseModel):
    """Request payload to record an escalation on an overdue pending tool approval."""

    tool_name: str | None = Field(default=None, description="Optional name of the tool.")
    reason: str = Field(
        default="Approval request overdue",
        max_length=1000,
        description="Reason or justification for escalating the approval request.",
    )
    escalation_level: str = Field(
        default="manager",
        description="Escalation target or severity level (e.g., 'manager', 'lead', 'security').",
    )
    actor: str | None = Field(default=None, description="Actor or system identity escalating approval.")


class EscalateToolApprovalResponse(BaseModel):
    """Response payload for recording an escalation on an approval."""

    success: bool = Field(description="Whether the escalation was recorded successfully.")
    operation_key: str = Field(description="Operation key of the escalated tool execution.")
    run_id: str = Field(description="Workflow run identifier.")
    tool_name: str = Field(description="Name of the tool.")
    escalated_at: str = Field(description="UTC timestamp when escalation was recorded.")
    escalation_reason: str = Field(description="Recorded reason or details for escalation.")
    idempotent: bool = Field(
        default=False,
        description="Whether this request was already escalated with the same details.",
    )
    audit_event_id: str | None = Field(
        default=None,
        description="Audit event identifier recorded for the escalation.",
    )


class ScanExpiredApprovalsResponse(BaseModel):
    """Response payload summarizing batch scan of expired/overdue approvals."""

    run_id: str | None = Field(default=None, description="Workflow run ID scanned, if scoped.")
    scanned_at: str = Field(description="UTC timestamp when the scan was executed.")
    expired_count: int = Field(default=0, description="Number of approvals newly transitioned to expired.")
    escalated_count: int = Field(default=0, description="Number of overdue approvals newly escalated.")
    expired_operation_keys: list[str] = Field(
        default_factory=list,
        description="Operation keys that were transitioned to expired.",
    )
    escalated_operation_keys: list[str] = Field(
        default_factory=list,
        description="Operation keys that were escalated.",
    )
    failed_count: int = Field(
        default=0,
        description="Number of approvals that encountered errors during the scan.",
    )
    failed_operation_keys: list[str] = Field(
        default_factory=list,
        description="Operation keys that failed to process during the scan.",
    )


class OutboxEventModel(BaseModel):
    """Durable transactional outbox notification event model."""

    event_id: str = Field(description="Unique outbox event identifier.")
    event_type: str = Field(description="Notification event type name.")
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Sanitized event payload dictionary.",
    )
    created_at: str = Field(description="ISO8601 creation timestamp.")
    status: str = Field(description="Delivery status: pending, claimed, delivered, or dead_letter.")
    attempt_count: int = Field(default=0, description="Total delivery attempts recorded.")
    next_retry_at: str = Field(description="Scheduled next retry timestamp.")
    last_error: str | None = Field(default=None, description="Sanitized last delivery error message.")
    claimed_by: str | None = Field(default=None, description="Worker ID currently claiming the event.")
    claimed_at: str | None = Field(default=None, description="Timestamp when event was claimed.")
    claim_expires_at: str | None = Field(default=None, description="Claim expiration timestamp.")
    delivered_at: str | None = Field(default=None, description="Timestamp when successfully delivered.")
    idempotency_key: str | None = Field(default=None, description="Stable deduplication key.")
    fence_token: int = Field(default=1, description="Monotonic fencing token for delivery ownership.")
    retry_count: int = Field(default=0, description="Total manual recovery retry attempts.")
    last_recovery_reason: str | None = Field(default=None, description="Reason for last recovery.")
    last_recovery_by: str | None = Field(default=None, description="Operator of last recovery.")
    last_recovery_at: str | None = Field(default=None, description="Timestamp of last recovery.")


class ListOutboxEventsResponse(BaseModel):
    """Response payload for listing outbox events."""

    items: list[OutboxEventModel] = Field(
        default_factory=list,
        description="List of matching outbox event records.",
    )
    total: int = Field(description="Count of returned items in this page.")
    limit: int = Field(description="Maximum items per page.")
    offset: int = Field(description="Pagination offset.")


class OutboxDiagnosticsResponse(BaseModel):
    """Operational diagnostics for transactional outbox and background dispatcher."""

    enabled: bool = Field(description="Whether outbox persistence is active.")
    total_events: int = Field(default=0, description="Total events recorded in the outbox table.")
    counts: dict[str, int] = Field(
        default_factory=dict,
        description="Event counts grouped by status (pending, claimed, delivered, dead_letter).",
    )
    oldest_pending_at: str | None = Field(
        default=None,
        description="Creation timestamp of oldest pending outbox event.",
    )
    oldest_pending_age_seconds: float | None = Field(
        default=None,
        description="Age in seconds of the oldest pending outbox event.",
    )
    newest_event_at: str | None = Field(
        default=None,
        description="Creation timestamp of newest recorded outbox event.",
    )
    retry_metrics: dict[str, Any] = Field(
        default_factory=dict,
        description="Durable metrics for event attempts, retries, and repeat failures.",
    )
    delivery_latency: dict[str, Any] | None = Field(
        default=None,
        description="Durable delivery latency statistics (avg, min, max, p95).",
    )
    recent_failures: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Recent delivery failures with sanitized error messages.",
    )
    expired_claims_count: int = Field(
        default=0,
        description="Number of currently claimed events whose claim lease has expired.",
    )
    expired_claims: list[dict[str, Any]] = Field(
        default_factory=list,
        description="List of currently expired event claims awaiting recovery.",
    )
    retry_limit_diagnostics: dict[str, Any] = Field(
        default_factory=dict,
        description="Diagnostics for events approaching or exceeding max retry limits.",
    )
    dispatcher: dict[str, Any] | None = Field(
        default=None,
        description="Operational diagnostics of the background outbox dispatcher.",
    )


class RetryOutboxEventRequest(BaseModel):
    """Request payload for retrying a failed or dead-letter outbox event."""

    reason: str | None = Field(
        default=None,
        description="Operator reason explaining why the event is being retried.",
    )
    operator: str | None = Field(
        default=None,
        description="Identifier of the operator performing the retry action.",
    )
    reset_attempts: bool = Field(
        default=False,
        description="Whether to reset the event's attempt counter to 0.",
    )
    force: bool = Field(
        default=False,
        description="Whether to force recovery even if an active claim lease is held.",
    )


class RetryOutboxEventResponse(BaseModel):
    """Response payload for retrying a failed or dead-letter outbox event."""

    event: OutboxEventModel = Field(description="Updated outbox event record.")
    retried: bool = Field(default=True, description="Whether the event was successfully reset to pending.")


class RecoverOutboxEventsRequest(BaseModel):
    """Request payload for operator batch recovery of eligible outbox events."""

    reason: str = Field(
        ...,
        min_length=1,
        description="Mandatory operator reason for the recovery action.",
    )
    event_ids: list[str] | None = Field(
        default=None,
        description="Optional list of specific event IDs to recover.",
    )
    status_filter: str | None = Field(
        default="dead_letter",
        description="Filter status for eligible recovery candidates.",
    )
    operator: str | None = Field(
        default=None,
        description="Identifier of the operator performing the recovery.",
    )
    reset_attempts: bool = Field(
        default=True,
        description="Whether to reset attempt counters to 0 for recovered events.",
    )
    max_events: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Maximum number of events to recover in this batch.",
    )


class RecoverOutboxEventsResponse(BaseModel):
    """Response payload summarizing operator batch recovery of outbox events."""

    recovered_count: int = Field(description="Total events successfully recovered.")
    skipped_count: int = Field(description="Events skipped because they were ineligible.")
    recovered_events: list[OutboxEventModel] = Field(description="List of recovered outbox events.")
    reason: str = Field(description="Audit reason recorded for the recovery action.")
    operator: str = Field(description="Operator identity recorded for the recovery action.")


class OutboxHistoryEntryModel(BaseModel):
    """Individual history record for an outbox event delivery or recovery action."""

    id: int = Field(description="Unique historical log entry sequence ID.")
    event_id: str = Field(description="Associated outbox event ID.")
    attempt_number: int = Field(description="Delivery attempt sequence number.")
    action: str = Field(description="Historical action: claim, delivered, failed, dead_letter, or retried.")
    status: str = Field(description="Resulting event status.")
    error_message: str | None = Field(default=None, description="Sanitized error message if failed.")
    claimed_by: str | None = Field(default=None, description="Worker ID that handled the action.")
    recorded_at: str = Field(description="Timestamp when this history record was written.")
    operator: str | None = Field(default=None, description="Operator identity if manually recovered.")
    reason: str | None = Field(default=None, description="Operator reason if manually recovered.")


class OutboxHistoryResponse(BaseModel):
    """Historical delivery and recovery log for an outbox event."""

    event_id: str = Field(description="Outbox event ID.")
    history: list[OutboxHistoryEntryModel] = Field(
        default_factory=list,
        description="Chronological log of attempts, deliveries, failures, and recovery actions.",
    )
    total: int = Field(description="Total history records returned.")


class DispatchOutboxResponse(BaseModel):
    """Response payload summarizing execution of an outbox dispatch cycle."""

    scanned_at: str = Field(description="UTC timestamp when the dispatch cycle ran.")
    claimed_count: int = Field(default=0, description="Number of events claimed in this cycle.")
    delivered_count: int = Field(default=0, description="Number of events successfully delivered.")
    failed_count: int = Field(default=0, description="Number of events that failed delivery and will retry.")
    dead_letter_count: int = Field(
        default=0,
        description="Number of events transitioned to dead_letter after exhausting retries.",
    )
    stale_count: int = Field(
        default=0,
        description="Number of events skipped due to stale dispatcher fencing conflicts.",
    )


class ReplayDiagnosticsResponse(BaseModel):
    """Operational diagnostics for webhook replay protection and maintenance."""

    enabled: bool = Field(description="Whether webhook replay protection is active.")
    storage_backend: str = Field(description="Replay storage backend: 'sqlite' or 'memory'.")
    retention_seconds: float | None = Field(
        default=None,
        description="Configured replay record retention TTL in seconds.",
    )
    cleanup_batch_size: int | None = Field(
        default=None,
        description="Configured batch size for expired record pruning.",
    )
    total_records: int = Field(default=0, description="Total replay records stored.")
    active_records: int = Field(default=0, description="Active, unexpired replay records.")
    expired_records: int = Field(default=0, description="Expired replay records awaiting pruning.")
    oldest_timestamp: int | None = Field(
        default=None,
        description="Timestamp of the oldest recorded event.",
    )
    newest_timestamp: int | None = Field(
        default=None,
        description="Timestamp of the newest recorded event.",
    )
    oldest_recorded_at: str | None = Field(
        default=None,
        description="UTC ISO timestamp of the oldest recorded event.",
    )
    newest_recorded_at: str | None = Field(
        default=None,
        description="UTC ISO timestamp of the newest recorded event.",
    )
    last_cleanup: dict[str, Any] | None = Field(
        default=None,
        description="Metadata of the most recent cleanup operation.",
    )
    scheduler_status: dict[str, Any] | None = Field(
        default=None,
        description="Operational status of the automated replay cleanup scheduler.",
    )


class ReplayCleanupRequest(BaseModel):
    """Request payload for manual operational cleanup of expired replay records."""

    limit: int | None = Field(
        default=None,
        ge=1,
        le=10000,
        description="Max expired records to delete in this batch (1 to 10000).",
    )
    reason: str | None = Field(
        default=None,
        description="Operator reason explaining this manual cleanup action.",
    )


class ReplayCleanupResponse(BaseModel):
    """Response payload summarizing execution of a replay cleanup operation."""

    success: bool = Field(default=True, description="Whether the cleanup operation succeeded.")
    deleted_count: int = Field(default=0, description="Number of expired records pruned.")
    duration_ms: float = Field(default=0.0, description="Execution duration in milliseconds.")
    cleaned_at: str = Field(description="UTC ISO timestamp when cleanup occurred.")
    limit: int = Field(description="Bounded limit applied to the batch cleanup.")
    trigger_type: str = Field(default="manual", description="Trigger source: 'manual' or 'scheduled'.")
    operator: str = Field(default="system", description="Identity of the operator initiating cleanup.")
    reason: str | None = Field(default=None, description="Operator reason for the cleanup action.")


# ── Celery Worker Observability & Operational Recovery Models ─────────────────


class CeleryWorkerModel(BaseModel):
    """Telemetry representation for an active, stale, or offline Celery worker."""

    worker_id: str = Field(description="Unique worker process or node identifier.")
    status: str = Field(description="Effective worker status: 'active', 'stale', or 'offline'.")
    registered_at: str = Field(description="UTC ISO timestamp when the worker registered.")
    last_heartbeat_at: str = Field(description="UTC ISO timestamp of most recent heartbeat.")
    heartbeat_age_seconds: float = Field(
        default=0.0,
        description="Elapsed seconds since last recorded heartbeat.",
    )
    is_stale: bool = Field(
        default=False,
        description="Whether elapsed time exceeds configured stale worker threshold.",
    )
    active_tasks: int = Field(default=0, description="Count of currently active running tasks.")
    queues: str = Field(
        default="orchestrator_workflows",
        description="Queues being consumed by this worker.",
    )
    last_completed_at: str | None = Field(
        default=None,
        description="UTC ISO timestamp of most recently completed task.",
    )
    last_failure_at: str | None = Field(
        default=None,
        description="UTC ISO timestamp of most recent task failure.",
    )
    last_failure_reason: str | None = Field(
        default=None,
        description="Sanitized failure reason of most recent task failure.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Worker host, pid, and platform metadata.",
    )


class CeleryWorkersResponse(BaseModel):
    """Response payload listing Celery worker statuses and fleet health."""

    total_workers: int = Field(description="Total number of registered Celery workers.")
    active_workers: int = Field(description="Number of workers currently active and healthy.")
    stale_workers: int = Field(description="Number of workers with expired heartbeat threshold.")
    offline_workers: int = Field(description="Number of workers gracefully shut down or marked offline.")
    stale_threshold_seconds: float = Field(description="Applied stale worker heartbeat threshold.")
    workers: list[CeleryWorkerModel] = Field(default_factory=list, description="Worker records.")


class CeleryRecentFailureModel(BaseModel):
    """Sanitized summary of a recent workflow run failure."""

    run_id: str = Field(description="Unique identifier of the failed run.")
    task: str = Field(description="Sanitized task description snippet.")
    updated_at: str = Field(description="UTC ISO timestamp when failure was recorded.")
    error: str = Field(description="Sanitized error description.")


class CeleryQueueMetricsResponse(BaseModel):
    """Operational queue and worker fleet telemetry derived from durable state."""

    queued_count: int = Field(description="Number of workflows waiting in queued state.")
    running_count: int = Field(description="Number of workflows currently executing.")
    waiting_approval_count: int = Field(
        description="Number of workflows paused waiting for human tool approval.",
    )
    completed_count: int = Field(description="Total count of completed workflow runs.")
    failed_count: int = Field(description="Total count of failed workflow runs.")
    retry_count: int = Field(description="Total count of recovered or resumed runs.")
    oldest_queued_age_seconds: float = Field(
        default=0.0,
        description="Age in seconds of the oldest pending queued workflow.",
    )
    active_worker_count: int = Field(description="Number of active, responsive workers.")
    stale_worker_count: int = Field(description="Number of workers missing heartbeats.")
    offline_worker_count: int = Field(description="Number of offline workers.")
    total_worker_count: int = Field(description="Total tracked worker count.")
    recent_failures: list[CeleryRecentFailureModel] = Field(
        default_factory=list,
        description="Sanitized recent failure summaries.",
    )


class StuckWorkflowModel(BaseModel):
    """Diagnostic details for a stuck or interrupted workflow run."""

    run_id: str = Field(description="Unique identifier of the stuck run.")
    task: str = Field(description="Sanitized task snippet.")
    status: str = Field(description="Current durable run status.")
    created_at: str = Field(description="UTC ISO timestamp when run was created.")
    updated_at: str = Field(description="UTC ISO timestamp of latest run update.")
    age_seconds: float = Field(description="Elapsed seconds since run creation.")
    owner_id: str | None = Field(default=None, description="Current worker lease owner if held.")
    fence_token: int = Field(default=0, description="Current fencing token.")
    stuck_reason: str = Field(
        description="Diagnosis reason: stuck_queued, orphaned_lease, lease_expired, etc.",
    )
    recovery_eligible: bool = Field(
        description="Whether this workflow is safe and eligible for operator recovery.",
    )


class StuckWorkflowsResponse(BaseModel):
    """Response payload listing stuck and interrupted workflows."""

    total_stuck: int = Field(description="Total count of workflows flagged as stuck.")
    eligible_count: int = Field(description="Count of workflows eligible for automated or manual recovery.")
    stuck_threshold_seconds: float = Field(description="Threshold in seconds used to evaluate stuck status.")
    workflows: list[StuckWorkflowModel] = Field(
        default_factory=list,
        description="List of stuck workflow diagnostics.",
    )


class RecoverWorkflowsRequest(BaseModel):
    """Operator request to safely recover eligible stuck or interrupted workflows."""

    run_id: str | None = Field(
        default=None,
        description="Single run ID to recover. Provide run_id or run_ids.",
    )
    run_ids: list[str] | None = Field(
        default=None,
        description="Batch of run IDs to recover. Provide run_id or run_ids.",
    )
    action: str = Field(
        default="requeue",
        description="Recovery action: 'requeue' (default), 'fail', or 'cancel'.",
    )
    operator: str | None = Field(
        default=None,
        description="Optional operator identity.",
    )
    reason: str = Field(
        min_length=5,
        description="Auditable explanation for operator recovery (min 5 characters).",
    )


class RecoveredWorkflowItem(BaseModel):
    """Result of an individual workflow recovery operation."""

    run_id: str = Field(description="Identifier of the recovered run.")
    success: bool = Field(description="Whether recovery succeeded for this run.")
    previous_status: str | None = Field(default=None, description="Previous run status.")
    new_status: str | None = Field(default=None, description="New status after recovery.")
    action: str = Field(description="Recovery action applied.")
    fence_token: int | None = Field(default=None, description="Incremented fencing token.")
    error: str | None = Field(default=None, description="Error message if recovery failed.")


class RecoverWorkflowsResponse(BaseModel):
    """Response payload summarizing operator workflow recovery execution."""

    success: bool = Field(description="Overall success of the recovery operation.")
    action: str = Field(description="Recovery action applied.")
    recovered_count: int = Field(default=0, description="Number of successfully recovered workflows.")
    failed_count: int = Field(default=0, description="Number of workflows that failed recovery.")
    operator: str = Field(description="Identity of operator who initiated recovery.")
    reason: str = Field(description="Operator-provided recovery explanation.")
    results: list[RecoveredWorkflowItem] = Field(
        default_factory=list,
        description="Per-run recovery results.",
    )


class BackupMetadataModel(BaseModel):
    """Detailed metadata describing a database backup archive."""

    backup_path: str = Field(description="Full filesystem path of the backup archive.")
    source_db_path: str = Field(description="Source SQLite database path.")
    timestamp: str = Field(description="UTC timestamp when the backup was created.")
    size_bytes: int = Field(description="Size in bytes of the backup file.")
    integrity: str = Field(description="Integrity verification status (e.g. 'ok').")
    table_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Per-table record counts at time of backup.",
    )
    duration_ms: float = Field(
        default=0.0,
        description="Backup creation duration in milliseconds.",
    )
    tag: str | None = Field(
        default=None,
        description="Optional tag label included in the backup name.",
    )
    manifest_path: str | None = Field(
        default=None,
        description="Full filesystem path of the companion integrity manifest.",
    )
    sha256: str | None = Field(
        default=None,
        description="Cryptographic SHA-256 checksum of the backup archive.",
    )
    manifest_status: str = Field(
        default="unverified",
        description="Integrity status of the manifest: verified, unverified, failed.",
    )
    signature_status: str = Field(
        default="unsigned",
        description="Signature status: 'verified', 'unsigned', 'missing_key', 'invalid'.",
    )
    key_id: str | None = Field(
        default=None,
        description="Identifier of key used to sign the backup manifest.",
    )


class BackupStatusResponse(BaseModel):
    """Response payload for database backup scheduler status and storage summary."""

    status: str = Field(
        description="Overall backup subsystem status: 'healthy', 'standby', 'disabled'.",
    )
    scheduler: BackupSchedulerDiagnostics = Field(
        description="Operational status and metrics of the backup scheduler.",
    )
    latest_backup: BackupMetadataModel | None = Field(
        default=None,
        description="Metadata of the most recent valid database backup archive.",
    )
    total_backups: int = Field(
        default=0,
        description="Total number of existing backup archives in storage.",
    )
    backup_dir: str = Field(description="Directory where backup archives are stored.")


class BackupTriggerRequest(BaseModel):
    """Optional parameters when triggering an on-demand database backup."""

    reason: str | None = Field(
        default=None,
        description="Auditable reason or operator description for manual backup trigger.",
    )
    tag: str | None = Field(
        default=None,
        description="Optional tag label to include in the backup filename.",
    )


class BackupTriggerResponse(BaseModel):
    """Response payload returned after triggering an on-demand database backup."""

    status: str = Field(description="Execution result: 'success', 'skipped', or 'failed'.")
    message: str = Field(description="Human-readable execution outcome summary.")
    metadata: BackupMetadataModel | None = Field(
        default=None,
        description="Backup archive metadata if backup was created.",
    )
    duration_ms: float = Field(
        default=0.0,
        description="Duration in milliseconds of the backup operation.",
    )
    error: str | None = Field(
        default=None,
        description="Sanitized error description if backup failed.",
    )
    fence_token: int | None = Field(
        default=None,
        description="Scheduler lease fence token under which backup ran.",
    )


class BackupHistoryResponse(BaseModel):
    """Response payload listing available database backup archives."""

    total_count: int = Field(description="Total number of backup archives found.")
    backup_dir: str = Field(description="Directory searched for backup archives.")
    backups: list[BackupMetadataModel] = Field(
        default_factory=list,
        description="List of available backup archives ordered newest to oldest.",
    )

class RestoreDrillResultModel(BaseModel):
    """Outcome of an isolated database restore drill and recovery verification."""

    drill_id: str = Field(description="Unique identifier for the restore drill run.")
    success: bool = Field(description="Whether the restore drill succeeded completely.")
    backup_path: str = Field(description="Path to the backup archive tested.")
    backup_timestamp: str | None = Field(
        default=None,
        description="Timestamp when the backup archive was created.",
    )
    backup_age_seconds: float = Field(
        default=0.0,
        description="Age in seconds of the backup archive at time of drill.",
    )
    duration_ms: float = Field(
        default=0.0,
        description="Total duration in milliseconds of the restore drill.",
    )
    manifest_status: str = Field(
        default="unverified",
        description="Integrity manifest status of tested backup: verified, unverified, failed.",
    )
    sha256: str | None = Field(
        default=None,
        description="SHA-256 checksum of the backup archive evaluated during drill.",
    )
    signature_status: str = Field(
        default="unsigned",
        description="Manifest signature verification outcome for the tested backup.",
    )
    key_id: str | None = Field(
        default=None,
        description="Key identifier of the tested backup manifest signature.",
    )
    target_temp_path: str | None = Field(
        default=None,
        description="Path to the isolated temporary SQLite database tested.",
    )
    integrity_check: str = Field(
        default="ok",
        description="SQLite PRAGMA integrity_check outcome.",
    )
    quick_check: str = Field(
        default="ok",
        description="SQLite PRAGMA quick_check outcome.",
    )
    foreign_key_check: str = Field(
        default="ok",
        description="SQLite PRAGMA foreign_key_check outcome.",
    )
    tables_verified: list[str] = Field(
        default_factory=list,
        description="List of verified table names present in the restored database.",
    )
    records_verified: int = Field(
        default=0,
        description="Number of sampled database records deserialized and validated.",
    )
    queries_verified: int = Field(
        default=0,
        description="Number of verification queries successfully executed.",
    )
    table_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Record counts by table in the restored database.",
    )
    errors: list[str] = Field(
        default_factory=list,
        description="List of verification errors encountered, if any.",
    )
    cleaned_up: bool = Field(
        default=True,
        description="Whether temporary drill databases and journals were cleaned up.",
    )
    timestamp: str = Field(
        description="UTC timestamp when the restore drill started.",
    )


class RestoreDrillStatusResponse(BaseModel):
    """Operational status and latest result for database restore drills."""

    enabled: bool = Field(description="Whether automated restore drills are enabled.")
    interval_seconds: float = Field(
        description="Configured interval in seconds between automated restore drill runs.",
    )
    latest_drill: dict[str, Any] | None = Field(
        default=None,
        description="Most recent recorded restore drill execution record.",
    )
    last_drill_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent restore drill attempt.",
    )
    last_drill_success_at: str | None = Field(
        default=None,
        description="UTC timestamp of the most recent successful restore drill.",
    )
    last_drill_status: str | None = Field(
        default=None,
        description="Status of the most recent restore drill.",
    )
    total_drills_count: int = Field(
        default=0,
        description="Total number of restore drills executed.",
    )
    drill_failure_count: int = Field(
        default=0,
        description="Total number of failed restore drills.",
    )


class RestoreDrillTriggerRequest(BaseModel):
    """Parameters for triggering an on-demand database restore drill."""

    backup_path: str | None = Field(
        default=None,
        description="Optional path to a specific backup archive to test (default: latest).",
    )
    reason: str | None = Field(
        default=None,
        description="Auditable operator explanation for triggering the restore drill.",
    )


class RestoreDrillTriggerResponse(BaseModel):
    """Response payload for on-demand database restore drill trigger."""

    status: str = Field(description="Execution result: 'success', 'skipped', or 'failed'.")
    message: str = Field(description="Human-readable outcome description.")
    drill: RestoreDrillResultModel | None = Field(
        default=None,
        description="Detailed restore drill verification result.",
    )
    duration_ms: float = Field(
        default=0.0,
        description="Execution duration in milliseconds.",
    )
    error: str | None = Field(
        default=None,
        description="Error message if restore drill failed.",
    )
    fence_token: int | None = Field(
        default=None,
        description="Scheduler fence token under which drill executed.",
    )


class RestoreDrillHistoryResponse(BaseModel):
    """Response payload listing historical database restore drill records."""

    drills: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Historical restore drill execution records ordered newest to oldest.",
    )
    count: int = Field(description="Number of restore drill records returned.")



class BackupVerifyRequest(BaseModel):
    """Optional payload to verify backup archive cryptographic integrity."""

    backup_path: str | None = Field(
        default=None,
        description="Optional filesystem path or filename of backup archive to verify.",
    )
    manifest_path: str | None = Field(
        default=None,
        description="Optional path to manifest file (defaults to <backup>.manifest.json).",
    )
    strict: bool | None = Field(
        default=None,
        description="Whether to strictly require an authenticated cryptographic signature.",
    )


class BackupVerifyResponse(BaseModel):
    """Response payload returned when verifying backup integrity manifest."""

    status: str = Field(
        description="Verification outcome: 'verified', 'unverified', or 'failed'.",
    )
    is_valid: bool = Field(
        description="True only if manifest exists and SHA-256 checksum matches.",
    )
    backup_path: str = Field(
        description="Path to the backup archive verified.",
    )
    manifest_path: str | None = Field(
        default=None,
        description="Path to the manifest file evaluated.",
    )
    expected_sha256: str | None = Field(
        default=None,
        description="SHA-256 checksum declared in manifest.",
    )
    computed_sha256: str | None = Field(
        default=None,
        description="Computed SHA-256 checksum of backup archive.",
    )
    expected_size_bytes: int | None = Field(
        default=None,
        description="Archive size in bytes declared in manifest.",
    )
    actual_size_bytes: int | None = Field(
        default=None,
        description="Actual archive size in bytes on disk.",
    )
    manifest: dict[str, Any] | None = Field(
        default=None,
        description="Parsed manifest metadata dictionary.",
    )
    error: str | None = Field(
        default=None,
        description="Error description if verification failed.",
    )
    duration_ms: float = Field(
        default=0.0,
        description="Duration in milliseconds of the verification operation.",
    )
    signature_status: str = Field(
        default="unsigned",
        description="Signature outcome: 'verified', 'unsigned', 'missing_key', 'invalid'.",
    )
    key_id: str | None = Field(
        default=None,
        description="Signing key identifier declared in manifest signature.",
    )
    signature_algorithm: str | None = Field(
        default=None,
        description="Cryptographic algorithm used for manifest signature.",
    )
    signature_version: int | None = Field(
        default=None,
        description="Signature envelope format version.",
    )


class BackupKeyringInfoResponse(BaseModel):
    """Safe operational diagnostic of backup signing keys without exposing secrets."""

    signing_enabled: bool = Field(description="Whether manifest signing is currently enabled.")
    active_key_id: str | None = Field(
        default=None,
        description="Active signing key identifier used for new backups.",
    )
    verification_key_ids: list[str] = Field(
        default_factory=list,
        description="List of available verification key IDs in keyring.",
    )
    verification_keys_count: int = Field(
        default=0,
        description="Total count of available verification keys.",
    )
    strict_signature_required: bool = Field(
        default=False,
        description="Whether authenticated signatures are strictly enforced.",
    )
