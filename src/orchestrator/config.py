"""Central configuration loaded from environment / .env with strict validation and safe diagnostics."""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Any

from pydantic import AliasChoices, Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LOOPBACK_HOSTS: frozenset[str] = frozenset({"127.0.0.1", "localhost", "::1", "testclient"})


def is_loopback_host(host: str | None) -> bool:
    """Return True if the given host binds exclusively to local loopback interfaces."""
    if not host:
        return True
    return host.strip().lower() in LOOPBACK_HOSTS


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM providers
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    openai_model: str = "gpt-4o-mini"
    anthropic_model: str = "claude-opus-4-8"
    default_llm_provider: str = Field(
        default="mock",
        validation_alias=AliasChoices(
            "default_llm_provider",
            "orchestrator_llm_provider",
            "llm_provider",
        ),
    )
    fallback_llm_provider: str = Field(
        default="mock",
        validation_alias=AliasChoices(
            "fallback_llm_provider",
            "orchestrator_fallback_llm_provider",
            "fallback_provider",
        ),
    )
    llm_custom_pricing: str = Field(
        default="",
        validation_alias=AliasChoices(
            "llm_custom_pricing",
            "orchestrator_custom_pricing",
            "custom_pricing",
        ),
    )

    # LLM safety limits
    llm_timeout_seconds: float = Field(
        default=30.0,
        validation_alias=AliasChoices(
            "llm_timeout_seconds",
            "orchestrator_llm_timeout",
            "llm_timeout",
        ),
    )
    llm_max_retries: int = Field(
        default=2,
        validation_alias=AliasChoices(
            "llm_max_retries",
            "orchestrator_llm_max_retries",
        ),
    )
    max_task_chars: int = 8000

    # Concurrency and execution limits
    max_specialist_concurrency: int = Field(
        default=4,
        validation_alias=AliasChoices(
            "max_specialist_concurrency",
            "orchestrator_max_specialist_concurrency",
            "specialist_concurrency",
        ),
    )
    max_concurrency: int | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "max_concurrency",
            "orchestrator_max_concurrency",
            "concurrency",
        ),
    )
    max_active_runs: int = Field(
        default=10,
        description="Maximum number of concurrently active workflow runs permitted globally.",
        validation_alias=AliasChoices(
            "max_active_runs",
            "orchestrator_max_active_runs",
            "api_max_active_runs",
        ),
    )
    admission_lease_ttl_seconds: float = Field(
        default=30.0,
        description="Lease TTL in seconds for cross-process workflow admission reservations.",
        validation_alias=AliasChoices(
            "admission_lease_ttl_seconds",
            "orchestrator_admission_lease_ttl_seconds",
            "lease_ttl_seconds",
        ),
    )
    admission_heartbeat_interval_seconds: float = Field(
        default=5.0,
        description="Interval in seconds between heartbeats renewing active workflow admission leases.",
        validation_alias=AliasChoices(
            "admission_heartbeat_interval_seconds",
            "orchestrator_admission_heartbeat_interval_seconds",
            "heartbeat_interval_seconds",
        ),
    )
    workflow_timeout_seconds: float | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "workflow_timeout_seconds",
            "orchestrator_workflow_timeout",
            "workflow_timeout",
        ),
    )

    # Persistence and checkpointing
    checkpoint_db_path: str = Field(
        default="orchestrator_checkpoints.db",
        validation_alias=AliasChoices(
            "checkpoint_db_path",
            "orchestrator_checkpoint_db_path",
        ),
    )
    checkpoint_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "checkpoint_enabled",
            "orchestrator_checkpoint_enabled",
        ),
    )

    # Database backup and disaster recovery
    backup_dir: str = Field(
        default="backups",
        validation_alias=AliasChoices(
            "backup_dir",
            "orchestrator_backup_dir",
        ),
    )
    backup_retention_count: int = Field(
        default=10,
        validation_alias=AliasChoices(
            "backup_retention_count",
            "orchestrator_backup_retention_count",
        ),
    )
    backup_retention_days: int = Field(
        default=30,
        validation_alias=AliasChoices(
            "backup_retention_days",
            "orchestrator_backup_retention_days",
        ),
    )
    backup_scheduler_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "backup_scheduler_enabled",
            "orchestrator_backup_scheduler_enabled",
        ),
    )
    backup_interval_seconds: float = Field(
        default=86400.0,
        validation_alias=AliasChoices(
            "backup_interval_seconds",
            "orchestrator_backup_interval_seconds",
        ),
    )
    backup_max_duration_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            "backup_max_duration_seconds",
            "orchestrator_backup_max_duration_seconds",
        ),
    )
    backup_max_retries: int = Field(
        default=3,
        validation_alias=AliasChoices(
            "backup_max_retries",
            "orchestrator_backup_max_retries",
        ),
    )
    backup_retry_backoff_seconds: float = Field(
        default=5.0,
        validation_alias=AliasChoices(
            "backup_retry_backoff_seconds",
            "orchestrator_backup_retry_backoff_seconds",
        ),
    )
    backup_max_backoff_seconds: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            "backup_max_backoff_seconds",
            "orchestrator_backup_max_backoff_seconds",
        ),
    )
    backup_scheduler_lease_ttl_seconds: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            "backup_scheduler_lease_ttl_seconds",
            "orchestrator_backup_scheduler_lease_ttl_seconds",
        ),
    )
    backup_scheduler_heartbeat_interval_seconds: float = Field(
        default=10.0,
        validation_alias=AliasChoices(
            "backup_scheduler_heartbeat_interval_seconds",
            "orchestrator_backup_scheduler_heartbeat_interval_seconds",
        ),
    )
    backup_require_manifest: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "backup_require_manifest",
            "orchestrator_backup_require_manifest",
        ),
        description="Whether backup integrity manifests are required during restore operations.",
    )
    backup_signing_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "backup_signing_key",
            "orchestrator_backup_signing_key",
        ),
        description="Active HMAC-SHA256 secret key for signing new backup manifests.",
    )
    backup_signing_key_id: str = Field(
        default="default",
        validation_alias=AliasChoices(
            "backup_signing_key_id",
            "orchestrator_backup_signing_key_id",
        ),
        description="Key identifier for the active backup signing key.",
    )
    backup_verification_keys: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "backup_verification_keys",
            "orchestrator_backup_verification_keys",
        ),
        description="Keyring of verification keys (JSON mapping key_id to secret, or comma-separated pairs).",
    )
    backup_require_signature: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "backup_require_signature",
            "orchestrator_backup_require_signature",
            "backup_strict_signature",
        ),
        description="Whether authenticated manifest signatures are strictly required for recovery.",
    )

    # Database automated restore drill & recovery verification
    restore_drill_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "restore_drill_enabled",
            "orchestrator_restore_drill_enabled",
            "backup_restore_drill_enabled",
        ),
        description="Whether automated periodic database backup restore drills are enabled.",
    )
    restore_drill_interval_seconds: float = Field(
        default=604800.0,
        validation_alias=AliasChoices(
            "restore_drill_interval_seconds",
            "orchestrator_restore_drill_interval_seconds",
            "backup_restore_drill_interval_seconds",
        ),
        description="Interval in seconds between automated restore drill runs (default 7 days).",
    )
    restore_drill_max_duration_seconds: float = Field(
        default=300.0,
        validation_alias=AliasChoices(
            "restore_drill_max_duration_seconds",
            "orchestrator_restore_drill_max_duration_seconds",
            "backup_restore_drill_max_duration_seconds",
        ),
        description="Maximum allowed duration in seconds for a restore drill before timeout.",
    )
    restore_drill_history_limit: int = Field(
        default=50,
        validation_alias=AliasChoices(
            "restore_drill_history_limit",
            "orchestrator_restore_drill_history_limit",
            "backup_restore_drill_history_limit",
        ),
        description="Maximum number of restore drill execution records retained in SQLite.",
    )
    restore_drill_lease_ttl_seconds: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            "restore_drill_lease_ttl_seconds",
            "orchestrator_restore_drill_lease_ttl_seconds",
            "backup_restore_drill_lease_ttl_seconds",
        ),
        description="Time-to-live in seconds for restore drill scheduler lease locks.",
    )

    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "orchestrator"
    postgres_user: str = "orchestrator"
    postgres_password: str = "orchestrator"

    # ChromaDB
    chroma_host: str = "localhost"
    chroma_port: int = 8000

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Human-in-the-loop thresholds
    confidence_threshold: float = 0.6
    quality_threshold: float = 0.6
    max_retries: int = 2

    # Review-UI authentication (VA-01)
    # Secure by default. Set REVIEW_AUTH_ENABLED=false ONLY for local demos.
    review_auth_enabled: bool = True
    # Comma-separated "name:token" pairs, e.g. "alice:s3cret,bob:hunter2".
    review_users: str = ""
    # Reviewer session lifetime, in minutes. After this long a reviewer must
    # re-authenticate. Kept short by default for a privileged approval surface;
    # set to 0 to disable expiry (not recommended).
    review_session_timeout_minutes: int = 15

    # Observability
    otel_exporter_otlp_endpoint: str = ""
    otel_service_name: str = "multi-agent-orchestrator"

    # HTTP API Server configuration
    api_host: str = Field(
        default="127.0.0.1",
        validation_alias=AliasChoices("api_host", "orchestrator_api_host"),
    )
    api_port: int = Field(
        default=8000,
        validation_alias=AliasChoices("api_port", "orchestrator_api_port"),
    )
    api_workers: int = 1
    api_max_request_chars: int = 8000
    api_token: str = Field(
        default="",
        validation_alias=AliasChoices("api_token", "orchestrator_api_token"),
    )
    api_token_previous: str = Field(
        default="",
        description="Previous API bearer token during credential rotation window.",
        validation_alias=AliasChoices(
            "api_token_previous",
            "orchestrator_api_token_previous",
        ),
    )
    api_cors_origins: str = Field(
        default="http://localhost:8501,http://127.0.0.1:8501",
        validation_alias=AliasChoices(
            "api_cors_origins",
            "orchestrator_cors_origins",
        ),
    )
    tool_approval_allowed_approvers: str = Field(
        default="",
        description=(
            "Comma-separated list of authorized approver identities or roles (e.g. 'admin,secops,*'). "
            "Empty permits any authenticated, non-anonymous named operator."
        ),
        validation_alias=AliasChoices(
            "tool_approval_allowed_approvers",
            "orchestrator_allowed_approvers",
            "allowed_approvers",
        ),
    )
    tool_approval_timeout_seconds: float = Field(
        default=3600.0,
        description=(
            "Default expiration timeout in seconds for pending tool approvals. "
            "Bounds: 1.0 to 604800.0 (7 days)."
        ),
        validation_alias=AliasChoices(
            "tool_approval_timeout_seconds",
            "orchestrator_tool_approval_timeout_seconds",
            "tool_approval_timeout",
        ),
    )
    tool_approval_escalation_threshold_seconds: float = Field(
        default=1800.0,
        description=(
            "Time threshold in seconds before approval expiration to trigger escalation warning. "
            "Bounds: 0.0 to 604800.0."
        ),
        validation_alias=AliasChoices(
            "tool_approval_escalation_threshold_seconds",
            "orchestrator_tool_approval_escalation_threshold_seconds",
            "tool_approval_escalation_threshold",
        ),
    )
    tool_approval_escalation_enabled: bool = Field(
        default=True,
        description="Whether automatic escalation tracking is enabled for overdue approvals.",
        validation_alias=AliasChoices(
            "tool_approval_escalation_enabled",
            "orchestrator_tool_approval_escalation_enabled",
        ),
    )
    tool_approval_scheduler_enabled: bool = Field(
        default=True,
        description=(
            "Whether automated background scanning for approval expiration and escalation is enabled."
        ),
        validation_alias=AliasChoices(
            "tool_approval_scheduler_enabled",
            "orchestrator_tool_approval_scheduler_enabled",
            "approval_scheduler_enabled",
        ),
    )
    tool_approval_scan_interval_seconds: float = Field(
        default=60.0,
        description=(
            "Interval in seconds between automated approval expiration and escalation scans. "
            "Bounds: 1.0 to 86400.0 (24 hours)."
        ),
        validation_alias=AliasChoices(
            "tool_approval_scan_interval_seconds",
            "orchestrator_tool_approval_scan_interval_seconds",
            "approval_scan_interval_seconds",
            "tool_approval_scan_interval",
        ),
    )
    tool_approval_scheduler_lease_ttl_seconds: float = Field(
        default=30.0,
        description=(
            "Lease TTL in seconds for cross-process approval lifecycle scheduler leadership. "
            "Bounds: 1.0 to 3600.0."
        ),
        validation_alias=AliasChoices(
            "tool_approval_scheduler_lease_ttl_seconds",
            "orchestrator_tool_approval_scheduler_lease_ttl_seconds",
            "approval_scheduler_lease_ttl_seconds",
            "scheduler_lease_ttl_seconds",
        ),
    )
    tool_approval_scheduler_heartbeat_interval_seconds: float = Field(
        default=5.0,
        description=(
            "Heartbeat renewal interval in seconds for scheduler leadership lease. "
            "Bounds: 0.1 to 300.0."
        ),
        validation_alias=AliasChoices(
            "tool_approval_scheduler_heartbeat_interval_seconds",
            "orchestrator_tool_approval_scheduler_heartbeat_interval_seconds",
            "approval_scheduler_heartbeat_interval_seconds",
            "scheduler_heartbeat_interval_seconds",
        ),
    )

    # Transactional Outbox for Reliable External Notifications
    outbox_enabled: bool = Field(
        default=True,
        description="Whether transactional outbox persistence is enabled.",
        validation_alias=AliasChoices(
            "outbox_enabled",
            "orchestrator_outbox_enabled",
            "tool_approval_outbox_enabled",
        ),
    )
    outbox_dispatcher_enabled: bool = Field(
        default=True,
        description="Whether background outbox dispatcher execution is enabled.",
        validation_alias=AliasChoices(
            "outbox_dispatcher_enabled",
            "orchestrator_outbox_dispatcher_enabled",
        ),
    )
    outbox_poll_interval_seconds: float = Field(
        default=2.0,
        description="Interval in seconds between outbox dispatch polls. Bounds: 0.1 to 300.0.",
        validation_alias=AliasChoices(
            "outbox_poll_interval_seconds",
            "orchestrator_outbox_poll_interval_seconds",
        ),
    )
    outbox_batch_size: int = Field(
        default=50,
        description="Maximum outbox events claimed per dispatch cycle. Bounds: 1 to 1000.",
        validation_alias=AliasChoices(
            "outbox_batch_size",
            "orchestrator_outbox_batch_size",
        ),
    )
    outbox_max_attempts: int = Field(
        default=5,
        description="Max delivery attempts before moving event to dead_letter. Bounds: 1 to 100.",
        validation_alias=AliasChoices(
            "outbox_max_attempts",
            "orchestrator_outbox_max_attempts",
        ),
    )
    outbox_retry_backoff_base_seconds: float = Field(
        default=2.0,
        description="Base backoff in seconds for failed event retries. Bounds: 0.1 to 60.0.",
        validation_alias=AliasChoices(
            "outbox_retry_backoff_base_seconds",
            "orchestrator_outbox_retry_backoff_base_seconds",
        ),
    )
    outbox_retry_backoff_max_seconds: float = Field(
        default=300.0,
        description="Maximum backoff in seconds for failed event retries. Bounds: 1.0 to 3600.0.",
        validation_alias=AliasChoices(
            "outbox_retry_backoff_max_seconds",
            "orchestrator_outbox_retry_backoff_max_seconds",
        ),
    )
    outbox_claim_ttl_seconds: float = Field(
        default=30.0,
        description="Lease TTL in seconds for claimed outbox events. Bounds: 1.0 to 3600.0.",
        validation_alias=AliasChoices(
            "outbox_claim_ttl_seconds",
            "orchestrator_outbox_claim_ttl_seconds",
        ),
    )

    # Production Notification Providers & Webhook Integration
    notification_provider: str = Field(
        default="test",
        description="Notification delivery provider ('test', 'webhook').",
        validation_alias=AliasChoices(
            "notification_provider",
            "orchestrator_notification_provider",
        ),
    )
    notification_webhook_url: str | None = Field(
        default=None,
        description="HTTPS destination URL for webhook notifications.",
        validation_alias=AliasChoices(
            "notification_webhook_url",
            "orchestrator_notification_webhook_url",
        ),
    )
    notification_webhook_auth_token: str | None = Field(
        default=None,
        description="Secret bearer or authentication token for webhook HTTP requests.",
        validation_alias=AliasChoices(
            "notification_webhook_auth_token",
            "orchestrator_notification_webhook_auth_token",
        ),
    )
    notification_webhook_auth_header: str = Field(
        default="Authorization",
        description="HTTP header name used for authorization credentials.",
        validation_alias=AliasChoices(
            "notification_webhook_auth_header",
            "orchestrator_notification_webhook_auth_header",
        ),
    )
    notification_webhook_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP connection and read timeout in seconds. Bounds: 0.5 to 120.0.",
        validation_alias=AliasChoices(
            "notification_webhook_timeout_seconds",
            "orchestrator_notification_webhook_timeout_seconds",
        ),
    )
    notification_webhook_tls_verify: bool = Field(
        default=True,
        description="Whether to verify TLS certificates on webhook HTTPS requests.",
        validation_alias=AliasChoices(
            "notification_webhook_tls_verify",
            "orchestrator_notification_webhook_tls_verify",
        ),
    )
    notification_webhook_max_response_bytes: int = Field(
        default=65536,
        description="Maximum response body bytes captured in error details. Bounds: 256 to 1048576.",
        validation_alias=AliasChoices(
            "notification_webhook_max_response_bytes",
            "orchestrator_notification_webhook_max_response_bytes",
        ),
    )
    notification_webhook_allow_private_ips: bool = Field(
        default=False,
        description="Allow private, loopback, or link-local destinations (SSRF bypass, testing only).",
        validation_alias=AliasChoices(
            "notification_webhook_allow_private_ips",
            "orchestrator_notification_webhook_allow_private_ips",
        ),
    )
    notification_webhook_allow_insecure_http: bool = Field(
        default=False,
        description="Allow unencrypted http:// scheme (testing only, production requires https://).",
        validation_alias=AliasChoices(
            "notification_webhook_allow_insecure_http",
            "orchestrator_notification_webhook_allow_insecure_http",
        ),
    )
    notification_webhook_signing_secret: str | None = Field(
        default=None,
        description="HMAC-SHA256 secret key for signing outbound webhook payloads.",
        validation_alias=AliasChoices(
            "notification_webhook_signing_secret",
            "orchestrator_notification_webhook_signing_secret",
            "webhook_signing_secret",
        ),
    )
    notification_webhook_signing_secret_previous: str | None = Field(
        default=None,
        description="Previous HMAC-SHA256 signing secret during rotation transition window.",
        validation_alias=AliasChoices(
            "notification_webhook_signing_secret_previous",
            "orchestrator_notification_webhook_signing_secret_previous",
            "webhook_signing_secret_previous",
        ),
    )
    notification_webhook_signature_header: str = Field(
        default="X-Webhook-Signature",
        description="HTTP header name for HMAC payload signature.",
        validation_alias=AliasChoices(
            "notification_webhook_signature_header",
            "orchestrator_notification_webhook_signature_header",
            "webhook_signature_header",
        ),
    )
    notification_webhook_signature_tolerance_seconds: float = Field(
        default=300.0,
        description="Timestamp tolerance window in seconds for webhook verification.",
        validation_alias=AliasChoices(
            "notification_webhook_signature_tolerance_seconds",
            "orchestrator_notification_webhook_signature_tolerance_seconds",
            "webhook_signature_tolerance_seconds",
        ),
    )
    notification_webhook_replay_protection_enabled: bool = Field(
        default=True,
        description="Enable replay protection for webhook signature verification.",
        validation_alias=AliasChoices(
            "notification_webhook_replay_protection_enabled",
            "orchestrator_notification_webhook_replay_protection_enabled",
            "webhook_replay_protection_enabled",
        ),
    )
    notification_webhook_replay_storage: str = Field(
        default="sqlite",
        description="Backend store for replay protection ('sqlite' or 'memory').",
        validation_alias=AliasChoices(
            "notification_webhook_replay_storage",
            "orchestrator_notification_webhook_replay_storage",
            "webhook_replay_storage",
        ),
    )
    notification_webhook_replay_retention_seconds: float = Field(
        default=86400.0,
        description="TTL retention window in seconds for replay protection records.",
        validation_alias=AliasChoices(
            "notification_webhook_replay_retention_seconds",
            "orchestrator_notification_webhook_replay_retention_seconds",
            "webhook_replay_retention_seconds",
        ),
    )
    notification_webhook_replay_cleanup_enabled: bool = Field(
        default=True,
        description="Enable periodic background cleanup of expired replay records.",
        validation_alias=AliasChoices(
            "notification_webhook_replay_cleanup_enabled",
            "orchestrator_notification_webhook_replay_cleanup_enabled",
            "webhook_replay_cleanup_enabled",
        ),
    )
    notification_webhook_replay_cleanup_interval_seconds: float = Field(
        default=300.0,
        description="Interval in seconds between automated replay record cleanup runs.",
        validation_alias=AliasChoices(
            "notification_webhook_replay_cleanup_interval_seconds",
            "orchestrator_notification_webhook_replay_cleanup_interval_seconds",
            "webhook_replay_cleanup_interval_seconds",
        ),
    )
    notification_webhook_replay_cleanup_batch_size: int = Field(
        default=500,
        description="Max records pruned per expired replay records cleanup batch.",
        validation_alias=AliasChoices(
            "notification_webhook_replay_cleanup_batch_size",
            "orchestrator_notification_webhook_replay_cleanup_batch_size",
            "webhook_replay_cleanup_batch_size",
        ),
    )

    # Request body and rate limits
    api_max_body_bytes: int = Field(
        default=1_000_000,
        validation_alias=AliasChoices(
            "api_max_body_bytes",
            "orchestrator_api_max_body_bytes",
            "orchestrator_max_body_bytes",
        ),
    )
    api_rate_limit_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "api_rate_limit_enabled",
            "orchestrator_rate_limit_enabled",
        ),
    )
    api_rate_limit_requests: int = Field(
        default=60,
        validation_alias=AliasChoices(
            "api_rate_limit_requests",
            "orchestrator_rate_limit_requests",
        ),
    )
    api_rate_limit_window_seconds: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            "api_rate_limit_window_seconds",
            "orchestrator_rate_limit_window_seconds",
        ),
    )
    api_trusted_proxies: str = Field(
        default="127.0.0.1,::1,localhost",
        validation_alias=AliasChoices(
            "api_trusted_proxies",
            "orchestrator_trusted_proxies",
        ),
    )

    # API Lifecycle: Graceful Shutdown & Interrupted Run Recovery
    api_shutdown_grace_seconds: float = Field(
        default=5.0,
        validation_alias=AliasChoices(
            "api_shutdown_grace_seconds",
            "orchestrator_shutdown_grace_seconds",
            "shutdown_grace_seconds",
        ),
    )
    api_recovery_on_startup: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "api_recovery_on_startup",
            "orchestrator_recovery_on_startup",
            "recovery_on_startup",
        ),
    )

    # Audit Retention, Storage Management & Safe Export
    audit_retention_days: int = Field(
        default=30,
        validation_alias=AliasChoices(
            "audit_retention_days",
            "orchestrator_audit_retention_days",
        ),
    )
    audit_cleanup_on_startup: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "audit_cleanup_on_startup",
            "orchestrator_audit_cleanup_on_startup",
        ),
    )
    audit_export_max_limit: int = Field(
        default=1000,
        validation_alias=AliasChoices(
            "audit_export_max_limit",
            "orchestrator_audit_export_max_limit",
        ),
    )

    # Idempotent Workflow Submission & Safe Retries
    idempotency_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "idempotency_enabled",
            "orchestrator_idempotency_enabled",
        ),
    )
    idempotency_retention_hours: int = Field(
        default=24,
        validation_alias=AliasChoices(
            "idempotency_retention_hours",
            "orchestrator_idempotency_retention_hours",
        ),
    )

    # Background Execution & Celery Configuration
    workflow_execution_backend: str = Field(
        default="direct",
        description="Workflow execution backend: 'direct' (synchronous), 'thread', or 'celery'.",
        validation_alias=AliasChoices(
            "workflow_execution_backend",
            "orchestrator_workflow_execution_backend",
        ),
    )
    celery_enabled: bool = Field(
        default=False,
        description="Whether Celery background workflow execution queue is enabled.",
        validation_alias=AliasChoices(
            "celery_enabled",
            "orchestrator_celery_enabled",
        ),
    )
    celery_broker_url: str = Field(
        default="redis://localhost:6379/0",
        description="Message broker URL for Celery task queuing.",
        validation_alias=AliasChoices(
            "celery_broker_url",
            "orchestrator_celery_broker_url",
            "celery_broker",
        ),
    )
    celery_result_backend: str | None = Field(
        default=None,
        description="Optional result backend URL for Celery task state tracking.",
        validation_alias=AliasChoices(
            "celery_result_backend",
            "orchestrator_celery_result_backend",
        ),
    )
    celery_task_default_queue: str = Field(
        default="orchestrator_workflows",
        description="Default Celery queue name for orchestration tasks.",
        validation_alias=AliasChoices(
            "celery_task_default_queue",
            "celery_default_queue",
            "orchestrator_celery_task_default_queue",
        ),
    )
    celery_worker_concurrency: int = Field(
        default=4,
        description="Number of concurrent worker processes/threads for Celery.",
        validation_alias=AliasChoices(
            "celery_worker_concurrency",
            "orchestrator_celery_worker_concurrency",
        ),
    )
    celery_task_time_limit: float | None = Field(
        default=1800.0,
        description="Hard time limit in seconds for Celery task execution.",
        validation_alias=AliasChoices(
            "celery_task_time_limit",
            "orchestrator_celery_task_time_limit",
        ),
    )
    celery_task_soft_time_limit: float | None = Field(
        default=1500.0,
        description="Soft time limit in seconds for Celery task execution.",
        validation_alias=AliasChoices(
            "celery_task_soft_time_limit",
            "orchestrator_celery_task_soft_time_limit",
        ),
    )
    celery_always_eager: bool = Field(
        default=False,
        description="Execute Celery tasks locally/synchronously without broker for testing.",
        validation_alias=AliasChoices(
            "celery_always_eager",
            "orchestrator_celery_always_eager",
        ),
    )
    celery_worker_heartbeat_interval_seconds: float = Field(
        default=15.0,
        description="Interval in seconds between worker heartbeat pulses.",
        validation_alias=AliasChoices(
            "celery_worker_heartbeat_interval_seconds",
            "orchestrator_celery_worker_heartbeat_interval_seconds",
        ),
    )
    celery_worker_stale_threshold_seconds: float = Field(
        default=60.0,
        description="Threshold in seconds after which a worker is considered stale/unresponsive.",
        validation_alias=AliasChoices(
            "celery_worker_stale_threshold_seconds",
            "orchestrator_celery_worker_stale_threshold_seconds",
        ),
    )
    celery_queue_monitoring_interval_seconds: float = Field(
        default=30.0,
        description="Interval in seconds between queue metrics monitoring checks.",
        validation_alias=AliasChoices(
            "celery_queue_monitoring_interval_seconds",
            "orchestrator_celery_queue_monitoring_interval_seconds",
        ),
    )
    celery_workflow_stuck_threshold_seconds: float = Field(
        default=300.0,
        description="Threshold in seconds after which a workflow without progress is considered stuck.",
        validation_alias=AliasChoices(
            "celery_workflow_stuck_threshold_seconds",
            "orchestrator_celery_workflow_stuck_threshold_seconds",
        ),
    )
    celery_recovery_batch_size: int = Field(
        default=50,
        description="Maximum number of stuck workflows processed per recovery operation.",
        validation_alias=AliasChoices(
            "celery_recovery_batch_size",
            "orchestrator_celery_recovery_batch_size",
        ),
    )

    # ── Field Validators ──────────────────────────────────────────────────────

    @field_validator("max_specialist_concurrency")
    @classmethod
    def validate_max_specialist_concurrency(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_specialist_concurrency must be an integer >= 0.")
        if v > 128:
            raise ValueError("max_specialist_concurrency cannot exceed 128.")
        return v

    @field_validator("max_concurrency")
    @classmethod
    def validate_max_concurrency(cls, v: int | None) -> int | None:
        if v is not None:
            if v < 1:
                raise ValueError("max_concurrency must be an integer >= 1.")
            if v > 128:
                raise ValueError("max_concurrency cannot exceed 128.")
        return v

    @field_validator("max_active_runs")
    @classmethod
    def validate_max_active_runs(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_active_runs must be an integer >= 1.")
        if v > 1024:
            raise ValueError("max_active_runs cannot exceed 1024.")
        return v

    @field_validator("admission_lease_ttl_seconds")
    @classmethod
    def validate_admission_lease_ttl(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("admission_lease_ttl_seconds must be a number >= 1.0.")
        if v > 3600.0:
            raise ValueError("admission_lease_ttl_seconds cannot exceed 3600.0 seconds.")
        return round(float(v), 2)

    @field_validator("admission_heartbeat_interval_seconds")
    @classmethod
    def validate_admission_heartbeat_interval(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("admission_heartbeat_interval_seconds must be a number >= 0.1.")
        if v > 300.0:
            raise ValueError("admission_heartbeat_interval_seconds cannot exceed 300.0 seconds.")
        return round(float(v), 2)

    @field_validator("api_port", "postgres_port", "chroma_port")
    @classmethod
    def validate_port_number(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError(f"Port number must be between 1 and 65535, got {v}.")
        return v

    @field_validator("api_host")
    @classmethod
    def validate_api_host(cls, v: str) -> str:
        clean = (v or "").strip()
        if not clean:
            raise ValueError("api_host must be a non-empty hostname or IP address.")
        return clean

    @field_validator("api_max_body_bytes")
    @classmethod
    def validate_api_max_body_bytes(cls, v: int) -> int:
        if v < 1:
            raise ValueError("api_max_body_bytes must be a positive integer >= 1.")
        if v > 100_000_000:
            raise ValueError("api_max_body_bytes cannot exceed 100MB (100,000,000 bytes).")
        return v

    @field_validator("api_rate_limit_requests")
    @classmethod
    def validate_rate_limit_requests(cls, v: int) -> int:
        if v < 1:
            raise ValueError("api_rate_limit_requests must be an integer >= 1.")
        return v

    @field_validator("api_rate_limit_window_seconds")
    @classmethod
    def validate_rate_limit_window(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("api_rate_limit_window_seconds must be a positive number greater than 0.")
        return v

    @field_validator("checkpoint_db_path")
    @classmethod
    def validate_checkpoint_db_path(cls, v: str) -> str:
        clean = (v or "").strip()
        if not clean:
            raise ValueError("checkpoint_db_path must be a non-empty file path or ':memory:'.")
        return clean

    @field_validator("backup_dir")
    @classmethod
    def validate_backup_dir(cls, v: str) -> str:
        clean = (v or "").strip()
        if not clean:
            raise ValueError("backup_dir must not be empty.")
        return clean

    @field_validator("backup_retention_count")
    @classmethod
    def validate_backup_retention_count(cls, v: int) -> int:
        if v < 0:
            raise ValueError("backup_retention_count must be non-negative (>= 0).")
        return v

    @field_validator("backup_retention_days")
    @classmethod
    def validate_backup_retention_days(cls, v: int) -> int:
        if v < 0:
            raise ValueError("backup_retention_days must be non-negative (>= 0).")
        return v

    @field_validator("backup_interval_seconds")
    @classmethod
    def validate_backup_interval(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("backup_interval_seconds must be at least 1.0 second.")
        if v > 2592000.0:
            raise ValueError("backup_interval_seconds cannot exceed 2592000.0 seconds (30 days).")
        return float(v)

    @field_validator("backup_max_duration_seconds")
    @classmethod
    def validate_backup_max_duration(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("backup_max_duration_seconds must be at least 1.0 second.")
        if v > 86400.0:
            raise ValueError("backup_max_duration_seconds cannot exceed 86400.0 seconds.")
        return float(v)

    @field_validator("backup_max_retries")
    @classmethod
    def validate_backup_max_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("backup_max_retries must be non-negative (>= 0).")
        if v > 20:
            raise ValueError("backup_max_retries cannot exceed 20.")
        return int(v)

    @field_validator("backup_retry_backoff_seconds")
    @classmethod
    def validate_backup_retry_backoff(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("backup_retry_backoff_seconds must be at least 0.1 seconds.")
        if v > 3600.0:
            raise ValueError("backup_retry_backoff_seconds cannot exceed 3600.0 seconds.")
        return float(v)

    @field_validator("backup_max_backoff_seconds")
    @classmethod
    def validate_backup_max_backoff(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("backup_max_backoff_seconds must be at least 0.1 seconds.")
        if v > 86400.0:
            raise ValueError("backup_max_backoff_seconds cannot exceed 86400.0 seconds.")
        return float(v)

    @field_validator("backup_scheduler_lease_ttl_seconds")
    @classmethod
    def validate_backup_scheduler_lease_ttl(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("backup_scheduler_lease_ttl_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError("backup_scheduler_lease_ttl_seconds cannot exceed 3600.0 seconds.")
        return float(v)

    @field_validator("backup_scheduler_heartbeat_interval_seconds")
    @classmethod
    def validate_backup_scheduler_heartbeat_interval(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("backup_scheduler_heartbeat_interval_seconds must be at least 0.1s.")
        if v > 300.0:
            raise ValueError("backup_scheduler_heartbeat_interval_seconds cannot exceed 300.0s.")
        return float(v)

    @field_validator("restore_drill_interval_seconds")
    @classmethod
    def validate_restore_drill_interval(cls, v: float) -> float:
        if v < 10.0:
            raise ValueError("restore_drill_interval_seconds must be at least 10.0 seconds.")
        if v > 31536000.0:
            raise ValueError(
                "restore_drill_interval_seconds cannot exceed 31536000.0 seconds (365 days)."
            )
        return float(v)

    @field_validator("restore_drill_max_duration_seconds")
    @classmethod
    def validate_restore_drill_max_duration(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("restore_drill_max_duration_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError(
                "restore_drill_max_duration_seconds cannot exceed 3600.0 seconds."
            )
        return float(v)

    @field_validator("restore_drill_history_limit")
    @classmethod
    def validate_restore_drill_history_limit(cls, v: int) -> int:
        if v < 1:
            raise ValueError("restore_drill_history_limit must be at least 1.")
        if v > 1000:
            raise ValueError("restore_drill_history_limit cannot exceed 1000.")
        return int(v)

    @field_validator("restore_drill_lease_ttl_seconds")
    @classmethod
    def validate_restore_drill_lease_ttl(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("restore_drill_lease_ttl_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError("restore_drill_lease_ttl_seconds cannot exceed 3600.0 seconds.")
        return float(v)

    @field_validator("default_llm_provider")
    @classmethod
    def validate_default_provider(cls, v: str) -> str:
        clean = (v or "").strip().lower()
        allowed = {"mock", "openai", "anthropic"}
        if clean not in allowed:
            raise ValueError(
                f"Invalid default_llm_provider '{v}'. Must be one of: {', '.join(sorted(allowed))}."
            )
        return clean

    @field_validator("fallback_llm_provider")
    @classmethod
    def validate_fallback_provider(cls, v: str) -> str:
        clean = (v or "").strip().lower()
        allowed = {"", "none", "mock", "openai", "anthropic"}
        if clean not in allowed:
            raise ValueError(
                f"Invalid fallback_llm_provider '{v}'. Must be one of: {', '.join(sorted(allowed))}."
            )
        return "" if clean == "none" else clean

    @field_validator("llm_max_retries")
    @classmethod
    def validate_llm_max_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("llm_max_retries must be a non-negative integer (>= 0).")
        if v > 20:
            raise ValueError("llm_max_retries cannot exceed 20.")
        return v

    @field_validator("llm_timeout_seconds")
    @classmethod
    def validate_llm_timeout_seconds(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("llm_timeout_seconds must be a positive number greater than 0.")
        if v > 600.0:
            raise ValueError("llm_timeout_seconds cannot exceed 600.0 seconds.")
        return v

    @field_validator("workflow_timeout_seconds")
    @classmethod
    def validate_workflow_timeout_seconds(cls, v: float | None) -> float | None:
        if v is not None:
            if v <= 0:
                raise ValueError("workflow_timeout_seconds must be a positive number greater than 0.")
            if v > 86400.0:
                raise ValueError("workflow_timeout_seconds cannot exceed 86400.0 seconds (24 hours).")
        return v

    @field_validator("max_task_chars", "api_max_request_chars")
    @classmethod
    def validate_char_limits(cls, v: int) -> int:
        if v < 1:
            raise ValueError("Character limit must be an integer >= 1.")
        return v

    @field_validator("confidence_threshold", "quality_threshold")
    @classmethod
    def validate_thresholds(cls, v: float) -> float:
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Threshold must be between 0.0 and 1.0, got {v}.")
        return v

    @field_validator("max_retries")
    @classmethod
    def validate_retries(cls, v: int) -> int:
        if v < 0:
            raise ValueError("max_retries must be an integer >= 0.")
        return v

    @field_validator("tool_approval_timeout_seconds")
    @classmethod
    def validate_tool_approval_timeout_seconds(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("tool_approval_timeout_seconds must be at least 1.0 second.")
        if v > 604800.0:
            raise ValueError(
                "tool_approval_timeout_seconds cannot exceed 604800.0 seconds (7 days)."
            )
        return v

    @field_validator("tool_approval_escalation_threshold_seconds")
    @classmethod
    def validate_tool_approval_escalation_threshold_seconds(cls, v: float) -> float:
        if v < 0.0:
            raise ValueError(
                "tool_approval_escalation_threshold_seconds must be a non-negative number."
            )
        if v > 604800.0:
            raise ValueError(
                "tool_approval_escalation_threshold_seconds cannot exceed 604800.0 seconds (7 days)."
            )
        return v

    @field_validator("tool_approval_scan_interval_seconds")
    @classmethod
    def validate_tool_approval_scan_interval_seconds(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("tool_approval_scan_interval_seconds must be at least 1.0 second.")
        if v > 86400.0:
            raise ValueError(
                "tool_approval_scan_interval_seconds cannot exceed 86400.0 seconds (24 hours)."
            )
        return v

    @field_validator("tool_approval_scheduler_lease_ttl_seconds")
    @classmethod
    def validate_tool_approval_scheduler_lease_ttl(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("tool_approval_scheduler_lease_ttl_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError(
                "tool_approval_scheduler_lease_ttl_seconds cannot exceed 3600.0 seconds."
            )
        return round(float(v), 2)

    @field_validator("tool_approval_scheduler_heartbeat_interval_seconds")
    @classmethod
    def validate_tool_approval_scheduler_heartbeat_interval(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError(
                "tool_approval_scheduler_heartbeat_interval_seconds must be at least 0.1 seconds."
            )
        if v > 300.0:
            raise ValueError(
                "tool_approval_scheduler_heartbeat_interval_seconds cannot exceed 300.0 seconds."
            )
        return round(float(v), 2)

    @field_validator("outbox_poll_interval_seconds")
    @classmethod
    def validate_outbox_poll_interval_seconds(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("outbox_poll_interval_seconds must be at least 0.1 seconds.")
        if v > 300.0:
            raise ValueError("outbox_poll_interval_seconds cannot exceed 300.0 seconds.")
        return round(float(v), 2)

    @field_validator("outbox_batch_size")
    @classmethod
    def validate_outbox_batch_size(cls, v: int) -> int:
        if v < 1:
            raise ValueError("outbox_batch_size must be at least 1.")
        if v > 1000:
            raise ValueError("outbox_batch_size cannot exceed 1000.")
        return int(v)

    @field_validator("outbox_max_attempts")
    @classmethod
    def validate_outbox_max_attempts(cls, v: int) -> int:
        if v < 1:
            raise ValueError("outbox_max_attempts must be at least 1.")
        if v > 100:
            raise ValueError("outbox_max_attempts cannot exceed 100.")
        return int(v)

    @field_validator("outbox_retry_backoff_base_seconds")
    @classmethod
    def validate_outbox_retry_backoff_base(cls, v: float) -> float:
        if v < 0.1:
            raise ValueError("outbox_retry_backoff_base_seconds must be at least 0.1 seconds.")
        if v > 60.0:
            raise ValueError("outbox_retry_backoff_base_seconds cannot exceed 60.0 seconds.")
        return round(float(v), 2)

    @field_validator("outbox_retry_backoff_max_seconds")
    @classmethod
    def validate_outbox_retry_backoff_max(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("outbox_retry_backoff_max_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError("outbox_retry_backoff_max_seconds cannot exceed 3600.0 seconds.")
        return round(float(v), 2)

    @field_validator("outbox_claim_ttl_seconds")
    @classmethod
    def validate_outbox_claim_ttl(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError("outbox_claim_ttl_seconds must be at least 1.0 second.")
        if v > 3600.0:
            raise ValueError("outbox_claim_ttl_seconds cannot exceed 3600.0 seconds.")
        return round(float(v), 2)

    @field_validator("notification_provider")
    @classmethod
    def validate_notification_provider(cls, v: str) -> str:
        val = str(v).strip().lower()
        if val not in ("test", "webhook"):
            raise ValueError(f"notification_provider must be 'test' or 'webhook', got '{v}'.")
        return val

    @field_validator("notification_webhook_timeout_seconds")
    @classmethod
    def validate_notification_webhook_timeout(cls, v: float) -> float:
        if v < 0.5:
            raise ValueError("notification_webhook_timeout_seconds must be at least 0.5 seconds.")
        if v > 120.0:
            raise ValueError("notification_webhook_timeout_seconds cannot exceed 120.0 seconds.")
        return round(float(v), 2)

    @field_validator("notification_webhook_max_response_bytes")
    @classmethod
    def validate_notification_webhook_max_bytes(cls, v: int) -> int:
        if v < 256:
            raise ValueError("notification_webhook_max_response_bytes must be at least 256 bytes.")
        if v > 1048576:
            raise ValueError("notification_webhook_max_response_bytes cannot exceed 1048576 bytes.")
        return int(v)

    @field_validator("notification_webhook_signature_tolerance_seconds")
    @classmethod
    def validate_signature_tolerance(cls, v: float) -> float:
        if v < 5.0:
            raise ValueError(
                "notification_webhook_signature_tolerance_seconds must be at least 5.0 seconds."
            )
        if v > 86400.0:
            raise ValueError(
                "notification_webhook_signature_tolerance_seconds cannot exceed 86400.0 seconds."
            )
        return round(float(v), 2)

    @field_validator("notification_webhook_replay_storage")
    @classmethod
    def validate_replay_storage(cls, v: str) -> str:
        s = str(v).lower().strip()
        if s not in {"sqlite", "memory"}:
            raise ValueError(
                f"notification_webhook_replay_storage must be 'sqlite' or 'memory', got '{v}'."
            )
        return s

    @field_validator("notification_webhook_replay_retention_seconds")
    @classmethod
    def validate_replay_retention_seconds(cls, v: float) -> float:
        if v < 60.0:
            raise ValueError(
                "notification_webhook_replay_retention_seconds must be at least 60.0 seconds."
            )
        if v > 2592000.0:
            raise ValueError(
                "notification_webhook_replay_retention_seconds cannot exceed 2592000.0 seconds (30 days)."
            )
        return round(float(v), 2)

    @field_validator("notification_webhook_replay_cleanup_interval_seconds")
    @classmethod
    def validate_replay_cleanup_interval(cls, v: float) -> float:
        if v < 1.0:
            raise ValueError(
                "notification_webhook_replay_cleanup_interval_seconds must be at least 1.0 seconds."
            )
        if v > 86400.0:
            raise ValueError(
                "notification_webhook_replay_cleanup_interval_seconds cannot exceed 86400.0 seconds."
            )
        return round(float(v), 2)

    @field_validator("notification_webhook_replay_cleanup_batch_size")
    @classmethod
    def validate_replay_cleanup_batch_size(cls, v: int) -> int:
        if v < 10:
            raise ValueError(
                "notification_webhook_replay_cleanup_batch_size must be at least 10."
            )
        if v > 10000:
            raise ValueError(
                "notification_webhook_replay_cleanup_batch_size cannot exceed 10000."
            )
        return int(v)

    @field_validator("audit_retention_days")
    @classmethod
    def validate_audit_retention_days(cls, v: int) -> int:
        if v < 0:
            raise ValueError("audit_retention_days must be an integer >= 0 (0 disables automatic retention).")
        return v

    @field_validator("audit_export_max_limit")
    @classmethod
    def validate_audit_export_max_limit(cls, v: int) -> int:
        if v < 1:
            raise ValueError("audit_export_max_limit must be an integer >= 1.")
        if v > 10000:
            raise ValueError("audit_export_max_limit cannot exceed 10000.")
        return v

    @field_validator("idempotency_retention_hours")
    @classmethod
    def validate_idempotency_retention_hours(cls, v: int) -> int:
        if v < 1:
            raise ValueError("idempotency_retention_hours must be an integer >= 1.")
        if v > 87600:
            raise ValueError("idempotency_retention_hours cannot exceed 87600 hours.")
        return v

    @field_validator("llm_custom_pricing")
    @classmethod
    def validate_custom_pricing(cls, v: str) -> str:
        clean = (v or "").strip()
        if not clean:
            return ""
        try:
            data = json.loads(clean)
        except Exception as exc:
            raise ValueError(f"llm_custom_pricing must be valid JSON: {exc}") from exc

        if not isinstance(data, dict):
            raise ValueError("llm_custom_pricing must be a JSON object mapping 'provider:model' to rates.")

        for key, rate in data.items():
            if not isinstance(rate, dict):
                raise ValueError(
                    f"Pricing rate for '{key}' must be a JSON object containing input and output rates."
                )
            in_rate = rate.get("input_cost_per_1m", rate.get("input"))
            out_rate = rate.get("output_cost_per_1m", rate.get("output"))
            if in_rate is None or out_rate is None:
                raise ValueError(
                    f"Pricing rate for '{key}' must specify 'input_cost_per_1m' and 'output_cost_per_1m'."
                )
            try:
                in_val = float(in_rate)
                out_val = float(out_rate)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"Pricing rate for '{key}' has non-numeric cost values.") from exc
            if in_val < 0.0 or out_val < 0.0:
                raise ValueError(f"Pricing rate for '{key}' cannot have negative rates.")

        return clean

    @field_validator("api_shutdown_grace_seconds")
    @classmethod
    def validate_api_shutdown_grace_seconds(cls, v: float) -> float:
        if v < 0.0:
            raise ValueError("api_shutdown_grace_seconds must be a non-negative float >= 0.0.")
        if v > 300.0:
            raise ValueError("api_shutdown_grace_seconds cannot exceed 300.0 seconds.")
        return round(float(v), 2)

    @field_validator("workflow_execution_backend")
    @classmethod
    def validate_workflow_execution_backend(cls, v: str) -> str:
        clean = (v or "").strip().lower()
        if clean in ("sync", "direct"):
            return "direct"
        if clean in ("thread", "celery"):
            return clean
        raise ValueError(
            f"Invalid workflow_execution_backend '{v}'. Must be one of: 'direct', 'thread', 'celery'."
        )

    @field_validator("celery_worker_concurrency")
    @classmethod
    def validate_celery_worker_concurrency(cls, v: int) -> int:
        if v < 1:
            raise ValueError("celery_worker_concurrency must be an integer >= 1.")
        if v > 128:
            raise ValueError("celery_worker_concurrency cannot exceed 128.")
        return v

    @field_validator("celery_task_time_limit")
    @classmethod
    def validate_celery_task_time_limit(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("celery_task_time_limit must be a positive float greater than 0.")
        return v

    @field_validator("celery_task_soft_time_limit")
    @classmethod
    def validate_celery_task_soft_time_limit(cls, v: float | None) -> float | None:
        if v is not None and v <= 0:
            raise ValueError("celery_task_soft_time_limit must be a positive float greater than 0.")
        return v

    @field_validator("celery_worker_heartbeat_interval_seconds")
    @classmethod
    def validate_celery_worker_heartbeat_interval_seconds(cls, v: float) -> float:
        if v < 1.0 or v > 300.0:
            raise ValueError(
                f"celery_worker_heartbeat_interval_seconds must be between 1.0 and 300.0, got {v}."
            )
        return float(v)

    @field_validator("celery_worker_stale_threshold_seconds")
    @classmethod
    def validate_celery_worker_stale_threshold_seconds(cls, v: float) -> float:
        if v < 5.0 or v > 3600.0:
            raise ValueError(
                f"celery_worker_stale_threshold_seconds must be between 5.0 and 3600.0, got {v}."
            )
        return float(v)

    @field_validator("celery_queue_monitoring_interval_seconds")
    @classmethod
    def validate_celery_queue_monitoring_interval_seconds(cls, v: float) -> float:
        if v < 1.0 or v > 600.0:
            raise ValueError(
                f"celery_queue_monitoring_interval_seconds must be between 1.0 and 600.0, got {v}."
            )
        return float(v)

    @field_validator("celery_workflow_stuck_threshold_seconds")
    @classmethod
    def validate_celery_workflow_stuck_threshold_seconds(cls, v: float) -> float:
        if v < 10.0 or v > 86400.0:
            raise ValueError(
                f"celery_workflow_stuck_threshold_seconds must be between 10.0 and 86400.0, got {v}."
            )
        return float(v)

    @field_validator("celery_recovery_batch_size")
    @classmethod
    def validate_celery_recovery_batch_size(cls, v: int) -> int:
        if v < 1 or v > 500:
            raise ValueError(
                f"celery_recovery_batch_size must be between 1 and 500, got {v}."
            )
        return int(v)

    def get_backup_verification_keyring(self) -> dict[str, str]:
        """Construct verification keyring mapping key_id to secret key string."""
        keyring: dict[str, str] = {}
        raw = (self.backup_verification_keys or "").strip()
        if raw:
            if raw.startswith("{"):
                try:
                    import json
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        for k, v in parsed.items():
                            if k and v and isinstance(k, str) and isinstance(v, str):
                                keyring[k.strip()] = v.strip()
                except Exception:
                    pass
            if not keyring:
                for part in raw.split(","):
                    item = part.strip()
                    if ":" in item:
                        kid, secret = item.split(":", 1)
                        if kid.strip() and secret.strip():
                            keyring[kid.strip()] = secret.strip()

        if self.backup_signing_key and self.backup_signing_key.strip():
            kid = (self.backup_signing_key_id or "default").strip()
            keyring[kid] = self.backup_signing_key.strip()

        return keyring

    def get_safe_backup_keyring_info(self) -> dict[str, Any]:
        """Return safe operational status of the backup keyring without leaking secrets."""
        keyring = self.get_backup_verification_keyring()
        has_active = bool(self.backup_signing_key and self.backup_signing_key.strip())
        return {
            "signing_enabled": has_active,
            "active_key_id": self.backup_signing_key_id if has_active else None,
            "verification_key_ids": sorted(keyring.keys()),
            "verification_keys_count": len(keyring),
            "strict_signature_required": self.backup_require_signature,
        }

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def effective_trusted_proxies(self) -> set[str]:
        return {p.strip().lower() for p in self.api_trusted_proxies.split(",") if p.strip()}

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def sanitized_postgres_dsn(self) -> str:
        pwd = "***" if self.postgres_password else ""
        netloc = f"{self.postgres_user}:{pwd}@{self.postgres_host}:{self.postgres_port}"
        return f"postgresql://{netloc}/{self.postgres_db}"

    @property
    def effective_concurrency(self) -> int:
        """Effective specialist concurrency limit, defaulting to a conservative 4."""
        if self.max_concurrency is not None:
            return max(1, self.max_concurrency)
        return max(1, self.max_specialist_concurrency)

    @property
    def active_provider(self) -> str:
        """Return the configured default provider, or mock if unset."""
        return self.default_llm_provider or "mock"


@lru_cache
def get_settings() -> Settings:
    return Settings()


def reset_settings() -> None:
    """Clear the cached settings instance so subsequent calls reload from environment."""
    get_settings.cache_clear()


def validate_configuration(settings: Settings | None = None) -> Settings:
    """Validate all application settings and ensure consistency before execution."""
    s = settings or get_settings()
    if not s.checkpoint_db_path.strip():
        raise ValueError("checkpoint_db_path must not be empty.")
    if not s.backup_dir.strip():
        raise ValueError("backup_dir must not be empty.")
    if s.backup_retention_count < 0:
        raise ValueError("backup_retention_count must be >= 0.")
    if s.backup_retention_days < 0:
        raise ValueError("backup_retention_days must be >= 0.")
    if not (1.0 <= s.backup_interval_seconds <= 2592000.0):
        raise ValueError(
            f"backup_interval_seconds must be between 1.0 and 2592000.0, got {s.backup_interval_seconds}."
        )
    if not (1.0 <= s.backup_max_duration_seconds <= 86400.0):
        raise ValueError(
            f"backup_max_duration_seconds must be between 1.0 and 86400.0, "
            f"got {s.backup_max_duration_seconds}."
        )
    if not (0 <= s.backup_max_retries <= 20):
        raise ValueError(
            f"backup_max_retries must be between 0 and 20, got {s.backup_max_retries}."
        )
    if not (0.1 <= s.backup_retry_backoff_seconds <= 3600.0):
        raise ValueError(
            f"backup_retry_backoff_seconds must be between 0.1 and 3600.0, "
            f"got {s.backup_retry_backoff_seconds}."
        )
    if not (0.1 <= s.backup_max_backoff_seconds <= 86400.0):
        raise ValueError(
            f"backup_max_backoff_seconds must be between 0.1 and 86400.0, "
            f"got {s.backup_max_backoff_seconds}."
        )
    if s.backup_retry_backoff_seconds > s.backup_max_backoff_seconds:
        raise ValueError(
            f"backup_retry_backoff_seconds ({s.backup_retry_backoff_seconds}) must be <= "
            f"backup_max_backoff_seconds ({s.backup_max_backoff_seconds})."
        )
    if not (1.0 <= s.backup_scheduler_lease_ttl_seconds <= 3600.0):
        raise ValueError(
            f"backup_scheduler_lease_ttl_seconds must be between 1.0 and 3600.0, "
            f"got {s.backup_scheduler_lease_ttl_seconds}."
        )
    if not (0.1 <= s.backup_scheduler_heartbeat_interval_seconds <= 300.0):
        raise ValueError(
            f"backup_scheduler_heartbeat_interval_seconds must be between 0.1 and 300.0, "
            f"got {s.backup_scheduler_heartbeat_interval_seconds}."
        )
    if (
        s.backup_scheduler_heartbeat_interval_seconds
        >= s.backup_scheduler_lease_ttl_seconds
    ):
        raise ValueError(
            f"backup_scheduler_heartbeat_interval_seconds "
            f"({s.backup_scheduler_heartbeat_interval_seconds}) must be strictly less than "
            f"backup_scheduler_lease_ttl_seconds ({s.backup_scheduler_lease_ttl_seconds})."
        )
    if not (10.0 <= s.restore_drill_interval_seconds <= 31536000.0):
        raise ValueError(
            f"restore_drill_interval_seconds must be between 10.0 and 31536000.0, "
            f"got {s.restore_drill_interval_seconds}."
        )
    if not (1.0 <= s.restore_drill_max_duration_seconds <= 3600.0):
        raise ValueError(
            f"restore_drill_max_duration_seconds must be between 1.0 and 3600.0, "
            f"got {s.restore_drill_max_duration_seconds}."
        )
    if not (1 <= s.restore_drill_history_limit <= 1000):
        raise ValueError(
            f"restore_drill_history_limit must be between 1 and 1000, "
            f"got {s.restore_drill_history_limit}."
        )
    if not (1.0 <= s.restore_drill_lease_ttl_seconds <= 3600.0):
        raise ValueError(
            f"restore_drill_lease_ttl_seconds must be between 1.0 and 3600.0, "
            f"got {s.restore_drill_lease_ttl_seconds}."
        )
    if not (1 <= s.api_port <= 65535):
        raise ValueError(f"api_port must be between 1 and 65535, got {s.api_port}.")
    if not (1 <= s.effective_concurrency <= 128):
        raise ValueError(
            f"Effective concurrency must be between 1 and 128, got {s.effective_concurrency}."
        )
    if not (1 <= s.max_active_runs <= 1024):
        raise ValueError(
            f"max_active_runs must be between 1 and 1024, got {s.max_active_runs}."
        )
    if not (1.0 <= s.admission_lease_ttl_seconds <= 3600.0):
        raise ValueError(
            f"admission_lease_ttl_seconds must be between 1.0 and 3600.0, "
            f"got {s.admission_lease_ttl_seconds}."
        )
    if not (0.1 <= s.admission_heartbeat_interval_seconds <= 300.0):
        raise ValueError(
            f"admission_heartbeat_interval_seconds must be between 0.1 and 300.0, "
            f"got {s.admission_heartbeat_interval_seconds}."
        )
    if s.admission_heartbeat_interval_seconds >= s.admission_lease_ttl_seconds:
        raise ValueError(
            f"admission_heartbeat_interval_seconds ({s.admission_heartbeat_interval_seconds}) "
            f"must be strictly less than admission_lease_ttl_seconds ({s.admission_lease_ttl_seconds})."
        )
    if not (1.0 <= s.tool_approval_scheduler_lease_ttl_seconds <= 3600.0):
        raise ValueError(
            f"tool_approval_scheduler_lease_ttl_seconds must be between 1.0 and 3600.0, "
            f"got {s.tool_approval_scheduler_lease_ttl_seconds}."
        )
    if not (0.1 <= s.tool_approval_scheduler_heartbeat_interval_seconds <= 300.0):
        raise ValueError(
            f"tool_approval_scheduler_heartbeat_interval_seconds must be between 0.1 and 300.0, "
            f"got {s.tool_approval_scheduler_heartbeat_interval_seconds}."
        )
    if (
        s.tool_approval_scheduler_heartbeat_interval_seconds
        >= s.tool_approval_scheduler_lease_ttl_seconds
    ):
        raise ValueError(
            f"tool_approval_scheduler_heartbeat_interval_seconds "
            f"({s.tool_approval_scheduler_heartbeat_interval_seconds}) must be strictly less than "
            f"tool_approval_scheduler_lease_ttl_seconds ({s.tool_approval_scheduler_lease_ttl_seconds})."
        )
    if s.audit_retention_days < 0:
        raise ValueError(f"audit_retention_days must be non-negative, got {s.audit_retention_days}.")
    if not (1 <= s.audit_export_max_limit <= 10000):
        raise ValueError(
            f"audit_export_max_limit must be between 1 and 10000, got {s.audit_export_max_limit}."
        )
    if s.idempotency_retention_hours < 1:
        raise ValueError(
            f"idempotency_retention_hours must be >= 1, got {s.idempotency_retention_hours}."
        )
    if not (0.1 <= s.outbox_poll_interval_seconds <= 300.0):
        raise ValueError(
            f"outbox_poll_interval_seconds must be between 0.1 and 300.0, "
            f"got {s.outbox_poll_interval_seconds}."
        )
    if not (1 <= s.outbox_batch_size <= 1000):
        raise ValueError(
            f"outbox_batch_size must be between 1 and 1000, got {s.outbox_batch_size}."
        )
    if not (1 <= s.outbox_max_attempts <= 100):
        raise ValueError(
            f"outbox_max_attempts must be between 1 and 100, got {s.outbox_max_attempts}."
        )
    if not (0.1 <= s.outbox_retry_backoff_base_seconds <= 60.0):
        raise ValueError(
            f"outbox_retry_backoff_base_seconds must be between 0.1 and 60.0, "
            f"got {s.outbox_retry_backoff_base_seconds}."
        )
    if not (1.0 <= s.outbox_retry_backoff_max_seconds <= 3600.0):
        raise ValueError(
            f"outbox_retry_backoff_max_seconds must be between 1.0 and 3600.0, "
            f"got {s.outbox_retry_backoff_max_seconds}."
        )
    if s.outbox_retry_backoff_base_seconds > s.outbox_retry_backoff_max_seconds:
        raise ValueError(
            f"outbox_retry_backoff_base_seconds ({s.outbox_retry_backoff_base_seconds}) "
            f"cannot exceed outbox_retry_backoff_max_seconds ({s.outbox_retry_backoff_max_seconds})."
        )
    if not (1.0 <= s.outbox_claim_ttl_seconds <= 3600.0):
        raise ValueError(
            f"outbox_claim_ttl_seconds must be between 1.0 and 3600.0, "
            f"got {s.outbox_claim_ttl_seconds}."
        )
    if s.notification_provider == "webhook":
        if not s.notification_webhook_url or not str(s.notification_webhook_url).strip():
            raise ValueError(
                "notification_webhook_url must be configured when notification_provider is 'webhook'."
            )
        from .notifications.security import validate_webhook_url

        try:
            validate_webhook_url(
                s.notification_webhook_url,
                allow_private_ips=s.notification_webhook_allow_private_ips,
                allow_insecure_http=s.notification_webhook_allow_insecure_http,
                resolve_dns=False,
            )
        except Exception as exc:
            raise ValueError(f"Invalid notification_webhook_url: {exc}") from exc
    if s.celery_worker_concurrency < 1 or s.celery_worker_concurrency > 128:
        raise ValueError(
            f"celery_worker_concurrency must be between 1 and 128, got {s.celery_worker_concurrency}."
        )
    if s.celery_task_time_limit is not None and s.celery_task_time_limit <= 0:
        raise ValueError("celery_task_time_limit must be > 0.")
    if s.celery_task_soft_time_limit is not None and s.celery_task_soft_time_limit <= 0:
        raise ValueError("celery_task_soft_time_limit must be > 0.")
    if (
        s.celery_task_time_limit is not None
        and s.celery_task_soft_time_limit is not None
        and s.celery_task_soft_time_limit > s.celery_task_time_limit
    ):
        raise ValueError(
            f"celery_task_soft_time_limit ({s.celery_task_soft_time_limit}) "
            f"cannot exceed celery_task_time_limit ({s.celery_task_time_limit})."
        )
    if s.celery_worker_heartbeat_interval_seconds >= s.celery_worker_stale_threshold_seconds:
        raise ValueError(
            f"celery_worker_heartbeat_interval_seconds ({s.celery_worker_heartbeat_interval_seconds}) "
            f"must be strictly less than celery_worker_stale_threshold_seconds "
            f"({s.celery_worker_stale_threshold_seconds})."
        )

    # ── Non-loopback network binding validation ───────────────────────────────
    if not is_loopback_host(s.api_host):
        import os

        clean_token = (
            s.api_token
            or getattr(s, "api_token_previous", "")
            or os.environ.get("ORCHESTRATOR_API_TOKEN")
            or os.environ.get("ORCHESTRATOR_API_TOKEN_PREVIOUS")
            or ""
        ).strip()
        if not clean_token:
            raise ValueError(
                f"Unsafe network binding rejected: Cannot bind API server to non-loopback host "
                f"'{s.api_host}' without authentication. Configure ORCHESTRATOR_API_TOKEN or "
                "bind to 127.0.0.1."
            )
        if clean_token.startswith("replace_with_"):
            raise ValueError(
                "Unsafe API token: ORCHESTRATOR_API_TOKEN contains an unreplaced placeholder value. "
                "Configure a production-grade secret token."
            )
        if len(clean_token) < 16:
            raise ValueError(
                f"Insecure API token: ORCHESTRATOR_API_TOKEN must be at least 16 characters for "
                f"non-loopback network binding, got {len(clean_token)}."
            )

    # ── Celery Broker & Result Backend URL Validation ─────────────────────────
    if s.celery_broker_url:
        from urllib.parse import urlsplit

        p_broker = urlsplit(s.celery_broker_url)
        valid_broker_schemes = {"redis", "rediss", "amqp", "amqps", "memory", "sqs"}
        if not p_broker.scheme or p_broker.scheme.lower() not in valid_broker_schemes:
            safe_b = sanitize_broker_url(s.celery_broker_url)
            raise ValueError(
                f"Invalid Celery broker URL '{safe_b}'. Supported schemes: "
                f"{', '.join(sorted(valid_broker_schemes))}."
            )

    if s.celery_result_backend:
        from urllib.parse import urlsplit

        p_res = urlsplit(s.celery_result_backend)
        valid_res_schemes = {
            "redis",
            "rediss",
            "rpc",
            "db+postgresql",
            "postgresql",
            "cache+memory",
            "mongodb",
        }
        if not p_res.scheme or p_res.scheme.lower() not in valid_res_schemes:
            safe_r = sanitize_broker_url(s.celery_result_backend)
            raise ValueError(
                f"Invalid Celery result backend URL '{safe_r}'. Supported schemes: "
                f"{', '.join(sorted(valid_res_schemes))}."
            )

    if s.redis_url:
        from urllib.parse import urlsplit

        p_redis = urlsplit(s.redis_url)
        if p_redis.scheme and p_redis.scheme.lower() not in {"redis", "rediss"}:
            safe_red = sanitize_broker_url(s.redis_url)
            raise ValueError(f"Invalid redis_url '{safe_red}'. Must use 'redis://' or 'rediss://'.")

    # ── Celery & Storage Compatibility ────────────────────────────────────────
    if (
        (s.workflow_execution_backend == "celery" or s.celery_enabled)
        and not s.celery_always_eager
        and s.checkpoint_db_path in (":memory:", "file::memory:?cache=shared")
    ):
        raise ValueError(
            "Incompatible configuration: Distributed Celery execution requires persistent shared "
            "storage. In-memory SQLite ':memory:' cannot be shared across API and Celery worker processes."
        )

    # ── Unreplaced Secret Placeholders Validation ─────────────────────────────
    if s.openai_api_key and s.openai_api_key.strip().startswith("replace_with_"):
        raise ValueError("openai_api_key contains an unreplaced placeholder value.")
    if s.anthropic_api_key and s.anthropic_api_key.strip().startswith("replace_with_"):
        raise ValueError("anthropic_api_key contains an unreplaced placeholder value.")
    if s.backup_signing_key and s.backup_signing_key.strip().startswith("replace_with_"):
        raise ValueError("backup_signing_key contains an unreplaced placeholder value.")
    if (
        s.notification_webhook_signing_secret
        and s.notification_webhook_signing_secret.strip().startswith("replace_with_")
    ):
        raise ValueError(
            "notification_webhook_signing_secret contains an unreplaced placeholder value."
        )
    if (
        s.notification_webhook_auth_token
        and s.notification_webhook_auth_token.strip().startswith("replace_with_")
    ):
        raise ValueError("notification_webhook_auth_token contains an unreplaced placeholder value.")

    return s


def validate_production_configuration(settings: Settings | None = None) -> Settings:
    """Validate strict production readiness rules, mandatory secrets, and operational constraints."""
    s = validate_configuration(settings)

    if not s.checkpoint_enabled:
        raise ValueError(
            "Production configuration error: checkpoint_enabled must be True for durable state persistence."
        )

    if s.checkpoint_db_path in (":memory:", "file::memory:?cache=shared"):
        raise ValueError(
            "Production configuration error: In-memory SQLite storage is forbidden in production."
        )

    if s.default_llm_provider == "openai" and not s.openai_api_key.strip():
        raise ValueError(
            "Production configuration error: openai_api_key is required when "
            "default_llm_provider is 'openai'."
        )
    if s.default_llm_provider == "anthropic" and not s.anthropic_api_key.strip():
        raise ValueError(
            "Production configuration error: anthropic_api_key is required when "
            "default_llm_provider is 'anthropic'."
        )
    if s.fallback_llm_provider == "openai" and not s.openai_api_key.strip():
        raise ValueError(
            "Production configuration error: openai_api_key is required when "
            "fallback_llm_provider is 'openai'."
        )
    if s.fallback_llm_provider == "anthropic" and not s.anthropic_api_key.strip():
        raise ValueError(
            "Production configuration error: anthropic_api_key is required when "
            "fallback_llm_provider is 'anthropic'."
        )

    if s.workflow_execution_backend == "celery" and s.celery_always_eager:
        raise ValueError(
            "Production configuration error: celery_always_eager must be False in production distributed "
            "execution."
        )

    if not is_loopback_host(s.api_host):
        import os

        clean_token = (
            s.api_token
            or getattr(s, "api_token_previous", "")
            or os.environ.get("ORCHESTRATOR_API_TOKEN")
            or ""
        ).strip()
        if not clean_token or clean_token.startswith("replace_with_") or len(clean_token) < 16:
            raise ValueError(
                "Production configuration error: Production API binding to non-loopback host requires "
                "a strong ORCHESTRATOR_API_TOKEN (minimum 16 characters)."
            )

    if s.notification_provider == "webhook":
        if not s.notification_webhook_signing_secret.strip():
            raise ValueError(
                "Production configuration error: notification_webhook_signing_secret is required "
                "in production to ensure HMAC payload integrity and replay protection."
            )
        if not s.notification_webhook_tls_verify:
            raise ValueError(
                "Production configuration error: notification_webhook_tls_verify must be True in production."
            )
        if s.notification_webhook_allow_insecure_http:
            raise ValueError(
                "Production configuration error: notification_webhook_allow_insecure_http must be False "
                "in production."
            )

    if not s.review_auth_enabled:
        raise ValueError(
            "Production configuration error: review_auth_enabled must be True in production."
        )

    return s


def mask_secret(secret: str | None, prefix_len: int = 4, suffix_len: int = 4) -> str:
    """Mask a secret value for safe diagnostic display, showing only prefix and suffix if long."""
    if not secret:
        return ""
    clean = str(secret).strip()
    if len(clean) <= (prefix_len + suffix_len + 3):
        return "[REDACTED]"
    return f"{clean[:prefix_len]}...{clean[-suffix_len:]}"


def sanitize_text(text: str, secrets: list[str | None] | None = None) -> str:
    """Replace occurrences of raw secrets in arbitrary text with [REDACTED]."""
    if not text:
        return text
    result = text
    target_secrets = list(secrets) if secrets is not None else []
    if not target_secrets:
        try:
            s = get_settings()
            target_secrets = [
                s.openai_api_key,
                s.anthropic_api_key,
                s.api_token,
                getattr(s, "api_token_previous", ""),
                s.postgres_password,
                s.notification_webhook_signing_secret,
                s.notification_webhook_signing_secret_previous,
                s.notification_webhook_auth_token,
            ]
        except Exception:
            target_secrets = []

    for sec in target_secrets:
        if sec and len(str(sec).strip()) >= 4:
            result = result.replace(str(sec).strip(), "[REDACTED]")

    import re

    patterns = [
        (re.compile(r"sk-[a-zA-Z0-9_\-]{16,}"), "[REDACTED_API_KEY]"),
        (re.compile(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{16,}"), "Bearer [REDACTED_TOKEN]"),
    ]
    for pat, rep in patterns:
        result = pat.sub(rep, result)

    return result


def sanitize_broker_url(url: str | None) -> str | None:
    """Mask password credentials and sensitive query parameters in connection strings."""
    if not url:
        return None
    try:
        from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

        parsed = urlsplit(url)
        netloc = parsed.netloc
        if parsed.password:
            user = parsed.username or ""
            host = parsed.hostname or ""
            netloc = f"{user}:***@{host}"
            if parsed.port:
                netloc += f":{parsed.port}"

        query = parsed.query
        if query:
            sensitive_params = {
                "token",
                "auth",
                "secret",
                "password",
                "key",
                "access_key",
                "api_key",
            }
            qsl = parse_qsl(query, keep_blank_values=True)
            sanitized_qsl = []
            for k, v in qsl:
                if k.lower() in sensitive_params and v:
                    sanitized_qsl.append((k, "***"))
                else:
                    sanitized_qsl.append((k, v))
            query = urlencode(sanitized_qsl)

        return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))
    except Exception:
        return "<redacted>"


def format_config_error(exc: Exception, settings: Settings | None = None) -> str:
    """Format configuration validation error into a concise, non-leaking message."""
    if isinstance(exc, ValidationError):
        messages = []
        for err in exc.errors():
            loc = " -> ".join(str(p) for p in err.get("loc", []))
            msg = err.get("msg", "Invalid value")
            if msg.startswith("Value error, "):
                msg = msg[len("Value error, ") :]
            messages.append(f"{loc}: {msg}" if loc else msg)
        raw_msg = "; ".join(messages)
    else:
        raw_msg = str(exc)

    raw_secrets = None
    if settings is not None:
        raw_secrets = [
            settings.openai_api_key,
            settings.anthropic_api_key,
            settings.api_token,
            getattr(settings, "api_token_previous", ""),
            settings.postgres_password,
            settings.notification_webhook_signing_secret,
            settings.notification_webhook_signing_secret_previous,
            settings.notification_webhook_auth_token,
        ]
    return sanitize_text(raw_msg, raw_secrets)


def get_startup_diagnostics(settings: Settings | None = None) -> dict[str, Any]:
    """Return a dictionary of safe, non-sensitive configuration diagnostics."""
    s = settings or get_settings()
    has_primary = bool(s.api_token.strip())
    has_prev = bool(getattr(s, "api_token_previous", "").strip())
    if has_primary and has_prev:
        auth_state = "enabled (token rotation active: primary + previous configured)"
    elif has_primary:
        auth_state = "enabled (token configured)"
    elif has_prev:
        auth_state = "enabled (previous token configured)"
    else:
        auth_state = "disabled (loopback dev mode)"
    pricing_state = (
        "custom rates configured" if bool(s.llm_custom_pricing.strip()) else "default provider rates"
    )

    return {
        "version": "0.1.0",
        "llm": {
            "default_provider": s.default_llm_provider,
            "fallback_provider": s.fallback_llm_provider or "(none)",
            "openai_model": s.openai_model,
            "anthropic_model": s.anthropic_model,
            "max_retries": s.llm_max_retries,
            "timeout_seconds": s.llm_timeout_seconds,
            "pricing": pricing_state,
            "has_openai_key": bool(s.openai_api_key.strip()),
            "has_anthropic_key": bool(s.anthropic_api_key.strip()),
        },
        "execution": {
            "backend": s.workflow_execution_backend,
            "max_active_runs": s.max_active_runs,
            "admission_lease_ttl_seconds": s.admission_lease_ttl_seconds,
            "admission_heartbeat_interval_seconds": s.admission_heartbeat_interval_seconds,
            "specialist_concurrency": s.effective_concurrency,
            "max_task_chars": s.max_task_chars,
            "workflow_timeout_seconds": s.workflow_timeout_seconds,
        },
        "celery": {
            "enabled": bool(s.celery_enabled or s.workflow_execution_backend == "celery"),
            "execution_backend": s.workflow_execution_backend,
            "broker_url": sanitize_broker_url(s.celery_broker_url),
            "result_backend": sanitize_broker_url(s.celery_result_backend),
            "default_queue": s.celery_task_default_queue,
            "worker_concurrency": s.celery_worker_concurrency,
            "task_time_limit": s.celery_task_time_limit,
            "task_soft_time_limit": s.celery_task_soft_time_limit,
            "always_eager": s.celery_always_eager,
            "worker_heartbeat_interval_seconds": s.celery_worker_heartbeat_interval_seconds,
            "worker_stale_threshold_seconds": s.celery_worker_stale_threshold_seconds,
            "queue_monitoring_interval_seconds": s.celery_queue_monitoring_interval_seconds,
            "workflow_stuck_threshold_seconds": s.celery_workflow_stuck_threshold_seconds,
            "recovery_batch_size": s.celery_recovery_batch_size,
        },
        "persistence": {
            "enabled": s.checkpoint_enabled,
            "backend": "sqlite",
            "db_path": s.checkpoint_db_path,
        },
        "backup": {
            "backup_dir": s.backup_dir,
            "retention_count": s.backup_retention_count,
            "retention_days": s.backup_retention_days,
            "scheduler_enabled": s.backup_scheduler_enabled,
            "interval_seconds": s.backup_interval_seconds,
            "max_duration_seconds": s.backup_max_duration_seconds,
            "max_retries": s.backup_max_retries,
            "require_manifest": s.backup_require_manifest,
            "require_signature": s.backup_require_signature,
            "signing_enabled": bool(s.backup_signing_key and s.backup_signing_key.strip()),
            "active_key_id": (
                s.backup_signing_key_id
                if (s.backup_signing_key and s.backup_signing_key.strip())
                else None
            ),
            "verification_key_ids": sorted(s.get_backup_verification_keyring().keys()),
        },
        "restore_drill": {
            "enabled": s.restore_drill_enabled,
            "interval_seconds": s.restore_drill_interval_seconds,
            "max_duration_seconds": s.restore_drill_max_duration_seconds,
            "history_limit": s.restore_drill_history_limit,
            "lease_ttl_seconds": s.restore_drill_lease_ttl_seconds,
        },
        "api": {
            "host": s.api_host,
            "port": s.api_port,
            "auth": auth_state,
            "token_rotation_active": bool(has_primary and has_prev),
            "max_body_bytes": s.api_max_body_bytes,
            "max_body_mb": round(s.api_max_body_bytes / 1_000_000, 2),
            "rate_limit_enabled": s.api_rate_limit_enabled,
            "rate_limit_requests": s.api_rate_limit_requests,
            "rate_limit_window_seconds": s.api_rate_limit_window_seconds,
            "shutdown_grace_seconds": s.api_shutdown_grace_seconds,
            "recovery_on_startup": s.api_recovery_on_startup,
        },
        "audit": {
            "retention_days": s.audit_retention_days,
            "cleanup_on_startup": s.audit_cleanup_on_startup,
            "export_max_limit": s.audit_export_max_limit,
        },
        "idempotency": {
            "enabled": s.idempotency_enabled,
            "retention_hours": s.idempotency_retention_hours,
        },
        "outbox": {
            "enabled": s.outbox_enabled,
            "dispatcher_enabled": s.outbox_dispatcher_enabled,
            "poll_interval_seconds": s.outbox_poll_interval_seconds,
            "batch_size": s.outbox_batch_size,
            "max_attempts": s.outbox_max_attempts,
            "claim_ttl_seconds": s.outbox_claim_ttl_seconds,
        },
        "notifications": {
            "provider": s.notification_provider,
            "webhook_url": (
                sanitize_broker_url(s.notification_webhook_url)
                if s.notification_webhook_url
                else None
            ),
            "has_auth_token": bool(s.notification_webhook_auth_token),
            "auth_header": s.notification_webhook_auth_header,
            "signing_enabled": bool(s.notification_webhook_signing_secret),
            "has_previous_signing_secret": bool(
                s.notification_webhook_signing_secret_previous
            ),
            "signature_header": s.notification_webhook_signature_header,
            "signature_tolerance_seconds": (
                s.notification_webhook_signature_tolerance_seconds
            ),
            "replay_protection_enabled": (
                s.notification_webhook_replay_protection_enabled
            ),
            "replay_storage": s.notification_webhook_replay_storage,
            "replay_retention_seconds": (
                s.notification_webhook_replay_retention_seconds
            ),
            "replay_cleanup_enabled": (
                s.notification_webhook_replay_cleanup_enabled
            ),
            "replay_cleanup_interval_seconds": (
                s.notification_webhook_replay_cleanup_interval_seconds
            ),
            "replay_cleanup_batch_size": (
                s.notification_webhook_replay_cleanup_batch_size
            ),
            "timeout_seconds": s.notification_webhook_timeout_seconds,
            "tls_verify": s.notification_webhook_tls_verify,
            "max_response_bytes": s.notification_webhook_max_response_bytes,
            "allow_private_ips": s.notification_webhook_allow_private_ips,
            "allow_insecure_http": s.notification_webhook_allow_insecure_http,
        },
    }


def format_diagnostics_summary(settings: Settings | None = None) -> str:
    """Format startup diagnostics into a clean, human-readable terminal summary."""
    diag = get_startup_diagnostics(settings)
    llm = diag["llm"]
    exec_cfg = diag["execution"]
    persist = diag["persistence"]
    api = diag["api"]

    rate_info = (
        f"enabled ({api['rate_limit_requests']} req / {int(api['rate_limit_window_seconds'])}s)"
        if api["rate_limit_enabled"]
        else "disabled"
    )

    timeout_info = (
        f"{exec_cfg['workflow_timeout_seconds']}s"
        if exec_cfg.get("workflow_timeout_seconds")
        else "none (unbounded)"
    )

    lines = [
        "Multi-Agent Orchestrator Configuration Diagnostics:",
        f"  - LLM Provider:        {llm['default_provider']} (fallback: {llm['fallback_provider']})",
        (
            f"  - LLM Safety:          timeout={llm['timeout_seconds']}s, "
            f"retries={llm['max_retries']}, pricing={llm['pricing']}"
        ),
        (
            f"  - Execution Backend:   {exec_cfg.get('backend', 'direct')} "
            f"(celery_enabled={diag.get('celery', {}).get('enabled', False)})"
        ),
        (
            f"  - Concurrency Limit:   {exec_cfg['specialist_concurrency']} specialists "
            f"(max active runs: {exec_cfg.get('max_active_runs', 10)}, workflow_timeout: {timeout_info})"
        ),
        (
            f"  - Persistence:         backend={persist['backend']}, "
            f"path={persist['db_path']}, enabled={persist['enabled']}"
        ),
        (
            f"  - Database Backup:     dir={diag.get('backup', {}).get('backup_dir', 'backups')}, "
            f"retention_count={diag.get('backup', {}).get('retention_count', 10)}, "
            f"scheduler_enabled={diag.get('backup', {}).get('scheduler_enabled', False)}"
        ),
        (
            f"  - Restore Drills:      enabled={diag.get('restore_drill', {}).get('enabled', False)}, "
            f"interval={diag.get('restore_drill', {}).get('interval_seconds', 604800.0)}s"
        ),
        f"  - API Server:          http://{api['host']}:{api['port']}",
        f"  - API Security:        auth={api['auth']}, max_body={api['max_body_mb']}MB",
        f"  - API Rate Limit:      {rate_info}",
        (
            f"  - API Lifecycle:       shutdown_grace={api['shutdown_grace_seconds']}s, "
            f"recovery_on_startup={api['recovery_on_startup']}"
        ),
    ]
    return "\n".join(lines)
