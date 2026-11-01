"""FastAPI route definitions for the orchestrator HTTP API."""

from __future__ import annotations

import datetime
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .. import __version__
from ..celery_app import get_celery_diagnostics
from ..config import get_settings
from ..evaluation.cases import DATASET_VERSION, get_evaluation_cases
from ..observability.audit import (
    AuditEventType,
    AuditOutcome,
    record_audit_event,
    sanitize_tool_approval,
    sanitize_tool_execution,
    serialize_audit_events_csv,
    serialize_audit_events_json,
    serialize_audit_events_jsonl,
)
from ..persistence.store import (
    CheckpointCorruptError,
    CheckpointIncompatibleError,
    CheckpointNotFoundError,
    CheckpointStore,
    ToolApprovalConflictError,
    ToolApprovalNotFoundError,
    ToolExecutionNotFoundError,
    ToolReconciliationConflictError,
)
from ..tools import (
    REGISTRY,
    RiskLevel,
    ToolApproverUnauthorizedError,
    compute_argument_fingerprint,
    compute_policy_hash,
    diagnose_tool_approval,
    verify_approver_authorization,
)
from .limiter import check_rate_limit, get_client_ip
from .manager import (
    BrokerUnavailableError,
    CapacityExhaustedError,
    RunConflictError,
    RunManager,
    ShutdownError,
    get_run_manager,
)
from .models import (
    ApplicationDiagnostics,
    ApprovalSchedulerDiagnostics,
    ApproveToolExecutionRequest,
    ApproveToolExecutionResponse,
    AuditCleanupRequest,
    AuditCleanupResponse,
    AuditEventModel,
    BackupHistoryResponse,
    BackupKeyringInfoResponse,
    BackupMetadataModel,
    BackupSchedulerDiagnostics,
    BackupStatusResponse,
    BackupTriggerRequest,
    BackupTriggerResponse,
    BackupVerifyRequest,
    BackupVerifyResponse,
    CancelRunRequest,
    CeleryQueueMetricsResponse,
    CeleryWorkerModel,
    CeleryWorkersResponse,
    CheckpointMetadata,
    CheckpointsListResponse,
    CreateRunRequest,
    DiagnoseToolApprovalRequest,
    DiagnosticsResponse,
    DispatchOutboxResponse,
    EscalateToolApprovalRequest,
    EscalateToolApprovalResponse,
    EvaluationCaseSummary,
    EvaluationSummaryResponse,
    ExecutionDiagnostics,
    ExpireToolApprovalRequest,
    ExpireToolApprovalResponse,
    HealthResponse,
    ListOutboxEventsResponse,
    OutboxDiagnosticsResponse,
    OutboxEventModel,
    OutboxHistoryEntryModel,
    OutboxHistoryResponse,
    PersistenceDiagnostics,
    ReadyResponse,
    ReconcileToolExecutionRequest,
    ReconcileToolExecutionResponse,
    RecoveredWorkflowItem,
    RecoverOutboxEventsRequest,
    RecoverOutboxEventsResponse,
    RecoverToolApprovalRequest,
    RecoverToolApprovalResponse,
    RecoverWorkflowsRequest,
    RecoverWorkflowsResponse,
    RecoveryDiagnostics,
    ReplayCleanupRequest,
    ReplayCleanupResponse,
    ReplayDiagnosticsResponse,
    RestoreDrillHistoryResponse,
    RestoreDrillResultModel,
    RestoreDrillStatusResponse,
    RestoreDrillTriggerRequest,
    RestoreDrillTriggerResponse,
    ResumeRunRequest,
    RetryOutboxEventRequest,
    RetryOutboxEventResponse,
    RevokeToolApprovalRequest,
    RevokeToolApprovalResponse,
    RunAuditResponse,
    RunResponse,
    RuntimeLimitsDiagnostics,
    RunToolApprovalsResponse,
    RunToolExecutionsResponse,
    ScanExpiredApprovalsResponse,
    StuckWorkflowModel,
    StuckWorkflowsResponse,
    ToolApprovalDiagnosticsResponse,
    ToolApprovalModel,
    ToolExecutionModel,
)
from .scheduler import get_approval_scheduler

logger = logging.getLogger(__name__)
router = APIRouter()
bearer_scheme = HTTPBearer(auto_error=False)


def verify_api_token(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    request: Request,
) -> None:
    """Validate Bearer token authentication when a token is configured.

    When ORCHESTRATOR_API_TOKEN is unset, permits unauthenticated requests on loopback.
    When a token is configured, requires a valid Bearer token compared in constant time.
    Supports secret rotation via comma-separated tokens or ORCHESTRATOR_API_TOKEN_PREVIOUS.
    """
    settings = get_settings()
    configured_token = (os.environ.get("ORCHESTRATOR_API_TOKEN") or settings.api_token or "").strip()
    previous_token = (
        os.environ.get("ORCHESTRATOR_API_TOKEN_PREVIOUS")
        or getattr(settings, "api_token_previous", "")
        or ""
    ).strip()

    valid_tokens = [t.strip() for t in configured_token.split(",") if t.strip()]
    if previous_token:
        valid_tokens.extend([t.strip() for t in previous_token.split(",") if t.strip()])

    if not valid_tokens:
        return

    client_ip = get_client_ip(request, settings.effective_trusted_proxies) if request else "127.0.0.1"
    req_path = request.url.path if request else "/unknown"

    if not credentials or not credentials.credentials:
        try:
            record_audit_event(
                event_type=AuditEventType.SECURITY_AUTH_FAILURE,
                outcome=AuditOutcome.DENIED,
                actor=client_ip,
                metadata={"reason": "missing_credentials", "path": req_path},
            )
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid authentication credentials.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    cred_bytes = credentials.credentials.encode("utf-8")
    is_valid = any(
        hmac.compare_digest(cred_bytes, tok.encode("utf-8"))
        for tok in valid_tokens
    )

    if not is_valid:
        try:
            record_audit_event(
                event_type=AuditEventType.SECURITY_AUTH_FAILURE,
                outcome=AuditOutcome.DENIED,
                actor=client_ip,
                metadata={"reason": "invalid_token", "path": req_path},
            )
        except Exception:
            pass

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token.",
            headers={"WWW-Authenticate": "Bearer"},
        )


def validate_idempotency_key(key: str | None) -> str | None:
    """Validate format and bounds of optional Idempotency-Key header."""
    if key is None:
        return None
    clean = key.strip()
    if not clean:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key header must not be empty or whitespace.",
        )
    if len(clean) > 128:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Idempotency-Key length ({len(clean)}) exceeds maximum allowed limit of 128 characters.",
        )
    if not all(33 <= ord(c) <= 126 for c in clean):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key contains invalid characters. Printable ASCII only.",
        )
    return clean


def get_client_scope(
    request: Request | None,
    credentials: HTTPAuthorizationCredentials | None = None,
) -> str:
    """Derive client identity scope for idempotency key isolation."""
    settings = get_settings()
    configured_token = (os.environ.get("ORCHESTRATOR_API_TOKEN") or settings.api_token or "").strip()

    if credentials and credentials.credentials:
        token_hash = hashlib.sha256(credentials.credentials.encode("utf-8")).hexdigest()[:16]
        return f"token:{token_hash}"
    elif configured_token:
        client_ip = get_client_ip(request, settings.effective_trusted_proxies) if request else "127.0.0.1"
        return f"ip:{client_ip}"
    else:
        client_ip = get_client_ip(request, settings.effective_trusted_proxies) if request else "127.0.0.1"
        return f"ip:{client_ip}"


def compute_request_fingerprint(req: CreateRunRequest) -> str:
    """Compute deterministic SHA-256 fingerprint of CreateRunRequest parameters."""
    normalized = {
        "task": str(req.task or "").strip(),
        "plan": req.plan,
        "max_concurrency": req.max_concurrency,
        "run_id": str(req.run_id or "").strip() or None,
        "background": bool(req.background),
        "timeout_seconds": req.timeout_seconds,
    }
    canonical_json = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def get_manager() -> RunManager:
    """Dependency provider for RunManager."""
    return get_run_manager()


def get_store() -> CheckpointStore:
    """Dependency provider for CheckpointStore."""
    return CheckpointStore()


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Service Liveness Telemetry",
    description="Check the operational liveness, version, persistence state, and LLM provider.",
)
def get_health(
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> HealthResponse:
    settings = get_settings()
    configured_token = (os.environ.get("ORCHESTRATOR_API_TOKEN") or settings.api_token or "").strip()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    service_status = "shutting_down" if manager.is_shutting_down else "healthy"

    return HealthResponse(
        status=service_status,
        version=__version__,
        timestamp=now,
        checkpoint_backend="sqlite",
        persistence_enabled=bool(store.enabled),
        active_llm_provider=settings.active_provider,
        active_runs_count=manager.active_runs_count(store=store),
        auth_required=bool(configured_token),
    )


@router.get(
    "/ready",
    response_model=ReadyResponse,
    summary="Service Readiness Probe",
    description="Check whether the service is ready to accept new workflow runs.",
    responses={
        200: {"model": ReadyResponse, "description": "Service is ready to accept runs."},
        503: {
            "model": ReadyResponse,
            "description": "Service is not ready (shutting down or database unhealthy).",
        },
    },
)
def get_readiness(
    response: Response,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ReadyResponse:
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # 1. Shutdown check: service must not accept runs when shutting down
    if manager.is_shutting_down:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        _, db_status = store.check_health()
        return ReadyResponse(
            status="not_ready",
            timestamp=now,
            accepting_runs=False,
            database=db_status,
            active_runs=manager.active_runs_count(store=store),
            reason="Service is shutting down",
        )

    # 2. Persistence dependency check
    db_healthy, db_status = store.check_health()
    if store.enabled and not db_healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadyResponse(
            status="not_ready",
            timestamp=now,
            accepting_runs=False,
            database="unhealthy",
            active_runs=manager.active_runs_count(store=store),
            reason=f"Database probe failed: {db_status}",
        )

    return ReadyResponse(
        status="ready",
        timestamp=now,
        accepting_runs=True,
        database=db_status,
        active_runs=manager.active_runs_count(store=store),
        reason=None,
    )


@router.get(
    "/diagnostics",
    response_model=DiagnosticsResponse,
    summary="Runtime Operational Diagnostics",
    description="Inspect operational health, lifecycle counts, persistence status, and runtime limits.",
    dependencies=[Depends(verify_api_token)],
)
def get_diagnostics(
    request: Request,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> DiagnosticsResponse:
    settings = get_settings()
    configured_token = (os.environ.get("ORCHESTRATOR_API_TOKEN") or settings.api_token or "").strip()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Application diagnostics
    app_status = "shutting_down" if manager.is_shutting_down else "healthy"
    app_diag = ApplicationDiagnostics(
        version=__version__,
        status=app_status,
        is_shutting_down=manager.is_shutting_down,
        started_at=getattr(manager, "started_at", None),
        auth_required=bool(configured_token),
    )

    # Execution diagnostics
    lifecycle_counts = store.get_status_counts() if store.enabled else {}
    global_active = manager.active_runs_count(store=store)
    global_capacity = max(0, manager.max_active_runs - global_active)
    exec_diag = ExecutionDiagnostics(
        active_runs_count=global_active,
        max_active_runs=manager.max_active_runs,
        available_capacity=global_capacity,
        lifecycle_counts=lifecycle_counts,
        specialist_concurrency=settings.effective_concurrency,
        max_task_chars=settings.max_task_chars,
        workflow_timeout_seconds=settings.workflow_timeout_seconds,
        global_active_runs_count=global_active if store.enabled else None,
        global_available_capacity=global_capacity if store.enabled else None,
        owner_id=getattr(manager, "owner_id", None),
        backend=settings.workflow_execution_backend,
        celery=get_celery_diagnostics(settings),
    )

    # Persistence diagnostics
    db_healthy, db_msg = store.check_health()
    if not store.enabled:
        db_status = "disabled"
    elif db_healthy:
        db_status = "healthy"
    else:
        db_status = "unhealthy"

    raw_path = str(store.db_path)
    if raw_path in (":memory:", "file::memory:?cache=shared"):
        safe_db_name = ":memory:"
    else:
        safe_db_name = os.path.basename(raw_path)

    persist_diag = PersistenceDiagnostics(
        enabled=bool(store.enabled),
        backend="sqlite",
        status=db_status,
        db_name=safe_db_name,
        detail=db_msg if not db_healthy else None,
    )

    # Runtime limits diagnostics
    limits_diag = RuntimeLimitsDiagnostics(
        max_active_runs=manager.max_active_runs,
        admission_lease_ttl_seconds=getattr(manager, "lease_ttl", settings.admission_lease_ttl_seconds),
        admission_heartbeat_interval_seconds=settings.admission_heartbeat_interval_seconds,
        shutdown_grace_seconds=settings.api_shutdown_grace_seconds,
        recovery_on_startup=settings.api_recovery_on_startup,
        rate_limit_enabled=settings.api_rate_limit_enabled,
        rate_limit_requests=settings.api_rate_limit_requests,
        rate_limit_window_seconds=settings.api_rate_limit_window_seconds,
        max_body_bytes=settings.api_max_body_bytes,
        max_body_mb=round(settings.api_max_body_bytes / 1_000_000, 2),
    )

    # Recovery diagnostics
    recovery_data = manager.get_recovery_summary()
    if not settings.api_recovery_on_startup or not store.enabled:
        rec_status = "disabled"
        recovered_count = 0
        recovered_ids: list[str] = []
        rec_time = None
    elif recovery_data:
        rec_status = str(recovery_data.get("status", "completed"))
        recovered_count = int(recovery_data.get("recovered_count", 0))
        recovered_ids = list(recovery_data.get("recovered_run_ids", []))
        rec_time = recovery_data.get("timestamp")
    else:
        rec_status = "pending"
        recovered_count = 0
        recovered_ids = []
        rec_time = None

    recovery_diag = RecoveryDiagnostics(
        status=rec_status,
        recovered_count=recovered_count,
        recovered_run_ids=recovered_ids,
        timestamp=rec_time,
    )

    # Automated approval lifecycle scheduler diagnostics
    scheduler = getattr(request.app.state, "approval_scheduler", None)
    if scheduler is None:
        scheduler = get_approval_scheduler(store=store, settings=settings)
    sched_diag = ApprovalSchedulerDiagnostics(**scheduler.get_status())

    # Automated database backup scheduler diagnostics
    backup_scheduler = getattr(request.app.state, "backup_scheduler", None)
    if backup_scheduler is None:
        from ..persistence import get_backup_scheduler

        backup_scheduler = get_backup_scheduler(store=store, settings=settings)
    backup_diag = BackupSchedulerDiagnostics(**backup_scheduler.get_status())

    return DiagnosticsResponse(
        timestamp=now,
        application=app_diag,
        execution=exec_diag,
        persistence=persist_diag,
        limits=limits_diag,
        recovery=recovery_diag,
        approval_scheduler=sched_diag,
        backup_scheduler=backup_diag,
    )



@router.post(
    "/runs",
    response_model=RunResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Start Workflow Run",
    description="Start a new multi-agent workflow execution synchronously or as background job.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def create_run(
    req: CreateRunRequest,
    request: Request,
    response: Response,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> RunResponse:
    clean_key = validate_idempotency_key(idempotency_key)
    settings = get_settings()

    # Fast-path for requests without Idempotency-Key or when persistence/idempotency is disabled
    if not clean_key or not getattr(settings, "idempotency_enabled", True) or not store.enabled:
        try:
            run_res = manager.start_run(
                task=req.task,
                plan=req.plan,
                max_concurrency=req.max_concurrency,
                run_id=req.run_id,
                background=req.background,
                store=store,
                timeout_seconds=req.timeout_seconds,
                execution_backend=req.execution_backend,
            )
            is_async = req.background or (req.execution_backend == "celery")
            if is_async:
                response.status_code = status.HTTP_202_ACCEPTED
            return run_res
        except BrokerUnavailableError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=str(exc),
            ) from exc
        except CapacityExhaustedError as exc:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=str(exc),
                headers={"Retry-After": "2"},
            ) from exc
        except ShutdownError as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        except RunConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Workflow execution encountered an internal error.",
            ) from exc

    # Idempotent execution lifecycle
    client_scope = get_client_scope(request, credentials)
    req_hash = compute_request_fingerprint(req)
    tentative_run_id = (req.run_id or "").strip() or f"run-{uuid.uuid4().hex[:12]}"

    acq_status, record = store.acquire_idempotency_key(
        client_scope=client_scope,
        key=clean_key,
        request_hash=req_hash,
        tentative_run_id=tentative_run_id,
    )

    if acq_status == "conflict":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Idempotency key '{clean_key}' was previously used with a different request payload.",
        )

    if acq_status == "in_progress":
        wait_timeout = float(req.timeout_seconds or 30.0)
        completed_record = store.wait_for_idempotency_completion(
            client_scope=client_scope,
            key=clean_key,
            timeout=wait_timeout,
        )
        if not completed_record or completed_record.get("status") != "completed":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"A concurrent request with idempotency key '{clean_key}' is still in progress. "
                    "Please retry shortly."
                ),
            )
        record = completed_record
        acq_status = "completed"

    if acq_status == "completed" and record:
        resp_code = record.get("response_code") or status.HTTP_201_CREATED
        response.status_code = resp_code
        response.headers["Idempotent-Replayed"] = "true"
        resp_json = record.get("response_json", "{}")
        try:
            return RunResponse.model_validate_json(resp_json)
        except Exception:
            return RunResponse(
                run_id=record["run_id"],
                task=req.task,
                status="completed",
            )

    try:
        run_res = manager.start_run(
            task=req.task,
            plan=req.plan,
            max_concurrency=req.max_concurrency,
            run_id=tentative_run_id,
            background=req.background,
            store=store,
            timeout_seconds=req.timeout_seconds,
            execution_backend=req.execution_backend,
        )
        is_async = req.background or (req.execution_backend == "celery")
        resp_code = status.HTTP_202_ACCEPTED if is_async else status.HTTP_201_CREATED
        response.status_code = resp_code

        store.finalize_idempotency_key(
            client_scope=client_scope,
            key=clean_key,
            response_code=resp_code,
            response_json=run_res.model_dump_json(),
        )
        return run_res
    except BrokerUnavailableError as exc:
        store.fail_idempotency_key(client_scope=client_scope, key=clean_key, error_detail=str(exc))
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except CapacityExhaustedError as exc:
        store.fail_idempotency_key(client_scope=client_scope, key=clean_key, error_detail=str(exc))
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
            headers={"Retry-After": "2"},
        ) from exc
    except (ShutdownError, ValueError, RunConflictError) as exc:
        store.fail_idempotency_key(client_scope=client_scope, key=clean_key, error_detail=str(exc))
        if isinstance(exc, ShutdownError):
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
        elif isinstance(exc, ValueError):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
        else:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except Exception as exc:
        store.fail_idempotency_key(client_scope=client_scope, key=clean_key, error_detail=str(exc))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Workflow execution encountered an internal error.",
        ) from exc


@router.get(
    "/runs/{run_id}",
    response_model=RunResponse,
    summary="Get Run Status and Results",
    description="Retrieve status, assembled output, subtasks, and HITL review metadata for a run ID.",
    dependencies=[Depends(verify_api_token)],
)
def get_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RunResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    try:
        return manager.get_run(clean_id, store=store)
    except CheckpointNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        ) from exc
    except (CheckpointIncompatibleError, CheckpointCorruptError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Checkpoint data is corrupted or incompatible with current schema version.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve run details.",
        ) from exc


@router.post(
    "/runs/{run_id}/resume",
    response_model=RunResponse,
    summary="Resume Interrupted Run",
    description="Resume an eligible interrupted, failed, or resolved workflow from its latest checkpoint.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def resume_run(
    run_id: str,
    req: ResumeRunRequest,
    response: Response,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RunResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    try:
        resumed = manager.resume_run(
            run_id=clean_id,
            max_concurrency=req.max_concurrency,
            background=req.background,
            store=store,
            timeout_seconds=req.timeout_seconds,
        )
        is_async = req.background or (req.execution_backend == "celery")
        if is_async:
            response.status_code = status.HTTP_202_ACCEPTED
        return resumed
    except CapacityExhaustedError as exc:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=str(exc),
            headers={"Retry-After": "2"},
        ) from exc
    except ShutdownError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except CheckpointNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        ) from exc
    except RunConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except (CheckpointIncompatibleError, CheckpointCorruptError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Checkpoint data is corrupted or incompatible with current schema version.",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Workflow resumption encountered an internal error.",
        ) from exc


@router.post(
    "/runs/{run_id}/cancel",
    response_model=RunResponse,
    summary="Cancel Workflow Run",
    description="Cooperatively cancel an active workflow execution, halting scheduling of future subtasks.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def cancel_run(
    run_id: str,
    manager: Annotated[RunManager, Depends(get_manager)],
    store: Annotated[CheckpointStore, Depends(get_store)],
    req: CancelRunRequest | None = None,
) -> RunResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    reason = req.reason if req else "User requested cancellation"

    try:
        return manager.cancel_run(clean_id, reason=reason, store=store)
    except CheckpointNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        ) from exc
    except RunConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Workflow cancellation encountered an internal error.",
        ) from exc


@router.get(
    "/runs/{run_id}/checkpoints",
    response_model=CheckpointsListResponse,
    summary="List Checkpoints Metadata",
    description="Inspect step metadata and transition boundaries for a run without exposing sensitive state.",
    dependencies=[Depends(verify_api_token)],
)
def list_checkpoints(
    run_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> CheckpointsListResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    summary = store.get_run_summary(clean_id)
    records = store.list_checkpoints(clean_id)

    if not summary and not records:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' was not found in the checkpoint store.",
        )

    meta = [
        CheckpointMetadata(
            step=r.step,
            transition=r.transition,
            schema_version=r.schema_version,
            created_at=r.created_at,
        )
        for r in records
    ]

    return CheckpointsListResponse(
        run_id=clean_id,
        checkpoints=meta,
        total_steps=len(meta),
    )


@router.get(
    "/runs/{run_id}/audit",
    response_model=RunAuditResponse,
    summary="Workflow Run Audit Event History",
    description=(
        "Retrieve structured, chronological audit events for a workflow run, bounded by limit and offset."
    ),
    dependencies=[Depends(verify_api_token)],
)
def get_run_audit(
    run_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    manager: Annotated[RunManager, Depends(get_manager)],
    limit: Annotated[int, Query(ge=1, le=500, description="Maximum events to return")] = 100,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
    event_type: Annotated[str | None, Query(description="Filter by specific event type")] = None,
) -> RunAuditResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    summary = store.get_run_summary(clean_id)
    is_active = manager.is_active(clean_id)
    total_events = store.count_audit_events(clean_id)

    if not summary and not is_active and total_events == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        )

    events_data = store.get_audit_events(
        run_id=clean_id,
        event_type=event_type,
        limit=limit,
        offset=offset,
    )

    events = [AuditEventModel(**e) for e in events_data]

    return RunAuditResponse(
        run_id=clean_id,
        total_events=total_events,
        limit=limit,
        offset=offset,
        events=events,
    )


@router.get(
    "/runs/{run_id}/audit/export",
    summary="Export Workflow Run Audit Events",
    description=(
        "Export audit events for a workflow run in JSON, JSONL, or CSV format "
        "with time range filtering and bounded limits."
    ),
    dependencies=[Depends(verify_api_token)],
)
def export_run_audit(
    run_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    manager: Annotated[RunManager, Depends(get_manager)],
    format: Annotated[str, Query(description="Export format: json, jsonl, or csv")] = "json",
    limit: Annotated[int, Query(ge=1, le=5000, description="Maximum events to export")] = 1000,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
    start_time: Annotated[str | None, Query(description="ISO 8601 start timestamp filter")] = None,
    end_time: Annotated[str | None, Query(description="ISO 8601 end timestamp filter")] = None,
    event_type: Annotated[str | None, Query(description="Filter by specific event type")] = None,
) -> Response:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty.")

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    summary = store.get_run_summary(clean_id)
    is_active = manager.is_active(clean_id)
    total_events = store.count_audit_events(clean_id)

    if not summary and not is_active and total_events == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        )

    fmt = format.lower().strip()
    if fmt not in ("json", "jsonl", "csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported format '{format}'. Supported formats: json, jsonl, csv.",
        )

    if start_time and end_time and start_time.strip() > end_time.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_time must be earlier than or equal to end_time.",
        )

    events_data = store.export_audit_events(
        run_id=clean_id,
        event_type=event_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )

    if fmt == "json":
        content = serialize_audit_events_json(events_data)
        media_type = "application/json"
        filename = f"audit-{clean_id}.json"
    elif fmt == "jsonl":
        content = serialize_audit_events_jsonl(events_data)
        media_type = "application/x-ndjson"
        filename = f"audit-{clean_id}.jsonl"
    else:  # csv
        content = serialize_audit_events_csv(events_data)
        media_type = "text/csv"
        filename = f"audit-{clean_id}.csv"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/runs/{run_id}/tools",
    response_model=RunToolExecutionsResponse,
    summary="List Workflow Tool Executions",
    description=(
        "Retrieve recorded tool executions for a workflow run, including operation keys, "
        "status, timestamps, and sanitized arguments with secrets redacted."
    ),
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/runs/{run_id}/tool-executions",
    response_model=RunToolExecutionsResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def list_run_tools(
    run_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    manager: Annotated[RunManager, Depends(get_manager)],
    status_filter: Annotated[str | None, Query(alias="status", description="Filter by status")] = None,
    tool_name: Annotated[str | None, Query(description="Filter by tool name")] = None,
) -> RunToolExecutionsResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    summary = store.get_run_summary(clean_id)
    is_active = manager.is_active(clean_id)
    raw_executions = store.list_tool_executions(
        clean_id,
        status=status_filter,
        tool_name=tool_name,
    )

    if not summary and not is_active and not raw_executions:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        )

    sanitized_items = [
        ToolExecutionModel(**sanitize_tool_execution(rec))
        for rec in raw_executions
    ]

    return RunToolExecutionsResponse(
        run_id=clean_id,
        total_executions=len(sanitized_items),
        executions=sanitized_items,
    )


@router.get(
    "/runs/{run_id}/tools/{operation_key}",
    response_model=ToolExecutionModel,
    summary="Get Specific Tool Execution Details",
    description="Retrieve sanitized execution state and reconciliation history for an operation key.",
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/runs/{run_id}/tool-executions/{operation_key}",
    response_model=ToolExecutionModel,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_run_tool_execution(
    run_id: str,
    operation_key: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ToolExecutionModel:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="operation_key must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    rec = store.get_tool_execution(clean_op_key)
    if not rec or rec.get("run_id") != clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Tool execution '{clean_op_key}' not found for run '{clean_run_id}'.",
        )

    return ToolExecutionModel(**sanitize_tool_execution(rec))


@router.post(
    "/runs/{run_id}/tools/{operation_key}/reconcile",
    response_model=ReconcileToolExecutionResponse,
    summary="Reconcile Uncertain Tool Execution",
    description=(
        "Explicitly record a verified outcome ('completed' or 'failed') for an uncertain tool execution. "
        "Guarantees atomic transition, audit logging, and idempotent repetition."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
@router.post(
    "/runs/{run_id}/tool-executions/{operation_key}/reconcile",
    response_model=ReconcileToolExecutionResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def reconcile_tool_execution(
    run_id: str,
    operation_key: str,
    req: ReconcileToolExecutionRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ReconcileToolExecutionResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="operation_key must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    raw_outcome = req.outcome.strip().lower() if req.outcome else ""
    if raw_outcome in ("success", "complete"):
        target_outcome = "completed"
    elif raw_outcome in ("failure", "fail"):
        target_outcome = "failed"
    elif raw_outcome in ("completed", "failed"):
        target_outcome = raw_outcome
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid reconciliation outcome '{req.outcome}'. Must be 'completed' or 'failed'.",
        )

    clean_reason = req.reason.strip() if req.reason else ""
    if not clean_reason:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Reconciliation reason is required and cannot be empty.",
        )

    # Derive operator identity from request or auth scope
    if req.operator and req.operator.strip():
        actor = req.operator.strip()
    else:
        actor = get_client_scope(request, credentials)

    try:
        result = store.reconcile_tool_execution(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            outcome=target_outcome,
            reason=clean_reason,
            result_text=req.result_text,
            error_text=req.error_text,
            actor=actor,
        )
    except ToolExecutionNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ToolReconciliationConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Tool reconciliation encountered an internal error.",
        ) from exc

    # Record Audit Event
    audit_ev = record_audit_event(
        event_type=AuditEventType.TOOL_RECONCILED,
        outcome=target_outcome,
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": result["tool_name"],
            "subtask_id": result["subtask_id"],
            "previous_status": result["previous_status"],
            "new_status": target_outcome,
            "reason": clean_reason,
            "idempotent": result["idempotent"],
        },
        store=store,
    )

    # Fetch updated tool execution for response payload
    rec = store.get_tool_execution(clean_op_key)
    sanitized_exec = ToolExecutionModel(**sanitize_tool_execution(rec)) if rec else None

    return ReconcileToolExecutionResponse(
        success=True,
        operation_key=clean_op_key,
        run_id=clean_run_id,
        previous_status=result["previous_status"],
        status=result["status"],
        reconciled_by=result["reconciled_by"],
        reconciled_at=result["reconciled_at"],
        reconciliation_reason=result["reconciliation_reason"],
        idempotent=result["idempotent"],
        audit_event_id=audit_ev.event_id if audit_ev else None,
        execution=sanitized_exec,
    )


@router.post(
    "/runs/{run_id}/tools/{operation_key}/approve",
    response_model=ApproveToolExecutionResponse,
    summary="Approve or Reject Tool Execution",
    description=(
        "Explicitly record a human or operator approval or rejection decision for a tool execution. "
        "Records audit events and persists the decision for idempotent tool execution and resume."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def approve_tool_execution(
    run_id: str,
    operation_key: str,
    req: ApproveToolExecutionRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ApproveToolExecutionResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()
    clean_tool_name = req.tool_name.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="operation_key must not be empty.",
        )
    if not clean_tool_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tool_name must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    clean_decision = str(req.decision or "").strip().lower()
    if clean_decision not in ("approved", "rejected"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid decision '{req.decision}'. Must be 'approved' or 'rejected'.",
        )

    actor = (
        req.approver.strip()
        if req.approver and req.approver.strip()
        else get_client_scope(request, credentials)
    )

    policy = REGISTRY.get_policy(clean_tool_name)
    try:
        verify_approver_authorization(actor, clean_tool_name, policy)
    except ToolApproverUnauthorizedError as auth_err:
        record_audit_event(
            event_type=AuditEventType.TOOL_APPROVER_DENIED,
            outcome="denied",
            run_id=clean_run_id,
            actor=actor or "unknown",
            metadata={
                "operation_key": clean_op_key,
                "tool_name": clean_tool_name,
                "reason": str(auth_err),
            },
            store=store,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(auth_err),
        ) from auth_err

    arg_fp: str | None = None
    if req.argument_fingerprint and req.argument_fingerprint.strip():
        arg_fp = req.argument_fingerprint.strip()
    elif req.arguments is not None:
        arg_fp = compute_argument_fingerprint(req.arguments)

    risk_level_str = (
        policy.risk_level.value
        if policy and isinstance(policy.risk_level, RiskLevel)
        else (str(policy.risk_level) if policy else None)
    )
    policy_hash_str = compute_policy_hash(policy) if policy else None

    try:
        record = store.record_tool_approval(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            tool_name=clean_tool_name,
            approver=actor,
            decision=clean_decision,
            reason=req.reason.strip() if req.reason else "",
            argument_fingerprint=arg_fp,
            risk_level=risk_level_str,
            policy_hash=policy_hash_str,
        )
    except ToolApprovalConflictError as conf_err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(conf_err)) from conf_err
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(val_err)) from val_err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record tool approval decision.",
        ) from exc

    event_type = (
        AuditEventType.TOOL_APPROVED
        if clean_decision == "approved"
        else AuditEventType.TOOL_POLICY_DENIED
    )
    audit_ev = record_audit_event(
        event_type=event_type,
        outcome=clean_decision,
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": clean_tool_name,
            "decision": clean_decision,
            "reason": record["reason"],
            "idempotent": record.get("idempotent", False),
            "argument_fingerprint": record.get("argument_fingerprint"),
            "risk_level": record.get("risk_level"),
        },
        store=store,
        dedup=True,
    )

    return ApproveToolExecutionResponse(
        success=True,
        operation_key=record["operation_key"],
        run_id=record["run_id"],
        tool_name=record["tool_name"],
        approver=record["approver"],
        decision=record["decision"],
        reason=record["reason"],
        created_at=record["created_at"],
        idempotent=record.get("idempotent", False),
        argument_fingerprint=record.get("argument_fingerprint"),
        risk_level=record.get("risk_level"),
        policy_hash=record.get("policy_hash"),
        expires_at=record.get("expires_at"),
        audit_event_id=audit_ev.event_id if audit_ev else None,
    )


@router.post(
    "/runs/{run_id}/tools/{operation_key}/revoke",
    response_model=RevokeToolApprovalResponse,
    summary="Revoke Tool Approval",
    description="Explicitly revoke an active tool approval for an operation key. Audited and idempotent.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def revoke_tool_approval(
    run_id: str,
    operation_key: str,
    req: RevokeToolApprovalRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RevokeToolApprovalResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()
    clean_tool_name = req.tool_name.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="operation_key must not be empty.",
        )
    if not clean_tool_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="tool_name must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    actor = (
        req.revoked_by.strip()
        if req.revoked_by and req.revoked_by.strip()
        else get_client_scope(request, credentials)
    )

    policy = REGISTRY.get_policy(clean_tool_name)
    try:
        verify_approver_authorization(actor, clean_tool_name, policy)
    except ToolApproverUnauthorizedError as auth_err:
        record_audit_event(
            event_type=AuditEventType.TOOL_APPROVER_DENIED,
            outcome="denied",
            run_id=clean_run_id,
            actor=actor or "unknown",
            metadata={
                "operation_key": clean_op_key,
                "tool_name": clean_tool_name,
                "action": "revoke",
                "reason": str(auth_err),
            },
            store=store,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(auth_err),
        ) from auth_err

    clean_reason = req.reason.strip() if req.reason else "Revoked by operator"

    try:
        record = store.revoke_tool_approval(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            tool_name=clean_tool_name,
            revoked_by=actor,
            reason=clean_reason,
        )
    except ToolApprovalNotFoundError as err:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(err)) from err
    except ToolApprovalConflictError as err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(err)) from err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to revoke tool approval.",
        ) from exc

    audit_ev = record_audit_event(
        event_type=AuditEventType.TOOL_APPROVAL_REVOKED,
        outcome="revoked",
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": clean_tool_name,
            "reason": record["revocation_reason"],
            "idempotent": record.get("idempotent", False),
        },
        store=store,
        dedup=True,
    )

    return RevokeToolApprovalResponse(
        success=True,
        operation_key=record["operation_key"],
        run_id=record["run_id"],
        tool_name=record["tool_name"],
        revoked_by=record["revoked_by"],
        decision=record["decision"],
        reason=record["revocation_reason"],
        revoked_at=record["revoked_at"],
        idempotent=record.get("idempotent", False),
        audit_event_id=audit_ev.event_id if audit_ev else None,
    )


@router.get(
    "/runs/{run_id}/tools/{operation_key}/approval",
    response_model=ToolApprovalModel,
    summary="Get Tool Execution Approval Decision",
    description="Retrieve the recorded approval decision for a tool execution.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def get_tool_approval(
    run_id: str,
    operation_key: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ToolApprovalModel:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="operation_key must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    record = store.get_tool_approval(clean_op_key)
    if not record or record.get("run_id") != clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No approval decision found for operation key '{clean_op_key}' in run '{clean_run_id}'.",
        )

    return ToolApprovalModel(**sanitize_tool_approval(record))


@router.get(
    "/runs/{run_id}/approvals",
    response_model=RunToolApprovalsResponse,
    summary="List Tool Approvals for a Run",
    description="Retrieve recorded tool approvals for a workflow run, optionally filtered by status or tool.",
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/runs/{run_id}/tools/approvals",
    response_model=RunToolApprovalsResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def list_run_tool_approvals(
    run_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    manager: Annotated[RunManager, Depends(get_run_manager)],
    status_filter: Annotated[
        str | None,
        Query(
            alias="status",
            description="Filter by approval status: pending, approved, rejected, revoked",
        ),
    ] = None,
    tool_name: Annotated[
        str | None,
        Query(description="Filter by tool name"),
    ] = None,
) -> RunToolApprovalsResponse:
    clean_id = run_id.strip()
    if not clean_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="run_id must not be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    summary = store.get_run_summary(clean_id)
    is_active = manager.is_active(clean_id)
    raw_approvals = store.list_tool_approvals(
        clean_id,
        status=status_filter,
        tool_name=tool_name,
    )

    if not summary and not is_active and not raw_approvals:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{clean_id}' not found.",
        )

    sanitized_items = [
        ToolApprovalModel(**sanitize_tool_approval(rec))
        for rec in raw_approvals
    ]

    return RunToolApprovalsResponse(
        run_id=clean_id,
        total_approvals=len(sanitized_items),
        total_count=len(sanitized_items),
        approvals=sanitized_items,
    )


@router.get(
    "/runs/{run_id}/tools/{operation_key}/diagnostics",
    response_model=ToolApprovalDiagnosticsResponse,
    summary="Get Tool Approval Diagnostics",
    description="Inspect why an operation is blocked, pending, stale, or safe to execute.",
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/runs/{run_id}/tools/{operation_key}/approval/diagnostics",
    response_model=ToolApprovalDiagnosticsResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_tool_approval_diagnostics(
    run_id: str,
    operation_key: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    tool_name: Annotated[str | None, Query(description="Optional tool name")] = None,
) -> ToolApprovalDiagnosticsResponse:
    clean_id = run_id.strip()
    clean_key = operation_key.strip()
    if not clean_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty."
        )
    if not clean_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="operation_key must not be empty."
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    diag = diagnose_tool_approval(
        run_id=clean_id,
        operation_key=clean_key,
        tool_name=tool_name,
        store=store,
    )

    ev_models = [
        AuditEventModel(
            event_id=ev["event_id"],
            timestamp=ev["timestamp"],
            event_type=ev["event_type"],
            outcome=ev["outcome"],
            run_id=ev.get("run_id"),
            correlation_id=ev.get("correlation_id"),
            actor=ev.get("actor"),
            metadata=ev.get("metadata") or {},
        )
        for ev in diag.get("relevant_audit_events", [])
    ]
    diag["relevant_audit_events"] = ev_models
    return ToolApprovalDiagnosticsResponse(**diag)


@router.post(
    "/runs/{run_id}/tools/{operation_key}/diagnostics",
    response_model=ToolApprovalDiagnosticsResponse,
    summary="Inspect Tool Approval Diagnostics with Current Arguments",
    description="Inspect approval state and test whether current invocation arguments match the approval.",
    dependencies=[Depends(verify_api_token)],
)
def inspect_tool_approval_diagnostics(
    run_id: str,
    operation_key: str,
    req: DiagnoseToolApprovalRequest,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ToolApprovalDiagnosticsResponse:
    clean_id = run_id.strip()
    clean_key = operation_key.strip()
    if not clean_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty."
        )
    if not clean_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="operation_key must not be empty."
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    diag = diagnose_tool_approval(
        run_id=clean_id,
        operation_key=clean_key,
        tool_name=req.tool_name,
        arguments=req.arguments,
        store=store,
    )

    ev_models = [
        AuditEventModel(
            event_id=ev["event_id"],
            timestamp=ev["timestamp"],
            event_type=ev["event_type"],
            outcome=ev["outcome"],
            run_id=ev.get("run_id"),
            correlation_id=ev.get("correlation_id"),
            actor=ev.get("actor"),
            metadata=ev.get("metadata") or {},
        )
        for ev in diag.get("relevant_audit_events", [])
    ]
    diag["relevant_audit_events"] = ev_models
    return ToolApprovalDiagnosticsResponse(**diag)


@router.post(
    "/runs/{run_id}/tools/{operation_key}/recover",
    response_model=RecoverToolApprovalResponse,
    summary="Recover Blocked or Stale Tool Approval",
    description=(
        "Privileged operational recovery: safely resets a blocked, stale, rejected, or revoked "
        "approval back to 'pending'. Does NOT automatically approve or redispatch the operation. "
        "Requires explicit operator identity and justification, and emits an audited event."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
@router.post(
    "/runs/{run_id}/tools/{operation_key}/reset-approval",
    response_model=RecoverToolApprovalResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def recover_tool_approval_endpoint(
    run_id: str,
    operation_key: str,
    req: RecoverToolApprovalRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RecoverToolApprovalResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()
    clean_tool_name = req.tool_name.strip()

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty."
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="operation_key must not be empty."
        )
    if not clean_tool_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="tool_name must not be empty."
        )

    clean_reason = req.reason.strip() if req.reason else ""
    if not clean_reason:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Recovery reason is required and cannot be empty.",
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    actor = (
        req.operator.strip()
        if req.operator and req.operator.strip()
        else get_client_scope(request, credentials)
    )

    policy = REGISTRY.get_policy(clean_tool_name)
    try:
        verify_approver_authorization(actor, clean_tool_name, policy)
    except ToolApproverUnauthorizedError as auth_err:
        record_audit_event(
            event_type=AuditEventType.TOOL_APPROVER_DENIED,
            outcome="denied",
            run_id=clean_run_id,
            actor=actor or "unknown",
            metadata={
                "operation_key": clean_op_key,
                "tool_name": clean_tool_name,
                "action": "recover",
                "reason": str(auth_err),
            },
            store=store,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(auth_err),
        ) from auth_err

    arg_fp: str | None = None
    if req.arguments is not None:
        arg_fp = compute_argument_fingerprint(req.arguments)

    risk_level_str = (
        policy.risk_level.value
        if policy and isinstance(policy.risk_level, RiskLevel)
        else (str(policy.risk_level) if policy else None)
    )
    policy_hash_str = compute_policy_hash(policy) if policy else None

    try:
        record = store.recover_tool_approval(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            tool_name=clean_tool_name,
            recovered_by=actor,
            reason=clean_reason,
            argument_fingerprint=arg_fp,
            risk_level=risk_level_str,
            policy_hash=policy_hash_str,
        )
    except ToolApprovalNotFoundError as not_found_err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(not_found_err),
        ) from not_found_err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to execute approval operational recovery.",
        ) from exc

    audit_ev = record_audit_event(
        event_type=AuditEventType.TOOL_APPROVAL_RECOVERED,
        outcome="recovered",
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": clean_tool_name,
            "reason": clean_reason,
            "new_decision": "pending",
        },
        store=store,
        dedup=True,
    )

    return RecoverToolApprovalResponse(
        success=True,
        operation_key=record["operation_key"],
        run_id=record["run_id"],
        tool_name=record["tool_name"],
        decision=record["decision"],
        reason=clean_reason,
        recovered_by=record["recovered_by"],
        recovered_at=record["created_at"],
        argument_fingerprint=record.get("argument_fingerprint"),
        risk_level=record.get("risk_level"),
        policy_hash=record.get("policy_hash"),
        expires_at=record.get("expires_at"),
        audit_event_id=audit_ev.event_id if audit_ev else None,
    )


@router.post(
    "/runs/{run_id}/tools/{operation_key}/expire",
    response_model=ExpireToolApprovalResponse,
    summary="Expire Tool Approval",
    description=(
        "Explicitly mark a tool approval as expired. "
        "Preserves safe lifecycle and emits an audit event."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def expire_tool_approval_endpoint(
    run_id: str,
    operation_key: str,
    req: ExpireToolApprovalRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ExpireToolApprovalResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()
    clean_tool_name = req.tool_name.strip() if req.tool_name else None

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty."
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="operation_key must not be empty."
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    actor = (
        req.actor.strip()
        if req.actor and req.actor.strip()
        else get_client_scope(request, credentials)
    )

    clean_reason = req.reason.strip() if req.reason else "Approval expired by operator"

    try:
        record = store.expire_tool_approval(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            tool_name=clean_tool_name,
            reason=clean_reason,
            force=req.force,
            actor=actor,
        )
    except ToolApprovalNotFoundError as not_found_err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(not_found_err),
        ) from not_found_err
    except ToolApprovalConflictError as conf_err:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(conf_err),
        ) from conf_err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record tool approval expiration.",
        ) from exc

    audit_ev = record_audit_event(
        event_type=AuditEventType.TOOL_APPROVAL_EXPIRED,
        outcome="expired",
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": record.get("tool_name"),
            "reason": clean_reason,
            "expires_at": record.get("expires_at"),
            "expired_at": record.get("expired_at"),
            "idempotent": record.get("idempotent", False),
        },
        store=store,
        dedup=True,
    )

    return ExpireToolApprovalResponse(
        success=True,
        operation_key=record["operation_key"],
        run_id=record["run_id"],
        tool_name=record["tool_name"],
        decision=record["decision"],
        expired_at=record["expired_at"] or record.get("updated_at") or "",
        reason=clean_reason,
        idempotent=record.get("idempotent", False),
        audit_event_id=record.get("audit_event_id") or (audit_ev.event_id if audit_ev else None),
    )


@router.post(
    "/runs/{run_id}/tools/{operation_key}/escalate",
    response_model=EscalateToolApprovalResponse,
    summary="Escalate Overdue Tool Approval",
    description="Record an escalation event for an overdue pending tool approval.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def escalate_tool_approval_endpoint(
    run_id: str,
    operation_key: str,
    req: EscalateToolApprovalRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> EscalateToolApprovalResponse:
    clean_run_id = run_id.strip()
    clean_op_key = operation_key.strip()
    clean_tool_name = req.tool_name.strip() if req.tool_name else None

    if not clean_run_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="run_id must not be empty."
        )
    if not clean_op_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="operation_key must not be empty."
        )

    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    actor = (
        req.actor.strip()
        if req.actor and req.actor.strip()
        else get_client_scope(request, credentials)
    )

    clean_reason = req.reason.strip() if req.reason else "Approval request overdue"

    try:
        record = store.escalate_tool_approval(
            run_id=clean_run_id,
            operation_key=clean_op_key,
            tool_name=clean_tool_name,
            reason=clean_reason,
            escalation_level=req.escalation_level,
            actor=actor,
        )
    except ToolApprovalNotFoundError as not_found_err:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(not_found_err),
        ) from not_found_err
    except ToolApprovalConflictError as conf_err:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(conf_err),
        ) from conf_err
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to record tool approval escalation.",
        ) from exc

    audit_ev = record_audit_event(
        event_type=AuditEventType.TOOL_APPROVAL_ESCALATED,
        outcome="escalated",
        run_id=clean_run_id,
        actor=actor,
        metadata={
            "operation_key": clean_op_key,
            "tool_name": record.get("tool_name"),
            "reason": clean_reason,
            "escalation_level": req.escalation_level,
            "escalated_at": record.get("escalated_at"),
            "idempotent": record.get("idempotent", False),
        },
        store=store,
        dedup=True,
    )

    return EscalateToolApprovalResponse(
        success=True,
        operation_key=record["operation_key"],
        run_id=record["run_id"],
        tool_name=record["tool_name"],
        escalated_at=record["escalated_at"] or record.get("updated_at") or "",
        escalation_reason=clean_reason,
        idempotent=record.get("idempotent", False),
        audit_event_id=record.get("audit_event_id") or (audit_ev.event_id if audit_ev else None),
    )


@router.post(
    "/runs/{run_id}/approvals/scan-expired",
    response_model=ScanExpiredApprovalsResponse,
    summary="Scan and Expire Overdue Approvals for Run",
    description="Scan pending approvals for a run, transitioning expired ones and identifying overdue ones.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
@router.post(
    "/approvals/scan-expired",
    response_model=ScanExpiredApprovalsResponse,
    summary="Global Scan and Expire Overdue Approvals",
    description="Scan pending approvals globally, transitioning expired ones and identifying overdue ones.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def scan_expired_approvals_endpoint(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
    run_id: str | None = None,
) -> ScanExpiredApprovalsResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    actor = get_client_scope(request, credentials)
    clean_run_id = run_id.strip() if run_id else None

    settings = get_settings()
    scheduler = getattr(request.app.state, "approval_scheduler", None)
    if scheduler is None:
        scheduler = get_approval_scheduler(store=store, settings=settings)

    scan_res = scheduler.run_scan_cycle(
        actor=actor,
        run_id=clean_run_id,
        store=store,
    )
    if not scan_res.get("success", True) and "error" in scan_res:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Approval scan failed: {scan_res['error']}",
        )

    return ScanExpiredApprovalsResponse(
        run_id=clean_run_id,
        scanned_at=scan_res.get("scanned_at")
        or datetime.datetime.now(datetime.timezone.utc).isoformat(),
        expired_count=scan_res.get("expired_count", 0),
        escalated_count=scan_res.get("escalated_count", 0),
        expired_operation_keys=scan_res.get("expired_operation_keys", []),
        escalated_operation_keys=scan_res.get("escalated_operation_keys", []),
        failed_count=scan_res.get("failed_count", 0),
        failed_operation_keys=scan_res.get("failed_operation_keys", []),
    )


@router.get(
    "/approvals/scheduler",
    response_model=ApprovalSchedulerDiagnostics,
    summary="Inspect Automated Approval Lifecycle Scheduler",
    description=(
        "Inspect the current operational status, scan intervals, execution counts, "
        "and health metrics of the automated approval lifecycle scheduler."
    ),
    dependencies=[Depends(verify_api_token)],
)
def get_approval_scheduler_status_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ApprovalSchedulerDiagnostics:
    settings = get_settings()
    scheduler = getattr(request.app.state, "approval_scheduler", None)
    if scheduler is None:
        scheduler = get_approval_scheduler(store=store, settings=settings)
    return ApprovalSchedulerDiagnostics(**scheduler.get_status())


@router.get(
    "/approvals/outbox",
    response_model=ListOutboxEventsResponse,
    summary="List Outbox Events",
    description="Query durable outbox events with status and event_type filters and pagination.",
    dependencies=[Depends(verify_api_token)],
)
def list_outbox_events_endpoint(
    store: Annotated[CheckpointStore, Depends(get_store)],
    status: Annotated[str | None, Query(description="Filter by delivery status")] = None,
    event_type: Annotated[str | None, Query(description="Filter by event type")] = None,
    limit: Annotated[int, Query(ge=1, le=500, description="Page limit")] = 50,
    offset: Annotated[int, Query(ge=0, description="Page offset")] = 0,
) -> ListOutboxEventsResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )
    events = store.get_outbox_events(status=status, event_type=event_type, limit=limit, offset=offset)
    models = [OutboxEventModel(**ev) for ev in events]
    return ListOutboxEventsResponse(items=models, total=len(models), limit=limit, offset=offset)


@router.get(
    "/approvals/outbox/diagnostics",
    response_model=OutboxDiagnosticsResponse,
    summary="Inspect Outbox Diagnostics",
    description="Inspect transactional outbox status counts and background dispatcher metrics.",
    dependencies=[Depends(verify_api_token)],
)
def get_outbox_diagnostics_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> OutboxDiagnosticsResponse:
    if not store.enabled:
        return OutboxDiagnosticsResponse(enabled=False, total_events=0, counts={})
    settings = get_settings()
    max_attempts = getattr(settings, "outbox_max_attempts", 5)
    diag = store.get_outbox_diagnostics(max_attempts=max_attempts)
    dispatcher = getattr(request.app.state, "outbox_dispatcher", None)
    disp_status = dispatcher.get_status() if dispatcher else None
    return OutboxDiagnosticsResponse(
        enabled=diag.get("enabled", True),
        total_events=diag.get("total_events", 0),
        counts=diag.get("counts", {}),
        oldest_pending_at=diag.get("oldest_pending_at"),
        oldest_pending_age_seconds=diag.get("oldest_pending_age_seconds"),
        newest_event_at=diag.get("newest_event_at"),
        retry_metrics=diag.get("retry_metrics", {}),
        delivery_latency=diag.get("delivery_latency"),
        recent_failures=diag.get("recent_failures", []),
        expired_claims_count=diag.get("expired_claims_count", 0),
        expired_claims=diag.get("expired_claims", []),
        retry_limit_diagnostics=diag.get("retry_limit_diagnostics", {}),
        dispatcher=disp_status,
    )


@router.post(
    "/approvals/outbox/{event_id}/retry",
    response_model=RetryOutboxEventResponse,
    summary="Retry Outbox Event",
    description="Manually reset a failed or dead-letter outbox event back to pending for retry.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def retry_outbox_event_endpoint(
    event_id: str,
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
    request_body: RetryOutboxEventRequest | None = None,
    reason: Annotated[str | None, Query(description="Operator reason")] = None,
    operator: Annotated[str | None, Query(description="Operator identity")] = None,
    reset_attempts: Annotated[bool, Query(description="Reset attempt counter to 0")] = False,
    force: Annotated[bool, Query(description="Force recovery of active claim")] = False,
) -> RetryOutboxEventResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    if request_body and request_body.reason is not None:
        if not request_body.reason.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Recovery reason cannot be empty.",
            )
        effective_reason = request_body.reason.strip()
    elif reason is not None:
        if not reason.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Recovery reason cannot be empty.",
            )
        effective_reason = reason.strip()
    else:
        effective_reason = "Operator manual retry via API"

    if request_body and request_body.operator:
        effective_operator = request_body.operator.strip()
    elif operator:
        effective_operator = operator.strip()
    else:
        effective_operator = get_client_scope(request, credentials)

    effective_reset = request_body.reset_attempts if request_body else reset_attempts
    effective_force = request_body.force if request_body else force

    try:
        updated = store.retry_outbox_event(
            event_id=event_id,
            reset_attempts=effective_reset,
            reason=effective_reason,
            operator=effective_operator,
            force=effective_force,
        )
        return RetryOutboxEventResponse(event=OutboxEventModel(**updated), retried=True)
    except CheckpointNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ValueError as val_err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(val_err)) from val_err
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


@router.post(
    "/approvals/outbox/recover",
    response_model=RecoverOutboxEventsResponse,
    summary="Batch Operator Recovery of Outbox Events",
    description="Batch recover eligible dead-letter or failed outbox events with mandatory reason.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def recover_outbox_events_endpoint(
    request: Request,
    req: RecoverOutboxEventsRequest,
    store: Annotated[CheckpointStore, Depends(get_store)],
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> RecoverOutboxEventsResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )
    clean_reason = req.reason.strip()
    if not clean_reason:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A non-empty recovery reason must be provided.",
        )
    operator = req.operator.strip() if req.operator else get_client_scope(request, credentials)
    result = store.recover_outbox_events(
        reason=clean_reason,
        event_ids=req.event_ids,
        status_filter=req.status_filter,
        operator=operator,
        reset_attempts=req.reset_attempts,
        max_events=req.max_events,
    )
    event_models = [OutboxEventModel(**ev) for ev in result.get("recovered_events", [])]
    return RecoverOutboxEventsResponse(
        recovered_count=result.get("recovered_count", 0),
        skipped_count=result.get("skipped_count", 0),
        recovered_events=event_models,
        reason=clean_reason,
        operator=operator,
    )


@router.get(
    "/approvals/outbox/{event_id}/history",
    response_model=OutboxHistoryResponse,
    summary="Inspect Outbox Event Delivery History",
    description="Query chronological delivery attempts, errors, and operator recovery actions.",
    dependencies=[Depends(verify_api_token)],
)
def get_outbox_event_history_endpoint(
    event_id: str,
    store: Annotated[CheckpointStore, Depends(get_store)],
    limit: Annotated[int, Query(ge=1, le=500, description="Page limit")] = 50,
) -> OutboxHistoryResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )
    history_rows = store.get_outbox_event_history(event_id=event_id, limit=limit)
    items = [OutboxHistoryEntryModel(**r) for r in history_rows]
    return OutboxHistoryResponse(
        event_id=event_id,
        history=items,
        total=len(items),
    )


@router.post(
    "/approvals/outbox/dispatch",
    response_model=DispatchOutboxResponse,
    summary="Trigger Outbox Dispatch Cycle",
    description="Manually execute a bounded outbox dispatch cycle.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def trigger_outbox_dispatch_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    batch_size: Annotated[int | None, Query(ge=1, le=500, description="Batch size")] = None,
) -> DispatchOutboxResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )
    from ..notifications import get_outbox_dispatcher

    dispatcher = getattr(request.app.state, "outbox_dispatcher", None)
    if dispatcher is None:
        dispatcher = get_outbox_dispatcher(store=store)

    result = dispatcher.run_dispatch_cycle(batch_size=batch_size)
    return DispatchOutboxResponse(
        scanned_at=(
            result.get("scanned_at") or datetime.datetime.now(datetime.timezone.utc).isoformat()
        ),
        claimed_count=result.get("claimed_count", 0),
        delivered_count=result.get("delivered_count", 0),
        failed_count=result.get("failed_count", 0),
        dead_letter_count=result.get("dead_letter_count", 0),
        stale_count=result.get("stale_count", 0),
    )


@router.get(
    "/notifications/replay/diagnostics",
    response_model=ReplayDiagnosticsResponse,
    summary="Inspect Webhook Replay Store Diagnostics",
    description="Inspect database-derived replay record metrics, expiration status, and maintenance history.",
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/approvals/outbox/replay/diagnostics",
    response_model=ReplayDiagnosticsResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_replay_diagnostics_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> ReplayDiagnosticsResponse:
    if not store.enabled:
        return ReplayDiagnosticsResponse(
            enabled=False,
            storage_backend="disabled",
            total_records=0,
            active_records=0,
            expired_records=0,
        )

    from ..notifications import get_default_replay_store

    replay_store = get_default_replay_store(store=store)
    if replay_store is None:
        return ReplayDiagnosticsResponse(
            enabled=False,
            storage_backend="disabled",
            total_records=0,
            active_records=0,
            expired_records=0,
        )

    diag = replay_store.get_diagnostics()

    scheduler = getattr(request.app.state, "replay_cleanup_scheduler", None)
    sched_status = scheduler.get_status() if scheduler else None

    return ReplayDiagnosticsResponse(
        enabled=diag.get("enabled", True),
        storage_backend=diag.get("storage_backend", "sqlite"),
        retention_seconds=diag.get("retention_seconds"),
        cleanup_batch_size=diag.get("cleanup_batch_size"),
        total_records=diag.get("total_records", 0),
        active_records=diag.get("active_records", 0),
        expired_records=diag.get("expired_records", 0),
        oldest_timestamp=diag.get("oldest_timestamp"),
        newest_timestamp=diag.get("newest_timestamp"),
        oldest_recorded_at=diag.get("oldest_recorded_at"),
        newest_recorded_at=diag.get("newest_recorded_at"),
        last_cleanup=diag.get("last_cleanup"),
        scheduler_status=sched_status,
    )


@router.post(
    "/notifications/replay/cleanup",
    response_model=ReplayCleanupResponse,
    summary="Trigger Bounded Webhook Replay Cleanup",
    description="Manually prune expired replay records up to bounded limit and record audit trail.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
@router.post(
    "/approvals/outbox/replay/cleanup",
    response_model=ReplayCleanupResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def trigger_replay_cleanup_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    body: ReplayCleanupRequest | None = None,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> ReplayCleanupResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CheckpointStore persistence is disabled.",
        )

    req = body or ReplayCleanupRequest()
    clean_limit = max(1, min(int(req.limit or 500), 10000))
    clean_reason = (req.reason or "manual operational cleanup").strip()

    operator = "operator"
    if credentials and credentials.credentials:
        token_val = credentials.credentials.strip()
        if len(token_val) >= 8:
            operator = f"token:{token_val[:4]}...{token_val[-4:]}"
        else:
            operator = "authenticated_user"

    from ..notifications import get_default_replay_store

    replay_store = get_default_replay_store(store=store)
    if replay_store is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook replay protection is disabled in settings.",
        )

    start_mono = time.monotonic()
    now_ts = time.time()
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    try:
        deleted = replay_store.cleanup_expired(
            now=now_ts,
            limit=clean_limit,
            trigger_type="manual",
            operator=operator,
            reason=clean_reason,
        )
        duration_ms = round((time.monotonic() - start_mono) * 1000.0, 3)

        try:
            from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

            record_audit_event(
                event_type=AuditEventType.NOTIFICATION_REPLAY_CLEANUP,
                outcome=AuditOutcome.SUCCESS,
                actor=operator,
                metadata={
                    "deleted_count": deleted,
                    "limit": clean_limit,
                    "reason": clean_reason,
                    "duration_ms": duration_ms,
                    "trigger_type": "manual",
                },
                store=store,
            )
        except Exception as audit_exc:
            logger.warning("Failed to record replay cleanup audit event: %s", audit_exc)

        return ReplayCleanupResponse(
            success=True,
            deleted_count=deleted,
            duration_ms=duration_ms,
            cleaned_at=now_iso,
            limit=clean_limit,
            trigger_type="manual",
            operator=operator,
            reason=clean_reason,
        )
    except Exception as exc:
        logger.error("Error executing manual replay cleanup: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Replay cleanup failed: {exc}",
        ) from exc



@router.get(
    "/audit/export",
    summary="Export System Audit Events",
    description=(
        "Export system-wide audit events in JSON, JSONL, or CSV format "
        "with time range filtering and bounded limits."
    ),
    dependencies=[Depends(verify_api_token)],
)
def export_system_audit(
    store: Annotated[CheckpointStore, Depends(get_store)],
    run_id: Annotated[str | None, Query(description="Filter by specific run ID")] = None,
    format: Annotated[str, Query(description="Export format: json, jsonl, or csv")] = "json",
    limit: Annotated[int, Query(ge=1, le=5000, description="Maximum events to export")] = 1000,
    offset: Annotated[int, Query(ge=0, description="Pagination offset")] = 0,
    start_time: Annotated[str | None, Query(description="ISO 8601 start timestamp filter")] = None,
    end_time: Annotated[str | None, Query(description="ISO 8601 end timestamp filter")] = None,
    event_type: Annotated[str | None, Query(description="Filter by specific event type")] = None,
) -> Response:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )

    fmt = format.lower().strip()
    if fmt not in ("json", "jsonl", "csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported format '{format}'. Supported formats: json, jsonl, csv.",
        )

    if start_time and end_time and start_time.strip() > end_time.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="start_time must be earlier than or equal to end_time.",
        )

    clean_run_id = run_id.strip() if run_id else None

    events_data = store.export_audit_events(
        run_id=clean_run_id,
        event_type=event_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
    )

    if fmt == "json":
        content = serialize_audit_events_json(events_data)
        media_type = "application/json"
        filename = "audit-export.json"
    elif fmt == "jsonl":
        content = serialize_audit_events_jsonl(events_data)
        media_type = "application/x-ndjson"
        filename = "audit-export.jsonl"
    else:  # csv
        content = serialize_audit_events_csv(events_data)
        media_type = "text/csv"
        filename = "audit-export.csv"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/audit/cleanup",
    response_model=AuditCleanupResponse,
    summary="Purge Expired Audit Events",
    description="Explicitly purge audit events older than retention cutoff or specified timestamp.",
    dependencies=[Depends(verify_api_token)],
)
def cleanup_audit_events(
    store: Annotated[CheckpointStore, Depends(get_store)],
    request: AuditCleanupRequest | None = None,
) -> AuditCleanupResponse:
    if not store.enabled:
        return AuditCleanupResponse(
            success=False,
            deleted_count=0,
            cutoff_timestamp=None,
            max_age_days=None,
            reason="Persistence is disabled",
        )

    req = request or AuditCleanupRequest()

    if req.max_age_days is not None and req.max_age_days < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="max_age_days must be non-negative.",
        )

    if req.before_timestamp:
        try:
            datetime.datetime.fromisoformat(req.before_timestamp.replace("Z", "+00:00"))
        except (ValueError, TypeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid before_timestamp format '{req.before_timestamp}'. Expected ISO 8601.",
            ) from exc

    if req.limit is not None and req.limit < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="limit must be greater than or equal to 1.",
        )

    result = store.purge_audit_events(
        max_age_days=req.max_age_days,
        before_timestamp=req.before_timestamp,
        limit=req.limit,
    )

    return AuditCleanupResponse(**result)


@router.get(
    "/evaluation",
    response_model=EvaluationSummaryResponse,
    summary="Evaluation Dataset and Telemetry",
    description="Inspect versioned evaluation cases and the latest available evaluation summary.",
    dependencies=[Depends(verify_api_token)],
)
def get_evaluation(
    tag: Annotated[str | None, Query(description="Filter cases by tag.")] = None,
) -> EvaluationSummaryResponse:
    tags = [tag.strip()] if tag and tag.strip() else None
    cases = get_evaluation_cases(tags=tags)

    case_summaries = [
        EvaluationCaseSummary(
            case_id=c.case_id,
            name=c.name,
            description=c.description,
            tags=c.tags,
            expected_status=c.expected.expected_status,
        )
        for c in cases
    ]

    return EvaluationSummaryResponse(
        dataset_version=DATASET_VERSION,
        total_cases=len(case_summaries),
        cases=case_summaries,
        latest_summary=None,
    )


# ── Celery Worker Observability & Operational Recovery Endpoints ─────────────


@router.get(
    "/celery/workers",
    response_model=CeleryWorkersResponse,
    summary="Celery Worker Fleet Telemetry",
    description="Inspect Celery worker health, registrations, active tasks, and heartbeats.",
    dependencies=[Depends(verify_api_token)],
)
def get_celery_workers(
    store: Annotated[CheckpointStore, Depends(get_store)],
    stale_threshold_seconds: Annotated[
        float | None,
        Query(ge=1.0, le=3600.0, description="Override stale worker heartbeat threshold."),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=1000, description="Max worker records to return."),
    ] = 100,
) -> CeleryWorkersResponse:
    """Retrieve operational telemetry and status for all registered Celery workers."""
    settings = get_settings()
    eff_threshold = (
        stale_threshold_seconds
        if stale_threshold_seconds is not None
        else settings.celery_worker_stale_threshold_seconds
    )
    raw_workers = store.list_worker_heartbeats(
        stale_threshold_seconds=eff_threshold,
        limit=limit,
    )
    worker_models = [CeleryWorkerModel(**w) for w in raw_workers]
    active_count = sum(1 for w in worker_models if w.status == "active")
    stale_count = sum(1 for w in worker_models if w.status == "stale")
    offline_count = sum(1 for w in worker_models if w.status == "offline")

    return CeleryWorkersResponse(
        total_workers=len(worker_models),
        active_workers=active_count,
        stale_workers=stale_count,
        offline_workers=offline_count,
        stale_threshold_seconds=eff_threshold,
        workers=worker_models,
    )


@router.get(
    "/celery/queue",
    response_model=CeleryQueueMetricsResponse,
    summary="Celery Queue and Execution Metrics",
    description="Derive operational queue metrics, worker fleet summary, and sanitized recent failures.",
    dependencies=[Depends(verify_api_token)],
)
def get_celery_queue(
    store: Annotated[CheckpointStore, Depends(get_store)],
    stale_threshold_seconds: Annotated[
        float | None,
        Query(ge=1.0, le=3600.0, description="Override stale worker heartbeat threshold."),
    ] = None,
) -> CeleryQueueMetricsResponse:
    """Derive durable operational metrics for Celery task queuing and worker fleet."""
    settings = get_settings()
    eff_threshold = (
        stale_threshold_seconds
        if stale_threshold_seconds is not None
        else settings.celery_worker_stale_threshold_seconds
    )
    metrics = store.get_celery_queue_metrics(stale_threshold_seconds=eff_threshold)
    return CeleryQueueMetricsResponse(**metrics)


@router.get(
    "/celery/stuck",
    response_model=StuckWorkflowsResponse,
    summary="Stuck and Interrupted Workflows",
    description="Identify workflows stuck in queued or running states without forward progress.",
    dependencies=[Depends(verify_api_token)],
)
def get_celery_stuck(
    store: Annotated[CheckpointStore, Depends(get_store)],
    stuck_threshold_seconds: Annotated[
        float | None,
        Query(ge=5.0, le=86400.0, description="Override stuck workflow age threshold."),
    ] = None,
    stale_worker_threshold_seconds: Annotated[
        float | None,
        Query(ge=1.0, le=3600.0, description="Override worker staleness threshold."),
    ] = None,
    limit: Annotated[
        int,
        Query(ge=1, le=500, description="Max stuck workflows to return."),
    ] = 50,
) -> StuckWorkflowsResponse:
    """List workflows diagnosed as stuck or interrupted without recent forward progress."""
    settings = get_settings()
    eff_stuck_threshold = (
        stuck_threshold_seconds
        if stuck_threshold_seconds is not None
        else settings.celery_workflow_stuck_threshold_seconds
    )
    eff_worker_threshold = (
        stale_worker_threshold_seconds
        if stale_worker_threshold_seconds is not None
        else settings.celery_worker_stale_threshold_seconds
    )
    stuck_items = store.get_stuck_workflows(
        stuck_threshold_seconds=eff_stuck_threshold,
        stale_worker_threshold_seconds=eff_worker_threshold,
        limit=limit,
    )
    models = [StuckWorkflowModel(**item) for item in stuck_items]
    eligible_count = sum(1 for m in models if m.recovery_eligible)

    return StuckWorkflowsResponse(
        total_stuck=len(models),
        eligible_count=eligible_count,
        stuck_threshold_seconds=eff_stuck_threshold,
        workflows=models,
    )


@router.post(
    "/celery/recover",
    response_model=RecoverWorkflowsResponse,
    summary="Safe Operator Recovery for Stuck Workflows",
    description="Safely recover stuck, orphaned, or interrupted workflows.",
    dependencies=[Depends(verify_api_token)],
)
def recover_celery_workflows(
    req: RecoverWorkflowsRequest,
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)],
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RecoverWorkflowsResponse:
    """Execute safe operator recovery on eligible stuck or interrupted workflows."""
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Persistence checkpointing is currently disabled.",
        )
    if not req.run_id and not req.run_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either run_id or run_ids must be provided.",
        )
    clean_reason = req.reason.strip()
    if len(clean_reason) < 5:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Recovery reason must be at least 5 characters.",
        )
    clean_action = req.action.strip().lower()
    if clean_action not in ("requeue", "fail", "cancel"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid recovery action '{req.action}'. Must be 'requeue', 'fail', or 'cancel'.",
        )

    operator = req.operator.strip() if req.operator else get_client_scope(request, credentials)
    target_ids = (
        [req.run_id.strip()]
        if req.run_id
        else [r.strip() for r in (req.run_ids or []) if r.strip()]
    )

    settings = get_settings()
    max_batch = settings.celery_recovery_batch_size
    if len(target_ids) > max_batch:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Batch size {len(target_ids)} exceeds max allowed batch size {max_batch}.",
        )

    results: list[RecoveredWorkflowItem] = []
    recovered_count = 0
    failed_count = 0

    for rid in target_ids:
        try:
            rec = store.recover_stuck_workflow(
                run_id=rid,
                operator=operator,
                reason=clean_reason,
                action=clean_action,
            )
            if clean_action == "requeue" and (
                settings.celery_enabled or settings.workflow_execution_backend == "celery"
            ):
                try:
                    summary = store.get_run_summary(rid)
                    task_text = (summary.get("task") if summary else "") or ""
                    from ..celery_app import execute_workflow_run

                    execute_workflow_run.delay(run_id=rid, task=task_text)
                except Exception as dispatch_exc:
                    logger.warning(
                        "Failed to re-enqueue Celery task for recovered run %s: %s",
                        rid,
                        dispatch_exc,
                    )

            results.append(
                RecoveredWorkflowItem(
                    run_id=rid,
                    success=True,
                    previous_status=rec.get("previous_status"),
                    new_status=rec.get("new_status"),
                    action=clean_action,
                    fence_token=rec.get("fence_token"),
                )
            )
            recovered_count += 1
        except Exception as exc:
            failed_count += 1
            results.append(
                RecoveredWorkflowItem(
                    run_id=rid,
                    success=False,
                    action=clean_action,
                    error=str(exc),
                )
            )

    return RecoverWorkflowsResponse(
        success=(failed_count == 0),
        action=clean_action,
        recovered_count=recovered_count,
        failed_count=failed_count,
        operator=operator,
        reason=clean_reason,
        results=results,
    )


# =============================================================================
# Automated Database Backup Scheduling & Diagnostics Endpoints
# =============================================================================


@router.get(
    "/backups/status",
    response_model=BackupStatusResponse,
    summary="Inspect Database Backup Scheduler Status",
    description=(
        "Inspect backup scheduler state, cross-process leadership, execution metrics, "
        "and storage summary."
    ),
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/backups/scheduler",
    response_model=BackupStatusResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_backup_status_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> BackupStatusResponse:
    settings = get_settings()
    from ..persistence import BackupManager, get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    sched_status = scheduler.get_status()
    mgr = BackupManager(
        db_path=store.db_path,
        backup_dir=scheduler.backup_dir,
        settings=settings,
    )
    backups = mgr.list_backups()
    latest_meta = None
    if backups:
        latest = backups[0]
        latest_meta = BackupMetadataModel(**latest.to_dict())

    if sched_status["running"] and sched_status["is_leader"]:
        subsys_status = "healthy"
    elif sched_status["running"]:
        subsys_status = "standby"
    elif sched_status["enabled"]:
        subsys_status = "active"
    else:
        subsys_status = "disabled"

    return BackupStatusResponse(
        status=subsys_status,
        scheduler=BackupSchedulerDiagnostics(**sched_status),
        latest_backup=latest_meta,
        total_backups=len(backups),
        backup_dir=scheduler.backup_dir,
    )


@router.post(
    "/backups/trigger",
    response_model=BackupTriggerResponse,
    summary="Trigger Database Backup Execution",
    description="Trigger an authorized immediate online database backup execution cycle.",
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def trigger_backup_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    body: BackupTriggerRequest | None = None,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> BackupTriggerResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CheckpointStore persistence is disabled.",
        )

    settings = get_settings()
    req = body or BackupTriggerRequest()

    operator = "operator"
    if credentials and credentials.credentials:
        token_val = credentials.credentials.strip()
        if len(token_val) >= 8:
            operator = f"token:{token_val[:4]}...{token_val[-4:]}"
        else:
            operator = "authenticated_user"

    from ..persistence import get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    res = scheduler.trigger_backup(
        actor="api",
        operator=operator,
        reason=req.reason or "Manual API backup trigger",
        tag=req.tag,
    )

    meta_model = None
    if res.get("metadata"):
        meta_model = BackupMetadataModel(**res["metadata"])

    return BackupTriggerResponse(
        status=res.get("status", "unknown"),
        message=res.get("message", ""),
        metadata=meta_model,
        duration_ms=float(res.get("duration_ms", 0.0)),
        error=res.get("error"),
        fence_token=res.get("fence_token"),
    )


@router.get(
    "/backups/history",
    response_model=BackupHistoryResponse,
    summary="List Database Backup Archives",
    description="Retrieve all existing online database backup archives with metadata and integrity status.",
    dependencies=[Depends(verify_api_token)],
)
def get_backup_history_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> BackupHistoryResponse:
    settings = get_settings()
    from ..persistence import BackupManager, get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    mgr = BackupManager(
        db_path=store.db_path,
        backup_dir=scheduler.backup_dir,
        settings=settings,
    )
    backups = mgr.list_backups()
    models = [BackupMetadataModel(**b.to_dict()) for b in backups]

    return BackupHistoryResponse(
        total_count=len(models),
        backup_dir=scheduler.backup_dir,
        backups=models,
    )


@router.get(
    "/backups/verify",
    response_model=BackupVerifyResponse,
    summary="Verify Database Backup Archive Integrity Manifest",
    description=(
        "Verify the SHA-256 cryptographic checksum and HMAC-SHA256 signature "
        "of a backup archive against its companion manifest."
    ),
    dependencies=[Depends(verify_api_token)],
)
def verify_backup_manifest_get_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    backup_path: str | None = Query(
        None,
        description="Path or filename of backup archive to verify (default: latest backup).",
    ),
    manifest_path: str | None = Query(
        None,
        description="Optional explicit path to the companion manifest file.",
    ),
    strict: bool | None = Query(
        None,
        description="Whether to strictly require an authenticated cryptographic signature.",
    ),
) -> BackupVerifyResponse:
    settings = get_settings()
    from ..persistence import BackupManager, get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    mgr = BackupManager(
        db_path=store.db_path,
        backup_dir=scheduler.backup_dir,
        settings=settings,
    )

    target_path = backup_path
    if not target_path or not target_path.strip():
        backups = mgr.list_backups()
        if not backups:
            raise HTTPException(
                status_code=404,
                detail="No backup archives found to verify.",
            )
        target_path = backups[0].backup_path
    elif not os.path.isabs(target_path):
        target_path = os.path.join(str(scheduler.backup_dir), target_path)

    res = mgr.verify_manifest(
        backup_path=target_path,
        manifest_path=manifest_path,
        strict_signature=strict,
    )
    return BackupVerifyResponse(**res.to_dict())


@router.post(
    "/backups/verify",
    response_model=BackupVerifyResponse,
    summary="Verify Database Backup Archive Integrity Manifest (POST)",
    description=(
        "Verify the SHA-256 cryptographic checksum and HMAC-SHA256 signature "
        "of a backup archive against its companion manifest."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def verify_backup_manifest_post_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    body: BackupVerifyRequest | None = None,
) -> BackupVerifyResponse:
    req = body or BackupVerifyRequest()
    return verify_backup_manifest_get_endpoint(
        request=request,
        store=store,
        backup_path=req.backup_path,
        manifest_path=req.manifest_path,
        strict=req.strict,
    )


@router.get(
    "/backups/keys",
    response_model=BackupKeyringInfoResponse,
    summary="Inspect Backup Signing Key Identifiers",
    description=(
        "Inspect active signing key ID and verification key IDs safely "
        "without exposing secret keys."
    ),
    dependencies=[Depends(verify_api_token)],
)
def get_backup_keys_endpoint(
    request: Request,
) -> BackupKeyringInfoResponse:
    settings = get_settings()
    info = settings.get_safe_backup_keyring_info()
    return BackupKeyringInfoResponse(**info)


# ============================================================================
# Automated Database Backup Restore Drill Verification Endpoints
# ============================================================================


@router.get(
    "/backups/drill/status",
    response_model=RestoreDrillStatusResponse,
    summary="Inspect Database Restore Drill Status",
    description=(
        "Inspect restore drill state, schedule settings, and most recent "
        "verification outcome."
    ),
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/backups/drills/status",
    response_model=RestoreDrillStatusResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_restore_drill_status_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
) -> RestoreDrillStatusResponse:
    settings = get_settings()
    from ..persistence import get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    latest = store.get_latest_restore_drill()
    all_drills = store.list_restore_drills(limit=500)
    failed_count = sum(1 for d in all_drills if d.get("status") == "failed")

    return RestoreDrillStatusResponse(
        enabled=scheduler.restore_drill_enabled,
        interval_seconds=scheduler.restore_drill_interval_seconds,
        latest_drill=latest,
        last_drill_at=latest.get("started_at") if latest else None,
        last_drill_success_at=(
            latest.get("completed_at") or latest.get("started_at")
            if (latest and latest.get("status") == "success")
            else None
        ),
        last_drill_status=latest.get("status") if latest else None,
        total_drills_count=len(all_drills),
        drill_failure_count=failed_count,
    )


@router.post(
    "/backups/drill/trigger",
    response_model=RestoreDrillTriggerResponse,
    summary="Trigger Database Restore Drill",
    description=(
        "Trigger an authorized on-demand non-destructive database restore drill "
        "against an existing backup in an isolated temporary database."
    ),
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
@router.post(
    "/backups/drills/trigger",
    response_model=RestoreDrillTriggerResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token), Depends(check_rate_limit)],
)
def trigger_restore_drill_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    body: RestoreDrillTriggerRequest | None = None,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)] = None,
) -> RestoreDrillTriggerResponse:
    if not store.enabled:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CheckpointStore persistence is disabled.",
        )

    settings = get_settings()
    req = body or RestoreDrillTriggerRequest()

    operator = "operator"
    if credentials and credentials.credentials:
        operator = f"token_{credentials.credentials[:6]}..."

    from ..persistence import get_backup_scheduler

    scheduler = getattr(request.app.state, "backup_scheduler", None)
    if scheduler is None:
        scheduler = get_backup_scheduler(store=store, settings=settings)

    res = scheduler.trigger_restore_drill(
        backup_path=req.backup_path,
        actor="api",
        operator=operator,
        reason=req.reason or "Manual API restore drill trigger",
    )

    drill_model: RestoreDrillResultModel | None = None
    if res.get("drill"):
        drill_model = RestoreDrillResultModel(**res["drill"])

    return RestoreDrillTriggerResponse(
        status=res.get("status", "failed"),
        message=res.get("message", "Restore drill execution finished."),
        drill=drill_model,
        duration_ms=res.get("duration_ms", 0.0),
        error=res.get("error"),
        fence_token=res.get("fence_token"),
    )


@router.get(
    "/backups/drill/history",
    response_model=RestoreDrillHistoryResponse,
    summary="List Database Restore Drill History",
    description="Retrieve historical database restore drill verification records.",
    dependencies=[Depends(verify_api_token)],
)
@router.get(
    "/backups/drills/history",
    response_model=RestoreDrillHistoryResponse,
    include_in_schema=False,
    dependencies=[Depends(verify_api_token)],
)
def get_restore_drill_history_endpoint(
    request: Request,
    store: Annotated[CheckpointStore, Depends(get_store)],
    limit: int = Query(50, ge=1, le=500, description="Max drill records to return."),
) -> RestoreDrillHistoryResponse:
    drills = store.list_restore_drills(limit=limit)
    return RestoreDrillHistoryResponse(
        drills=drills,
        count=len(drills),
    )

