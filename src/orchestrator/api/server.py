"""FastAPI application factory and HTTP server launcher."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from .. import __version__
from ..config import (
    format_diagnostics_summary,
    get_settings,
    is_loopback_host,
    validate_configuration,
)
from ..persistence.store import CheckpointStore
from .manager import get_run_manager
from .middleware import RequestBodyLimitMiddleware
from .routes import router

logger = logging.getLogger(__name__)


def validate_network_binding(host: str | None, token: str | None = None) -> None:
    """Ensure non-loopback network binding requires a configured API token."""
    clean_host = (host or "127.0.0.1").strip().lower()
    clean_token = (
        token
        if token is not None
        else (os.environ.get("ORCHESTRATOR_API_TOKEN") or get_settings().api_token or "")
    ).strip()

    if not is_loopback_host(clean_host) and not clean_token:
        raise ValueError(
            f"Unsafe network binding rejected: Cannot bind API server to non-loopback host '{host}' "
            "without authentication. Configure ORCHESTRATOR_API_TOKEN or bind to 127.0.0.1."
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI application lifespan manager for startup recovery and graceful shutdown."""
    settings = get_settings()
    manager = get_run_manager()
    store = CheckpointStore()

    # Startup recovery: identify and recover runs interrupted during a previous process run
    if store.enabled and getattr(settings, "api_recovery_on_startup", True):
        try:
            recovered = manager.recover_interrupted_runs(store=store)
            if recovered:
                logger.info(
                    "Startup recovery identified and marked %d interrupted run(s): %s",
                    len(recovered),
                    recovered,
                )
        except Exception as exc:
            logger.warning("Startup interrupted-run recovery encountered an error: %s", exc)

    # Startup audit retention cleanup: ONLY executed if explicitly configured
    if store.enabled and getattr(settings, "audit_cleanup_on_startup", False):
        try:
            purge_res = store.purge_audit_events(max_age_days=settings.audit_retention_days)
            if purge_res.get("deleted_count", 0) > 0:
                logger.info(
                    "Startup audit cleanup purged %d expired event(s) (cutoff: %s)",
                    purge_res["deleted_count"],
                    purge_res.get("cutoff_timestamp"),
                )
        except Exception as exc:
            logger.warning("Startup audit cleanup encountered an error: %s", exc)

    # Startup idempotency key recovery: recover any lingering in-progress records
    if store.enabled and getattr(settings, "idempotency_enabled", True):
        try:
            recovered_idemp = store.recover_in_progress_idempotency_keys()
            if recovered_idemp > 0:
                logger.info(
                    "Startup recovery processed %d in-progress idempotency key(s)",
                    recovered_idemp,
                )
        except Exception as exc:
            logger.warning("Startup idempotency recovery encountered an error: %s", exc)

    try:
        from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

        record_audit_event(
            event_type=AuditEventType.SERVICE_STARTUP,
            outcome=AuditOutcome.SUCCESS,
            actor="system",
            metadata={
                "version": __version__,
                "recovery_on_startup": getattr(settings, "api_recovery_on_startup", True),
            },
            store=store,
        )
    except Exception:
        pass

    # Start automated approval lifecycle scheduler if enabled and persistence is active
    from .scheduler import get_approval_scheduler

    scheduler = get_approval_scheduler(store=store)
    if store.enabled and scheduler.enabled:
        try:
            scheduler.start()
        except Exception as exc:
            logger.warning("Failed to start automated approval lifecycle scheduler: %s", exc)

    app.state.approval_scheduler = scheduler

    # Start transactional outbox dispatcher if enabled and persistence is active
    from ..notifications import get_outbox_dispatcher

    outbox_dispatcher = get_outbox_dispatcher(store=store, settings=settings)
    if store.enabled and outbox_dispatcher.enabled:
        try:
            outbox_dispatcher.start()
        except Exception as exc:
            logger.warning("Failed to start transactional outbox dispatcher: %s", exc)

    app.state.outbox_dispatcher = outbox_dispatcher

    # Start automated replay cleanup scheduler if enabled and persistence is active
    from ..notifications import get_replay_cleanup_scheduler

    replay_scheduler = get_replay_cleanup_scheduler(store=store, settings=settings)
    if store.enabled and replay_scheduler.enabled:
        try:
            replay_scheduler.start()
        except Exception as exc:
            logger.warning("Failed to start automated replay cleanup scheduler: %s", exc)

    app.state.replay_cleanup_scheduler = replay_scheduler

    # Start automated database backup scheduler if enabled and persistence is active
    from ..persistence import get_backup_scheduler

    backup_scheduler = get_backup_scheduler(store=store, settings=settings)
    if store.enabled and backup_scheduler.enabled:
        try:
            backup_scheduler.start()
        except Exception as exc:
            logger.warning("Failed to start automated database backup scheduler: %s", exc)

    app.state.backup_scheduler = backup_scheduler

    yield

    # Graceful shutdown: stop schedulers within bounded time
    grace = getattr(settings, "api_shutdown_grace_seconds", 5.0)
    try:
        backup_scheduler.stop(timeout=min(2.0, grace))
    except Exception as exc:
        logger.warning("Error stopping database backup scheduler: %s", exc)

    try:
        replay_scheduler.stop(timeout=min(2.0, grace))
    except Exception as exc:
        logger.warning("Error stopping replay cleanup scheduler: %s", exc)

    try:
        scheduler.stop(timeout=min(2.0, grace))
    except Exception as exc:
        logger.warning("Error stopping approval lifecycle scheduler: %s", exc)

    try:
        outbox_dispatcher.stop(timeout=min(2.0, grace))
    except Exception as exc:
        logger.warning("Error stopping transactional outbox dispatcher: %s", exc)

    # Graceful shutdown: signal active runs cooperatively and wait bounded grace period
    logger.info("Initiating graceful server shutdown (grace period: %.1fs)...", grace)
    try:
        manager.shutdown(grace_period=grace, store=store)
        logger.info("Graceful server shutdown completed successfully.")
    except Exception as exc:
        logger.warning("Error during graceful server shutdown: %s", exc)

    try:
        from ..observability.audit import AuditEventType, AuditOutcome, record_audit_event

        record_audit_event(
            event_type=AuditEventType.SERVICE_SHUTDOWN,
            outcome=AuditOutcome.SUCCESS,
            actor="system",
            metadata={"grace_seconds": grace},
            store=store,
        )
    except Exception:
        pass


def create_app() -> FastAPI:
    """Instantiate and configure the FastAPI application."""
    app = FastAPI(
        title="Multi-Agent Orchestrator API",
        version=__version__,
        description=(
            "Production-ready HTTP API for the multi-agent orchestration engine, "
            "supporting persistent workflow state, bounded specialist concurrency, and HITL."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        lifespan=lifespan,
    )

    # Controlled exception handlers preventing secret/traceback leakage
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        errors = exc.errors()
        error_msgs = []
        for err in errors:
            loc = " -> ".join(str(item) for item in err.get("loc", []))
            msg = err.get("msg", "Invalid value")
            error_msgs.append(f"{loc}: {msg}")
        detail = "; ".join(error_msgs) if error_msgs else "Invalid request payload."
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": detail},
        )

    @app.exception_handler(HTTPException)
    async def http_exception_handler(
        request: Request,
        exc: HTTPException,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": str(exc.detail)},
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def general_exception_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        logger.exception("Unhandled server exception during request %s: %s", request.url.path, exc)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "An internal server error occurred."},
        )

    # Mount core API routes
    app.include_router(router)

    # Configure restrictive CORS middleware
    settings = get_settings()
    configured_cors = (
        os.environ.get("ORCHESTRATOR_CORS_ORIGINS")
        or settings.api_cors_origins
        or "http://localhost:8501,http://127.0.0.1:8501"
    )
    allowed_origins = [o.strip() for o in configured_cors.split(",") if o.strip()]

    # Request body size enforcement (protect against oversized Content-Length and chunked bodies)
    app.add_middleware(RequestBodyLimitMiddleware)

    # Restrictive CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "Accept"],
    )

    return app


def run_server(
    host: str | None = None,
    port: int | None = None,
    **uvicorn_kwargs: Any,
) -> None:
    """Launch the uvicorn ASGI server with project configuration defaults."""
    import uvicorn

    settings = get_settings()
    target_host = (host or settings.api_host or "127.0.0.1").strip()
    target_port = port if port is not None else settings.api_port

    if not target_host:
        raise ValueError("Invalid API host: host cannot be empty.")

    try:
        target_port_int = int(target_port)
        if not (1 <= target_port_int <= 65535):
            raise ValueError(f"Invalid API port: {target_port}. Port must be between 1 and 65535.")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Invalid API port: {target_port}. Port must be between 1 and 65535.") from exc

    validate_network_binding(target_host)
    validate_configuration(settings)

    logger.info("Starting Orchestrator API server on %s:%s", target_host, target_port_int)
    logger.info("Startup diagnostics:\n%s", format_diagnostics_summary(settings))
    app = create_app()
    uvicorn.run(
        app,
        host=target_host,
        port=target_port_int,
        **uvicorn_kwargs,
    )
