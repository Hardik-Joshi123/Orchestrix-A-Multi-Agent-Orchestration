"""Reusable HTTP API client for the Multi-Agent Orchestration System."""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import httpx

from .models import (
    ApprovalSchedulerDiagnostics,
    ApproveToolExecutionResponse,
    AuditCleanupResponse,
    CeleryQueueMetricsResponse,
    CeleryWorkersResponse,
    CheckpointsListResponse,
    DiagnosticsResponse,
    DispatchOutboxResponse,
    EscalateToolApprovalResponse,
    EvaluationSummaryResponse,
    ExpireToolApprovalResponse,
    HealthResponse,
    ListOutboxEventsResponse,
    OutboxDiagnosticsResponse,
    OutboxHistoryResponse,
    ReadyResponse,
    ReconcileToolExecutionResponse,
    RecoverOutboxEventsResponse,
    RecoverToolApprovalResponse,
    RecoverWorkflowsResponse,
    ReplayCleanupResponse,
    ReplayDiagnosticsResponse,
    RetryOutboxEventResponse,
    RevokeToolApprovalResponse,
    RunAuditResponse,
    RunResponse,
    RunToolApprovalsResponse,
    RunToolExecutionsResponse,
    ScanExpiredApprovalsResponse,
    StuckWorkflowsResponse,
    ToolApprovalDiagnosticsResponse,
    ToolApprovalModel,
    ToolExecutionModel,
)

logger = logging.getLogger(__name__)

_SENSITIVE_PATTERNS = re.compile(r"(?i)(password|secret|api[_-]?key|token|bearer|auth|traceback)")


class ApiClientError(Exception):
    """Base exception for all API client errors."""

    def __init__(
        self,
        message: str,
        status_code: int | None = None,
        error_type: str = "ClientError",
        detail: Any = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_type = error_type
        self.detail = detail

    def __str__(self) -> str:
        if self.status_code:
            return f"[{self.status_code} {self.error_type}] {self.message}"
        return f"[{self.error_type}] {self.message}"


class ApiConnectionError(ApiClientError):
    """Raised when the API server is unreachable, connection refused, or timed out."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(message=message, status_code=None, error_type="ConnectionError", detail=detail)


class ApiAuthenticationError(ApiClientError):
    """Raised when authentication credentials are missing or invalid (HTTP 401)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=401,
            error_type="AuthenticationError",
            detail=detail,
        )


class ApiForbiddenError(ApiClientError):
    """Raised when access to a resource is forbidden (HTTP 403)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=403,
            error_type="ForbiddenError",
            detail=detail,
        )



class ApiValidationError(ApiClientError):
    """Raised when client input fails validation (HTTP 400 or 422)."""

    def __init__(self, message: str, status_code: int = 400, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=status_code,
            error_type="ValidationError",
            detail=detail,
        )


class ApiNotFoundError(ApiClientError):
    """Raised when requested run or resource is not found (HTTP 404)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(message=message, status_code=404, error_type="NotFoundError", detail=detail)


class ApiConflictError(ApiClientError):
    """Raised when an operation conflicts with existing workflow state (HTTP 409)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(message=message, status_code=409, error_type="ConflictError", detail=detail)


class ApiRateLimitError(ApiClientError):
    """Raised when request rate limit is exceeded (HTTP 429)."""

    def __init__(self, message: str, retry_after: int | None = None, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=429,
            error_type="RateLimitError",
            detail=detail,
        )
        self.retry_after = retry_after


class ApiCapacityExhaustedError(ApiRateLimitError):
    """Raised when workflow admission is rejected due to active capacity exhaustion (HTTP 429)."""

    def __init__(self, message: str, retry_after: int | None = None, detail: Any = None) -> None:
        super().__init__(message=message, retry_after=retry_after, detail=detail)
        self.error_type = "CapacityExhaustedError"


class ApiPayloadTooLargeError(ApiClientError):
    """Raised when request payload exceeds configured size limit (HTTP 413)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=413,
            error_type="PayloadTooLargeError",
            detail=detail,
        )


class ApiServerError(ApiClientError):
    """Raised when the server encounters an internal error (HTTP 500)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(message=message, status_code=500, error_type="ServerError", detail=detail)


class ApiServiceUnavailableError(ApiClientError):
    """Raised when the server is unavailable or shutting down (HTTP 503)."""

    def __init__(self, message: str, detail: Any = None) -> None:
        super().__init__(
            message=message,
            status_code=503,
            error_type="ServiceUnavailableError",
            detail=detail,
        )


def _sanitize_message(raw_msg: str) -> str:
    """Sanitize error messages to ensure no secrets or stack traces leak to the UI."""
    if not raw_msg:
        return "An error occurred."
    lines = raw_msg.splitlines()
    if any("Traceback (most recent call last)" in line for line in lines):
        return "Internal server execution failed. Check server logs for diagnostics."
    # Redact potential keys and bearer tokens
    redacted = re.sub(r"(sk-[a-zA-Z0-9_\-]{8,})", "[REDACTED_API_KEY]", raw_msg)
    redacted = re.sub(r"(?i)(bearer\s+[a-zA-Z0-9_\-\.]+)", "Bearer [REDACTED_TOKEN]", redacted)
    return redacted


class OrchestratorApiClient:
    """Client for communicating with the Multi-Agent Orchestrator HTTP API."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8000",
        api_token: str | None = None,
        timeout: float = 30.0,
        max_task_chars: int = 8000,
        client: httpx.Client | None = None,
        auth_token: str | None = None,
        token: str | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        resolved_token = (
            api_token
            if api_token is not None
            else (auth_token if auth_token is not None else token)
        )
        self.api_token = (
            resolved_token
            if resolved_token is not None
            else os.environ.get("ORCHESTRATOR_API_TOKEN", "")
        ).strip()
        self.timeout = timeout
        self.max_task_chars = max_task_chars

        resolved_client = client or http_client
        headers = {"Accept": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"

        self._client = resolved_client or httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers=headers,
        )
        self._owns_client = resolved_client is None

    def __repr__(self) -> str:
        token_state = "configured" if self.api_token else "unset"
        return f"<OrchestratorApiClient base_url={self.base_url!r} token={token_state}>"

    def close(self) -> None:
        """Close client sessions and resources."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> OrchestratorApiClient:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    def _handle_response_error(self, response: httpx.Response) -> None:
        """Parse and translate non-2xx HTTP responses into typed ApiClientError."""
        status_code = response.status_code
        detail_msg = ""
        error_payload = None

        try:
            error_payload = response.json()
            if isinstance(error_payload, dict):
                detail_val = error_payload.get("detail")
                if isinstance(detail_val, str):
                    detail_msg = detail_val
                elif isinstance(detail_val, list):
                    # FastAPI validation error list
                    detail_msg = "; ".join(
                        f"{e.get('loc', ['field'])[-1]}: {e.get('msg', 'invalid')}"
                        for e in detail_val
                        if isinstance(e, dict)
                    )
                else:
                    detail_msg = str(detail_val or "")
        except Exception:
            detail_msg = response.text or f"HTTP {status_code}"

        clean_detail = _sanitize_message(detail_msg) or f"Request failed with status {status_code}."

        if status_code == 401:
            clean_msg = clean_detail or "Authentication required. Missing or invalid Bearer token."
            raise ApiAuthenticationError(message=clean_msg, detail=error_payload)
        elif status_code == 403:
            clean_msg = clean_detail or "Access forbidden. Insufficient permissions."
            raise ApiForbiddenError(message=clean_msg, detail=error_payload)
        elif status_code == 429:
            retry_after_hdr = response.headers.get("retry-after")
            retry_after_val: int | None = None
            if retry_after_hdr:
                try:
                    retry_after_val = int(retry_after_hdr)
                except (ValueError, TypeError):
                    retry_after_val = None
            if "capacity" in clean_detail.lower():
                raise ApiCapacityExhaustedError(
                    message=clean_detail,
                    retry_after=retry_after_val,
                    detail=error_payload,
                )
            raise ApiRateLimitError(
                message=clean_detail,
                retry_after=retry_after_val,
                detail=error_payload,
            )
        elif status_code == 413:
            raise ApiPayloadTooLargeError(message=clean_detail, detail=error_payload)
        elif status_code in (400, 422):
            raise ApiValidationError(message=clean_detail, status_code=status_code, detail=error_payload)
        elif status_code == 404:
            raise ApiNotFoundError(message=clean_detail, detail=error_payload)
        elif status_code == 409:
            raise ApiConflictError(message=clean_detail, detail=error_payload)
        elif status_code == 503:
            raise ApiServiceUnavailableError(message=clean_detail, detail=error_payload)
        elif status_code >= 500:
            raise ApiServerError(message=clean_detail, detail=error_payload)
        else:
            raise ApiClientError(
                message=clean_detail,
                status_code=status_code,
                error_type="HttpError",
                detail=error_payload,
            )

    def _request(
        self,
        method: str,
        path: str,
        json: Any = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        raise_for_status: bool = True,
    ) -> httpx.Response:
        """Execute HTTP request with connection error and timeout handling."""
        req_headers = dict(headers or {})
        if self.api_token:
            req_headers["Authorization"] = f"Bearer {self.api_token}"

        # Ensure request has an absolute URL if the underlying client lacks a base_url
        client_has_base = bool(self._client.base_url and str(self._client.base_url).rstrip("/"))
        req_url = path if client_has_base else f"{self.base_url}{path}"

        try:
            resp = self._client.request(
                method=method,
                url=req_url,
                json=json,
                params=params,
                headers=req_headers or None,
            )
        except httpx.ConnectError as exc:
            msg = (
                f"Cannot connect to Orchestrator API at {self.base_url}. "
                "Ensure the API server is running (e.g. 'orchestrator --serve')."
            )
            logger.error("Connection failed to %s%s: %s", self.base_url, path, exc)
            raise ApiConnectionError(message=msg, detail=str(exc)) from exc
        except httpx.TimeoutException as exc:
            msg = f"Request to {self.base_url}{path} timed out after {self.timeout}s."
            logger.error("Timeout connecting to %s%s: %s", self.base_url, path, exc)
            raise ApiConnectionError(message=msg, detail=str(exc)) from exc
        except httpx.RequestError as exc:
            msg = f"Network communication error contacting {self.base_url}: {exc}"
            logger.error("Network error on %s%s: %s", self.base_url, path, exc)
            raise ApiConnectionError(message=msg, detail=str(exc)) from exc

        if raise_for_status and resp.is_error:
            self._handle_response_error(resp)

        return resp

    # -------------------------------------------------------------------------
    # Core API Endpoints
    # -------------------------------------------------------------------------

    def get_health(self) -> HealthResponse:
        """GET /health — Service health and readiness telemetry."""
        resp = self._request("GET", "/health")
        return HealthResponse.model_validate(resp.json())

    def get_readiness(self) -> ReadyResponse:
        """GET /ready — Service readiness probe."""
        resp = self._request("GET", "/ready", raise_for_status=False)
        return ReadyResponse.model_validate(resp.json())

    def is_ready(self) -> bool:
        """Convenience helper checking whether the service is accepting runs."""
        try:
            ready = self.get_readiness()
            return bool(ready.accepting_runs and ready.status == "ready")
        except Exception:
            return False

    def get_diagnostics(self) -> DiagnosticsResponse:
        """GET /diagnostics — Runtime operational diagnostics snapshot."""
        resp = self._request("GET", "/diagnostics")
        return DiagnosticsResponse.model_validate(resp.json())

    def create_run(
        self,
        task: str,
        max_concurrency: int | None = None,
        background: bool = False,
        run_id: str | None = None,
        timeout_seconds: float | None = None,
        idempotency_key: str | None = None,
        execution_backend: str | None = None,
    ) -> RunResponse:
        """POST /runs — Submit and start a new workflow run, optionally with an Idempotency-Key."""
        clean_task = str(task or "").strip()
        if not clean_task:
            raise ApiValidationError("Task instruction must not be empty.", status_code=400)
        if len(clean_task) > self.max_task_chars:
            raise ApiValidationError(
                f"Task exceeds maximum allowed length of {self.max_task_chars} characters.",
                status_code=400,
            )
        if max_concurrency is not None and max_concurrency < 1:
            raise ApiValidationError("max_concurrency must be an integer >= 1.", status_code=400)
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ApiValidationError(
                "timeout_seconds must be a positive number greater than 0.",
                status_code=400,
            )

        req_headers: dict[str, str] = {}
        if idempotency_key is not None:
            clean_key = str(idempotency_key).strip()
            if not clean_key:
                raise ApiValidationError(
                    "Idempotency-Key must not be empty or whitespace.",
                    status_code=400,
                )
            if len(clean_key) > 128:
                raise ApiValidationError(
                    f"Idempotency-Key length ({len(clean_key)}) exceeds maximum limit of 128 characters.",
                    status_code=400,
                )
            if not all(33 <= ord(c) <= 126 for c in clean_key):
                raise ApiValidationError(
                    "Idempotency-Key contains invalid characters. Printable ASCII only.",
                    status_code=400,
                )
            req_headers["Idempotency-Key"] = clean_key

        payload: dict[str, Any] = {
            "task": clean_task,
            "background": background,
        }
        if max_concurrency is not None:
            payload["max_concurrency"] = max_concurrency
        if run_id:
            payload["run_id"] = run_id.strip()
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds
        if execution_backend is not None:
            payload["execution_backend"] = execution_backend

        resp = self._request("POST", "/runs", json=payload, headers=req_headers)
        return RunResponse.model_validate(resp.json())

    def get_run(self, run_id: str) -> RunResponse:
        """GET /runs/{run_id} — Retrieve status, assembled output, and HITL telemetry."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)

        resp = self._request("GET", f"/runs/{clean_id}")
        return RunResponse.model_validate(resp.json())

    def resume_run(
        self,
        run_id: str,
        max_concurrency: int | None = None,
        background: bool = False,
        timeout_seconds: float | None = None,
    ) -> RunResponse:
        """POST /runs/{run_id}/resume — Resume an interrupted workflow execution."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)
        if max_concurrency is not None and max_concurrency < 1:
            raise ApiValidationError("max_concurrency must be an integer >= 1.", status_code=400)
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ApiValidationError(
                "timeout_seconds must be a positive number greater than 0.",
                status_code=400,
            )

        payload: dict[str, Any] = {"background": background}
        if max_concurrency is not None:
            payload["max_concurrency"] = max_concurrency
        if timeout_seconds is not None:
            payload["timeout_seconds"] = timeout_seconds

        resp = self._request("POST", f"/runs/{clean_id}/resume", json=payload)
        return RunResponse.model_validate(resp.json())

    def cancel_run(
        self,
        run_id: str,
        reason: str = "User requested cancellation",
    ) -> RunResponse:
        """POST /runs/{run_id}/cancel — Cooperatively cancel an active run."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)

        payload = {"reason": reason}
        resp = self._request("POST", f"/runs/{clean_id}/cancel", json=payload)
        return RunResponse.model_validate(resp.json())

    def get_checkpoints(self, run_id: str) -> CheckpointsListResponse:
        """GET /runs/{run_id}/checkpoints — Inspect step metadata and transitions."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)

        resp = self._request("GET", f"/runs/{clean_id}/checkpoints")
        return CheckpointsListResponse.model_validate(resp.json())

    def get_run_audit(
        self,
        run_id: str,
        limit: int = 100,
        offset: int = 0,
        event_type: str | None = None,
    ) -> RunAuditResponse:
        """GET /runs/{run_id}/audit — Retrieve structured, paginated audit events."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)
        if limit < 1 or limit > 500:
            raise ApiValidationError("limit must be between 1 and 500.", status_code=400)
        if offset < 0:
            raise ApiValidationError("offset must be non-negative.", status_code=400)

        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if event_type:
            params["event_type"] = event_type.strip()

        resp = self._request("GET", f"/runs/{clean_id}/audit", params=params)
        return RunAuditResponse.model_validate(resp.json())

    def export_run_audit(
        self,
        run_id: str,
        format: str = "json",
        limit: int = 1000,
        offset: int = 0,
        start_time: str | None = None,
        end_time: str | None = None,
        event_type: str | None = None,
    ) -> str:
        """GET /runs/{run_id}/audit/export — Export structured audit events as json, jsonl, or csv string."""
        clean_id = str(run_id or "").strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.", status_code=400)
        fmt = (format or "json").lower().strip()
        if fmt not in ("json", "jsonl", "csv"):
            raise ApiValidationError(
                f"Invalid format '{format}'. Supported formats: json, jsonl, csv.",
                status_code=400,
            )

        params: dict[str, Any] = {"format": fmt, "limit": limit, "offset": offset}
        if start_time:
            params["start_time"] = start_time.strip()
        if end_time:
            params["end_time"] = end_time.strip()
        if event_type:
            params["event_type"] = event_type.strip()

        resp = self._request("GET", f"/runs/{clean_id}/audit/export", params=params)
        return resp.text

    def export_audit(
        self,
        run_id: str | None = None,
        format: str = "json",
        limit: int = 1000,
        offset: int = 0,
        start_time: str | None = None,
        end_time: str | None = None,
        event_type: str | None = None,
    ) -> str:
        """GET /audit/export — Export system or run-filtered audit events as json, jsonl, or csv string."""
        fmt = (format or "json").lower().strip()
        if fmt not in ("json", "jsonl", "csv"):
            raise ApiValidationError(
                f"Invalid format '{format}'. Supported formats: json, jsonl, csv.",
                status_code=400,
            )

        params: dict[str, Any] = {"format": fmt, "limit": limit, "offset": offset}
        if run_id:
            params["run_id"] = run_id.strip()
        if start_time:
            params["start_time"] = start_time.strip()
        if end_time:
            params["end_time"] = end_time.strip()
        if event_type:
            params["event_type"] = event_type.strip()

        resp = self._request("GET", "/audit/export", params=params)
        return resp.text

    def cleanup_audit_events(
        self,
        max_age_days: int | None = None,
        before_timestamp: str | None = None,
        limit: int | None = None,
    ) -> AuditCleanupResponse:
        """POST /audit/cleanup — Purge audit events older than retention cutoff or timestamp."""
        payload: dict[str, Any] = {}
        if max_age_days is not None:
            payload["max_age_days"] = max_age_days
        if before_timestamp is not None:
            payload["before_timestamp"] = before_timestamp
        if limit is not None:
            payload["limit"] = limit

        resp = self._request("POST", "/audit/cleanup", json=payload)
        return AuditCleanupResponse.model_validate(resp.json())

    def get_evaluation(self, tag: str | None = None) -> EvaluationSummaryResponse:
        """GET /evaluation — Retrieve versioned evaluation dataset cases."""
        params = {"tag": tag.strip()} if tag and tag.strip() else None
        resp = self._request("GET", "/evaluation", params=params)
        return EvaluationSummaryResponse.model_validate(resp.json())

    def get_run_tools(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> RunToolExecutionsResponse:
        """GET /runs/{run_id}/tools — Retrieve recorded tool executions with sanitized details."""
        clean_id = run_id.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        params: dict[str, str] = {}
        if status:
            params["status"] = status.strip()
        if tool_name:
            params["tool_name"] = tool_name.strip()
        resp = self._request("GET", f"/runs/{clean_id}/tools", params=params if params else None)
        return RunToolExecutionsResponse.model_validate(resp.json())

    def get_tool_execution(
        self,
        run_id: str,
        operation_key: str,
    ) -> ToolExecutionModel:
        """GET /runs/{run_id}/tools/{operation_key} — Retrieve a specific tool execution."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        resp = self._request("GET", f"/runs/{clean_id}/tools/{clean_key}")
        return ToolExecutionModel.model_validate(resp.json())

    def reconcile_tool_execution(
        self,
        run_id: str,
        operation_key: str,
        outcome: str,
        reason: str,
        result_text: str | None = None,
        error_text: str | None = None,
        operator: str | None = None,
    ) -> ReconcileToolExecutionResponse:
        """POST /runs/{run_id}/tools/{operation_key}/reconcile — Reconcile an uncertain execution."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        if not reason or not reason.strip():
            raise ApiValidationError("reason is required and cannot be empty.")
        payload: dict[str, Any] = {
            "outcome": outcome.strip(),
            "reason": reason.strip(),
        }
        if result_text is not None:
            payload["result_text"] = result_text
        if error_text is not None:
            payload["error_text"] = error_text
        if operator is not None:
            payload["operator"] = operator.strip()

        resp = self._request("POST", f"/runs/{clean_id}/tools/{clean_key}/reconcile", json=payload)
        return ReconcileToolExecutionResponse.model_validate(resp.json())

    def approve_tool_execution(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        decision: str = "approved",
        reason: str = "",
        approver: str | None = None,
        argument_fingerprint: str | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> ApproveToolExecutionResponse:
        """POST /runs/{run_id}/tools/{operation_key}/approve — Record tool approval decision."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        clean_tool = tool_name.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        if not clean_tool:
            raise ApiValidationError("tool_name must not be empty.")
        payload: dict[str, Any] = {
            "tool_name": clean_tool,
            "decision": decision.strip().lower(),
            "reason": reason.strip(),
        }
        if approver is not None:
            payload["approver"] = approver.strip()
        if argument_fingerprint is not None:
            payload["argument_fingerprint"] = argument_fingerprint.strip()
        if arguments is not None:
            payload["arguments"] = arguments

        resp = self._request("POST", f"/runs/{clean_id}/tools/{clean_key}/approve", json=payload)
        return ApproveToolExecutionResponse.model_validate(resp.json())

    def revoke_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        reason: str = "",
        revoked_by: str | None = None,
    ) -> RevokeToolApprovalResponse:
        """POST /runs/{run_id}/tools/{operation_key}/revoke — Revoke tool approval."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        clean_tool = tool_name.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        if not clean_tool:
            raise ApiValidationError("tool_name must not be empty.")
        payload: dict[str, Any] = {
            "tool_name": clean_tool,
            "reason": reason.strip(),
        }
        if revoked_by is not None:
            payload["revoked_by"] = revoked_by.strip()

        resp = self._request("POST", f"/runs/{clean_id}/tools/{clean_key}/revoke", json=payload)
        return RevokeToolApprovalResponse.model_validate(resp.json())

    def get_tool_approval(
        self,
        run_id: str,
        operation_key: str,
    ) -> ToolApprovalModel:
        """GET /runs/{run_id}/tools/{operation_key}/approval — Retrieve approval decision."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")

        resp = self._request("GET", f"/runs/{clean_id}/tools/{clean_key}/approval")
        return ToolApprovalModel.model_validate(resp.json())

    def list_tool_approvals(
        self,
        run_id: str,
        status: str | None = None,
        tool_name: str | None = None,
    ) -> RunToolApprovalsResponse:
        """GET /runs/{run_id}/approvals — List recorded tool approvals with optional filtering."""
        clean_id = run_id.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        params: dict[str, str] = {}
        if status:
            params["status"] = status.strip()
        if tool_name:
            params["tool_name"] = tool_name.strip()
        resp = self._request(
            "GET",
            f"/runs/{clean_id}/approvals",
            params=params if params else None,
        )
        return RunToolApprovalsResponse.model_validate(resp.json())

    def get_tool_approval_diagnostics(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> ToolApprovalDiagnosticsResponse:
        """GET or POST /runs/{run_id}/tools/{operation_key}/diagnostics — Diagnose blocked approval."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")

        if arguments is not None:
            payload: dict[str, Any] = {"arguments": arguments}
            if tool_name:
                payload["tool_name"] = tool_name.strip()
            resp = self._request(
                "POST",
                f"/runs/{clean_id}/tools/{clean_key}/diagnostics",
                json=payload,
            )
        else:
            params: dict[str, str] = {}
            if tool_name:
                params["tool_name"] = tool_name.strip()
            resp = self._request(
                "GET",
                f"/runs/{clean_id}/tools/{clean_key}/diagnostics",
                params=params if params else None,
            )
        return ToolApprovalDiagnosticsResponse.model_validate(resp.json())

    def recover_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str,
        reason: str,
        operator: str | None = None,
        arguments: dict[str, Any] | None = None,
    ) -> RecoverToolApprovalResponse:
        """POST /runs/{run_id}/tools/{operation_key}/recover — Reset blocked approval back to pending."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        clean_tool = tool_name.strip()
        clean_reason = reason.strip() if reason else ""
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        if not clean_tool:
            raise ApiValidationError("tool_name must not be empty.")
        if not clean_reason:
            raise ApiValidationError("reason is required and cannot be empty.")

        payload: dict[str, Any] = {
            "tool_name": clean_tool,
            "reason": clean_reason,
        }
        if operator is not None:
            payload["operator"] = operator.strip()
        if arguments is not None:
            payload["arguments"] = arguments

        resp = self._request(
            "POST",
            f"/runs/{clean_id}/tools/{clean_key}/recover",
            json=payload,
        )
        return RecoverToolApprovalResponse.model_validate(resp.json())

    def expire_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Approval expired by operator",
        actor: str | None = None,
        force: bool = False,
    ) -> ExpireToolApprovalResponse:
        """POST /runs/{run_id}/tools/{operation_key}/expire — Explicitly expire tool approval."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        payload: dict[str, Any] = {
            "reason": reason.strip(),
            "force": force,
        }
        if tool_name is not None:
            payload["tool_name"] = tool_name.strip()
        if actor is not None:
            payload["actor"] = actor.strip()

        resp = self._request(
            "POST",
            f"/runs/{clean_id}/tools/{clean_key}/expire",
            json=payload,
        )
        return ExpireToolApprovalResponse.model_validate(resp.json())

    def escalate_tool_approval(
        self,
        run_id: str,
        operation_key: str,
        tool_name: str | None = None,
        reason: str = "Approval request overdue",
        escalation_level: str = "manager",
        actor: str | None = None,
    ) -> EscalateToolApprovalResponse:
        """POST /runs/{run_id}/tools/{operation_key}/escalate — Escalate overdue pending approval."""
        clean_id = run_id.strip()
        clean_key = operation_key.strip()
        if not clean_id:
            raise ApiValidationError("run_id must not be empty.")
        if not clean_key:
            raise ApiValidationError("operation_key must not be empty.")
        payload: dict[str, Any] = {
            "reason": reason.strip(),
            "escalation_level": escalation_level.strip(),
        }
        if tool_name is not None:
            payload["tool_name"] = tool_name.strip()
        if actor is not None:
            payload["actor"] = actor.strip()

        resp = self._request(
            "POST",
            f"/runs/{clean_id}/tools/{clean_key}/escalate",
            json=payload,
        )
        return EscalateToolApprovalResponse.model_validate(resp.json())

    def scan_expired_approvals(
        self,
        run_id: str | None = None,
    ) -> ScanExpiredApprovalsResponse:
        """POST /runs/{run_id}/approvals/scan-expired or POST /approvals/scan-expired."""
        endpoint = (
            f"/runs/{run_id.strip()}/approvals/scan-expired"
            if run_id and run_id.strip()
            else "/approvals/scan-expired"
        )
        resp = self._request("POST", endpoint)
        return ScanExpiredApprovalsResponse.model_validate(resp.json())

    def get_approval_scheduler_status(self) -> ApprovalSchedulerDiagnostics:
        """GET /approvals/scheduler - inspect background approval lifecycle scheduler status."""
        resp = self._request("GET", "/approvals/scheduler")
        return ApprovalSchedulerDiagnostics.model_validate(resp.json())

    def list_outbox_events(
        self,
        status: str | None = None,
        event_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> ListOutboxEventsResponse:
        """GET /approvals/outbox - list durable outbox events with optional status filtering."""
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if status is not None:
            params["status"] = status.strip()
        if event_type is not None:
            params["event_type"] = event_type.strip()
        resp = self._request("GET", "/approvals/outbox", params=params)
        return ListOutboxEventsResponse.model_validate(resp.json())

    def get_outbox_diagnostics(self) -> OutboxDiagnosticsResponse:
        """GET /approvals/outbox/diagnostics - inspect outbox metrics and dispatcher status."""
        resp = self._request("GET", "/approvals/outbox/diagnostics")
        return OutboxDiagnosticsResponse.model_validate(resp.json())

    def retry_outbox_event(
        self,
        event_id: str,
        reset_attempts: bool = False,
        reason: str | None = None,
        operator: str | None = None,
        force: bool = False,
    ) -> RetryOutboxEventResponse:
        """POST /approvals/outbox/{event_id}/retry - manually reset a failed event to pending."""
        body: dict[str, Any] = {"reset_attempts": reset_attempts, "force": force}
        if reason is not None:
            body["reason"] = reason.strip()
        if operator is not None:
            body["operator"] = operator.strip()
        resp = self._request(
            "POST",
            f"/approvals/outbox/{event_id.strip()}/retry",
            json=body,
        )
        return RetryOutboxEventResponse.model_validate(resp.json())

    def recover_outbox_events(
        self,
        reason: str,
        event_ids: list[str] | None = None,
        status_filter: str | None = "dead_letter",
        operator: str | None = None,
        reset_attempts: bool = True,
        max_events: int = 50,
    ) -> RecoverOutboxEventsResponse:
        """POST /approvals/outbox/recover - batch operator recovery of eligible outbox events."""
        body: dict[str, Any] = {
            "reason": reason.strip(),
            "status_filter": status_filter,
            "reset_attempts": reset_attempts,
            "max_events": max_events,
        }
        if event_ids is not None:
            body["event_ids"] = event_ids
        if operator is not None:
            body["operator"] = operator.strip()
        resp = self._request("POST", "/approvals/outbox/recover", json=body)
        return RecoverOutboxEventsResponse.model_validate(resp.json())

    def get_outbox_event_history(
        self,
        event_id: str,
        limit: int = 50,
    ) -> OutboxHistoryResponse:
        """GET /approvals/outbox/{event_id}/history - inspect delivery attempt history."""
        params = {"limit": limit}
        resp = self._request(
            "GET",
            f"/approvals/outbox/{event_id.strip()}/history",
            params=params,
        )
        return OutboxHistoryResponse.model_validate(resp.json())

    def dispatch_outbox_events(
        self,
        batch_size: int | None = None,
    ) -> DispatchOutboxResponse:
        """POST /approvals/outbox/dispatch - manually trigger a bounded outbox dispatch cycle."""
        params = {}
        if batch_size is not None:
            params["batch_size"] = int(batch_size)
        resp = self._request("POST", "/approvals/outbox/dispatch", params=params)
        return DispatchOutboxResponse.model_validate(resp.json())

    def get_replay_diagnostics(self) -> ReplayDiagnosticsResponse:
        """GET /notifications/replay/diagnostics - inspect webhook replay diagnostics."""
        resp = self._request("GET", "/notifications/replay/diagnostics")
        return ReplayDiagnosticsResponse.model_validate(resp.json())

    def cleanup_replay_records(
        self,
        limit: int | None = None,
        reason: str | None = None,
    ) -> ReplayCleanupResponse:
        """POST /notifications/replay/cleanup - manually prune expired replay records."""
        body: dict[str, Any] = {}
        if limit is not None:
            body["limit"] = int(limit)
        if reason is not None:
            body["reason"] = str(reason).strip()
        resp = self._request("POST", "/notifications/replay/cleanup", json=body)
        return ReplayCleanupResponse.model_validate(resp.json())

    def get_celery_workers(
        self,
        stale_threshold_seconds: float | None = None,
        limit: int | None = None,
    ) -> CeleryWorkersResponse:
        """GET /celery/workers - inspect Celery worker fleet status and heartbeats."""
        params: dict[str, Any] = {}
        if stale_threshold_seconds is not None:
            params["stale_threshold_seconds"] = float(stale_threshold_seconds)
        if limit is not None:
            params["limit"] = int(limit)
        resp = self._request("GET", "/celery/workers", params=params)
        return CeleryWorkersResponse.model_validate(resp.json())

    def get_celery_queue(
        self,
        stale_threshold_seconds: float | None = None,
    ) -> CeleryQueueMetricsResponse:
        """GET /celery/queue - operational queue metrics and fleet summary."""
        params: dict[str, Any] = {}
        if stale_threshold_seconds is not None:
            params["stale_threshold_seconds"] = float(stale_threshold_seconds)
        resp = self._request("GET", "/celery/queue", params=params)
        return CeleryQueueMetricsResponse.model_validate(resp.json())

    def get_celery_stuck(
        self,
        stuck_threshold_seconds: float | None = None,
        stale_worker_threshold_seconds: float | None = None,
        limit: int | None = None,
    ) -> StuckWorkflowsResponse:
        """GET /celery/stuck - list workflows diagnosed as stuck or interrupted."""
        params: dict[str, Any] = {}
        if stuck_threshold_seconds is not None:
            params["stuck_threshold_seconds"] = float(stuck_threshold_seconds)
        if stale_worker_threshold_seconds is not None:
            params["stale_worker_threshold_seconds"] = float(stale_worker_threshold_seconds)
        if limit is not None:
            params["limit"] = int(limit)
        resp = self._request("GET", "/celery/stuck", params=params)
        return StuckWorkflowsResponse.model_validate(resp.json())

    def recover_celery_workflows(
        self,
        run_id: str | None = None,
        run_ids: list[str] | None = None,
        action: str = "requeue",
        reason: str = "Operator manual recovery",
        operator: str | None = None,
    ) -> RecoverWorkflowsResponse:
        """POST /celery/recover - safely recover eligible stuck or interrupted workflows."""
        body: dict[str, Any] = {
            "action": action,
            "reason": reason,
        }
        if run_id is not None:
            body["run_id"] = run_id
        if run_ids is not None:
            body["run_ids"] = run_ids
        if operator is not None:
            body["operator"] = operator
        resp = self._request("POST", "/celery/recover", json=body)
        return RecoverWorkflowsResponse.model_validate(resp.json())


OrchestratorClient = OrchestratorApiClient



