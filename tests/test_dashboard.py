"""Focused tests for the Orchestrator API Client and Interactive Dashboard helpers."""

from __future__ import annotations

import httpx
import pytest
from ui.api_client import create_ui_client, get_default_api_base_url
from ui.dashboard_helpers import (
    check_resume_eligibility,
    classify_subtasks,
    format_checkpoint_records,
    format_cost_display,
    format_status_badge,
    sanitize_ui_error,
)

from orchestrator.api.client import (
    ApiConflictError,
    ApiConnectionError,
    ApiNotFoundError,
    ApiServerError,
    ApiValidationError,
    OrchestratorApiClient,
    _sanitize_message,
)

# -----------------------------------------------------------------------------
# 1. API Client Mocking Fixtures
# -----------------------------------------------------------------------------


class MockTransport(httpx.BaseTransport):
    """Deterministic mock transport returning custom responses based on path and method."""

    def __init__(self, handler):
        self.handler = handler

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        return self.handler(request)


@pytest.fixture
def mock_client_factory():
    """Create an OrchestratorApiClient backed by a custom request handler."""

    def _factory(handler):
        transport = MockTransport(handler)
        httpx_client = httpx.Client(transport=transport, base_url="http://mock-api.test")
        return OrchestratorApiClient(
            base_url="http://mock-api.test",
            timeout=5.0,
            client=httpx_client,
        )

    return _factory


# -----------------------------------------------------------------------------
# 2. API Client Core Endpoint Tests
# -----------------------------------------------------------------------------


def test_client_get_health(mock_client_factory):
    """GET /health must parse into strongly typed HealthResponse."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/health"
        return httpx.Response(
            status_code=200,
            json={
                "status": "healthy",
                "version": "0.1.0",
                "timestamp": "2026-10-21T12:00:00Z",
                "checkpoint_backend": "sqlite",
                "persistence_enabled": True,
                "active_llm_provider": "mock",
                "active_runs_count": 0,
            },
        )

    client = mock_client_factory(handler)
    health = client.get_health()
    assert health.status == "healthy"
    assert health.version == "0.1.0"
    assert health.active_llm_provider == "mock"
    assert health.active_runs_count == 0


def test_client_create_run_success(mock_client_factory):
    """POST /runs must send payload and parse into RunResponse."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/runs"
        import json

        body = json.loads(request.content.decode("utf-8"))
        assert body["task"] == "Analyze memory and throughput"
        assert body["max_concurrency"] == 2
        assert body["background"] is False

        return httpx.Response(
            status_code=201,
            json={
                "run_id": "run-xyz-123",
                "task": body["task"],
                "status": "completed",
                "answer": "Memory consumption is optimal.",
                "completed_subtasks": ["s1", "s2"],
                "failed_subtasks": [],
                "blocked_subtasks": [],
                "latest_step": 3,
                "review": {"quality": 0.95, "confidence": 0.90},
                "usage": {"total_tokens": 150, "estimated_cost_usd": 0.0003},
            },
        )

    client = mock_client_factory(handler)
    run = client.create_run("Analyze memory and throughput", max_concurrency=2)
    assert run.run_id == "run-xyz-123"
    assert run.status == "completed"
    assert len(run.completed_subtasks) == 2
    assert run.review.quality == 0.95


def test_client_get_run_success(mock_client_factory):
    """GET /runs/{run_id} must retrieve execution state."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/runs/run-test-456"
        return httpx.Response(
            status_code=200,
            json={
                "run_id": "run-test-456",
                "task": "Query indexing analysis",
                "status": "running",
                "completed_subtasks": ["s1"],
                "failed_subtasks": [],
                "blocked_subtasks": [],
                "latest_step": 1,
            },
        )

    client = mock_client_factory(handler)
    run = client.get_run("run-test-456")
    assert run.run_id == "run-test-456"
    assert run.status == "running"
    assert run.completed_subtasks == ["s1"]


def test_client_resume_run_success(mock_client_factory):
    """POST /runs/{run_id}/resume must resume workflow and return updated RunResponse."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/runs/run-resume-789/resume"
        return httpx.Response(
            status_code=200,
            json={
                "run_id": "run-resume-789",
                "task": "Resumed task",
                "status": "completed",
                "completed_subtasks": ["s1", "s2"],
                "failed_subtasks": [],
                "blocked_subtasks": [],
                "latest_step": 4,
            },
        )

    client = mock_client_factory(handler)
    run = client.resume_run("run-resume-789", max_concurrency=3)
    assert run.run_id == "run-resume-789"
    assert run.status == "completed"
    assert run.latest_step == 4


def test_client_get_checkpoints(mock_client_factory):
    """GET /runs/{run_id}/checkpoints must return list of step metadata."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/runs/run-cp-111/checkpoints"
        return httpx.Response(
            status_code=200,
            json={
                "run_id": "run-cp-111",
                "total_steps": 2,
                "checkpoints": [
                    {
                        "step": 1,
                        "transition": "init",
                        "schema_version": 1,
                        "created_at": "2026-10-21T12:00:00Z",
                    },
                    {
                        "step": 2,
                        "transition": "decompose",
                        "schema_version": 1,
                        "created_at": "2026-10-21T12:00:01Z",
                    },
                ],
            },
        )

    client = mock_client_factory(handler)
    cp_resp = client.get_checkpoints("run-cp-111")
    assert cp_resp.run_id == "run-cp-111"
    assert cp_resp.total_steps == 2
    assert len(cp_resp.checkpoints) == 2
    assert cp_resp.checkpoints[0].transition == "init"


def test_client_get_evaluation(mock_client_factory):
    """GET /evaluation must return dataset metadata and cases."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/evaluation"
        if request.url.query.decode() == "tag=concurrency":
            return httpx.Response(
                status_code=200,
                json={
                    "dataset_version": "1.0.0",
                    "total_cases": 1,
                    "cases": [
                        {
                            "case_id": "case_03_concurrent_independent",
                            "name": "Concurrent Independent Subtasks",
                            "description": "3 independent specialist subtasks running in parallel",
                            "tags": ["concurrency", "parallel"],
                            "expected_status": "completed",
                        }
                    ],
                    "latest_summary": None,
                },
            )
        return httpx.Response(
            status_code=200,
            json={
                "dataset_version": "1.0.0",
                "total_cases": 9,
                "cases": [],
                "latest_summary": None,
            },
        )

    client = mock_client_factory(handler)

    # All cases
    eval_all = client.get_evaluation()
    assert eval_all.dataset_version == "1.0.0"
    assert eval_all.total_cases == 9

    # Filtered by tag
    eval_tag = client.get_evaluation(tag="concurrency")
    assert eval_tag.total_cases == 1
    assert eval_tag.cases[0].case_id == "case_03_concurrent_independent"


# -----------------------------------------------------------------------------
# 3. Client Validation & Error Handling Tests
# -----------------------------------------------------------------------------


def test_client_input_validation():
    """Client must perform fast local validation before network calls."""
    client = OrchestratorApiClient(base_url="http://dummy")

    # 1. Empty task
    with pytest.raises(ApiValidationError) as exc1:
        client.create_run("")
    assert "empty" in str(exc1.value).lower()

    # 2. Oversized task
    with pytest.raises(ApiValidationError) as exc2:
        client.create_run("X" * 8005)
    assert "exceeds maximum allowed length" in str(exc2.value).lower()

    # 3. Invalid concurrency < 1
    with pytest.raises(ApiValidationError) as exc3:
        client.create_run("Valid task", max_concurrency=0)
    assert "max_concurrency" in str(exc3.value)

    # 4. Empty run_id on get_run
    with pytest.raises(ApiValidationError) as exc4:
        client.get_run("")
    assert "run_id must not be empty" in str(exc4.value)

    # 5. Empty run_id on resume_run
    with pytest.raises(ApiValidationError) as exc5:
        client.resume_run("   ")
    assert "run_id must not be empty" in str(exc5.value)


def test_client_http_error_translation(mock_client_factory):
    """Non-2xx HTTP responses must map cleanly to specific ApiClientError subclasses."""

    # 400 Validation Error
    c_400 = mock_client_factory(
        lambda r: httpx.Response(status_code=400, json={"detail": "Bad parameter instruction."})
    )
    with pytest.raises(ApiValidationError) as exc_val:
        c_400.create_run("Task instruction")
    assert exc_val.value.status_code == 400
    assert "Bad parameter instruction." in str(exc_val.value)

    # 404 Not Found
    c_404 = mock_client_factory(
        lambda r: httpx.Response(status_code=404, json={"detail": "Run 'unknown-123' not found."})
    )
    with pytest.raises(ApiNotFoundError) as exc_nf:
        c_404.get_run("unknown-123")
    assert exc_nf.value.status_code == 404
    assert "Run 'unknown-123' not found." in str(exc_nf.value)

    # 409 Conflict
    c_409 = mock_client_factory(
        lambda r: httpx.Response(status_code=409, json={"detail": "Run is already executing."})
    )
    with pytest.raises(ApiConflictError) as exc_conflict:
        c_409.resume_run("run-active")
    assert exc_conflict.value.status_code == 409
    assert "Run is already executing." in str(exc_conflict.value)

    # 500 Server Error
    c_500 = mock_client_factory(
        lambda r: httpx.Response(status_code=500, json={"detail": "Database unavailable."})
    )
    with pytest.raises(ApiServerError) as exc_srv:
        c_500.get_health()
    assert exc_srv.value.status_code == 500
    assert "Database unavailable." in str(exc_srv.value)


def test_client_connection_error_and_timeout():
    """Network connection errors and timeouts must raise ApiConnectionError."""

    def connect_fail_transport(request: httpx.Request):
        raise httpx.ConnectError("Connection refused by target machine", request=request)

    client_conn = OrchestratorApiClient(
        base_url="http://offline-host:8000",
        client=httpx.Client(transport=MockTransport(connect_fail_transport)),
    )
    with pytest.raises(ApiConnectionError) as exc_conn:
        client_conn.get_health()
    assert "Cannot connect to Orchestrator API" in str(exc_conn.value)

    def timeout_transport(request: httpx.Request):
        raise httpx.TimeoutException("Read timed out", request=request)

    client_timeout = OrchestratorApiClient(
        base_url="http://slow-host:8000",
        timeout=1.0,
        client=httpx.Client(transport=MockTransport(timeout_transport)),
    )
    with pytest.raises(ApiConnectionError) as exc_timeout:
        client_timeout.get_health()
    assert "timed out after" in str(exc_timeout.value)


# -----------------------------------------------------------------------------
# 4. Message Sanitization & Security Tests
# -----------------------------------------------------------------------------


def test_error_message_sanitization():
    """Error sanitization must strip tracebacks and redact API keys."""
    # Traceback stripping
    raw_tb = (
        "Traceback (most recent call last):\n"
        '  File "builder.py", line 42, in run_task\n'
        "RuntimeError: Failed internal call"
    )
    sanitized_tb = _sanitize_message(raw_tb)
    assert "Traceback" not in sanitized_tb
    assert "Internal server execution failed" in sanitized_tb

    # API key stripping
    raw_key = "Failed request with sk-abcdef1234567890qwertyuiop on OpenAI provider"
    sanitized_key = _sanitize_message(raw_key)
    assert "sk-abcdef1234567890qwertyuiop" not in sanitized_key
    assert "[REDACTED_API_KEY]" in sanitized_key

    # UI sanitizer
    ui_clean = sanitize_ui_error("Error with sk-mysecretkey12345678")
    assert "sk-mysecretkey12345678" not in ui_clean
    assert "[REDACTED]" in ui_clean


# -----------------------------------------------------------------------------
# 5. Dashboard Helpers & Formatting Tests
# -----------------------------------------------------------------------------


def test_format_status_badge():
    """format_status_badge must map intermediate and terminal statuses accurately."""
    cases = [
        ("running", "Running", "⏳", "in_progress"),
        ("completed", "Completed", "✅", "success"),
        ("partial", "Partial Success", "⚠️", "partial"),
        ("failed", "Failed", "❌", "failure"),
        ("blocked", "Blocked", "🚫", "blocked"),
        ("rejected", "Rejected", "⛔", "failure"),
        ("approved", "Completed", "✅", "success"),
        ("edited", "Edited & Approved", "✏️", "success"),
        ("pending", "Pending", "🕒", "pending"),
        ("escalated", "Pending Human Review", "🔔", "escalated"),
        ("unknown_state", "Unknown_state", "ℹ️", "unknown"),
    ]

    for raw, expected_label, expected_icon, expected_cat in cases:
        badge = format_status_badge(raw)
        assert badge["label"] == expected_label
        assert badge["icon"] == expected_icon
        assert badge["category"] == expected_cat


def test_classify_subtasks():
    """classify_subtasks must deduplicate and separate completed, failed, and blocked."""
    data = {
        "completed_subtasks": ["s1", "s2", "s1"],
        "failed_subtasks": ["s3", "s3"],
        "blocked_subtasks": ["s4"],
    }
    grouped = classify_subtasks(data)
    assert grouped["completed"] == ["s1", "s2"]
    assert grouped["failed"] == ["s3"]
    assert grouped["blocked"] == ["s4"]


def test_check_resume_eligibility():
    """check_resume_eligibility must guard against running, completed, and unreviewed runs."""
    # 1. In-flight run -> ineligible
    el_running, reason_running = check_resume_eligibility({"status": "running"})
    assert el_running is False
    assert "in-flight" in reason_running.lower()

    # 2. Fully completed run -> ineligible
    el_done, reason_done = check_resume_eligibility({
        "status": "completed",
        "completed_subtasks": ["s1", "s2"],
        "failed_subtasks": [],
        "blocked_subtasks": [],
    })
    assert el_done is False
    assert "already fully completed" in reason_done.lower()

    # 3. Unresolved escalated run -> ineligible
    el_hitl, reason_hitl = check_resume_eligibility({
        "status": "pending",
        "hitl": {"pending_review": True, "escalated": True, "resolution": None},
    })
    assert el_hitl is False
    assert "awaiting human reviewer resolution" in reason_hitl.lower()

    # 4. Failed/interrupted workflow -> eligible
    el_interrupted, reason_interrupted = check_resume_eligibility({
        "status": "partial",
        "completed_subtasks": ["s1"],
        "failed_subtasks": ["s2"],
        "blocked_subtasks": [],
    })
    assert el_interrupted is True
    assert "eligible for resumption" in reason_interrupted.lower()


def test_format_checkpoint_records():
    """format_checkpoint_records must generate display rows without state dictionaries."""
    checkpoints = [
        {"step": 1, "transition": "init", "schema_version": 1, "created_at": "2026-10-21T10:00:00Z"},
        {
            "step": 2,
            "transition": "specialist_analysis",
            "schema_version": 1,
            "created_at": "2026-10-21T10:00:02Z",
        },
    ]
    records = format_checkpoint_records(checkpoints)
    assert len(records) == 2
    assert records[0]["Step"] == 1
    assert records[0]["Transition"] == "init"
    assert records[0]["Schema"] == "v1"
    assert "state" not in records[0]
    assert "state_json" not in records[0]


def test_format_cost_display():
    """format_cost_display must distinguish confirmed vs estimated rates."""
    # 1. Telemetry absent
    no_usage = format_cost_display(None)
    assert no_usage["available"] is False
    assert no_usage["cost_display"] == "N/A"

    # 2. Confirmed rate
    confirmed = format_cost_display({
        "total_tokens": 1000,
        "total_input_tokens": 700,
        "total_output_tokens": 300,
        "successful_requests": 2,
        "estimated_cost_usd": 0.001500,
        "has_unknown_cost": False,
    })
    assert confirmed["available"] is True
    assert confirmed["is_estimated"] is False
    assert confirmed["cost_display"] == "$0.001500"
    assert "Confirmed" in confirmed["notice"]

    # 3. Estimated / partial rate
    partial = format_cost_display({
        "total_tokens": 2500,
        "total_input_tokens": 2000,
        "total_output_tokens": 500,
        "successful_requests": 3,
        "estimated_cost_usd": 0.002200,
        "has_unknown_cost": True,
    })
    assert partial["available"] is True
    assert partial["is_estimated"] is True
    assert "(Estimated)" in partial["cost_display"]
    assert "Partial/estimated rate" in partial["notice"]

    # 4. Unknown cost (tokens present, cost is None)
    unknown = format_cost_display({
        "total_tokens": 800,
        "total_input_tokens": 600,
        "total_output_tokens": 200,
        "successful_requests": 1,
        "estimated_cost_usd": None,
    })
    assert unknown["available"] is True
    assert unknown["is_estimated"] is True
    assert unknown["cost_display"] == "Unknown"


def test_ui_client_factory():
    """create_ui_client and get_default_api_base_url must return valid client instance."""
    default_url = get_default_api_base_url()
    assert default_url.startswith("http://")

    ui_client = create_ui_client()
    assert isinstance(ui_client, OrchestratorApiClient)
    assert ui_client.base_url == default_url
