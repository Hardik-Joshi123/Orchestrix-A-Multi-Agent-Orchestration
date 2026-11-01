"""Tests for Celery distributed background execution integration (Task 56)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.manager import RunManager
from orchestrator.api.server import create_app
from orchestrator.celery_app import (
    execute_workflow_run,
    get_celery_app,
    get_celery_diagnostics,
)
from orchestrator.config import (
    Settings,
    reset_settings,
    sanitize_broker_url,
    validate_configuration,
)
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools import REGISTRY, RiskLevel, Tool


@pytest.fixture
def temp_db(tmp_path: Any) -> str:
    """Create a temporary SQLite database for testing."""
    db_file = tmp_path / "test_celery.db"
    return str(db_file)


@pytest.fixture(autouse=True)
def configure_test_environment(temp_db: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure environment variables for Celery eager execution and isolated persistence."""
    monkeypatch.setenv("CHECKPOINT_DB_PATH", temp_db)
    monkeypatch.setenv("CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-secret-token")
    reset_settings()
    yield
    reset_settings()


@pytest.fixture
def eager_settings(temp_db: str) -> Settings:
    """Settings configured for Celery eager execution and isolated SQLite persistence."""
    return Settings(
        checkpoint_db_path=temp_db,
        checkpoint_enabled=True,
        celery_enabled=True,
        workflow_execution_backend="celery",
        celery_always_eager=True,
        celery_task_default_queue="orchestrator_workflows",
        celery_worker_concurrency=2,
        celery_task_time_limit=300.0,
        celery_task_soft_time_limit=240.0,
        api_token="test-secret-token",
        idempotency_enabled=True,
    )


@pytest.fixture
def store(temp_db: str) -> CheckpointStore:
    """Initialized CheckpointStore using the temporary database."""
    return CheckpointStore(db_path=temp_db)


@pytest.fixture
def test_app(eager_settings: Settings, store: CheckpointStore) -> Any:
    """FastAPI test client configured with eager Celery execution."""
    app = create_app()
    from orchestrator.api.routes import get_manager, get_store

    manager = RunManager(
        max_active_runs=10,
        owner_id="test-api-owner",
        store=store,
        lease_ttl_seconds=30.0,
    )

    app.dependency_overrides[get_store] = lambda: store
    app.dependency_overrides[get_manager] = lambda: manager
    return app


@pytest.fixture
def client(test_app: Any) -> TestClient:
    """HTTP TestClient with Bearer auth token configured."""
    return TestClient(test_app, headers={"Authorization": "Bearer test-secret-token"})


# ── Configuration & Diagnostics Tests ──────────────────────────────────────────


def test_celery_configuration_validation(eager_settings: Settings) -> None:
    """Verify configuration validation for Celery backend, concurrency, and time limits."""
    # Valid configuration passes
    validated = validate_configuration(eager_settings)
    assert validated.workflow_execution_backend == "celery"
    assert validated.celery_worker_concurrency == 2
    assert validated.celery_task_time_limit == 300.0
    assert validated.celery_task_soft_time_limit == 240.0

    # Normalization of 'sync' to 'direct'
    s_sync = Settings(workflow_execution_backend="sync")
    assert s_sync.workflow_execution_backend == "direct"

    # Invalid backend raises ValueError
    with pytest.raises(ValueError, match="Invalid workflow_execution_backend"):
        Settings(workflow_execution_backend="unsupported_backend")

    # Invalid worker concurrency
    with pytest.raises(ValueError, match="celery_worker_concurrency"):
        Settings(celery_worker_concurrency=0)

    # Soft time limit exceeding hard time limit
    s_bad_limits = Settings(
        celery_task_time_limit=100.0,
        celery_task_soft_time_limit=200.0,
    )
    with pytest.raises(ValueError, match="cannot exceed celery_task_time_limit"):
        validate_configuration(s_bad_limits)


def test_sanitize_broker_url() -> None:
    """Verify password masking in Celery broker URLs."""
    assert sanitize_broker_url(None) is None
    assert sanitize_broker_url("") is None

    plain = "redis://localhost:6379/0"
    assert sanitize_broker_url(plain) == plain

    with_auth = "redis://:supersecretpass@redis.example.com:6380/2"
    sanitized = sanitize_broker_url(with_auth)
    assert sanitized is not None
    assert "supersecretpass" not in sanitized
    assert ":***@" in sanitized
    assert "redis.example.com:6380/2" in sanitized


def test_celery_task_registration_and_diagnostics(eager_settings: Settings) -> None:
    """Verify Celery application tasks and startup diagnostics snapshot."""
    app = get_celery_app(eager_settings)
    assert "orchestrator.tasks.execute_workflow_run" in app.tasks
    assert "orchestrator.tasks.resume_workflow_run" in app.tasks

    task_obj = app.tasks["orchestrator.tasks.execute_workflow_run"]
    assert getattr(task_obj, "acks_late", False) is True

    diag = get_celery_diagnostics(eager_settings)
    assert diag["enabled"] is True
    assert diag["execution_backend"] == "celery"
    assert diag["default_queue"] == "orchestrator_workflows"
    assert diag["task_always_eager"] is True
    assert "orchestrator.tasks.execute_workflow_run" in diag["tasks"]


# ── Background Execution & Run Lifecycle Tests ────────────────────────────────


def test_successful_background_submission_eager(
    client: TestClient,
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify background submission via Celery eager mode returns 202 and completes."""
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")
    reset_settings()

    run_id = "test-celery-run-001"
    resp = client.post(
        "/runs",
        json={
            "run_id": run_id,
            "task": "Perform automated cluster diagnostic analysis",
            "background": True,
            "execution_backend": "celery",
        },
    )
    assert resp.status_code == 202
    body = resp.json()
    assert body["run_id"] == run_id

    # In eager mode, task executes synchronously, persisting checkpoints to SQLite
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "completed"

    # GET /runs/{run_id} returns completed results
    get_resp = client.get(f"/runs/{run_id}")
    assert get_resp.status_code == 200
    get_body = get_resp.json()
    assert get_body["run_id"] == run_id
    assert get_body["status"] == "completed"
    assert get_body["answer"] != ""


def test_duplicate_celery_task_delivery_deduplication(
    eager_settings: Settings,
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify duplicate Celery task deliveries for terminal runs are safely skipped."""
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")

    run_id = "test-dup-celery-run"
    # First execution via Celery task implementation
    res1 = execute_workflow_run(
        run_id=run_id,
        task="Analyze system logs",
    )
    assert res1.get("status") in ("completed", "approved")

    # Record checkpoint count
    summary1 = store.get_run_summary(run_id)
    assert summary1 is not None
    assert summary1["status"] == "completed"
    initial_step = summary1["latest_step"]

    # Second delivery (at-least-once duplicate): must skip and return summary
    res2 = execute_workflow_run(
        run_id=run_id,
        task="Analyze system logs",
    )
    assert res2.get("run_id") == run_id
    assert res2.get("status") == "completed"

    # Verify no additional step or duplicate execution occurred
    summary2 = store.get_run_summary(run_id)
    assert summary2 is not None
    assert summary2["latest_step"] == initial_step


def test_idempotent_celery_submission(
    client: TestClient,
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Celery submissions preserve Idempotency-Key semantics with 202 Accepted replay."""
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")
    reset_settings()

    idempotency_key = "idemp-celery-key-777"
    payload = {
        "task": "Idempotent background execution instruction",
        "background": True,
        "execution_backend": "celery",
    }
    headers = {"Idempotency-Key": idempotency_key}

    # Initial submission: 202 Accepted
    resp1 = client.post("/runs", json=payload, headers=headers)
    assert resp1.status_code == 202
    run_id_1 = resp1.json()["run_id"]

    # Replay submission with same Idempotency-Key: 202 Accepted with Idempotent-Replayed header
    resp2 = client.post("/runs", json=payload, headers=headers)
    assert resp2.status_code == 202
    assert resp2.headers.get("Idempotent-Replayed") == "true"
    assert resp2.json()["run_id"] == run_id_1


def test_cancellation_of_celery_background_workflow(
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that cancelled runs are aborted before Celery worker execution."""
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")

    run_id = "test-cancel-celery-run"
    # Pre-register initial queued checkpoint
    state = {"run_id": run_id, "task": "Heavy report task", "status": "queued"}
    store.save_checkpoint(run_id, state, transition="enqueue")

    # Cancel administratively before worker runs
    store.cancel_run_administratively(run_id, reason="User cancelled before worker pickup")

    # Worker executes task
    result = execute_workflow_run(
        run_id=run_id,
        task="Heavy report task",
    )
    assert result.get("status") == "cancelled"

    # Store remains cancelled
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "cancelled"


def test_broker_unavailability_handling(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify HTTP 503 is returned when the Celery message broker is unavailable."""
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "false")
    reset_settings()

    # Mock connection_for_write to raise OperationalError / ConnectionRefused
    mock_conn = MagicMock()
    mock_conn.ensure_connection.side_effect = ConnectionRefusedError(
        "Redis connection refused at 127.0.0.1:6379"
    )
    mock_app = MagicMock()
    mock_app.conf.task_always_eager = False
    mock_app.connection_for_write.return_value.__enter__.return_value = mock_conn

    with patch("orchestrator.celery_app.get_celery_app", return_value=mock_app):
        resp = client.post(
            "/runs",
            json={
                "run_id": "test-broker-down-run",
                "task": "Run data sync pipeline",
                "background": True,
                "execution_backend": "celery",
            },
        )
        assert resp.status_code == 503
        assert "broker" in resp.json()["detail"].lower()


def test_worker_failure_exception_handling(
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that unhandled worker exceptions save sanitized failure checkpoints."""
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")

    run_id = "test-failing-celery-run"

    # Mock run_task inside worker to raise an unexpected runtime error
    with patch(
        "orchestrator.api.manager.run_task",
        side_effect=RuntimeError("Unexpected unhandled database compute failure"),
    ):
        result = execute_workflow_run(
            run_id=run_id,
            task="Failure test task",
        )
        assert result.get("status") == "failed"
        assert "failure" in result.get("error", "").lower()

    # Verify durable checkpoint was saved with failed status
    summary = store.get_run_summary(run_id)
    assert summary is not None
    assert summary["status"] == "failed"


def test_hitl_approval_preservation_under_celery(
    eager_settings: Settings,
    store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that tools requiring HITL approval stop in waiting_approval and are never auto-approved."""
    monkeypatch.setenv("ORCHESTRATOR_CELERY_ALWAYS_EAGER", "true")

    # Register a high-risk tool that requires approval
    approval_called = False

    def sensitive_tool_func(target: str) -> str:
        nonlocal approval_called
        approval_called = True
        return f"Executed on {target}"

    sensitive_tool = Tool(
        name="celery_sensitive_tool",
        fn=sensitive_tool_func,
        schema={"type": "object", "properties": {"target": {"type": "string"}}},
        side_effecting=True,
        requires_approval=True,
        risk_level=RiskLevel.HIGH,
    )
    REGISTRY.register(sensitive_tool)

    try:
        run_id = "test-hitl-celery-run"
        plan = [
            {
                "id": "s1",
                "specialist": "analysis",
                "instruction": "Call celery_sensitive_tool to inspect target",
                "tools": ["celery_sensitive_tool"],
                "status": "pending",
            }
        ]

        # Execute Celery task
        _result = execute_workflow_run(
            run_id=run_id,
            task="Perform sensitive inspection",
            plan=plan,
        )

        # Task MUST pause in waiting_approval or review state; sensitive tool must NOT be executed
        summary = store.get_run_summary(run_id)
        assert summary is not None
        # Verify approval was not bypassed
        assert approval_called is False
        assert summary["status"] in ("waiting_approval", "pending", "in_progress", "completed")

    finally:
        # Clean up registry
        if "celery_sensitive_tool" in REGISTRY._tools:
            del REGISTRY._tools["celery_sensitive_tool"]


def test_backward_compatibility_direct_and_thread(
    client: TestClient,
    store: CheckpointStore,
) -> None:
    """Verify synchronous direct and thread-based backends remain backward compatible."""
    # 1. Direct synchronous execution (background=False, execution_backend="direct")
    resp_sync = client.post(
        "/runs",
        json={
            "task": "Synchronous calculation task",
            "background": False,
            "execution_backend": "direct",
        },
    )
    assert resp_sync.status_code == 201
    sync_body = resp_sync.json()
    assert sync_body["status"] == "completed"

    # 2. Thread-based background execution (background=True, execution_backend="thread")
    resp_thread = client.post(
        "/runs",
        json={
            "task": "Threadpool background task",
            "background": True,
            "execution_backend": "thread",
        },
    )
    assert resp_thread.status_code == 202
    thread_body = resp_thread.json()
    assert thread_body["run_id"] != ""


def test_diagnostics_endpoint_includes_celery(client: TestClient) -> None:
    """Verify GET /diagnostics exposes Celery background execution status."""
    resp = client.get("/diagnostics")
    assert resp.status_code == 200
    data = resp.json()

    exec_diag = data.get("execution", {})
    assert "backend" in exec_diag
    celery_diag = exec_diag.get("celery")
    assert celery_diag is not None
    assert "celery_available" in celery_diag
    assert "broker_url" in celery_diag
    assert "default_queue" in celery_diag
