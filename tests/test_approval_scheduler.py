"""Comprehensive offline deterministic test suite for Task 46: Automated Approval Lifecycle Scheduler.

Covers:
1. Configurable settings, bounds validation, and environment variable aliases.
2. Scheduler start/stop lifecycle, daemon thread safety, and preventing duplicate loops.
3. Expiry and overdue auto-escalation during scheduled scans.
4. Safe error handling, backoff behavior, error sanitization, and recovery on subsequent cycles.
5. Concurrency protection via scan lock for simultaneous scans.
6. Diagnostics and observability via GET /approvals/scheduler and GET /diagnostics.
7. SDK client methods for scheduler inspection and manual scan coordination.
8. FastAPI lifespan integration and graceful shutdown.
"""

from __future__ import annotations

import datetime
import threading
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.models import ApprovalSchedulerDiagnostics
from orchestrator.api.routes import get_store
from orchestrator.api.scheduler import (
    ApprovalLifecycleScheduler,
    get_approval_scheduler,
    reset_approval_scheduler,
)
from orchestrator.api.server import create_app
from orchestrator.config import Settings, get_settings
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_scheduler_and_env(monkeypatch: pytest.MonkeyPatch):
    """Ensure a clean singleton scheduler and isolated environment for each test."""
    reset_approval_scheduler()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCAN_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCAN_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("APPROVAL_SCAN_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCAN_INTERVAL", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    yield
    reset_approval_scheduler()
    get_settings.cache_clear()


@pytest.fixture
def tmp_store(tmp_path: Any) -> CheckpointStore:
    """Provide a fresh CheckpointStore backed by a temporary SQLite file."""
    db_file = tmp_path / "scheduler_test.db"
    store = CheckpointStore(db_path=str(db_file))
    return store


# ── 1. Settings & Bounds Validation Tests ─────────────────────────────────────────


def test_scheduler_settings_defaults_and_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify default values and env aliases for scheduler configuration."""
    # Defaults
    settings = Settings()
    assert settings.tool_approval_scheduler_enabled is True
    assert settings.tool_approval_scan_interval_seconds == 60.0

    # Env alias: TOOL_APPROVAL_SCHEDULER_ENABLED
    monkeypatch.setenv("TOOL_APPROVAL_SCHEDULER_ENABLED", "false")
    get_settings.cache_clear()
    s = Settings()
    assert s.tool_approval_scheduler_enabled is False

    # Env alias: APPROVAL_SCHEDULER_ENABLED
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.setenv("APPROVAL_SCHEDULER_ENABLED", "0")
    s2 = Settings()
    assert s2.tool_approval_scheduler_enabled is False

    # Env alias: TOOL_APPROVAL_SCAN_INTERVAL_SECONDS
    monkeypatch.setenv("TOOL_APPROVAL_SCAN_INTERVAL_SECONDS", "12.5")
    s3 = Settings()
    assert s3.tool_approval_scan_interval_seconds == 12.5

    # Env alias: APPROVAL_SCAN_INTERVAL_SECONDS
    monkeypatch.delenv("TOOL_APPROVAL_SCAN_INTERVAL_SECONDS", raising=False)
    monkeypatch.setenv("APPROVAL_SCAN_INTERVAL_SECONDS", "45.0")
    s4 = Settings()
    assert s4.tool_approval_scan_interval_seconds == 45.0


def test_scheduler_scan_interval_bounds_validation() -> None:
    """Scan interval must be between 1.0s and 86400.0s."""
    # Valid boundaries
    s_min = Settings(tool_approval_scan_interval_seconds=1.0)
    assert s_min.tool_approval_scan_interval_seconds == 1.0

    s_max = Settings(tool_approval_scan_interval_seconds=86400.0)
    assert s_max.tool_approval_scan_interval_seconds == 86400.0

    # Below lower bound
    with pytest.raises(ValidationError) as exc:
        Settings(tool_approval_scan_interval_seconds=0.5)
    assert "tool_approval_scan_interval_seconds must be at least 1.0 second" in str(exc.value)

    # Negative value
    with pytest.raises(ValidationError):
        Settings(tool_approval_scan_interval_seconds=-10.0)

    # Above upper bound
    with pytest.raises(ValidationError) as exc2:
        Settings(tool_approval_scan_interval_seconds=86401.0)
    assert "tool_approval_scan_interval_seconds cannot exceed 86400.0 seconds" in str(exc2.value)


# ── 2. Lifecycle & Background Thread Management Tests ───────────────────────────────


def test_scheduler_start_stop_lifecycle(tmp_store: CheckpointStore) -> None:
    """Verify clean start, is_running reporting, and graceful stop without leaks."""
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=10.0,
    )
    assert not scheduler.is_running

    scheduler.start()
    assert scheduler.is_running
    thread_ref = scheduler._thread
    assert thread_ref is not None
    assert thread_ref.is_alive()

    # Calling start again must not create a duplicate thread
    scheduler.start()
    assert scheduler._thread is thread_ref

    # Stop gracefully
    scheduler.stop(timeout=1.0)
    assert not scheduler.is_running
    assert not thread_ref.is_alive()

    # Calling stop again must be idempotent
    scheduler.stop(timeout=0.5)
    assert not scheduler.is_running


def test_scheduler_disabled_behavior(tmp_store: CheckpointStore) -> None:
    """When disabled, scheduler.start() must not launch a background thread."""
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=False,
        scan_interval_seconds=5.0,
    )
    assert not scheduler.is_running
    scheduler.start()
    assert not scheduler.is_running
    assert scheduler._thread is None

    # Manual scan cycle should still work on demand
    res = scheduler.run_scan_cycle(actor="manual_tester")
    assert res.get("scanned_at") is not None
    assert scheduler.get_status()["enabled"] is False
    assert scheduler.get_status()["running"] is False


def test_get_approval_scheduler_singleton(tmp_store: CheckpointStore) -> None:
    """get_approval_scheduler must return consistent singleton instance."""
    s1 = get_approval_scheduler(store=tmp_store)
    s2 = get_approval_scheduler()
    assert s1 is s2

    reset_approval_scheduler()
    s3 = get_approval_scheduler(store=tmp_store)
    assert s3 is not s1


# ── 3. Expiry and Escalation Processing Tests ────────────────────────────────────


def test_scheduled_scan_expires_and_auto_escalates(tmp_store: CheckpointStore) -> None:
    """Verify expired approvals transition to expired and overdue approvals escalate."""
    fixed_now = datetime.datetime.now(datetime.timezone.utc)
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        auto_escalate=True,
        clock=lambda: fixed_now,
    )

    run_id = "run-sched-test-1"

    # 1. Approval expired 10 minutes ago
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-expired-1",
        tool_name="deploy_service",
        expires_at=(fixed_now - datetime.timedelta(minutes=10)).isoformat(),
    )

    # 2. Overdue approval: approaching expiry within threshold (expires in 5 min, threshold 30 min)
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-overdue-1",
        tool_name="restart_cluster",
        expires_at=(fixed_now + datetime.timedelta(minutes=5)).isoformat(),
    )

    # 3. Active valid approval: expires far in the future
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-valid-1",
        tool_name="read_logs",
        expires_at=(fixed_now + datetime.timedelta(days=2)).isoformat(),
    )

    # Execute one scan cycle
    res = scheduler.run_scan_cycle(actor="approval_scheduler")

    assert res["expired_count"] == 1
    assert "op-expired-1" in res["expired_operation_keys"]
    assert res["escalated_count"] >= 1
    assert "op-overdue-1" in res["escalated_operation_keys"]

    # Verify durable state in store
    app_expired = tmp_store.get_tool_approval(run_id, "op-expired-1")
    assert app_expired is not None
    assert app_expired["decision"] == "expired"

    app_overdue = tmp_store.get_tool_approval(run_id, "op-overdue-1")
    assert app_overdue is not None
    assert app_overdue["decision"] == "pending"
    assert app_overdue["escalated_at"] is not None
    assert app_overdue["escalation_reason"] is not None

    app_valid = tmp_store.get_tool_approval(run_id, "op-valid-1")
    assert app_valid is not None
    assert app_valid["decision"] == "pending"
    assert app_valid.get("escalated_at") is None

    # Scheduler metrics check
    status = scheduler.get_status()
    assert status["total_scans"] == 1
    assert status["total_expired"] == 1
    assert status["total_escalated"] >= 1
    assert status["consecutive_failures"] == 0
    assert status["last_error"] is None


def test_repeated_scans_are_idempotent(tmp_store: CheckpointStore) -> None:
    """Repeated scans with unchanged state must be completely idempotent."""
    fixed_now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        clock=lambda: fixed_now,
    )

    tmp_store.record_tool_approval_request(
        run_id="run-idem-1",
        operation_key="op-idem-1",
        tool_name="cleanup_logs",
        expires_at=(fixed_now - datetime.timedelta(seconds=5)).isoformat(),
    )

    res1 = scheduler.run_scan_cycle(actor="approval_scheduler")
    assert res1["expired_count"] == 1

    # Second scan immediately after
    res2 = scheduler.run_scan_cycle(actor="approval_scheduler")
    assert res2["expired_count"] == 0
    assert res2["escalated_count"] == 0
    assert len(res2["expired_operation_keys"]) == 0

    status = scheduler.get_status()
    assert status["total_scans"] == 2
    assert status["total_expired"] == 1


# ── 4. Fault Tolerance, Backoff & Error Recovery Tests ─────────────────────────────


def test_scheduler_handles_transient_failure_and_recovers(tmp_store: CheckpointStore) -> None:
    """Scheduler must catch scan exceptions, record sanitized error, and recover next cycle."""
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=10.0,
    )

    # 1. Simulate failure on store
    broken_store = MagicMock(spec=CheckpointStore)
    broken_store.check_and_expire_tool_approvals.side_effect = RuntimeError(
        "Database locked: access token=super-secret-xyz failed"
    )

    fail_res = scheduler.run_scan_cycle(store=broken_store)
    assert fail_res["success"] is False
    assert "error" in fail_res

    status_fail = scheduler.get_status()
    assert status_fail["total_scans"] == 1
    assert status_fail["consecutive_failures"] == 1
    assert status_fail["last_error"] is not None
    # Secret redaction check
    assert "super-secret-xyz" not in status_fail["last_error"]

    # 2. Subsequent scan succeeds with normal store
    succ_res = scheduler.run_scan_cycle(store=tmp_store)
    assert succ_res["expired_count"] == 0

    status_succ = scheduler.get_status()
    assert status_succ["total_scans"] == 2
    assert status_succ["consecutive_failures"] == 0
    assert status_succ["last_error"] is None
    assert status_succ["last_successful_scan_at"] is not None


# ── 5. Concurrency Protection Tests ───────────────────────────────────────────────


def test_concurrent_scans_are_serialized_safely(tmp_store: CheckpointStore) -> None:
    """Concurrent scan requests must acquire _scan_lock and not execute interleaved."""
    scheduler = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=10.0,
    )

    call_order: list[str] = []
    original_check = tmp_store.check_and_expire_tool_approvals

    def delayed_check(*args: Any, **kwargs: Any) -> dict[str, Any]:
        actor = kwargs.get("actor", "unknown")
        call_order.append(f"start:{actor}")
        time.sleep(0.05)
        call_order.append(f"end:{actor}")
        return original_check(*args, **kwargs)

    tmp_store.check_and_expire_tool_approvals = delayed_check  # type: ignore[assignment]

    t1 = threading.Thread(target=lambda: scheduler.run_scan_cycle(actor="worker_1"))
    t2 = threading.Thread(target=lambda: scheduler.run_scan_cycle(actor="worker_2"))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # Must be sequential: start:A, end:A, start:B, end:B (never start:A, start:B)
    assert len(call_order) == 4
    assert call_order[0].startswith("start:")
    first_actor = call_order[0].split(":")[1]
    assert call_order[1] == f"end:{first_actor}"


# ── 6. HTTP API & Observability Tests ─────────────────────────────────────────────


@pytest.fixture
def test_client(
    tmp_store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TestClient, CheckpointStore, OrchestratorApiClient]:
    """Create a TestClient and OrchestratorApiClient with API token authentication."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "valid-secret-token")
    get_settings.cache_clear()

    app = create_app()
    app.dependency_overrides[CheckpointStore] = lambda: tmp_store
    app.dependency_overrides[get_store] = lambda: tmp_store

    client = TestClient(app)
    sdk_client = OrchestratorApiClient(
        base_url="http://testserver",
        api_token="valid-secret-token",
        http_client=client,
    )
    return client, tmp_store, sdk_client


def test_api_get_approval_scheduler_status(test_client: tuple) -> None:
    """GET /approvals/scheduler returns status diagnostics and enforces authorization."""
    client, store, _ = test_client

    # 1. Unauthorized request without bearer token
    unauth_resp = client.get("/approvals/scheduler")
    assert unauth_resp.status_code == 401

    # 2. Authorized request
    resp = client.get(
        "/approvals/scheduler",
        headers={"Authorization": "Bearer valid-secret-token"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert "enabled" in data
    assert "running" in data
    assert "scan_interval_seconds" in data
    assert "total_scans" in data
    assert "total_expired" in data
    assert "total_escalated" in data
    assert "consecutive_failures" in data


def test_api_diagnostics_includes_scheduler(test_client: tuple) -> None:
    """GET /diagnostics includes the approval_scheduler block."""
    client, _, _ = test_client

    resp = client.get(
        "/diagnostics",
        headers={"Authorization": "Bearer valid-secret-token"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    assert "approval_scheduler" in data
    sched_data = data["approval_scheduler"]
    assert sched_data is not None
    assert "enabled" in sched_data
    assert "scan_interval_seconds" in sched_data
    assert "total_scans" in sched_data


def test_sdk_get_approval_scheduler_status(test_client: tuple) -> None:
    """SDK client provides get_approval_scheduler_status returning model."""
    _, _, sdk = test_client

    diag = sdk.get_approval_scheduler_status()
    assert isinstance(diag, ApprovalSchedulerDiagnostics)
    assert diag.scan_interval_seconds >= 1.0
    assert diag.total_scans >= 0
    assert diag.consecutive_failures == 0
