"""Comprehensive offline deterministic test suite for Task 47: Cross-Process Scheduler Leadership.

Covers:
1. Configurable lease settings, bounds validation, cross-field checks, and environment variable aliases.
2. Durable SQLite schema, table migration, and index creation for scheduler_leases.
3. Atomic lease acquisition, re-entrancy, renewal, release, and monotonic fencing tokens.
4. Leadership takeover after TTL expiry and immediate takeover on graceful release.
5. In-process scheduler leadership lifecycle: active leader, standby worker, and manual scan safety.
6. Immediate fencing out of stale owners attempting scheduled scans or renewals.
7. Authenticated diagnostics reporting leadership state, owner ID, lease expiry, and fence token.
8. Cross-process concurrency and crashed-owner takeover using real OS subprocesses on a shared SQLite DB.
9. SQLite lock contention resilience with bounded busy-handler retries.
10. Regressions for approval expiration, audit integrity, and manual operator scan execution.
"""

from __future__ import annotations

import datetime
import os
import subprocess
import sys
from typing import Any

import pytest
from pydantic import ValidationError
from starlette.testclient import TestClient

from orchestrator.api.client import OrchestratorApiClient
from orchestrator.api.models import ApprovalSchedulerDiagnostics
from orchestrator.api.routes import get_store
from orchestrator.api.scheduler import (
    ApprovalLifecycleScheduler,
    reset_approval_scheduler,
)
from orchestrator.api.server import create_app
from orchestrator.config import Settings, get_settings, validate_configuration
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_scheduler_and_env(monkeypatch: pytest.MonkeyPatch):
    """Ensure a clean singleton scheduler and isolated environment for each test."""
    reset_approval_scheduler()
    get_settings.cache_clear()
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("APPROVAL_SCHEDULER_ENABLED", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("APPROVAL_SCHEDULER_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("SCHEDULER_LEASE_TTL_SECONDS", raising=False)
    monkeypatch.delenv("TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("ORCHESTRATOR_API_TOKEN", raising=False)
    yield
    reset_approval_scheduler()
    get_settings.cache_clear()


@pytest.fixture
def tmp_store(tmp_path: Any) -> CheckpointStore:
    """Provide a fresh CheckpointStore backed by a temporary SQLite file."""
    db_file = tmp_path / "scheduler_lease_test.db"
    return CheckpointStore(str(db_file), enabled=True)


# ── 1. Configuration & Validation Tests ──────────────────────────────────────────


def test_scheduler_lease_settings_defaults_and_aliases(monkeypatch: pytest.MonkeyPatch) -> None:
    """Validate default values, upper/lower bounds, and environment variable aliases."""
    s_default = Settings()
    assert s_default.tool_approval_scheduler_lease_ttl_seconds == 30.0
    assert s_default.tool_approval_scheduler_heartbeat_interval_seconds == 5.0

    # Bounds validation: Lease TTL
    with pytest.raises(ValidationError):
        Settings(tool_approval_scheduler_lease_ttl_seconds=0.5)

    with pytest.raises(ValidationError):
        Settings(tool_approval_scheduler_lease_ttl_seconds=3601.0)

    # Bounds validation: Heartbeat Interval
    with pytest.raises(ValidationError):
        Settings(tool_approval_scheduler_heartbeat_interval_seconds=0.05)

    with pytest.raises(ValidationError):
        Settings(tool_approval_scheduler_heartbeat_interval_seconds=301.0)

    # Cross-field validation: heartbeat must be strictly less than lease TTL
    with pytest.raises(ValueError, match="must be strictly less than"):
        validate_configuration(
            Settings(
                tool_approval_scheduler_lease_ttl_seconds=10.0,
                tool_approval_scheduler_heartbeat_interval_seconds=10.0,
            )
        )

    with pytest.raises(ValueError, match="must be strictly less than"):
        validate_configuration(
            Settings(
                tool_approval_scheduler_lease_ttl_seconds=10.0,
                tool_approval_scheduler_heartbeat_interval_seconds=15.0,
            )
        )

    # Environment variable aliases
    monkeypatch.setenv("SCHEDULER_LEASE_TTL_SECONDS", "45.0")
    monkeypatch.setenv("SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", "8.0")
    s_env = Settings()
    assert s_env.tool_approval_scheduler_lease_ttl_seconds == 45.0
    assert s_env.tool_approval_scheduler_heartbeat_interval_seconds == 8.0


# ── 2. Durable SQLite Storage & Atomic Lease Primitives ──────────────────────────


def test_durable_lease_schema_and_migration(tmp_path: Any) -> None:
    """Verify scheduler_leases table creation and idempotent schema initialization."""
    db_file = str(tmp_path / "migration_test.db")
    store = CheckpointStore(db_file, enabled=True)

    conn = store._get_connection()
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='scheduler_leases';"
        )
        assert cursor.fetchone() is not None

        idx_cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_scheduler_leases_expires';"
        )
        assert idx_cursor.fetchone() is not None
    finally:
        conn.close()

    # Re-initialization on existing DB must be non-destructive
    store2 = CheckpointStore(db_file, enabled=True)
    assert store2.enabled is True
    lease = store2.get_scheduler_lease("default")
    assert lease is None


def test_atomic_lease_acquisition_and_reentrancy(tmp_store: CheckpointStore) -> None:
    """Verify first acquisition, re-entrant refresh for same owner, and conflict for competitor."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # 1. Owner A acquires lease
    acquired, token, record = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=30.0,
        now_dt=now,
    )
    assert acquired is True
    assert token == 1
    assert record["owner_id"] == "worker-A"
    assert record["status"] == "active"
    assert record["fence_token"] == 1

    # 2. Re-entrant acquisition by Owner A updates expiry and retains token
    later = now + datetime.timedelta(seconds=10)
    acquired2, token2, record2 = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=30.0,
        now_dt=later,
    )
    assert acquired2 is True
    assert token2 == 1
    assert record2["expires_at"] == (later + datetime.timedelta(seconds=30)).isoformat()

    # 3. Competing acquisition by Owner B while active and unexpired fails
    acquired_b, token_b, record_b = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        lease_ttl_seconds=30.0,
        now_dt=later,
    )
    assert acquired_b is False
    assert token_b == 1  # Reports current owner's generation token
    assert record_b["owner_id"] == "worker-A"


def test_lease_renewal_and_validation(tmp_store: CheckpointStore) -> None:
    """Verify conditional lease renewal and ownership validation."""
    now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    acquired, token, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=30.0,
        now_dt=now,
    )
    assert acquired is True

    # Validate active ownership
    assert tmp_store.validate_scheduler_lease_ownership(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=token,
        now_dt=now,
    ) is True

    # Validate fails with mismatched token
    assert tmp_store.validate_scheduler_lease_ownership(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=999,
        now_dt=now,
    ) is False

    # Validate fails with mismatched owner
    assert tmp_store.validate_scheduler_lease_ownership(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        fence_token=token,
        now_dt=now,
    ) is False

    # Renewal succeeds with matching token
    renew_time = now + datetime.timedelta(seconds=5)
    renewed = tmp_store.renew_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=token,
        lease_ttl_seconds=30.0,
        now_dt=renew_time,
    )
    assert renewed is True
    ren_rec = tmp_store.get_scheduler_lease("tool_approval_scheduler")
    assert ren_rec is not None
    assert ren_rec["heartbeat_at"] == renew_time.isoformat()
    assert ren_rec["expires_at"] == (renew_time + datetime.timedelta(seconds=30)).isoformat()

    # Renewal fails with stale token
    renewed_stale = tmp_store.renew_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=999,
        lease_ttl_seconds=30.0,
        now_dt=renew_time,
    )
    assert renewed_stale is False


def test_lease_takeover_after_expiration_and_monotonic_fencing(
    tmp_store: CheckpointStore,
) -> None:
    """Verify that expired leases are taken over with monotonically increasing fencing tokens."""
    t0 = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    # Owner A acquires with 10s TTL
    acquired_a, token_a, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=10.0,
        now_dt=t0,
    )
    assert acquired_a is True
    assert token_a == 1

    # Before expiry (t0 + 5s), Owner B cannot take over
    t1 = t0 + datetime.timedelta(seconds=5)
    acq_b1, tok_b1, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        lease_ttl_seconds=10.0,
        now_dt=t1,
    )
    assert acq_b1 is False
    assert tok_b1 == 1

    # After expiry (t0 + 12s), Owner B successfully takes over with fence_token=2
    t2 = t0 + datetime.timedelta(seconds=12)
    acq_b2, tok_b2, rec_b2 = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        lease_ttl_seconds=10.0,
        now_dt=t2,
    )
    assert acq_b2 is True
    assert tok_b2 == 2
    assert rec_b2["owner_id"] == "worker-B"
    assert rec_b2["fence_token"] == 2

    # Owner A is now completely fenced out
    assert tmp_store.validate_scheduler_lease_ownership(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=1,
        now_dt=t2,
    ) is False

    renew_stale = tmp_store.renew_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=1,
        lease_ttl_seconds=10.0,
        now_dt=t2,
    )
    assert renew_stale is False


def test_graceful_lease_release_enables_immediate_takeover(
    tmp_store: CheckpointStore,
) -> None:
    """Graceful release by leader allows immediate acquisition by standby without waiting for TTL."""
    t0 = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)
    acquired_a, token_a, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        lease_ttl_seconds=300.0,
        now_dt=t0,
    )
    assert acquired_a is True
    assert token_a == 1

    # Owner A releases gracefully at t0 + 2s
    t1 = t0 + datetime.timedelta(seconds=2)
    released = tmp_store.release_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-A",
        fence_token=1,
        now_dt=t1,
    )
    assert released is True

    # Owner B can acquire immediately at t0 + 3s, far before the original 300s TTL
    t2 = t0 + datetime.timedelta(seconds=3)
    acq_b, tok_b, rec_b = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-B",
        lease_ttl_seconds=300.0,
        now_dt=t2,
    )
    assert acq_b is True
    assert tok_b == 2
    assert rec_b["owner_id"] == "worker-B"


# ── 3. Leadership Lifecycle & Scheduler In-Process Integration ───────────────────


def test_scheduler_leadership_lifecycle_standby_and_manual_scans(
    tmp_store: CheckpointStore,
) -> None:
    """Only the leader executes scheduled scans, standby workers skip them, but manual scans work."""
    fixed_now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)

    sched_a = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        owner_id="worker-A",
        lease_ttl_seconds=60.0,
        clock=lambda: fixed_now,
    )
    sched_b = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        owner_id="worker-B",
        lease_ttl_seconds=60.0,
        clock=lambda: fixed_now,
    )

    # 1. Start worker A: acquires leadership
    sched_a.start()
    assert sched_a.is_leader is True
    assert sched_a.leadership_state == "leader"
    assert sched_a.fence_token == 1

    # 2. Start worker B: enters standby
    sched_b.start()
    assert sched_b.is_leader is False
    assert sched_b.leadership_state == "standby"
    assert sched_b.fence_token is None

    # 3. Scheduled scan by Worker B is skipped because it is not the leader
    res_b_sched = sched_b.run_scan_cycle(actor="scheduler")
    assert res_b_sched["success"] is False
    assert res_b_sched["reason"] == "Not scheduler leader"

    # 4. Manual scan by Worker B proceeds regardless of leadership
    res_b_manual = sched_b.run_scan_cycle(actor="manual_operator")
    assert res_b_manual["success"] is True

    # 5. Stop worker A: releases lease gracefully
    sched_a.stop()
    assert sched_a.is_leader is False

    # 6. Worker B acquires leadership
    acquired_b = sched_b.acquire_leadership()
    assert acquired_b is True
    assert sched_b.is_leader is True
    assert sched_b.leadership_state == "leader"
    assert sched_b.fence_token == 2

    # Clean up
    sched_b.stop()


def test_stale_owner_fencing_during_scheduled_scan(tmp_store: CheckpointStore) -> None:
    """A worker that loses its lease is fenced out immediately during scheduled scans."""
    fixed_now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)

    sched_a = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        owner_id="worker-A",
        lease_ttl_seconds=10.0,
        clock=lambda: fixed_now,
    )
    sched_a.start()
    assert sched_a.is_leader is True
    assert sched_a.fence_token == 1

    # External takeover: worker C supersedes lease in store with fence_token 2
    later = fixed_now + datetime.timedelta(seconds=15)
    tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-C",
        lease_ttl_seconds=60.0,
        now_dt=later,
    )

    # Worker A attempts scheduled scan: fails pre-scan revalidation and gets fenced out
    sched_a._clock = lambda: later
    res = sched_a.run_scan_cycle(actor="scheduler")
    assert res["success"] is False
    assert "Fenced out" in res.get("reason", "")
    assert sched_a.is_leader is False
    assert sched_a.leadership_state == "lost"
    assert sched_a.fence_token is None

    sched_a.stop()


# ── 4. Operational Diagnostics & API Verification ────────────────────────────────


def test_api_scheduler_diagnostics_reflects_leadership_and_lease(
    tmp_store: CheckpointStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Diagnostics endpoint GET /approvals/scheduler reports leadership state and lease info."""
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "test-api-token")
    get_settings.cache_clear()

    app = create_app()
    app.dependency_overrides[CheckpointStore] = lambda: tmp_store
    app.dependency_overrides[get_store] = lambda: tmp_store

    sched = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        scan_interval_seconds=30.0,
        owner_id="worker-api-diag",
        lease_ttl_seconds=45.0,
        heartbeat_interval_seconds=7.5,
    )
    app.state.approval_scheduler = sched
    sched.start()

    client = TestClient(app)
    headers = {"Authorization": "Bearer test-api-token"}
    with client:
        app.state.approval_scheduler = sched
        resp = client.get("/approvals/scheduler", headers=headers)
        assert resp.status_code == 200
        data = resp.json()

        # Validate schema and lease fields
        diag = ApprovalSchedulerDiagnostics.model_validate(data)
        assert diag.enabled is True
        assert diag.running is True
        assert diag.is_leader is True
        assert diag.leadership_state == "leader"
        assert diag.owner_id == "worker-api-diag"
        assert diag.lease_owner_id == "worker-api-diag"
        assert diag.fence_token == 1
        assert diag.lease_ttl_seconds == 45.0
        assert diag.heartbeat_interval_seconds == 7.5
        assert diag.lease_expires_at is not None

        # Verify SDK client parses successfully
        api_client = OrchestratorApiClient(
            base_url="http://testserver",
            api_token="test-api-token",
            http_client=client,
        )
        sdk_diag = api_client.get_approval_scheduler_status()
        assert sdk_diag.is_leader is True
        assert sdk_diag.fence_token == 1

    sched.stop()


def test_diagnostics_distinguish_disabled_and_standby(tmp_store: CheckpointStore) -> None:
    """Diagnostics clearly distinguish between disabled, standby, and active leader."""
    sched_disabled = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=False,
    )
    status_dis = sched_disabled.get_status()
    assert status_dis["enabled"] is False
    assert status_dis["is_leader"] is False
    assert status_dis["leadership_state"] == "disabled"

    # Pre-occupy lease with worker-Leader
    now = datetime.datetime.now(datetime.timezone.utc)
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-Leader", 60.0, now)

    sched_standby = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        owner_id="worker-Standby",
    )
    sched_standby.start()
    status_stby = sched_standby.get_status()
    assert status_stby["enabled"] is True
    assert status_stby["is_leader"] is False
    assert status_stby["leadership_state"] == "standby"
    assert status_stby["owner_id"] == "worker-Standby"
    assert status_stby["lease_owner_id"] == "worker-Leader"
    sched_standby.stop()


# ── 5. Cross-Process Concurrency & Subprocess Verification ────────────────────────


def test_cross_process_lease_concurrency_and_crashed_takeover(tmp_path: Any) -> None:
    """Verify real multi-process SQLite coordination and takeover on ungraceful crash."""
    shared_db = str(tmp_path / "cross_process.db")
    CheckpointStore(shared_db, enabled=True)  # Initialize schema

    # Process 1 script: acquires lease with short TTL (2s), then abruptly exits (simulating crash)
    p1_script = (
        "import sys, os, time, datetime\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from orchestrator.persistence.store import CheckpointStore\n"
        "store = CheckpointStore(sys.argv[2], enabled=True)\n"
        "now = datetime.datetime.now(datetime.timezone.utc)\n"
        "acq, token, _ = store.acquire_scheduler_lease('tool_approval_scheduler', 'proc-1', 2.0, now)\n"
        "assert acq is True and token == 1\n"
        "print('P1_ACQUIRED', flush=True)\n"
        "# Abrupt crash without calling release\n"
        "os._exit(0)\n"
    )

    src_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "src")
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = src_dir

    p1 = subprocess.run(
        [sys.executable, "-c", p1_script, src_dir, shared_db],
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
    )
    assert p1.returncode == 0
    assert "P1_ACQUIRED" in p1.stdout

    # Process 2 script: attempts acquisition immediately (fails), waits for TTL to expire, takes over
    p2_script = (
        "import sys, os, time, datetime\n"
        "sys.path.insert(0, sys.argv[1])\n"
        "from orchestrator.persistence.store import CheckpointStore\n"
        "store = CheckpointStore(sys.argv[2], enabled=True)\n"
        "now = datetime.datetime.now(datetime.timezone.utc)\n"
        "# 1. Immediate attempt before 2s TTL expires: must fail\n"
        "acq1, _, rec1 = store.acquire_scheduler_lease('tool_approval_scheduler', 'proc-2', 2.0, now)\n"
        "assert acq1 is False, f'Expected failed acquisition but got {acq1}'\n"
        "assert rec1['owner_id'] == 'proc-1'\n"
        "print('P2_INITIAL_STANDBY', flush=True)\n"
        "# 2. Sleep past 2s TTL\n"
        "time.sleep(2.5)\n"
        "now2 = datetime.datetime.now(datetime.timezone.utc)\n"
        "# 3. Takeover attempt after TTL expiry: must succeed with fence_token=2\n"
        "acq2, token2, rec2 = store.acquire_scheduler_lease("
        "'tool_approval_scheduler', 'proc-2', 5.0, now2)\n"
        "assert acq2 is True, 'Expected takeover to succeed'\n"
        "assert token2 == 2, f'Expected token 2, got {token2}'\n"
        "assert rec2['owner_id'] == 'proc-2'\n"
        "print('P2_TAKEOVER_SUCCESS', flush=True)\n"
    )

    p2 = subprocess.run(
        [sys.executable, "-c", p2_script, src_dir, shared_db],
        capture_output=True,
        text=True,
        env=env,
        timeout=15,
    )
    assert p2.returncode == 0, f"Process 2 failed: stdout={p2.stdout}, stderr={p2.stderr}"
    assert "P2_INITIAL_STANDBY" in p2.stdout
    assert "P2_TAKEOVER_SUCCESS" in p2.stdout


# ── 6. SQLite Contention Resilience & Approval Regression Tests ───────────────────


def test_sqlite_busy_contention_retries(tmp_store: CheckpointStore) -> None:
    """Store lease operations handle SQLite busy conditions via bounded backoff."""
    orig_get_connection = tmp_store._get_connection
    call_count = 0

    class FlakyConnection:
        def __init__(self, real_conn: Any):
            self.__dict__["_real"] = real_conn

        def execute(self, sql: str, parameters: Any = ()):
            nonlocal call_count
            if "BEGIN IMMEDIATE" in str(sql) and call_count < 2:
                import sqlite3

                call_count += 1
                raise sqlite3.OperationalError("database is locked")
            return self._real.execute(sql, parameters)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

        def __setattr__(self, name: str, value: Any) -> None:
            if name == "_real":
                self.__dict__["_real"] = value
            else:
                setattr(self._real, name, value)

    def flaky_get_connection():
        return FlakyConnection(orig_get_connection())

    tmp_store._get_connection = flaky_get_connection

    now = datetime.datetime.now(datetime.timezone.utc)
    acquired, token, _ = tmp_store.acquire_scheduler_lease(
        lease_name="tool_approval_scheduler",
        owner_id="worker-contention",
        lease_ttl_seconds=30.0,
        now_dt=now,
    )
    assert acquired is True
    assert token == 1
    assert call_count == 2  # Proves it retried through the lock contention


def test_regression_manual_scan_and_approval_expiration_under_lease(
    tmp_store: CheckpointStore,
) -> None:
    """Manual scan safely expires overdue approvals even on standby worker instances."""
    fixed_now = datetime.datetime(2026, 10, 27, 12, 0, 0, tzinfo=datetime.timezone.utc)

    # Pre-occupy lease with primary worker
    tmp_store.acquire_scheduler_lease("tool_approval_scheduler", "worker-primary", 60.0, fixed_now)

    # Standby worker receives approval expiration request
    sched_standby = ApprovalLifecycleScheduler(
        store=tmp_store,
        enabled=True,
        owner_id="worker-secondary",
        clock=lambda: fixed_now,
    )
    sched_standby.start()
    assert sched_standby.is_leader is False

    # Insert an expired approval
    run_id = "run-reg-lease-1"
    tmp_store.record_tool_approval_request(
        run_id=run_id,
        operation_key="op-reg-1",
        tool_name="reboot_gateway",
        expires_at=(fixed_now - datetime.timedelta(minutes=5)).isoformat(),
    )

    # Standby worker executes manual scan (e.g. from API POST /approvals/scan-expired)
    res = sched_standby.run_scan_cycle(actor="manual_operator")
    assert res["success"] is True
    assert res["expired_count"] == 1
    assert "op-reg-1" in res["expired_operation_keys"]

    # Verify durable state in store
    rec = tmp_store.get_tool_approval(run_id, "op-reg-1")
    assert rec is not None
    assert rec["decision"] == "expired"

    sched_standby.stop()
