"""Deterministic test suite for webhook replay store maintenance, diagnostics, and cleanup."""

from __future__ import annotations

import concurrent.futures
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from orchestrator.api.routes import get_store
from orchestrator.api.server import create_app
from orchestrator.config import Settings
from orchestrator.notifications.cleanup import (
    ReplayCleanupScheduler,
    get_replay_cleanup_scheduler,
    reset_replay_cleanup_scheduler,
)
from orchestrator.notifications.signing import (
    InMemoryReplayCache,
    SqliteReplayStore,
    WebhookSignatureVerifier,
    format_signature_header,
    generate_webhook_signature,
)
from orchestrator.observability.audit import AuditEventType
from orchestrator.persistence.store import CheckpointStore


def test_replay_diagnostics_derivation_sqlite(tmp_path: Path) -> None:
    """Verify durable SQLite replay store exposes accurate bounded metrics and diagnostics."""
    db_file = str(tmp_path / "diag_test.db")
    store = SqliteReplayStore(db_path=db_file, retention_seconds=60.0, cleanup_batch_size=100)

    # Initially empty
    diag0 = store.get_diagnostics(now=1000.0)
    assert diag0["enabled"] is True
    assert diag0["storage_backend"] == "sqlite"
    assert diag0["total_records"] == 0
    assert diag0["active_records"] == 0
    assert diag0["expired_records"] == 0
    assert diag0["last_cleanup"] is None

    # Insert 3 active records and 2 expired records
    # Record at t=900 with tolerance=30 expires at 960 (expired at t=1000)
    assert store.check_and_record("evt-exp-1", timestamp=900, tolerance_seconds=30.0, now=900.0) is True
    assert store.check_and_record("evt-exp-2", timestamp=905, tolerance_seconds=30.0, now=905.0) is True
    # Record at t=980 with tolerance=30 expires at 1040 (active at t=1000)
    assert store.check_and_record("evt-act-1", timestamp=980, tolerance_seconds=30.0, now=980.0) is True
    assert store.check_and_record("evt-act-2", timestamp=985, tolerance_seconds=30.0, now=985.0) is True
    assert store.check_and_record("evt-act-3", timestamp=990, tolerance_seconds=30.0, now=990.0) is True

    diag1 = store.get_diagnostics(now=1000.0)
    assert diag1["total_records"] == 5
    assert diag1["active_records"] == 3
    assert diag1["expired_records"] == 2
    assert diag1["oldest_timestamp"] == 900
    assert diag1["newest_timestamp"] == 990
    assert diag1["retention_seconds"] == 60.0
    assert diag1["cleanup_batch_size"] == 100

    # Prune expired records
    deleted = store.cleanup_expired(now=1000.0, trigger_type="manual", operator="admin", reason="test prune")
    assert deleted == 2

    # Verify updated diagnostics after cleanup
    diag2 = store.get_diagnostics(now=1000.0)
    assert diag2["total_records"] == 3
    assert diag2["active_records"] == 3
    assert diag2["expired_records"] == 0
    assert diag2["last_cleanup"] is not None
    assert diag2["last_cleanup"]["deleted_count"] == 2
    assert diag2["last_cleanup"]["operator"] == "admin"
    assert diag2["last_cleanup"]["reason"] == "test prune"
    assert diag2["last_cleanup"]["trigger_type"] == "manual"


def test_replay_diagnostics_derivation_memory() -> None:
    """Verify in-memory replay cache derives diagnostics and cleanup metadata safely."""
    cache = InMemoryReplayCache(max_entries=50)

    # Insert 2 expired and 1 active relative to t=1000.0
    cache.check_and_record("evt-mem-exp1", timestamp=960, tolerance_seconds=30.0, now=960.0)
    cache.check_and_record("evt-mem-exp2", timestamp=965, tolerance_seconds=30.0, now=965.0)
    cache.check_and_record("evt-mem-act", timestamp=980, tolerance_seconds=30.0, now=970.0)

    diag = cache.get_diagnostics(now=1000.0)
    assert diag["storage_backend"] == "memory"
    assert diag["total_records"] == 3
    assert diag["active_records"] == 1
    assert diag["expired_records"] == 2

    pruned = cache.cleanup_expired(now=1000.0, trigger_type="scheduled")
    assert pruned == 2

    diag_after = cache.get_diagnostics(now=1000.0)
    assert diag_after["total_records"] == 1
    assert diag_after["last_cleanup"]["deleted_count"] == 2
    assert diag_after["last_cleanup"]["trigger_type"] == "scheduled"


def test_replay_cleanup_bounded_batches_and_safety(tmp_path: Path) -> None:
    """Verify cleanup enforces limits and never prunes active replay records."""
    db_file = str(tmp_path / "batch_test.db")
    store = SqliteReplayStore(db_path=db_file, retention_seconds=60.0, cleanup_batch_size=50)

    # Insert 15 expired records (timestamp 800, expires at 860 < 1000)
    for i in range(15):
        store.check_and_record(f"evt-exp-{i}", timestamp=800, tolerance_seconds=30.0, now=800.0)

    # Insert 5 active records (timestamp 980, expires at 1040 >= 1000)
    for i in range(5):
        store.check_and_record(f"evt-act-{i}", timestamp=980, tolerance_seconds=30.0, now=980.0)

    assert store.get_count(active_only=False) == 20
    assert store.get_count(active_only=True, now=1000.0) == 5

    # Run cleanup with limit of 10: only 10 expired records deleted
    deleted_first = store.cleanup_expired(now=1000.0, limit=10)
    assert deleted_first == 10
    assert store.get_count(active_only=False) == 10
    assert store.get_count(active_only=True, now=1000.0) == 5

    # Run remaining cleanup: remaining 5 expired records deleted
    deleted_second = store.cleanup_expired(now=1000.0, limit=10)
    assert deleted_second == 5
    assert store.get_count(active_only=False) == 5

    # All 5 active records remain intact and prevent replay!
    secret = "whsec_batch_test"
    verifier = WebhookSignatureVerifier(secrets=secret, replay_store=store)
    payload = b'{"msg": "test"}'
    _, sig = generate_webhook_signature(payload, secret, timestamp=980, event_id="evt-act-0")
    header = format_signature_header(980, sig)

    with pytest.raises(Exception, match="Replay detected"):
        verifier.verify(payload, header, event_id="evt-act-0", now=1000.0)


def test_replay_cleanup_scheduler_lifecycle(tmp_path: Path) -> None:
    """Verify background scheduler lifecycle, duplicate start prevention, and graceful shutdown."""
    db_file = str(tmp_path / "scheduler_lifecycle.db")
    checkpoint_store = CheckpointStore(db_path=db_file)
    scheduler = ReplayCleanupScheduler(
        store=checkpoint_store,
        interval_seconds=1.0,
        batch_size=50,
        owner_id="test-cleaner-worker-1",
    )

    try:
        assert scheduler.is_running is False
        scheduler.start()
        assert scheduler.is_running is True

        # Duplicate start prevention (idempotent call does not launch another thread)
        first_thread = scheduler._thread
        scheduler.start()
        assert scheduler._thread is first_thread

        status = scheduler.get_status()
        assert status["enabled"] is True
        assert status["is_running"] is True
        assert status["owner_id"] == "test-cleaner-worker-1"
        assert status["batch_size"] == 50
    finally:
        scheduler.stop(timeout=2.0)
        assert scheduler.is_running is False


def test_replay_cleanup_scheduler_leadership_lease(tmp_path: Path) -> None:
    """Verify cross-process lease guarantees only leader executes cleanup cycles."""
    db_file = str(tmp_path / "scheduler_lease.db")
    store = CheckpointStore(db_path=db_file)
    replay_store = SqliteReplayStore(store=store, retention_seconds=60.0)

    # Populate 5 expired records
    for i in range(5):
        replay_store.check_and_record(f"evt-lease-{i}", timestamp=800, tolerance_seconds=30.0, now=800.0)

    current_time = 1000.0
    sched1 = ReplayCleanupScheduler(
        store=store,
        replay_store=replay_store,
        owner_id="worker-leader",
        clock=lambda: current_time,
    )
    sched2 = ReplayCleanupScheduler(
        store=store,
        replay_store=replay_store,
        owner_id="worker-standby",
        clock=lambda: current_time,
    )

    try:
        # Worker 1 runs cycle -> becomes leader and deletes expired records
        res1 = sched1.run_cleanup_cycle(now=current_time)
        assert res1["is_leader"] is True
        assert res1["leadership_state"] == "leader"
        assert res1["deleted_count"] == 5

        # Add 3 more expired records
        for i in range(3):
            replay_store.check_and_record(
                f"evt-lease-new-{i}", timestamp=800, tolerance_seconds=30.0, now=800.0
            )

        # Worker 2 attempts cycle -> detects worker 1 lease is active, operates as standby, prunes 0
        res2 = sched2.run_cleanup_cycle(now=current_time)
        assert res2["is_leader"] is False
        assert res2["leadership_state"] == "standby"
        assert res2["deleted_count"] == 0

        # Worker 1 gracefully stops and releases lease
        sched1.stop(timeout=1.0)

        # Advance time slightly and let Worker 2 attempt cycle again
        current_time = 1001.0
        res3 = sched2.run_cleanup_cycle(now=current_time)
        assert res3["is_leader"] is True
        assert res3["leadership_state"] == "leader"
        assert res3["deleted_count"] == 3
    finally:
        sched1.stop(timeout=1.0)
        sched2.stop(timeout=1.0)


def test_concurrent_replay_registration_during_cleanup(tmp_path: Path) -> None:
    """Verify concurrent registration and cleanup cycles run safely without locks or deadlocks."""
    db_file = str(tmp_path / "concurrent_cleanup.db")
    checkpoint_store = CheckpointStore(db_path=db_file)
    store = SqliteReplayStore(store=checkpoint_store, retention_seconds=60.0)

    # Prepopulate with 50 expired records
    for i in range(50):
        store.check_and_record(f"pre-exp-{i}", timestamp=800, tolerance_seconds=30.0, now=800.0)

    errors: list[Exception] = []

    def writer_task(worker_id: int) -> None:
        for j in range(20):
            try:
                event_id = f"evt-live-{worker_id}-{j}"
                assert store.check_and_record(event_id, timestamp=1000, tolerance_seconds=30.0, now=1000.0)
            except Exception as e:
                errors.append(e)

    def cleaner_task() -> None:
        for _ in range(5):
            try:
                store.cleanup_expired(now=1000.0, limit=20)
            except Exception as e:
                errors.append(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(writer_task, i) for i in range(4)]
        futures += [executor.submit(cleaner_task) for _ in range(3)]
        concurrent.futures.wait(futures)

    assert not errors, f"Errors encountered during concurrent execution: {errors}"
    # Verify all 80 live records remain active
    assert store.get_count(active_only=True, now=1000.0) == 80


def test_restart_persistence_of_cleanup_history(tmp_path: Path) -> None:
    """Verify cleanup audit history is persisted durably across process restarts."""
    db_file = str(tmp_path / "history_restart.db")

    # Session 1: perform cleanup
    store1 = CheckpointStore(db_path=db_file)
    replay1 = SqliteReplayStore(store=store1, retention_seconds=60.0)
    replay1.check_and_record("evt-sess1", timestamp=800, tolerance_seconds=30.0, now=800.0)
    replay1.cleanup_expired(now=1000.0, operator="sess1_op", reason="pre-reboot cleanup")

    # Session 2: new store instances against same SQLite DB
    store2 = CheckpointStore(db_path=db_file)
    replay2 = SqliteReplayStore(store=store2, retention_seconds=60.0)

    diag = replay2.get_diagnostics(now=1000.0)
    assert diag["last_cleanup"] is not None
    assert diag["last_cleanup"]["operator"] == "sess1_op"
    assert diag["last_cleanup"]["reason"] == "pre-reboot cleanup"
    assert diag["last_cleanup"]["deleted_count"] == 1

    history = replay2.get_cleanup_history(limit=10)
    assert len(history) == 1
    assert history[0]["operator"] == "sess1_op"
    assert history[0]["deleted_count"] == 1


def test_replay_diagnostics_api_authenticated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify GET /notifications/replay/diagnostics returns redacted diagnostics under auth."""
    db_file = str(tmp_path / "api_diag.db")
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "secret_auth_token_12345")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("NOTIFICATION_WEBHOOK_REPLAY_PROTECTION_ENABLED", "true")

    checkpoint_store = CheckpointStore(db_path=db_file)
    app = create_app()
    app.dependency_overrides[get_store] = lambda: checkpoint_store
    client = TestClient(app)

    # Prepopulate records using current system time
    import time
    now_curr = time.time()
    store = SqliteReplayStore(store=checkpoint_store, retention_seconds=3600.0)
    store.check_and_record("evt-api-1", timestamp=int(now_curr), tolerance_seconds=300.0, now=now_curr)

    # 1. Unauthenticated request -> 401
    resp_unauth = client.get("/notifications/replay/diagnostics")
    assert resp_unauth.status_code == 401

    # 2. Authenticated request -> 200 with safe aggregate metrics
    headers = {"Authorization": "Bearer secret_auth_token_12345"}
    resp_auth = client.get("/notifications/replay/diagnostics", headers=headers)
    assert resp_auth.status_code == 200
    data = resp_auth.json()

    assert data["enabled"] is True
    assert data["storage_backend"] == "sqlite"
    assert data["total_records"] == 1
    assert data["active_records"] == 1
    assert "evt-api-1" not in str(data)  # Zero payload or event identity leakage!

    # 3. Test route alias /approvals/outbox/replay/diagnostics
    resp_alias = client.get("/approvals/outbox/replay/diagnostics", headers=headers)
    assert resp_alias.status_code == 200
    assert resp_alias.json()["total_records"] == 1


def test_manual_cleanup_api_authenticated_and_audit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify POST /notifications/replay/cleanup prunes expired records and records audit trail."""
    db_file = str(tmp_path / "api_cleanup.db")
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", "cleaner_token_abcdef123")
    monkeypatch.setenv("ORCHESTRATOR_CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("NOTIFICATION_WEBHOOK_REPLAY_PROTECTION_ENABLED", "true")

    checkpoint_store = CheckpointStore(db_path=db_file)
    app = create_app()
    app.dependency_overrides[get_store] = lambda: checkpoint_store
    client = TestClient(app)

    # Prepopulate 1 expired record (past retention) and 1 active record
    import time
    now_curr = time.time()
    store = SqliteReplayStore(store=checkpoint_store, retention_seconds=60.0)
    # Expired: recorded 120s ago with 60s retention
    store.check_and_record(
        "evt-api-exp-1", timestamp=int(now_curr - 120), tolerance_seconds=30.0, now=now_curr - 120
    )
    # Active: recorded right now
    store.check_and_record(
        "evt-api-act-1", timestamp=int(now_curr), tolerance_seconds=300.0, now=now_curr
    )

    headers = {"Authorization": "Bearer cleaner_token_abcdef123"}
    payload = {"limit": 100, "reason": "operator quarterly prune"}

    resp = client.post("/notifications/replay/cleanup", headers=headers, json=payload)
    assert resp.status_code == 200
    res_data = resp.json()
    assert res_data["success"] is True
    assert res_data["deleted_count"] == 1
    assert res_data["reason"] == "operator quarterly prune"

    # Verify audit event was persisted
    audit_events = checkpoint_store.get_audit_events(limit=10)
    target_type = AuditEventType.NOTIFICATION_REPLAY_CLEANUP
    replay_audit = [e for e in audit_events if e.get("event_type") == target_type]
    assert len(replay_audit) == 1
    audit_rec = replay_audit[0]
    assert audit_rec["metadata"]["deleted_count"] == 1
    assert audit_rec["metadata"]["reason"] == "operator quarterly prune"


def test_replay_config_validation() -> None:
    """Verify configuration validation for cleanup interval and settings."""
    # Interval below 1.0s
    with pytest.raises(ValidationError, match="notification_webhook_replay_cleanup_interval_seconds"):
        Settings(notification_webhook_replay_cleanup_interval_seconds=0.5)

    # Interval above 86400s
    with pytest.raises(ValidationError, match="notification_webhook_replay_cleanup_interval_seconds"):
        Settings(notification_webhook_replay_cleanup_interval_seconds=100000.0)

    # Valid settings
    s = Settings(
        notification_webhook_replay_cleanup_enabled=True,
        notification_webhook_replay_cleanup_interval_seconds=120.0,
        notification_webhook_replay_cleanup_batch_size=250,
    )
    assert s.notification_webhook_replay_cleanup_enabled is True
    assert s.notification_webhook_replay_cleanup_interval_seconds == 120.0
    assert s.notification_webhook_replay_cleanup_batch_size == 250


def test_singleton_reset_and_factory(tmp_path: Path) -> None:
    """Verify singleton getter and reset isolation functions."""
    db_file = str(tmp_path / "singleton.db")
    store = CheckpointStore(db_path=db_file)

    reset_replay_cleanup_scheduler()
    s1 = get_replay_cleanup_scheduler(store=store)
    s2 = get_replay_cleanup_scheduler(store=store)
    assert s1 is s2

    reset_replay_cleanup_scheduler()
    s3 = get_replay_cleanup_scheduler(store=store)
    assert s3 is not s1
    reset_replay_cleanup_scheduler()
