# Task 55: Replay Store Maintenance & Operational Diagnostics

## Executive Summary
Task 55 implements an automated maintenance lifecycle, database-derived operational diagnostics,
and an authenticated administrative maintenance API for the durable webhook replay protection system
introduced in Task 54.

In Task 54, replay records were persisted in SQLite table `webhook_replay_records` with atomic conflict
resolution. However, without proactive maintenance, expired replay records could accumulate over time,
gradually increasing database footprint and query index overhead.

Task 55 establishes:
1. **Automated Bounded Cleanup**: A dedicated background scheduler (`ReplayCleanupScheduler`) using
   cross-process leadership leases (`scheduler_leases`) to safely prune expired records in bounded batches.
2. **Durable Maintenance History**: A dedicated `webhook_replay_cleanup_log` table persisting execution
   metrics (timestamp, deleted count, duration, trigger type, operator, reason) across process restarts.
3. **Database-Derived Diagnostics**: Safe aggregate metrics (total, active, expired counts, oldest/newest
   timestamps, and last cleanup status) exposing zero secret keys, payloads, or event identities.
4. **Administrative Maintenance API**: Authenticated operational endpoints allowing operators to inspect
   replay metrics and execute bounded manual prunes with full audit event logging.

---

## Architecture & Lifecycle Implementation

### 1. Persistent Maintenance Log Schema
Every cleanup run (automated or manual) records an entry in `webhook_replay_cleanup_log`:
```sql
CREATE TABLE IF NOT EXISTS webhook_replay_cleanup_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleaned_at TEXT NOT NULL,
    deleted_count INTEGER NOT NULL,
    duration_ms REAL NOT NULL,
    trigger_type TEXT NOT NULL,
    operator TEXT,
    reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_webhook_replay_cleanup_cleaned
ON webhook_replay_cleanup_log(cleaned_at);
```

### 2. Bounded Pruning Mechanics
Pruning is executed by `CheckpointStore.cleanup_expired_webhook_replays()`:
- Enforces hard bounds on batch limit (`max(1, min(limit, 10000))`, default 500).
- Atomic subquery deletion matching only records where `expires_at < current_timestamp`.
- Active replay identities are never touched or deleted.
- Execution duration and count are logged to `webhook_replay_cleanup_log`.

### 3. Cross-Process Leadership Leases
`ReplayCleanupScheduler` coordinates across worker instances using existing `CheckpointStore` leases:
- Lease name: `"webhook_replay_cleanup"`.
- Monotonic generation tokens (`fence_token`) prevent split-brain execution across workers.
- Standby worker processes detect active leader leases and skip redundant cleanup cycles.
- On graceful shutdown, the leader immediately releases its lease via `release_scheduler_lease()`,
  allowing standby instances to seamlessly assume leadership.

---

## Operational Diagnostics & Redaction Guarantees

### 1. Aggregate Diagnostic Derivation
`CheckpointStore.get_webhook_replay_diagnostics()` derives system status purely via aggregate SQL queries:
- `total_records`: `COUNT(*)`
- `active_records`: `COUNT(*) WHERE expires_at >= now`
- `expired_records`: `COUNT(*) WHERE expires_at < now`
- `oldest_timestamp`, `newest_timestamp`: `MIN(timestamp)`, `MAX(timestamp)`
- `oldest_recorded_at`, `newest_recorded_at`: `MIN(recorded_at)`, `MAX(recorded_at)`
- `last_cleanup`: Most recent row from `webhook_replay_cleanup_log`.

### 2. Zero-Leakage Privacy Safeguards
Replay diagnostics expose purely statistical and operational metadata:
- No UUIDs or event identities are returned.
- No JSON payload bodies or headers are inspected or exposed.
- No HMAC secrets, signing keys, or tokens are included in responses.

---

## Administrative Maintenance API

### 1. REST Endpoints
| Endpoint | Method | Authentication | Rate Limited | Description |
| :--- | :--- | :--- | :--- | :--- |
| `/notifications/replay/diagnostics` | GET | Bearer Token | No | Inspect replay diagnostics |
| `/approvals/outbox/replay/diagnostics` | GET | Bearer Token | No | Route alias for diagnostics |
| `/notifications/replay/cleanup` | POST | Bearer Token | Yes | Trigger bounded manual prune |
| `/approvals/outbox/replay/cleanup` | POST | Bearer Token | Yes | Route alias for manual prune |

### 2. Audit Trail Integration
Every manual cleanup invocation emits an immutable audit event:
- Event type: `AuditEventType.NOTIFICATION_REPLAY_CLEANUP` (`"notification.replay_cleanup"`).
- Outcome: `AuditOutcome.SUCCESS`.
- Actor: Authenticated operator token identity.
- Metadata: `{"deleted_count": n, "limit": l, "reason": "...", "duration_ms": d}`.

### 3. SDK Client Enhancements
`OrchestratorClient` in `src/orchestrator/api/client.py` provides typed SDK methods:
- `get_replay_diagnostics() -> ReplayDiagnosticsResponse`
- `cleanup_replay_records(limit=None, reason=None) -> ReplayCleanupResponse`

---

## Configuration Reference

| Environment Variable / Setting | Type | Default | Bounds | Description |
| :--- | :--- | :--- | :--- | :--- |
| `NOTIFICATION_WEBHOOK_REPLAY_CLEANUP_ENABLED` | bool | `true` | bool | Enable auto cleanup |
| `NOTIFICATION_WEBHOOK_REPLAY_CLEANUP_INTERVAL_SECONDS` | float | `300.0` | 1–86400s | Cleanup interval |
| `NOTIFICATION_WEBHOOK_REPLAY_CLEANUP_BATCH_SIZE` | int | `500` | 10–10000 | Max records per batch |
| `NOTIFICATION_WEBHOOK_REPLAY_RETENTION_SECONDS` | float | `86400.0` | 60–2592000s | Replay retention TTL |

---

## Verification & Test Results

### 1. Test Suite: `tests/test_replay_store_maintenance.py`
All 11 deterministic tests pass cleanly:
- `test_replay_diagnostics_derivation_sqlite`: Derived metrics for active and expired SQLite records.
- `test_replay_diagnostics_derivation_memory`: Metrics and cleanup metadata for in-memory backend.
- `test_replay_cleanup_bounded_batches_and_safety`: Verifies batch limits and active record protection.
- `test_replay_cleanup_scheduler_lifecycle`: Verifies start, duplicate prevention, and graceful shutdown.
- `test_replay_cleanup_scheduler_leadership_lease`: Verifies cross-process lease and standby behavior.
- `test_concurrent_replay_registration_during_cleanup`: Multi-threaded stress test with concurrent writers.
- `test_restart_persistence_of_cleanup_history`: Verifies cleanup log survives process restart.
- `test_replay_diagnostics_api_authenticated`: Verifies 200 OK under auth and 401 without auth.
- `test_manual_cleanup_api_authenticated_and_audit`: Verifies manual cleanup endpoint and audit recording.
- `test_replay_config_validation`: Validates config boundaries and error handling.
- `test_singleton_reset_and_factory`: Verifies singleton lifecycle and isolation.

### 2. Comprehensive Test Execution
- `tests/test_replay_store_maintenance.py`: 11 passed.
- `tests/test_durable_replay_protection.py`: 11 passed.
- `tests/test_webhook_signing.py`: 15 passed.
- `tests/test_notification_webhook.py`: 31 passed.
- `tests/test_transactional_outbox.py`: 12 passed.
- `tests/test_outbox_observability_recovery.py`: 10 passed.
- **Total Notification Suite**: 90 passed (100% green, 0 failures, 0 regressions).
- **Linter**: `python -m ruff check src tests` passed with 0 errors.
- **Line Length Check**: All lines strictly adhere to $\le 110$ characters.

---

## Files Modified / Created

### New Files
- `src/orchestrator/notifications/cleanup.py`: `ReplayCleanupScheduler` and singleton helpers.
- `tests/test_replay_store_maintenance.py`: Deterministic test suite for maintenance & diagnostics.
- `reports/TASK_55_REPLAY_STORE_MAINTENANCE.md`: This comprehensive technical deliverable report.

### Modified Files
- `src/orchestrator/persistence/store.py`: Added `webhook_replay_cleanup_log` table, updated
  `cleanup_expired_webhook_replays()`, added `get_webhook_replay_diagnostics()` and
  `get_webhook_replay_cleanup_history()`.
- `src/orchestrator/notifications/signing.py`: Added `get_diagnostics()` to `BaseReplayStore`,
  `InMemoryReplayCache`, and `SqliteReplayStore`.
- `src/orchestrator/notifications/__init__.py`: Exported cleanup scheduler and factory functions.
- `src/orchestrator/config.py`: Added replay cleanup settings, validators, and diagnostics integration.
- `src/orchestrator/api/models.py`: Added `ReplayDiagnosticsResponse`, `ReplayCleanupRequest`,
  `ReplayCleanupResponse`.
- `src/orchestrator/api/routes.py`: Added authenticated `/notifications/replay/diagnostics` and
  `/notifications/replay/cleanup` endpoints with audit event emission.
- `src/orchestrator/api/client.py`: Added SDK methods `get_replay_diagnostics` and `cleanup_replay_records`.
- `src/orchestrator/api/server.py`: Integrated `replay_cleanup_scheduler` into FastApi server lifespan.
- `src/orchestrator/observability/audit.py`: Added `AuditEventType.NOTIFICATION_REPLAY_CLEANUP`.
- `README.md`: Added replay cleanup settings to configuration table.
- `docs/DEVELOPER_GUIDE.md`: Added Section 12.20 ("Replay Store Maintenance & Operational Diagnostics").

---

## Known Limitations & Operational Considerations
1. **SQLite Concurrency Contention**: Highly active databases under extreme write loads may experience
   transient table locks if cleanup batches are set excessively large; the default batch size of 500
   maintains low transaction latency ($< 5\text{ ms}$).
2. **Clock Drift**: Across independent multi-process nodes, NTP synchronization is recommended to avoid
   discrepancies between lease expiration and local node timestamps.
