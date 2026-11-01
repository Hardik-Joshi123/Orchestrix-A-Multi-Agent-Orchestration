# Task 47 Report: Cross-Process Scheduler Leadership & Lease Safety

## 1. Executive Summary

Task 47 establishes a robust, distributed cross-process leadership coordination mechanism for the approval
lifecycle scheduler across multiple API worker processes (such as Uvicorn multi-worker configurations or
replicated service instances). By leveraging atomic SQLite transactions on a durable `scheduler_leases` table,
the system guarantees that normally exactly one worker process holds the leadership lease and performs
automated expiration and overdue escalation scans at a time, eliminating duplicate queries, redundant scan
cycles, and database lock contention.

Core security and recovery invariants preserved:
- **Zero Auto-Approval & Zero Auto-Redispatch**: The scheduler strictly identifies expired approvals and
  transitions them to terminal `expired` status. It never auto-approves or re-executes tools.
- **Strict Fencing Token Monotonicity**: Every lease takeover generates an incremented generation token
  (`fence_token`), preventing zombie or stale owners from executing scheduled scans after temporary stalls.
- **Immediate Pre-Scan Revalidation**: Active leaders validate lease ownership and matching `fence_token`
  immediately prior to executing any scheduled scan cycle.
- **Operational Manual Scans**: Standby workers can safely execute on-demand manual scans
  (`POST /approvals/scan-expired` and SDK `scan_expired_approvals()`) at any time, serialized via `_scan_lock`.

---

## 2. Architecture & Implementation

### 2.1 Shared Scheduler Lease Schema & Migration

Durable lease persistence is implemented directly within `CheckpointStore` (`src/orchestrator/persistence/store.py`):
- **Table Definition**:
  ```sql
  CREATE TABLE IF NOT EXISTS scheduler_leases (
      lease_name TEXT PRIMARY KEY,
      owner_id TEXT NOT NULL,
      status TEXT NOT NULL,
      acquired_at TEXT NOT NULL,
      heartbeat_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      fence_token INTEGER NOT NULL DEFAULT 1
  );
  CREATE INDEX IF NOT EXISTS idx_scheduler_leases_expires
      ON scheduler_leases (lease_name, expires_at);
  ```
- **Migration Safety**: `_init_db()` verifies table and column existence non-destructively, safely preserving
  pre-existing database tables and records without data loss.

### 2.2 Atomic SQLite Operations & Monotonic Fencing

All lease primitives execute inside `BEGIN IMMEDIATE;` transactions backed by a bounded busy-retry backoff
loop (25 attempts with linear backoff) to handle SQLite lock contention:
1. `acquire_scheduler_lease(lease_name, owner_id, lease_ttl_seconds, now_dt)`:
   - **Initial Acquisition**: Inserts new lease record with `status = 'active'`, `fence_token = 1`.
   - **Re-entrant Refresh**: If the caller already owns the active lease, refreshes `expires_at` and
     retains the current `fence_token`.
   - **Takeover after Expiry**: If the current lease is expired (`expires_at <= now`) or released, assigns
     ownership to the new caller and increments `fence_token = existing_token + 1`.
   - **Active Conflict**: If another worker holds an active, unexpired lease, acquisition is rejected
     (`acquired = False`), returning the active owner's details.
2. `renew_scheduler_lease(lease_name, owner_id, fence_token, lease_ttl_seconds, now_dt)`:
   - Conditionally updates `heartbeat_at` and `expires_at` only if `owner_id` and `fence_token` match and
     the record is currently `active`.
3. `release_scheduler_lease(lease_name, owner_id, fence_token, now_dt)`:
   - Sets `status = 'released'` and `expires_at = now_iso`, enabling immediate takeover by standby workers
     without waiting for TTL expiry.
4. `validate_scheduler_lease_ownership(lease_name, owner_id, fence_token, now_dt)`:
   - Validates that the caller is the current active lease holder with matching `fence_token` and that
     `now < expires_at`.

### 2.3 Leadership Lifecycle & Stale-Owner Protection

Implemented in `ApprovalLifecycleScheduler` (`src/orchestrator/api/scheduler.py`):
- **Worker Identification**: Generates a unique instance identity `scheduler-{pid}-{uuid[:8]}` on startup,
  or accepts an explicit `owner_id`.
- **Startup & Background Loop**:
  - Attempts initial lease acquisition upon start. If successful, transitions to `leadership_state = "leader"`.
    Otherwise, enters `leadership_state = "standby"`.
  - In the background daemon thread:
    - If leader: issues periodic heartbeats every `heartbeat_interval` seconds. If heartbeat fails (superseded
      or expired), transitions immediately to `"lost"`.
    - If standby: periodically attempts acquisition / takeover every `heartbeat_interval` seconds.
- **Pre-Scan Fencing Revalidation**:
  - Before executing any scheduled scan (`actor == "scheduler"`), the scheduler queries
    `validate_scheduler_lease_ownership()`.
  - If validation fails, the worker is immediately fenced out (`leadership_state = "lost"`, `is_leader = False`),
    and the scan cycle aborts with reason `"Fenced out: scheduler lease lost"`.
- **Immediate Takeover on Graceful Shutdown**:
  - Stopping a leader worker invokes `release_scheduler_lease()`, setting the lease to `released`.
  - Standby workers can acquire the lease immediately without waiting for the 30-second TTL to lapse.
- **Crash Recovery**:
  - If a leader process crashes abruptly (e.g. `os._exit(0)` or SIGKILL), its un-renewed lease expires after
    `tool_approval_scheduler_lease_ttl_seconds`.
  - Standby workers detect expiration and take over leadership with an incremented `fence_token`.

---

## 3. Configuration & Bounds Validation

Runtime settings in `src/orchestrator/config.py`:
- `tool_approval_scheduler_lease_ttl_seconds` (default `30.0`):
  - Bounds: $1.0 \le \text{TTL} \le 3600.0$ seconds.
  - Aliases: `TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`,
    `ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`,
    `APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`, `SCHEDULER_LEASE_TTL_SECONDS`.
- `tool_approval_scheduler_heartbeat_interval_seconds` (default `5.0`):
  - Bounds: $0.1 \le \text{Heartbeat} \le 300.0$ seconds.
  - Aliases: `TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`,
    `ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`,
    `APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`, `SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`.
- **Cross-Field Validation**:
  - `validate_configuration()` strictly enforces that `heartbeat_interval < lease_ttl`.

---

## 4. Diagnostics & Observability

The `GET /approvals/scheduler` endpoint and SDK `client.get_approval_scheduler_status()` return enriched
telemetry via `ApprovalSchedulerDiagnostics` (`src/orchestrator/api/models.py`):

```json
{
  "enabled": true,
  "running": true,
  "scan_interval_seconds": 60.0,
  "last_scan_at": "2026-10-27T12:00:00+00:00",
  "last_successful_scan_at": "2026-10-27T12:00:00+00:00",
  "next_scheduled_scan_at": "2026-10-27T12:01:00+00:00",
  "total_scans": 42,
  "total_expired": 3,
  "total_escalated": 5,
  "consecutive_failures": 0,
  "last_error": null,
  "is_leader": true,
  "leadership_state": "leader",
  "owner_id": "scheduler-worker-1",
  "lease_owner_id": "scheduler-worker-1",
  "lease_expires_at": "2026-10-27T12:00:30+00:00",
  "lease_renewed_at": "2026-10-27T12:00:05+00:00",
  "fence_token": 1,
  "lease_ttl_seconds": 30.0,
  "heartbeat_interval_seconds": 5.0
}
```

- **Clear State Representation**: `leadership_state` clearly reflects `"leader"`, `"standby"`, `"lost"`,
  `"stopped"`, or `"disabled"`.
- **Cluster Owner Transparency**: When a worker is in standby, `lease_owner_id` reports the identity of
  the current lease holder in the cluster.
- **Zero Leakage**: No internal database credentials, tokens, or sensitive argument payloads are exposed.

---

## 5. Testing & Verification

Comprehensive deterministic tests are provided in `tests/test_scheduler_lease.py`:

| Test Name | Focus Area | Result |
| :--- | :--- | :--- |
| `test_scheduler_lease_settings_defaults_and_aliases` | Bounds, aliases, and cross-field validation | PASSED |
| `test_durable_lease_schema_and_migration` | Schema initialization, indices, and migration safety | PASSED |
| `test_atomic_lease_acquisition_and_reentrancy` | First acquisition, re-entrancy, conflict rejection | PASSED |
| `test_lease_renewal_and_validation` | Conditional renewal, token validation, stale token rejection | PASSED |
| `test_lease_takeover_after_expiration_and_monotonic_fencing` | Takeover after TTL expiry, fence token increment | PASSED |
| `test_graceful_lease_release_enables_immediate_takeover` | Graceful shutdown release, immediate takeover | PASSED |
| `test_scheduler_leadership_lifecycle_standby_and_manual_scans` | Leader scan execution, standby skip, manual scan safety | PASSED |
| `test_stale_owner_fencing_during_scheduled_scan` | Pre-scan revalidation, immediate fencing of stale owners | PASSED |
| `test_api_scheduler_diagnostics_reflects_leadership_and_lease` | REST endpoint, diagnostics schema, and SDK validation | PASSED |
| `test_diagnostics_distinguish_disabled_and_standby` | Diagnostic state distinction (leader vs standby vs disabled) | PASSED |
| `test_cross_process_lease_concurrency_and_crashed_takeover` | Real OS subprocesses, abrupt crash, and expired takeover | PASSED |
| `test_sqlite_busy_contention_retries` | SQLite busy-lock backoff retry handling | PASSED |
| `test_regression_manual_scan_and_approval_expiration_under_lease` | Task 45 approval expiration safety under multi-worker lease | PASSED |

### Regression Verification Across Tasks 40–46
- `tests/test_scheduler_lease.py`: 13 passed in 5.65s
- `tests/test_approval_scheduler.py`: 12 passed in 3.01s
- `tests/test_approval_expiration.py`: 15 passed in 2.96s
- `tests/test_tools_safety.py`: 11 passed
- `tests/test_tool_permission_policies.py`: 18 passed
- `tests/test_tool_execution_idempotency.py`: 12 passed
- `tests/test_tool_execution_reconciliation.py`: 17 passed
- `tests/test_tool_approval_integrity.py`: 15 passed
- `tests/test_tool_approval_observability.py`: 15 passed
- **Total**: 128/128 tests passing cleanly with zero regressions.
- **Linter**: `python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests` passed with 0 errors.

---

## 6. Deliverables & Modified Files

- `src/orchestrator/config.py`:
  - Added lease configuration settings, environment variable aliases, bounds validators, and cross-field checks.
- `src/orchestrator/persistence/store.py`:
  - Added `scheduler_leases` table and index schema migration.
  - Implemented `acquire_scheduler_lease`, `renew_scheduler_lease`, `release_scheduler_lease`,
    `validate_scheduler_lease_ownership`, and `get_scheduler_lease`.
  - Added delegation methods to `FencedCheckpointStore`.
- `src/orchestrator/api/models.py`:
  - Extended `ApprovalSchedulerDiagnostics` with leadership and lease telemetry fields.
- `src/orchestrator/api/scheduler.py`:
  - Implemented distributed leadership lifecycle, background heartbeat renewal, pre-scan ownership
    revalidation, graceful release on shutdown, and standby state handling.
- `tests/test_scheduler_lease.py`:
  - Comprehensive 13-test test suite covering storage, scheduler, diagnostics, and real cross-process subprocesses.
- `docs/DEVELOPER_GUIDE.md`:
  - Added Section 12.12 documenting architecture, sequence diagram, durable storage, fencing tokens,
    configuration, and operational diagnostics.
