# Task 37: Cross-Process Workflow Admission Coordination Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 37 extends bounded workflow admission (Task 36) to operate reliably across multiple worker processes sharing a single SQLite persistence store. Prior to Task 36/37, admission limits were tracked strictly in-memory per-process, meaning $N$ worker processes could each independently admit up to `max_active_runs` (e.g. $4 \times 10 = 40$ concurrent workflows), potentially overwhelming shared database locks, memory, and host CPU.

Task 37 delivers **atomic, cross-process admission reservations**:
- **Global Capacity Enforcement**: When SQLite persistence is enabled, `max_active_runs` is enforced as a single global ceiling across all processes sharing the database.
- **Database-Level Atomic Leases**: Admissions are managed in a dedicated `workflow_admissions` table inside `BEGIN IMMEDIATE` transactions, providing guaranteed serialization across concurrent OS processes.
- **Heartbeat & Lease Expiration**: Each worker process periodically renews its active workflow leases. Abandoned reservations from ungracefully killed processes expire automatically after `admission_lease_ttl_seconds`, preventing permanent capacity starvation.
- **Live Worker Shielding During Startup Recovery**: When a process boots or restarts, interrupted-run recovery (`find_interrupted_runs`) checks active lease heartbeats. Workflows actively executing under surviving, live worker processes are strictly protected from being erroneously marked as `interrupted`.
- **Idempotency & Replay Exemption**: Replays of completed workflows via `Idempotency-Key` never consume admission capacity. Failed admissions release any temporary reservations and mark idempotency keys as `failed` for safe client retries.
- **Resumption Alignment**: `POST /runs/{id}/resume` atomically acquires global admission capacity before dispatching worker threads.
- **Enhanced Diagnostics**: `/health`, `/ready`, and `/diagnostics` endpoints report `global_active_runs_count`, `global_available_capacity`, process `owner_id`, and configured lease timing parameters.
- **Exhaustive Testing**: 9 comprehensive multi-worker tests in `tests/test_cross_process_admission.py` and 97 total regression tests passing cleanly offline.

---

## 2. Configuration & Timing Parameters

In `src/orchestrator/config.py`:

| Parameter | Environment Variable | Default | Valid Range | Description |
|---|---|---|---|---|
| `max_active_runs` | `ORCHESTRATOR_MAX_ACTIVE_RUNS`<br>`MAX_ACTIVE_RUNS` | `10` | `1 .. 1024` | Global ceiling on concurrently active workflow runs across all processes. |
| `admission_lease_ttl_seconds` | `ORCHESTRATOR_ADMISSION_LEASE_TTL_SECONDS`<br>`ADMISSION_LEASE_TTL_SECONDS` | `30.0` | `1.0 .. 3600.0` | Inactivity duration after which an unrenewed lease is considered abandoned. |
| `admission_heartbeat_interval_seconds` | `ORCHESTRATOR_ADMISSION_HEARTBEAT_INTERVAL_SECONDS`<br>`ADMISSION_HEARTBEAT_INTERVAL_SECONDS` | `5.0` | `0.1 .. 300.0` | Period between heartbeat refreshes for active leases by the worker process. |

### Validation & Diagnostic Checks
- **Cross-Field Validation**: Enforces `admission_heartbeat_interval_seconds < admission_lease_ttl_seconds` to guarantee that heartbeats fire multiple times before lease expiration.
- **Startup Diagnostics**: `get_startup_diagnostics()` exposes `diag["runtime_limits"]["admission_lease_ttl_seconds"]` and `diag["runtime_limits"]["admission_heartbeat_interval_seconds"]`.
- **Formatted Terminal Output**: `format_diagnostics_summary()` renders active admission bounds and lease intervals on startup.

---

## 3. Database Schema & Migration

### 3.1 `workflow_admissions` Table
Stored in `orchestrator_checkpoints.db`:

```sql
CREATE TABLE IF NOT EXISTS workflow_admissions (
    run_id TEXT PRIMARY KEY,
    owner_id TEXT NOT NULL,
    status TEXT NOT NULL,
    acquired_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_workflow_admissions_expires_at ON workflow_admissions (expires_at);
CREATE INDEX IF NOT EXISTS idx_workflow_admissions_heartbeat_at ON workflow_admissions (heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_workflow_admissions_owner_id ON workflow_admissions (owner_id);
```

### 3.2 Safe Schema Migration
In `CheckpointStore._init_db`, backward-compatible schema migration automatically adds `expires_at` with an empty default to existing databases from earlier tasks, backfilling timestamps where necessary without table drops or data loss.

---

## 4. Atomic Cross-Process Admission Strategy

### 4.1 Transactional Reservation Protocol
When a process attempts to start or resume a workflow:
1. **Local Pre-Check**: Checks if the local process is shutting down or has exceeded process bounds.
2. **`BEGIN IMMEDIATE` Lock Acquisition**: Initiates a write transaction on SQLite. In WAL mode, `BEGIN IMMEDIATE` immediately obtains a `RESERVED` lock, guaranteeing serial execution of admission checks across all processes.
3. **Stale Lease Eviction**: Automatically deletes abandoned leases where `expires_at < now`.
4. **Re-Entrancy Check**: If `run_id` already holds an active lease for this `owner_id`, its lease is refreshed and returns `True`.
5. **Capacity Verification**: Counts surviving active leases (`COUNT(*) FROM workflow_admissions WHERE status = 'active'`).
   - If `count >= max_active_runs`: Rollback transaction and raise `CapacityExhaustedError` (mapped to `HTTP 429 Too Many Requests`).
   - If `count < max_active_runs`: Insert reservation with `expires_at = now + lease_ttl` and commit.

```
Process A                        SQLite Database                       Process B
    │                                   │                                  │
    ├── BEGIN IMMEDIATE ───────────────>│ (Acquires Reserved Lock)         │
    │   Count active: 9 < 10            │                                  ├── BEGIN IMMEDIATE
    │   INSERT run_A                    │                                  │   (Waits for lock...)
    │   COMMIT ────────────────────────>│                                  │
    │                                   │<── Lock granted to Process B ────┤
    │                                   │    Count active: 10 == 10        │
    │                                   │    ROLLBACK (Capacity Full) ─────┤
    │                                   │                                  ▼
    ▼                                                              HTTP 429 Rejected
Dispatch Worker A
```

### 4.2 Autonomous Lease Heartbeats
- Each `RunManager` initializes a unique identifier: `owner_id = "worker-{pid}-{uuid}"`.
- A background daemon thread executes `heartbeat_workflow_admissions(owner_id, active_ids, lease_ttl_seconds)` every `admission_heartbeat_interval_seconds`.
- The query updates `heartbeat_at = now` and `expires_at = now + lease_ttl` for all runs actively owned by this process.

### 4.3 Guaranteed Slot Release
Admissions are released (`release_workflow_admission(run_id)`) inside `finally:` blocks in all terminal execution paths:
- Workflow completion (`completed`)
- Workflow failure (`failed`)
- Cooperative cancellation (`cancelled`)
- Execution deadline timeout (`timed_out`)
- Process graceful shutdown drain (`interrupted`)

---

## 5. Startup Recovery & Multi-Worker Isolation

### 5.1 Shielding Live Workers During Crash Recovery
Prior to Task 37, startup recovery marked all runs in `status IN ('running', 'in_progress')` as `interrupted`. In a multi-worker setup, if worker $B$ restarted while worker $A$ was executing a workflow, worker $B$ would prematurely terminate worker $A$'s live run.

Task 37 updates `find_interrupted_runs(lease_ttl_seconds)` with an active lease shield:
```sql
SELECT r.run_id 
FROM runs r 
LEFT JOIN workflow_admissions w ON r.run_id = w.run_id 
WHERE r.status IN ('running', 'in_progress')
  AND (
      w.run_id IS NULL 
      OR (w.expires_at != '' AND w.expires_at < :now_iso)
      OR (w.expires_at = '' AND w.heartbeat_at < :stale_cutoff)
  )
```
- **Live Protection**: If an active run has a lease with a valid heartbeat, it is omitted from the recovery list.
- **Dead Process Recovery**: If a worker process crashed hard (e.g. `kill -9`), its heartbeat stops. Once `expires_at` passes, the lease is identified as stale, and any surviving or restarting worker safely marks the run as `interrupted` and clears the admission slot.

---

## 6. Runtime Diagnostics & Operational Endpoints

The API diagnostic models and routes are updated:
- `ExecutionDiagnostics`:
  - `global_active_runs_count`: Active admissions reported from SQLite store (falls back to process count in memory mode).
  - `global_available_capacity`: Remaining capacity slots ($\max(0, \text{max\_active\_runs} - \text{global\_active\_runs\_count})$).
  - `owner_id`: Unique identifier of the handling worker instance.
- `RuntimeLimitsDiagnostics`:
  - `admission_lease_ttl_seconds`: Configured lease TTL.
  - `admission_heartbeat_interval_seconds`: Configured heartbeat pulse period.
- Operational Endpoints (`/health`, `/ready`, `/diagnostics`):
  - Pass the database store to `manager.active_runs_count(store=store)` for cluster-accurate active load reporting.
  - `/ready` returns `status: "ready"` only when shutdown is not in progress, database is healthy, and `global_available_capacity > 0`.

---

## 7. Scope of Coordination & Deployment Guidance

- **Supported Scope**: Multi-process worker instances accessing the **same shared SQLite database file** (e.g., Uvicorn multi-worker mode with `--workers N`, Gunicorn process pool, or multiple container tasks on a shared volume with file locking).
- **Cluster / Multi-Host Boundaries**: Separate physical hosts without a shared SQLite file do not share admission limits. For multi-host deployments:
  - Route traffic via reverse proxy with `least_conn` load balancing.
  - Or use a centralized database (PostgreSQL) / distributed lock manager (Redis).

---

## 8. Verification & Test Coverage

### 8.1 Dedicated Multi-Worker Test Suite (`tests/test_cross_process_admission.py`)
| Test Case | Description | Result |
|---|---|---|
| `test_admission_lease_configuration` | Validates lease TTL, heartbeat interval, and cross-field validation rules. | Passed |
| `test_cross_process_capacity_enforced` | Spawns multiple `RunManager` instances sharing SQLite; verifies aggregate capacity blocks excess runs with `CapacityExhaustedError`. | Passed |
| `test_atomic_cross_process_admission_race` | Concurrently rushes multiple worker processes against the final available slot; verifies exactly one succeeds. | Passed |
| `test_capacity_released_across_workers` | Verifies run completion in Worker 1 immediately frees capacity for Worker 2. | Passed |
| `test_stale_admission_reclaimed_after_ttl` | Simulates an abandoned lease from a dead process; verifies active workers reclaim slot after TTL. | Passed |
| `test_active_runs_shielded_during_recovery` | Verifies startup recovery on Worker B does not interrupt active runs owned by live Worker A. | Passed |
| `test_resume_respects_cross_process_capacity` | Confirms workflow resumption respects global admission bounds across workers. | Passed |
| `test_idempotent_replay_at_capacity` | Ensures completed idempotent replays succeed even when global database capacity is 100% full. | Passed |
| `test_diagnostics_reflects_global_capacity` | Confirms `/diagnostics` outputs accurate global active runs, capacity, and owner ID. | Passed |

### 8.2 Full Verification Suite
```powershell
# Diagnostics and admission coordination
pytest tests/test_health_diagnostics.py tests/test_cross_process_admission.py tests/test_admission_backpressure.py
# 35 passed in 14.16s

# Lifecycle, security, audit, and recovery regressions
pytest tests/test_idempotent_submissions.py tests/test_shutdown_recovery.py tests/test_endpoint_security.py tests/test_audit_logging.py tests/test_audit_retention_export.py
# 62 passed in 21.08s
```

**Total Tests Verified:** 97 tests passed, 0 failures, 0 regressions.
