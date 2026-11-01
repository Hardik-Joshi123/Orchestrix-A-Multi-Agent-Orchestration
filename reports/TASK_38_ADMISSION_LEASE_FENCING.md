# Task 38: Admission Lease Fencing & Ownership Safety Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 38 hardens the cross-process workflow admission coordination system established in Task 37 against stale workers, delayed heartbeats, temporary SQLite contention, and lease-expiration races.

In a distributed or multi-worker architecture sharing SQLite storage, process pauses (such as CPU starvation, extended garbage collection, or transient I/O delays) can cause a worker's lease heartbeat to lapse. Once the lease TTL expires, another worker process may legitimately reclaim capacity and resume or restart execution of that workflow. Without fencing, the original "zombie" worker could subsequently wake up, execute graph steps, and overwrite newer checkpoints in SQLite or persist a terminal `failed` state over an active or completed workflow.

Task 38 eliminates these split-brain and state-corruption risks through:
- **Monotonic Fencing Tokens**: Every workflow admission increments a generational `fence_token` tracked atomically in both `runs` and `workflow_admissions`.
- **Ownership-Conditional Renewals and Releases**: Stale workers cannot renew or release a newer worker's reservation. `heartbeat_active_runs` and `release_workflow_admission` validate `(owner_id, fence_token)` pairs.
- **Fenced Checkpoint Persistence**: `save_checkpoint` enforces fencing token validation against the database state, raising `AdmissionFencingError` if a write attempt is made by a superseded or expired owner.
- **Workflow Graph Halting**: Graph execution catches `AdmissionFencingError`, flags cooperative cancellation, and immediately terminates without persisting corrupted state.
- **Zombie State Suppression**: When a worker detects that its lease has expired or its generation has been fenced out, `RunManager` suppresses writing terminal failure checkpoints to SQLite.
- **Transient SQLite Failure Tolerance**: Heartbeat cycles gracefully tolerate temporary database lock contention up to `admission_lease_ttl_seconds` before marking `lease_lost` and initiating cooperative cancellation.
- **Exhaustive Verification**: 7 dedicated fencing tests in `tests/test_admission_fencing.py` and 104 total regression tests passing cleanly offline.

---

## 2. Fencing Architecture & Token Semantics

```
Worker A (Generation 1)                   SQLite Persistence                Worker B (Generation 2)
         │                                       │                                   │
         ├── Acquire Admission (Token=1) ───────>│                                   │
         │   runs.fence_token = 1                │                                   │
         │   admissions(token=1, owner=A)        │                                   │
         │                                       │                                   │
      [Worker A Pauses / Delayed Heartbeat]      │                                   │
         │                                       │                                   │
         │ (Lease expires after 30.0s TTL)       │                                   │
         │                                       │◄── Reclaim & Resume (Token=2) ────┤
         │                                       │    runs.fence_token = 2           │
         │                                       │    admissions(token=2, owner=B)   │
         │                                       │                                   │
         ├── Checkpoint Write (Token=1) ────────>│                                   │
         │   save_checkpoint(token=1, owner=A)   │ [REJECT: Token 1 < Active 2]      │
         │◄── Raises AdmissionFencingError ──────│                                   │
         │                                       │                                   │
         ▼                                       │                                   ▼
Halts Cooperatively                              │                          Continues Execution
Suppresses State Overwrite                       │                          Saves Gen-2 Checkpoints
```

### 2.1 Monotonic Token Progression
1. **Schema Additions**:
   - `runs.fence_token` (`INTEGER NOT NULL DEFAULT 0`): Records the highest fencing token ever issued for this workflow.
   - `workflow_admissions.fence_token` (`INTEGER NOT NULL DEFAULT 1`): Records the active generation token for the current lease reservation.
2. **Atomic Token Generation**:
   During `acquire_workflow_admission`, inside a `BEGIN IMMEDIATE` transaction:
   $$\text{new\_token} = \max(\text{runs.fence\_token}, 0) + 1$$
   The increment is atomic and strictly monotonic, ensuring that even if `workflow_admissions` rows are deleted during lease expiration or process crash recovery, the next acquisition or resumption will strictly increment beyond all previous generations.
3. **Re-Entrancy Token Retention**:
   When a worker re-enters `acquire_workflow_admission` for a run it already owns with an active lease, its existing `fence_token` is preserved rather than unnecessarily bumped.

---

## 3. Ownership-Conditional Renewals and Releases

### 3.1 Lease Renewals (`heartbeat_active_runs`)
Rather than blindly updating rows by `run_id`, `heartbeat_active_runs` takes a dictionary of `{run_id: fence_token}`:
```sql
UPDATE workflow_admissions
SET heartbeat_at = :heartbeat_at,
    expires_at = :expires_at
WHERE run_id = :run_id
  AND owner_id = :owner_id
  AND fence_token = :fence_token;
```
If an active run fails to renew (e.g. because another worker reclaimed the slot after TTL expiration), the method identifies it in `failed_ids`. The heartbeat worker immediately sets `lease_lost = True` on the run's tracking record and requests cooperative cancellation via `cancellation_token.request_cancellation()`.

### 3.2 Lease Releases (`release_workflow_admission`)
When a workflow completes or terminates:
```sql
DELETE FROM workflow_admissions
WHERE run_id = :run_id
  AND (:owner_id IS NULL OR owner_id = :owner_id)
  AND (:fence_token IS NULL OR fence_token = :fence_token);
```
A delayed or recovering worker can never clear a newer worker's reservation.

---

## 4. Guarded Checkpoint Transitions (`FencedCheckpointStore`)

### 4.1 Persistence Layer Validation
In `CheckpointStore.save_checkpoint(..., owner_id=None, fence_token=None)`:
1. If `fence_token` is supplied, the store inspects `runs.fence_token`:
   - If `fence_token < current_db_token`, raises `AdmissionFencingError`.
2. If `workflow_admissions` has an active record for this run:
   - If `admission.owner_id != owner_id` or `admission.fence_token != fence_token`, raises `AdmissionFencingError`.
3. On successful save, updates `runs.fence_token = MAX(runs.fence_token, excluded.fence_token)`.

### 4.2 Scoped Fencing Wrapper (`store.with_fencing`)
To prevent needing to thread `owner_id` and `fence_token` through every workflow node and agent, `CheckpointStore.with_fencing(owner_id, fence_token)` wraps the store inside a `FencedCheckpointStore` proxy that automatically injects the bound fencing tokens into all checkpoint calls.

### 4.3 Graph Execution Interception
In `src/orchestrator/graph/builder.py`:
- All checkpoint save points catch `AdmissionFencingError`.
- Upon catching fencing errors, the orchestrator:
  1. Logs an error indicating that admission ownership was lost.
  2. Flags `cancellation_token.request_cancellation(reason="admission_lease_fenced")`.
  3. Re-raises the error to halt graph execution immediately.

### 4.4 Suppression of Zombie State Overwrites
In `RunManager._run_worker` and `RunManager._resume_worker`:
When execution finishes with an exception:
- If the exception is `AdmissionFencingError` or `run_info.lease_lost` is `True`:
  - `RunManager` logs a warning and **suppresses writing `failed_state` to SQLite**.
  - This prevents an expired worker from overwriting the database state of a successor generation that has already resumed or completed the workflow.

---

## 5. Resilience to Transient SQLite Contention

Worker heartbeat pulses handle temporary SQLite locks gracefully:
1. In `RunManager._heartbeat_loop_pulse(store)`:
   - Heartbeat queries catch `sqlite3.OperationalError` (e.g. database locked) and general exceptions.
   - For each active run, the pulse checks the elapsed time since `last_heartbeat_at`:
     $$\Delta t = \text{now} - \text{last\_heartbeat\_at}$$
   - If $\Delta t < \text{admission\_lease\_ttl\_seconds}$, the error is treated as transient and execution continues.
   - Only when $\Delta t \ge \text{admission\_lease\_ttl\_seconds}$ without a confirmed renewal is the run flagged with `lease_lost = True` and cooperatively cancelled.

---

## 6. Operational Boundaries & Known Limitations

- **External Side Effects**: Idempotency and fencing guard persistence integrity within SQLite. Any non-transactional external side effects dispatched by a specialist before fencing is detected (e.g. outbound webhooks, external REST API mutations) cannot be automatically rolled back.
- **Cooperative Thread Cancellation**: Python threads run cooperatively. Workers check cancellation tokens and fencing at node boundaries and checkpoint saves. Uncooperative or blocking external system calls will halt on the next checkpoint boundary.
- **Multi-Process SQLite Locking**: Coordination applies to worker processes accessing the same shared SQLite database. Cross-host deployments with isolated local databases require a centralized lock service or relational database backend.

---

## 7. Modified & New Files

| File | Changes Made |
|---|---|
| `src/orchestrator/persistence/store.py` | Added `AdmissionFencingError`, monotonic `fence_token` columns in `runs` and `workflow_admissions`, fencing checks in `save_checkpoint`, conditional renewals in `heartbeat_active_runs`, conditional deletes in `release_workflow_admission`, `FencedCheckpointStore`, and `store.with_fencing()`. |
| `src/orchestrator/persistence/__init__.py` | Exported `AdmissionFencingError` and `FencedCheckpointStore`. |
| `src/orchestrator/graph/cancellation.py` | Added `request_cancellation` alias to `CancellationToken`. |
| `src/orchestrator/graph/builder.py` | Added explicit `AdmissionFencingError` handling to cancel and abort execution on fencing violation. |
| `src/orchestrator/api/manager.py` | Integrated `fence_token` tracking in `_active_runs`, wrapped stores with `FencedCheckpointStore`, modularized `_heartbeat_loop_pulse`, tolerated transient heartbeat errors up to TTL, and suppressed terminal failure writes when fenced out. |
| `src/orchestrator/config.py` | Fixed line-length formatting violations. |
| `docs/DEVELOPER_GUIDE.md` | Added `fence_token` to Section 11.1 and added comprehensive Section 11.4 on lease fencing mechanics, conditional renewals, transient failure tolerance, and operational boundaries. |
| `tests/test_admission_fencing.py` | Added 7 dedicated deterministic unit and integration tests. |
| `tests/test_cross_process_admission.py` | Cleaned up unused imports and variables. |

---

## 8. Verification & Test Coverage

### 8.1 Dedicated Fencing Test Suite (`tests/test_admission_fencing.py`)

| Test Case | Scenario Verified | Result |
|---|---|---|
| `test_expired_owner_cannot_renew_or_release_newer_lease` | Verifies a stale worker generation cannot renew or release a newer worker's active reservation. | Passed |
| `test_delayed_heartbeat_racing_with_lease_reclamation` | Simulates heartbeat delay beyond TTL, slot reclamation by a second worker, and subsequent fencing rejection when worker 1 attempts renewal. | Passed |
| `test_temporary_sqlite_errors_during_heartbeat` | Verifies transient database locks during heartbeats are tolerated without aborting runs within the lease TTL. | Passed |
| `test_worker_losing_ownership_before_state_transition` | Confirms `save_checkpoint` raises `AdmissionFencingError` when a worker's ownership is lost, preventing state overwrite. | Passed |
| `test_recovery_and_resume_races_across_separate_managers` | Simulates two separate `RunManager` processes racing on resume; verifies monotonic tokens prevent zombie terminal writes. | Passed |
| `test_normal_lifecycle_completion_cancellation_timeout_shutdown` | Confirms normal completion, cancellation, timeout, and shutdown paths release fenced leases cleanly. | Passed |
| `test_worker_fencing_during_graph_execution` | Executes a multi-step workflow where fencing occurs mid-graph; verifies execution halts cleanly and does not persist corrupt failure states. | Passed |

### 8.2 Full Verification Suite Results

```powershell
# 1. Code style and lint check
python -m ruff check src tests
# All checks passed!

# 2. Dedicated Fencing Tests
python -m pytest tests/test_admission_fencing.py
# 7 passed in 4.59s

# 3. Admission, Backpressure, Cross-Process & Diagnostics Tests
python -m pytest tests/test_admission_fencing.py tests/test_cross_process_admission.py tests/test_admission_backpressure.py tests/test_health_diagnostics.py
# 42 passed in 15.77s

# 4. Idempotency, Shutdown Recovery, Security, Audit & Retention Tests
python -m pytest tests/test_idempotent_submissions.py tests/test_shutdown_recovery.py tests/test_endpoint_security.py tests/test_audit_logging.py tests/test_audit_retention_export.py
# 62 passed in 20.75s
```

**Total Tests Verified:** 104 tests passed, 0 failures, 0 regressions.
