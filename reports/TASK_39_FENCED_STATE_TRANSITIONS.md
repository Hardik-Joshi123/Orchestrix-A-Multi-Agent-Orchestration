# Task 39: Atomic Fenced Workflow State Transitions Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 39 audits and hardens all workflow state transitions and metadata mutations across the orchestrator lifecycle. While Task 38 introduced lease fencing tokens and ownership checks, state transitions (such as completions, failures, cancellations, timeouts, shutdown interruptions, and resumes) previously relied on multiple non-atomic queries or unguarded database updates. A lagging or zombie worker could still attempt to write conflicting terminal states, overwrite newly completed workflows, or pollute in-memory result caches.

Task 39 delivers **strictly atomic, fencing-aware state transitions**:
- **Atomic Validation & Mutation**: All state mutations (`save_checkpoint`, `recover_run_as_interrupted`, `cancel_run_administratively`) execute under a single `BEGIN IMMEDIATE` transaction. Ownership and fencing tokens are validated under the reserved write lock prior to modifying `checkpoints` or `runs`.
- **Terminal State Immutability**: Workflows that reach `completed`, `approved`, or `edited` cannot be transitioned to any other status (`failed`, `cancelled`, `timed_out`, `running`, or `interrupted`) by any caller. Stale workers are permanently barred from regressing terminal workflows.
- **Race-Free Resumption**: Resumed workflows acquire a strictly monotonic generation token ($F_{\text{resume}} = \text{runs.fence\_token} + 1$). The new generation immediately locks out the old worker from committing any delayed subtasks.
- **Privileged Administrative Operations**:
  - `cancel_run_administratively`: Atomically bumps `runs.fence_token`, purges active admission leases, and sets `runs.status = 'cancelled'`. Rejects cancellation of already-completed runs with `CheckpointConflictError` (`HTTP 409 Conflict`).
  - `recover_run_as_interrupted`: Atomically transitions dead/abandoned runs to `interrupted`, bumps `fence_token`, and cleans up admission leases while shielding live workers with fresh heartbeats (unless forced during shutdown).
- **In-Memory Cache Shielding**: When a worker encounters `AdmissionFencingError` or `lease_lost = True`, `RunManager` evicts the run from `_results_cache` rather than caching a false failure state, ensuring subsequent queries fetch authoritative database state.
- **Comprehensive Verification**: 8 dedicated deterministic unit/integration tests in `tests/test_fenced_state_transitions.py` and 112 total regression tests passing cleanly offline.

---

## 2. State-Transition Audit & Lifecycle Model

### 2.1 Audited Persistence Paths

| Path | Source | Trigger / Transition | Previous Vulnerability | Hardened Behavior in Task 39 |
|---|---|---|---|---|
| **Graph Init & Planning** | `builder.py` | `init`, `plan` | Uncredentialed writes could overwrite leased runs. | Atomic `BEGIN IMMEDIATE` validation. Unfenced writes to actively leased runs are rejected with `AdmissionFencingError`. |
| **Subtask Execution** | `builder.py` | `subtask_complete`, `subtask_failed`, `blocked`, `escalate` | Stale worker could append steps to `checkpoints` after lease expired. | Atomic check validates `fence_token >= db_fence` and lease validity before writing `checkpoints` or `runs`. |
| **Workflow Completion** | `builder.py` | `reduce` (`completed`) | Completed status could be overwritten by a delayed worker failing or timing out. | Once terminal (`completed`), status is immutable. Subsequent writes raise `AdmissionFencingError` and rollback. |
| **Worker Failure Handling** | `manager.py` | `error` (`failed`) | Zombie worker could write `failed` over an active/completed successor in DB and memory. | If fenced out, `save_checkpoint` is skipped and `_results_cache` is evicted (`pop`) rather than recording a false failure. |
| **Administrative Cancellation** | `manager.py` | `cancelled` | Load checkpoint followed by uncoordinated write could race with worker completion. | Dedicated `cancel_run_administratively` bumps `fence_token` atomically and rejects cancellation if already terminal. |
| **Shutdown Drain** | `manager.py` | `interrupted` | Lingering workers past grace period were shielded or could overwrite recovery. | `recover_run_as_interrupted(force=True, owner_id=...)` atomically marks `interrupted` and bumps `fence_token`. |
| **Startup Crash Recovery** | `store.py` | `interrupted` | Recovery check and write were separate, risking races with resuming workers. | Executed inside a single `BEGIN IMMEDIATE` transaction; bumps `fence_token` to lock out zombie workers. |
| **Workflow Resumption** | `builder.py` | `resume` (`running`) | Old worker could write subtasks after resume started. | Resume increments `runs.fence_token`; old worker generation is fenced out on its very next database access. |

### 2.2 Lifecycle State Transition Matrix

| Current DB Status | Target Status | Allowed? | Required Conditions |
|---|---|---|---|
| `pending` | `running` | **Yes** | Valid admission lease with matching `(owner_id, fence_token)`. |
| `running` | `completed` | **Yes** | Valid admission lease; writes final results and answer. |
| `running` | `failed` | **Yes** | Valid admission lease; records error. |
| `running` | `cancelled` | **Yes** | Valid admission lease OR privileged administrative cancellation. |
| `running` | `timed_out` | **Yes** | Valid admission lease; deadline exceeded. |
| `running` | `interrupted` | **Yes** | Privileged recovery or shutdown drain (`force=True`). |
| **`completed`** | *Any Status* | **NO** | **Permanently Immutable.** Raises `AdmissionFencingError`. |
| **`approved` / `edited`** | *Any Status* | **NO** | **Permanently Immutable.** Raises `AdmissionFencingError`. |
| `cancelled` | `running` | **Yes** | **Legitimate Resume Only**: `req_token > db_fence`, `transition="resume"`. |
| `timed_out` | `running` | **Yes** | **Legitimate Resume Only**: `req_token > db_fence`, `transition="resume"`. |
| `failed` | `running` | **Yes** | **Legitimate Resume Only**: `req_token > db_fence`, `transition="resume"`. |
| `interrupted` | `running` | **Yes** | **Legitimate Resume Only**: `req_token > db_fence`, `transition="resume"`. |
| `cancelled` / `failed` / `timed_out` | `completed` | **NO** | Stale worker cannot complete a terminated run without prior resume. |

---

## 3. Implementation Details

### 3.1 Atomic Fenced Checkpoint Persistence (`save_checkpoint`)
In `src/orchestrator/persistence/store.py`:
```python
conn.isolation_level = None
conn.execute("BEGIN IMMEDIATE;")
# 1. Fetch current run row and admission lease under reserved lock
# 2. Validate caller generation against db_fence and active admission lease
# 3. Validate lifecycle state transition rules (terminal immutability)
# 4. Insert next step into checkpoints
# 5. Upsert runs summary
conn.execute("COMMIT;")
```
If any check fails, the transaction immediately issues `ROLLBACK;` and raises `AdmissionFencingError`. Zero checkpoint records are created, preventing corrupt step records in `checkpoints` from being loaded by `load_latest_checkpoint`.

### 3.2 Privileged Administrative Cancellation (`cancel_run_administratively`)
In `src/orchestrator/persistence/store.py`:
- Atomically inspects `runs.status` under `BEGIN IMMEDIATE`.
- If already `cancelled`, returns the latest checkpoint idempotently.
- If already terminal (`completed`, `failed`, `timed_out`, etc.), raises `CheckpointConflictError` (mapped to `HTTP 409 Conflict` by `RunConflictError`).
- Bumps `new_fence = db_fence + 1` and deletes from `workflow_admissions`.
- Appends `cancelled` checkpoint and updates `runs.status = 'cancelled'`.

### 3.3 Atomic Interrupted-Run Recovery (`recover_run_as_interrupted`)
In `src/orchestrator/persistence/store.py`:
- Checks `runs.status` and `workflow_admissions` under `BEGIN IMMEDIATE`.
- If active with valid lease (`expires_at >= now`), shields live worker unless `force=True` or `owner_id` matches caller.
- Bumps `new_fence = db_fence + 1` and purges admission.
- Appends `interrupted` checkpoint and updates `runs.status = 'interrupted'`.

### 3.4 In-Memory Cache Protection
In `src/orchestrator/api/manager.py`:
- In `_run_worker` and `_resume_worker`: When `is_fencing_err or lease_lost` is detected, the run is evicted from `self._results_cache.pop(run_id, None)`.
- In `cancel_run`: Verifies SQLite `get_run_summary` for terminal statuses before attempting to cancel an active in-memory run.

---

## 4. Modified & New Files

| File | Changes Made |
|---|---|
| `src/orchestrator/persistence/store.py` | Added `CheckpointConflictError`, wrapped `save_checkpoint` in atomic `BEGIN IMMEDIATE` with lifecycle transition checks, implemented `cancel_run_administratively`, hardened `recover_run_as_interrupted` to be fully atomic under lock with `force` and `owner_id` parameters, added `fence_token` to `get_run_summary` and `list_runs`, updated `FencedCheckpointStore.save_checkpoint`. |
| `src/orchestrator/persistence/__init__.py` | Exported `CheckpointConflictError`. |
| `src/orchestrator/graph/builder.py` | Explicitly caught and re-raised `AdmissionFencingError` at `init`, `plan`, `blocked`, `reduce`, `cancelled`, `timed_out`, and `resume` checkpoints. |
| `src/orchestrator/api/manager.py` | Imported `CheckpointConflictError`, delegated non-local cancellations to `store.cancel_run_administratively`, checked SQLite terminal state before cancelling active runs, and evicted `_results_cache` on fencing conflict. |
| `src/orchestrator/hitl/queue.py` | Passed `privileged=True` during review resolution checkpointing. |
| `docs/DEVELOPER_GUIDE.md` | Added Section 11.5 detailing atomic state transitions, terminal state immutability, administrative operations, and cache protection. |
| `tests/test_fenced_state_transitions.py` | Added 8 deterministic test cases verifying terminal transitions, racing completion vs cancellation/timeout, resume races, atomicity, and recovery. |

---

## 5. Verification & Test Coverage

### 5.1 Dedicated Transition Test Suite (`tests/test_fenced_state_transitions.py`)

| Test Case | Scenario Verified | Result |
|---|---|---|
| `test_stale_worker_attempting_all_terminal_transitions` | Stale worker generation 1 attempts `completed`, `failed`, `cancelled`, `timed_out`, and `running`; all 5 are rejected with `AdmissionFencingError`. | Passed |
| `test_completion_racing_with_cancellation` | Worker completes first; subsequent administrative cancellation is rejected with `CheckpointConflictError` / `RunConflictError`. | Passed |
| `test_cancellation_racing_with_completion` | Administrative cancellation commits first; delayed worker write is rejected with `AdmissionFencingError`. Status remains `cancelled`. | Passed |
| `test_completion_racing_with_timeout` | Run completes successfully; subsequent timeout trigger is rejected. Run remains `completed` with final answer intact. | Passed |
| `test_resume_racing_with_delayed_old_worker_update` | Run interrupted; Worker 2 resumes with generation 3; old Worker 1 write with generation 1 is rejected. Resumed run completes cleanly. | Passed |
| `test_atomic_ownership_validation_and_write` | Concurrent threads racing to complete vs cancel; verified atomic lock guarantees exactly one winner with zero corrupt rows. | Passed |
| `test_legitimate_shutdown_and_recovery_transitions` | Recovery bumps fence token to 2, transitions dead run to `interrupted`, and shields live running workflow. | Passed |
| `test_terminal_state_cannot_be_regressed_by_unfenced_calls` | Direct unfenced `save_checkpoint` cannot regress a completed run back to `running`. | Passed |

### 5.2 Full Test Suite Results

```powershell
# 1. Code style and lint check
python -m ruff check src tests
# All checks passed!

# 2. Dedicated Fenced State Transition Tests
python -m pytest tests/test_fenced_state_transitions.py
# 8 passed in 1.85s

# 3. Admission, Backpressure, Cross-Process & Diagnostics Tests
python -m pytest tests/test_fenced_state_transitions.py tests/test_admission_fencing.py tests/test_cross_process_admission.py tests/test_admission_backpressure.py tests/test_health_diagnostics.py
# 50 passed in 17.58s

# 4. Idempotency, Shutdown Recovery, Security, Audit & Retention Tests
python -m pytest tests/test_idempotent_submissions.py tests/test_shutdown_recovery.py tests/test_endpoint_security.py tests/test_audit_logging.py tests/test_audit_retention_export.py
# 62 passed in 23.29s
```

**Total Tests Verified:** 112 passed across all operational suites, 0 failures, 0 regressions.

---

## 6. Guarantees Established & Known Limitations

### Guarantees Established
1. **Persistence Atomicity**: Checkpoint creation and `runs` summary updates are strictly atomic. There is no intermediate window where step data is inserted without corresponding runs validation.
2. **Terminal Immutability**: Completed, approved, and edited workflows can never be regressed or overwritten.
3. **Generational Exclusivity**: Bumping `runs.fence_token` on resume, recovery, or cancellation permanently invalidates all previous worker generations.
4. **Cache Consistency**: Memory cache discrepancies cannot mask or corrupt persistent database states.

### Known Limitations
1. **External Side Effects**: Idempotency and fencing guard persistence integrity within SQLite. Third-party side effects dispatched by a specialist before fencing is detected (such as outbound webhooks or external API calls) cannot be automatically rolled back.
2. **Cooperative Thread Cancellation**: Python execution threads terminate cooperatively. Workers check cancellation and fencing at node boundaries and checkpoint saves; blocking external system calls will halt on the next checkpoint boundary.
