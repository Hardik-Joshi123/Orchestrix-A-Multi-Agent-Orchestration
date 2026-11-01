# Task 36: Bounded Workflow Admission & Backpressure Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 36 introduces bounded workflow admission and backpressure to the Multi-Agent Orchestration System. In high-concurrency environments, unconstrained workflow execution leads to thread pool starvation, memory bloat, GIL contention, and database lock timeouts.

Task 36 implements predictable, concurrent-safe admission limits:
- **Configurable Active Run Limit (`max_active_runs`)**: Bound on concurrently active workflow runs (default: `10`, range: `1 .. 1024`), validated at startup and exposed in system diagnostics.
- **Fail-Fast Backpressure (`HTTP 429 Too Many Requests`)**: Excess workflow submissions on `POST /runs` and `POST /runs/{id}/resume` are immediately rejected with standard `Retry-After: 2` headers and clear diagnostic error messages, preventing unbounded queue build-up.
- **Atomic Admission Decision**: Admission checks and in-process slot reservations are performed atomically under `RunManager._lock`, completely eliminating check-then-act races across concurrent HTTP threads.
- **Idempotency & Replay Exemption**: Replays of previously completed workflow runs using their `Idempotency-Key` return original results (`Idempotent-Replayed: true`) without consuming capacity or being blocked by backpressure.
- **Leak-Free Slot Release**: Guaranteed release of capacity slots across all terminal lifecycle transitions (success, unhandled failure, cooperative cancellation, deadline timeout, shutdown interruption). Failed admission marks idempotency records as `failed`, enabling immediate client retries.
- **Process Startup Recovery Isolation**: Interrupted runs recovered from crashed processes do not execute automatically and consume zero active capacity.
- **Python Client SDK Support**: `OrchestratorApiClient` maps HTTP 429 capacity rejections to `ApiCapacityExhaustedError` (subclass of `ApiRateLimitError`).
- **Comprehensive Verification**: 15 deterministic tests in `tests/test_admission_backpressure.py` and 82 regression tests across all related subsystems passed with zero errors.

---

## 2. Configuration & Defaults

In `src/orchestrator/config.py`:

| Parameter | Environment Variable | Default | Valid Range | Description |
|---|---|---|---|---|
| `max_active_runs` | `ORCHESTRATOR_MAX_ACTIVE_RUNS`<br>`MAX_ACTIVE_RUNS`<br>`API_MAX_ACTIVE_RUNS` | `10` | `1 .. 1024` | Maximum number of concurrently active workflow runs permitted in-process. |

### Validation & Startup Diagnostics
- **Pydantic Field Validator**: Rejects values `< 1` or `> 1024` with `ValueError`.
- **`validate_configuration()`**: Enforces runtime consistency.
- **`get_startup_diagnostics()`**: Exposes `diag["execution"]["max_active_runs"]`.
- **`format_diagnostics_summary()`**: Formats terminal output:
  `"  - Concurrency Limit: 4 specialists (max active runs: 10, workflow_timeout: none (unbounded))"`

---

## 3. Capacity Semantics & Overload Responses

### 3.1 Lifecycle Accounting
- **Active Runs**: An execution is active strictly while tracked in `RunManager._active_runs` (`status == "running"` or in-process cooperative cancellation).
- **Slot Release**: `_unregister_active(run_id)` is invoked in a `finally:` block in both background executor threads and synchronous callers, releasing capacity upon:
  1. Successful completion (`completed`).
  2. Unhandled exception / graph failure (`failed`).
  3. Cooperative cancellation (`cancelled`).
  4. Execution deadline budget expiration (`timed_out`).
  5. Graceful shutdown / startup recovery (`interrupted`).
- **Pre-execution Cleanup**: If worker dispatch or argument validation fails before execution begins, the reserved slot is immediately unregistered.

### 3.2 Overload HTTP Response
When `manager.active_runs_count() >= manager.max_active_runs`, new submissions (`POST /runs` and `POST /runs/{id}/resume`) return:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 2
Content-Type: application/json

{
  "detail": "Workflow admission rejected: server capacity limit of 10 active runs reached. Please retry shortly."
}
```

### 3.3 Shutdown Precedence
Graceful shutdown takes precedence over capacity limits. Requests arriving while `manager.is_shutting_down` is `True` return `HTTP 503 Service Unavailable` with `detail: "Application is shutting down; new workflow runs cannot be accepted."`.

---

## 4. Concurrency Safety & Race Prevention

### 4.1 Check-Then-Act Race Elimination
In `RunManager.start_run` and `resume_run`:
```python
with run_lock:
    with self._lock:
        if self._is_shutting_down:
            raise ShutdownError(...)
        if clean_run_id in self._active_runs:
            raise RunConflictError(...)
        if len(self._active_runs) >= self._max_active_runs:
            raise CapacityExhaustedError(...)
        # Atomically reserve slot under lock to prevent check-then-act races across concurrent run_locks
        self._active_runs[clean_run_id] = {
            "run_id": clean_run_id,
            "task": clean_task,
            "status": "running",
            "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
```
Because the capacity check and `self._active_runs` reservation occur atomically within `with self._lock:`, simultaneous threads contending for the final slot cannot both observe capacity below the threshold. Exactly one thread wins admission, and all others are rejected.

### 4.2 Idempotency Key Handling under Capacity Exhaustion
- **Completed Replays**: `routes.py` checks for existing completed idempotency records before invoking `manager.start_run()`. Replays return `HTTP 201 Created` with header `Idempotent-Replayed: true` and consume zero active capacity.
- **Rejected In-Flight Keys**: When a new request is rejected with `CapacityExhaustedError`, `routes.py` calls `store.fail_idempotency_key(client_scope, clean_key)`. The key transitions to `status = 'failed'` in SQLite, allowing the client to safely retry with the same key once capacity becomes available.

---

## 5. Verification & Test Results

### 5.1 Dedicated Test Suite (`tests/test_admission_backpressure.py`)

15 deterministic, offline unit and integration tests were executed:

| Test Name | Verified Behavior | Result |
|---|---|---|
| `test_config_max_active_runs_validation` | Validates default (10), valid bounds (1..1024), invalid bounds, aliases, and diagnostics. | PASSED |
| `test_submission_below_capacity` | Verifies execution below capacity returns 202 and increments active count. | PASSED |
| `test_rejection_at_capacity` | Verifies excess submission returns 429, Retry-After header, and capacity error message. | PASSED |
| `test_concurrent_requests_racing_for_last_available_slot` | 10 threads race at a barrier for 1 slot; exactly 1 wins (202), 9 rejected (429). | PASSED |
| `test_capacity_release_after_success` | Verifies capacity slot is released upon successful completion. | PASSED |
| `test_capacity_release_after_failure` | Verifies capacity slot is released when a run raises an unhandled error. | PASSED |
| `test_capacity_release_after_cancellation` | Verifies capacity slot is released when an in-flight run is cancelled. | PASSED |
| `test_capacity_release_after_timeout` | Verifies capacity slot is released when a run times out. | PASSED |
| `test_resume_behavior_at_capacity` | Verifies resume is rejected when capacity is full and succeeds once capacity frees. | PASSED |
| `test_startup_recovery_does_not_consume_capacity` | Verifies runs recovered at startup are marked interrupted and consume 0 slots. | PASSED |
| `test_shutdown_precedes_capacity_rejection` | Verifies shutdown returns 503 Service Unavailable, preceding capacity checks. | PASSED |
| `test_idempotent_replay_while_capacity_is_full` | Verifies replaying completed run succeeds with 201 and `Idempotent-Replayed` even when full. | PASSED |
| `test_failed_admission_does_not_orphan_idempotency_key` | Verifies rejected admission marks key failed and enables subsequent retry. | PASSED |
| `test_diagnostics_reflects_capacity_limits_and_active_usage` | Verifies `/diagnostics` accurately reports `max_active_runs` and `available_capacity`. | PASSED |
| `test_client_sdk_capacity_exhausted_error` | Verifies `OrchestratorApiClient` translates 429 to `ApiCapacityExhaustedError`. | PASSED |

**Command:** `python -m pytest tests/test_admission_backpressure.py -v`  
**Outcome:** `15 passed in 6.30s`

### 5.2 Full Regression Test Suite

All critical operational and lifecycle test suites were executed:
- `tests/test_admission_backpressure.py`: 15 passed
- `tests/test_idempotent_submissions.py`: 14 passed
- `tests/test_health_diagnostics.py`: 11 passed
- `tests/test_api.py`: 16 passed
- `tests/test_concurrency.py`: 14 passed
- `tests/test_shutdown_recovery.py`: 11 passed
- `tests/test_audit_retention_export.py`: 16 passed

**Total:** `97 passed in 64.09s`  
**Linter:** `python -m ruff check src tests` returned `All checks passed!`.

---

## 6. Files Changed

| File Path | Description of Changes |
|---|---|
| `src/orchestrator/config.py` | Added `max_active_runs` setting (default: 10), bounds validator (1..1024), cross-validation in `validate_configuration()`, and diagnostic exposure in `get_startup_diagnostics()` and `format_diagnostics_summary()`. |
| `src/orchestrator/api/models.py` | Added `max_active_runs` and `available_capacity` to `ExecutionDiagnostics`, and `max_active_runs` to `RuntimeLimitsDiagnostics`. |
| `src/orchestrator/api/manager.py` | Added `CapacityExhaustedError`. Added `max_active_runs` and `available_capacity` properties to `RunManager`. Implemented atomic reservation and backpressure checks in `start_run()` and `resume_run()`. Guaranteed cleanup on dispatch/validation failure. |
| `src/orchestrator/api/routes.py` | Mapped `CapacityExhaustedError` to `HTTP 429 Too Many Requests` with `Retry-After: 2` header across `POST /runs` and `POST /runs/{id}/resume`. Added idempotency key failure transition on capacity rejection. Updated `/diagnostics` telemetry. |
| `src/orchestrator/api/client.py` | Added `ApiCapacityExhaustedError` (subclass of `ApiRateLimitError`) and response translation. |
| `tests/test_config_validation.py` | Added `MAX_ACTIVE_RUNS` env variable cleanup to test fixtures. |
| `tests/test_admission_backpressure.py` | Created 15 deterministic unit and integration tests covering all admission and backpressure requirements. |
| `docs/DEVELOPER_GUIDE.md` | Added Section 11 (*Bounded Workflow Admission & Backpressure*) covering mechanics, sizing formulas, overload responses, and process-local limitations. |

---

## 7. Remaining Limitations

1. **Process-Local Concurrency Scope**: Admission limits are enforced in-process by the active `RunManager` instance. If multiple API server worker processes or containers run behind a load balancer without centralized state, each process enforces its own `max_active_runs` limit. Cluster-wide admission requires an ingress load balancer configured with `least_conn` routing.
2. **Specialist Concurrency Multiplier**: While active workflows are bounded by `max_active_runs`, each workflow can concurrently execute up to `effective_concurrency` specialist agents. Total potential thread utilization is $\text{max\_active\_runs} \times \text{effective\_concurrency}$. Operators should tune both parameters in tandem.

---

## 8. Git Status & Compliance

As required by project instructions, no `git commit` or `git push` was performed. All working tree changes remain intact and uncommitted.

```
On branch master
Changes not staged for commit:
  modified:   .gitignore
  modified:   Makefile
  modified:   README.md
  modified:   docs/DEVELOPER_GUIDE.md
  modified:   pyproject.toml
  modified:   requirements.txt
  modified:   src/orchestrator/api/client.py
  modified:   src/orchestrator/api/manager.py
  modified:   src/orchestrator/api/models.py
  modified:   src/orchestrator/api/routes.py
  modified:   src/orchestrator/config.py
  ...
Untracked files:
  tests/test_admission_backpressure.py
  reports/TASK_36_ADMISSION_BACKPRESSURE.md
```

Task 36 is complete. Execution halted before Task 37.
