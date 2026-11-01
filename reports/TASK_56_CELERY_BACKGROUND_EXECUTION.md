# Task 56: Celery Background Workflow Execution Integration

## Executive Summary

Task 56 integrates Celery as an optional distributed background execution backend for long-running
multi-agent orchestration workflows. The implementation guarantees full backward compatibility with
existing synchronous API callers and in-process thread execution while preserving workflow safety,
SQLite persistence, cross-process admission bounds, fencing tokens, cooperative cancellation, and
human-in-the-loop (HITL) review gates.

---

## 1. Architecture & Execution Abstractions

### 1.1 Backend Abstraction Matrix
The system introduces a unified backend execution abstraction configured globally via
`WORKFLOW_EXECUTION_BACKEND` or per-request in `CreateRunRequest.execution_backend`:

| Backend | Mode | Behavior | HTTP Status |
| :--- | :--- | :--- | :--- |
| `direct` | Synchronous | Blocks caller until workflow run finishes in-process. | 200 / 201 |
| `thread` | In-process Async | Submits run to in-process `ThreadPoolExecutor`. | 202 Accepted |
| `celery` | Distributed Async | Enqueues run to Celery broker; executed by workers. | 202 Accepted |

### 1.2 Celery Application Architecture (`orchestrator.celery_app`)
- **Factory & Singleton**: `get_celery_app(settings=None)` configures and returns the Celery instance.
- **Queue**: Dedicated task queue `orchestrator_workflows` (configurable via `CELERY_TASK_DEFAULT_QUEUE`).
- **Reliability Settings**:
  - `task_acks_late = True`: Tasks are acknowledged only after execution concludes.
  - `task_reject_on_worker_lost = True`: Lost workers cause unacknowledged tasks to be requeued.
  - `worker_prefetch_multiplier = 1`: Prevents worker task starvation or unfair pre-allocation.
  - `task_time_limit` & `task_soft_time_limit`: Enforces execution deadlines.
  - `task_always_eager`: Supports local synchronous test execution with zero external broker dependencies.

---

## 2. Celery Task Lifecycle & Deduplication

### 2.1 Task Registration
Tasks are registered with distinct names and late acknowledgement:
- `orchestrator.tasks.execute_workflow_run`: Primary task initiating new workflow execution.
- `orchestrator.tasks.resume_workflow_run`: Resumption task for interrupted or approved workflows.

### 2.2 Deduplication & At-Least-Once Delivery
Celery provides at-least-once delivery guarantees. To prevent redundant or conflicting executions:
1. **Terminal Status Check**: Upon dispatch, the worker inspects `store.get_run_summary(run_id)`.
   If the run is already in a terminal state (`completed`, `cancelled`, `timed_out`, `failed`, `approved`,
   `edited`, `rejected`), the worker logs a warning and returns the existing summary without re-executing.
2. **Approval Status Check**: If the run is in `waiting_approval` status, the worker skips duplicate
   execution, preserving operator review state.
3. **Cross-Process Admission & Fencing**: The worker invokes `store.acquire_workflow_admission(...)`.
   If another worker is actively holding the admission lease, the redundant delivery cannot obtain the
   slot or increment the monotonic fencing token.

---

## 3. Human-in-the-Loop (HITL) Preservation

The Celery worker executes the orchestration graph via `RunManager.execute_dispatched_run`:
- When a specialist invokes a tool requiring approval (e.g., mutating or high-risk actions), the graph
  pauses immediately.
- A checkpoint snapshot is saved with `status="waiting_approval"` and `transition="waiting_approval"`.
- A durable outbox notification event is persisted in `outbox_events` within the same transaction.
- The Celery worker completes the task in `waiting_approval` status and releases its admission reservation.
- **Guarantee**: Celery workers never auto-approve, bypass, or ignore human review requirements.
  Resumption occurs only when an authorized operator grants approval via the API or Review Console.

---

## 4. API & Client Integration

### 4.1 Asynchronous Run Submission
- `POST /runs` accepts `execution_backend: "direct" | "thread" | "celery"` and `background: bool`.
- Submissions using Celery return `HTTP 202 Accepted` with initial status `queued` or `running`.
- Preserves idempotency: requests providing an `Idempotency-Key` cache and safely replay the 202 response.

### 4.2 Broker Failure Handling
If the Celery message broker is unreachable (e.g. Redis connection refused):
- The API catches connection exceptions and translates them into `BrokerUnavailableError`.
- Returns `HTTP 503 Service Unavailable` with a descriptive error message.
- Any tentative queued run record is cleaned up immediately, allowing callers to retry safely.

### 4.3 Cooperative Cancellation
- `POST /runs/{run_id}/cancel` invokes `RunManager.cancel_run`.
- The run record is transitioned to `status="cancelled"` and its fence token is incremented.
- If a Celery worker subsequently dequeues the task, it detects the `cancelled` state and aborts
  before starting workflow execution.

---

## 5. Worker Operations & Commands

### 5.1 CLI Startup
Run a dedicated worker process consuming from the orchestration queue:
```bash
celery -A orchestrator.celery_app worker -l info -Q orchestrator_workflows
```

### 5.2 Graceful Shutdown
Workers handle `SIGTERM` and `SIGINT` gracefully:
- Active tasks complete their current graph steps or transition to interrupted state.
- Lifecycle signals (`worker_ready`, `worker_shutdown`) log worker status.

---

## 6. Verification & Test Results

### 6.1 Test Suite (`tests/test_celery_execution.py`)
12 targeted deterministic test cases verify all Task 56 requirements:
1. `test_celery_configuration_validation`: Configuration bounds, normalization, and time limits.
2. `test_sanitize_broker_url`: Redaction of credentials from Redis connection URIs.
3. `test_celery_task_registration_and_diagnostics`: Task registration and diagnostics snapshot.
4. `test_successful_background_submission_eager`: Asynchronous submission returning 202 and completing.
5. `test_duplicate_celery_task_delivery_deduplication`: Safe skip of duplicate deliveries.
6. `test_idempotent_celery_submission`: Replay of 202 responses under `Idempotency-Key`.
7. `test_cancellation_of_celery_background_workflow`: Cooperative cancellation before worker execution.
8. `test_broker_unavailability_handling`: HTTP 503 response on broker connection failure.
9. `test_worker_failure_exception_handling`: Sanitized failure checkpoints on worker errors.
10. `test_hitl_approval_preservation_under_celery`: Verification that approvals are never bypassed.
11. `test_backward_compatibility_direct_and_thread`: Backward compatibility with direct and thread modes.
12. `test_diagnostics_endpoint_includes_celery`: Verification of `GET /diagnostics` snapshot.

### 6.2 Regression Status
- `pytest tests/test_celery_execution.py`: 12 passed.
- Full regression suite (104 tests across API, security, cancellation, outbox, replay, and Celery):
  **104 passed in 40.53s**.
- Linter: `python -m ruff check src tests` passed with **zero errors**.
- Line length: Verified all lines $\le 110$ characters across all modified and newly created files.
