# Task 57: Celery Worker Observability, Monitoring & Operational Recovery

## Executive Summary

Task 57 introduces production-grade observability, telemetry monitoring, and safe operational recovery
capabilities for the Celery distributed background execution system introduced in Task 56. Distributed
worker processes periodically record heartbeats to a shared SQLite database (`celery_worker_heartbeats`),
enabling API processes, schedulers, and human operators to monitor fleet health, derive durable queue
metrics, diagnose stuck or interrupted workflows, and execute safe, auditable recoveries across worker
crashes, orphaned leases, and network interruptions.

---

## 1. Worker Health & Durable Heartbeats

### 1.1 Durable Telemetry Schema (`celery_worker_heartbeats`)
Heartbeat and fleet health telemetry are persisted to SQLite so all API processes and workers share
durable visibility across process boundaries:

| Column | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `worker_id` | `TEXT` | PRIMARY KEY | Unique worker identity (e.g., `celery-worker-{pid}`) |
| `status` | `TEXT` | `'active'` | Worker status (`'active'`, `'stale'`, `'offline'`) |
| `registered_at` | `TEXT` | NOT NULL | UTC ISO timestamp of initial worker registration |
| `last_heartbeat_at` | `TEXT` | NOT NULL | UTC ISO timestamp of most recent heartbeat pulse |
| `active_tasks` | `INTEGER` | `0` | Count of currently active workflow executions |
| `queues` | `TEXT` | NOT NULL | Queue names consumed by this worker |
| `last_completed_at` | `TEXT` | NULL | Timestamp of most recently completed task |
| `last_failure_at` | `TEXT` | NULL | Timestamp of most recent task failure |
| `last_failure_reason` | `TEXT` | NULL | Sanitized failure explanation (tokens redacted) |
| `metadata_json` | `TEXT` | `'{}'` | Hostname, pid, and platform metadata |

### 1.2 Heartbeat Tracker Daemon (`CeleryWorkerHeartbeatTracker`)
- Workers start a lightweight background daemon thread pulsing heartbeats every
  `celery_worker_heartbeat_interval_seconds` (default: 15.0s).
- Signals `@worker_ready` and `@worker_shutdown` automatically register workers and mark them offline.
- Task lifecycle hooks (`on_task_start`, `on_task_success`, `on_task_failure`) dynamically update
  active task counts and completion/failure timestamps.
- Pruning method `store.prune_worker_heartbeats(offline_retention_days=7)` safely purges stale offline
  records without removing active workers.

### 1.3 Stale Worker Detection
A worker is automatically classified as `stale` if its recorded status is `active` but its last heartbeat
age exceeds `celery_worker_stale_threshold_seconds` (default: 60.0s).

---

## 2. Queue & Task Observability

### 2.1 Database-Derived Metrics
Operational queue metrics are derived directly from SQLite persisted state (`runs`, `workflow_admissions`,
`celery_worker_heartbeats`):
- `queued_count`: Workflows in `queued` status waiting for execution.
- `running_count`: Workflows in `running` or `in_progress` status.
- `waiting_approval_count`: Workflows paused waiting for human review.
- `completed_count` & `failed_count`: Total terminal run counts.
- `retry_count`: Total recovered workflow runs (from audit log).
- `oldest_queued_age_seconds`: Age in seconds of the oldest pending queued workflow.
- `active_worker_count`, `stale_worker_count`, `offline_worker_count`, `total_worker_count`.
- `recent_failures`: Recent failed workflow runs with sanitized error messages.

### 2.2 Strict Payload & Credential Sanitization
All metrics, diagnostic messages, failure reasons, and tracebacks are strictly scrubbed with
`_SENSITIVE_KEYS` regular expressions to prevent secret, token, password, or broker URL leakage.

---

## 3. Stuck & Interrupted Workflow Diagnosis

Workflows lacking forward progress are categorized based on durable state:
- `stuck_queued`: Workflows in `queued` status exceeding `celery_workflow_stuck_threshold_seconds` (300s).
- `orphaned_lease`: Workflows in `running` status without an associated `workflow_admissions` lease.
- `lease_expired`: Workflows whose admission lease expiry has elapsed.
- `expired_lease_dead_worker`: Lease expired and owning worker is missing heartbeats or stale.
- `deadline_exceeded`: Workflows running longer than threshold but actively holding an unexpired lease.

### Durable Ownership Safety Invariant
A missing heartbeat never assumes a worker is dead without checking durable ownership
(`workflow_admissions`). If an active, unexpired lease is held (`expires_at >= now`), the workflow is
marked `recovery_eligible: False`. The system will NEVER overwrite valid active worker ownership.

---

## 4. Safe Operational Recovery Protocol

Operators can trigger safe, auditable recovery via `POST /celery/recover` or `store.recover_stuck_workflow`:
1. **Meaningful Operator Reason**: Requires an auditable reason $\ge 5$ characters.
2. **Atomic SQLite Transaction**: Executed under `BEGIN IMMEDIATE` to prevent race conditions.
3. **Terminal Run Protection**: Runs in `completed` or `cancelled` status cannot be recovered.
4. **HITL Approval Preservation**: Workflows in `waiting_approval` status cannot be requeued without explicit
   human review decision. Recovery strictly preserves HITL safety boundaries.
5. **Active Lease Protection**: Fails with `RecoveryError` if an active unexpired admission lease is held.
6. **Monotonic Fencing Increment**: Increments fencing token (`new_fence = max(run_fence, adm_fence) + 1`),
   fencing out any delayed execution from previous or partitioned worker nodes.
7. **Action Application**:
   - `requeue`: Resets status to `queued`, deletes stale admission, saves checkpoint with transition
     `recover`, and re-dispatches Celery task.
   - `fail`: Marks status as `failed` with operator reason.
   - `cancel`: Marks status as `cancelled`.
8. **Audit Trail**: Atomically records `AuditEventType.WORKFLOW_RECOVERED` ("workflow.recovered").

---

## 5. API Endpoints & Python SDK

### 5.1 HTTP REST Endpoints
- `GET /celery/workers`: Fleet health, active tasks, heartbeat ages, and stale worker detection.
- `GET /celery/queue`: Operational metrics, queue counts, and sanitized failure summaries.
- `GET /celery/stuck`: Diagnostic list of stuck, orphaned, or interrupted workflows.
- `POST /celery/recover`: Authenticated operator recovery (supports single `run_id` or batch `run_ids`).

### 5.2 Python SDK Client (`OrchestratorClient` / `OrchestratorApiClient`)
- `client.get_celery_workers(stale_threshold_seconds=None, limit=100)`
- `client.get_celery_queue(stale_threshold_seconds=None)`
- `client.get_celery_stuck(stuck_threshold_seconds=None, stale_worker_threshold_seconds=None, limit=50)`
- `client.recover_celery_workflows(run_id=None, run_ids=None, action="requeue", reason="...", operator=None)`

---

## 6. Configuration Reference

| Setting | Default | Range | Description |
| :--- | :--- | :--- | :--- |
| `celery_worker_heartbeat_interval_seconds` | `15.0` | 1.0–300.0s | Interval between worker heartbeats |
| `celery_worker_stale_threshold_seconds` | `60.0` | 5.0–3600.0s | Stale worker threshold |
| `celery_queue_monitoring_interval_seconds` | `30.0` | 1.0–600.0s | Queue monitoring interval |
| `celery_workflow_stuck_threshold_seconds` | `300.0` | 10–86400s | Stuck workflow age threshold |
| `celery_recovery_batch_size` | `50` | 1–500 | Max runs processed per recovery batch |

---

## 7. Verification & Test Coverage Summary

- **New Test Suite**: `tests/test_celery_observability_recovery.py` (12 tests, 100% passing).
  - Worker heartbeat recording, querying, and updating.
  - Stale worker detection across dynamic thresholds.
  - Tracker daemon lifecycle and sanitized error tracking.
  - Offline worker pruning with active retention.
  - Queue metrics calculation and secret redaction.
  - Stuck workflow categorization (queued, orphaned, expired, active lease).
  - Recovery state reset, fencing increment, and audit recording.
  - Recovery safety constraints (reason length, terminal run, unexpired lease, HITL).
  - API routes verification (`GET /celery/workers`, `GET /celery/queue`,
    `GET /celery/stuck`, `POST /celery/recover`).
  - Python SDK client typed method verification.
  - Configuration consistency validation (heartbeat < stale threshold).
- **Regression Suite**: 148 tests passing across Celery execution, API, security, cancellation,
  outbox, signing, replay protection, and health diagnostics.
- **Code Quality**: 100% clean `ruff check`, zero line length violations ($\le 110$ characters).
