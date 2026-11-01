# Task 46 Report: Automated Approval Lifecycle Scheduler

## 1. Executive Summary

Task 46 introduces an automated background scheduler (`ApprovalLifecycleScheduler`) for managing the
lifecycle of time-limited tool approvals. Building directly upon the durable timestamps, batch scanning,
and escalation foundations established in Task 45, this subsystem autonomously identifies expired approvals,
transitions them to terminal `expired` status, and auto-escalates overdue pending requests without requiring
continuous operator polling or manual trigger calls.

All background operations strictly uphold core execution safety invariants:
- **Zero Auto-Approval**: Approvals are never automatically granted.
- **Zero Auto-Redispatch**: Tools are never automatically invoked or redispatched upon timeout or escalation.
- **Strict Human Oversight**: Any reconsideration of an expired or overdue operation requires explicit,
  audited operator recovery and re-approval before downstream execution.

---

## 2. Architecture & Implementation

### 2.1 Configuration & Bounds Validation

The scheduler is governed by runtime settings in `src/orchestrator/config.py`:
- `tool_approval_scheduler_enabled` (default `True`): Controls automated scanning. Supports aliases:
  `ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_ENABLED`, `TOOL_APPROVAL_SCHEDULER_ENABLED`, `APPROVAL_SCHEDULER_ENABLED`.
  When disabled, the background thread does not start, while on-demand manual endpoints remain fully operational.
- `tool_approval_scan_interval_seconds` (default `60.0`): Configures scan cycle periodicity. Supports aliases:
  `ORCHESTRATOR_TOOL_APPROVAL_SCAN_INTERVAL_SECONDS`, `TOOL_APPROVAL_SCAN_INTERVAL_SECONDS`,
  `APPROVAL_SCAN_INTERVAL_SECONDS`, `TOOL_APPROVAL_SCAN_INTERVAL`. Validated strictly within bounds
  $1.0 \le \text{interval} \le 86400.0$ seconds (24 hours).
- `tool_approval_escalation_enabled` (default `True`): Controls whether overdue pending approvals are
  auto-escalated during scheduled scan cycles.

### 2.2 Background Scheduler (`ApprovalLifecycleScheduler`)

Implemented in `src/orchestrator/api/scheduler.py`:
- **Daemon Thread Management**: Launches a single background thread (`approval-lifecycle-scheduler`).
  Repeated calls to `scheduler.start()` are no-ops, preventing duplicate background loops within the same
  process.
- **Deterministic Offline Testing**: Supports an injectable clock `Callable[[], datetime.datetime]`,
  enabling instantaneous and repeatable verification without real-time sleep delays.
- **Graceful Shutdown**: The `stop(timeout=2.0)` method signals thread shutdown via `threading.Event`,
  waking any sleeping scan cycle immediately and joining the thread cleanly.
- **Process Singleton Management**: `get_approval_scheduler()` provides consistent process-wide access,
  while `reset_approval_scheduler()` guarantees thread cleanup and isolation between unit test fixtures.

### 2.3 Expiry and Overdue Auto-Escalation Processing

During each scan cycle (`run_scan_cycle`):
1. **Approval Expiration**: Calls `CheckpointStore.check_and_expire_tool_approvals(before_timestamp=now)`,
   transitioning approvals where `expires_at <= now` to `decision = 'expired'`, updating `expired_at`, and
   emitting `tool.approval_expired` audit events.
2. **Overdue Auto-Escalation**: When `auto_escalate` is enabled, queries pending approvals via
   `CheckpointStore.check_overdue_approvals(now_timestamp=now)`. Approvals approaching expiration or waiting
   longer than `tool_approval_escalation_threshold_seconds` are marked escalated via
   `CheckpointStore.escalate_tool_approval`, recording `escalated_at`, `escalation_reason`, and emitting
   `tool.approval_escalated` audit events.
3. **Idempotence**: Repeated scans against unchanged database state perform zero duplicate updates and
   generate zero duplicate audit events.

### 2.4 Concurrency Protection & Fault Tolerance

- **Mutual Exclusion (`_scan_lock`)**: A dedicated scan lock synchronizes background automated scans with
  on-demand manual trigger requests (`POST /approvals/scan-expired`), preventing SQLite lock conflicts.
- **Transaction Safety**: Batch database updates in `store.py` commit `UPDATE` queries before emitting audit
  events, preventing SQLite nested lock contention.
- **Resilient Error Recovery**: Any unhandled exception during a scan cycle is captured safely. Error messages
  are sanitized with `sanitize_error` (redacting credentials, secrets, tokens, and passwords). Failures
  increment `consecutive_failures`, record `last_error`, apply temporary backoff (`min(5.0, scan_interval)`),
  and never terminate the scheduler loop thread. Subsequent successful cycles reset failure counters.

### 2.5 Observability & API Endpoints

- **`GET /approvals/scheduler`**: Dedicated authenticated endpoint returning `ApprovalSchedulerDiagnostics`:
  `enabled`, `running`, `scan_interval_seconds`, `last_scan_at`, `last_successful_scan_at`,
  `next_scheduled_scan_at`, `total_scans`, `total_expired`, `total_escalated`, `consecutive_failures`,
  and `last_error`.
- **`GET /diagnostics`**: Extended to embed `approval_scheduler` as a top-level telemetry block.
- **SDK Client**: `OrchestratorApiClient.get_approval_scheduler_status()` provides typed access to scheduler
  diagnostics, and `scan_expired_approvals()` triggers manual batch scans.

---

## 3. Verification & Test Results

### 3.1 Unit and Integration Test Suite (`tests/test_approval_scheduler.py`)

All 12 tests passed:
- `test_scheduler_settings_defaults_and_aliases`: Default values and environment variable alias precedence.
- `test_scheduler_scan_interval_bounds_validation`: Lower ($< 1.0$) and upper ($> 86400.0$) bound rejection.
- `test_scheduler_start_stop_lifecycle`: Start, duplicate thread avoidance, and clean stop.
- `test_scheduler_disabled_behavior`: Disabling background loop while preserving manual scans.
- `test_get_approval_scheduler_singleton`: Singleton consistency and reset isolation.
- `test_scheduled_scan_expires_and_auto_escalates`: Expiration of timed-out approvals and auto-escalation of
  overdue pending approvals.
- `test_repeated_scans_are_idempotent`: Idempotent repeat scanning with zero redundant transitions.
- `test_scheduler_handles_transient_failure_and_recovers`: Error capture, token redaction, and self-healing.
- `test_concurrent_scans_are_serialized_safely`: Mutex synchronization under concurrent scan load.
- `test_api_get_approval_scheduler_status`: Authentication enforcement and diagnostics schema verification.
- `test_api_diagnostics_includes_scheduler`: Embedded scheduler diagnostics in `/diagnostics`.
- `test_sdk_get_approval_scheduler_status`: Python SDK client method validation.

### 3.2 Regression Verification

- `tests/test_approval_expiration.py`: 15 passed in 2.55s.
- `tests/test_tools_safety.py`: 11 passed.
- `tests/test_tool_permission_policies.py`: 18 passed.
- `tests/test_tool_execution_idempotency.py`: 12 passed.
- `tests/test_tool_execution_reconciliation.py`: 17 passed.
- `tests/test_tool_approval_integrity.py`: 15 passed.
- `tests/test_tool_approval_observability.py`: 15 passed.
- **Total Regression Suite**: 103 passed with 0 failures.

### 3.3 Code Quality & Linter

`python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests` executed cleanly with
zero warnings or errors. All line lengths and docstrings remain wrapped $\le 110$ characters.

---

## 4. Operational Invariants & Guidelines

1. **Non-Intrusive Background Execution**: The scheduler runs as a daemon thread and does not block API
   startup or shutdown.
2. **Safe Defaults**: By default, scans run every 60 seconds with auto-escalation enabled.
3. **Recovery After Expiration**: An expired operation cannot be executed by retrying or resuming.
   Operators must submit `POST /runs/{run_id}/tools/{operation_key}/recover` followed by explicit approval.
4. **Credential Safety**: Internal errors never leak authorization tokens, bearer credentials, or database
   connection strings in scheduler telemetry.
