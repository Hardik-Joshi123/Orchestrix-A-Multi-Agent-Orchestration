# Task 48 Report: Scheduler Scan Atomicity & Partial-Failure Recovery

## 1. Executive Summary

Task 48 hardens approval lifecycle scans against partial failures, retries, and process interruptions
while strictly preserving cross-process lease leadership and admission fencing. Batch scans now execute
with item-level savepoint isolation and pre-commit fencing verification.

Key guarantees established:
- **Item-Level Atomicity via Nested Savepoints**: If a batch scan encounters a failure on an individual
  approval row (e.g. database constraint, corrupted data, or row lock), only that row rolls back to its
  savepoint (`SAVEPOINT sp_expire_{idx}`). Other valid rows commit cleanly.
- **Idempotent Retries & Skip-Forward Progress**: Approvals successfully expired are committed in terminal
  `expired` state and skipped on subsequent scans. Failed rows remain `pending` and are retried on the
  next scan cycle without duplicates or inconsistent intermediate states.
- **Deduplication of Logical Audit Events**: Audit event recording for `tool.approval_expired` and
  `tool.approval_escalated` extracts the approval's `operation_key` from event metadata and enforces
  idempotent deduplication (`dedup=True`). Retrying scans or executing manual direct expirations never
  generates duplicate logical audit records.
- **Pre-Commit Fencing Rollback**: A scheduler worker that loses its lease or is superseded while a scheduled
  scan is in flight cannot commit dirty state. Pre-commit fencing validation detects the superseded lease,
  rolls back the transaction (`conn.rollback()`), and transitions state cleanly to `"lost"`.
- **Manual Scan Safety**: Standby and manual callers (`POST /approvals/scan-expired` and SDK) continue to
  function safely alongside active scheduler leadership without requiring leadership leases.

---

## 2. Architecture & Implementation Summary

### 2.1 Persistence Layer (`src/orchestrator/persistence/store.py`)

1. **`SchedulerFencingError(CheckpointError)`**:
   - New dedicated exception raised when a scheduler worker fails lease ownership or fencing validation.
2. **Audit Event Deduplication (`record_audit_event`)**:
   - When `dedup=True` and `metadata` contains `operation_key`, queries existing audit records for matching
     `(run_id, event_type, outcome)` and checks `json_extract(metadata_json, '$.operation_key')` (with
     fallback to SQLite `LIKE`).
   - If an event already exists for that operation key, skips insertion and returns the existing event.
3. **Item-Level Atomicity in `check_and_expire_tool_approvals`**:
   - Outer transaction: `BEGIN IMMEDIATE;` with busy retries (`max_retries = 25`, backoff).
   - Upfront fencing validation: If `expected_fence_token` or `expected_owner_id` is supplied, validates
     lease ownership before reading or mutating rows.
   - Row-level savepoints: Iterates candidate rows within `SAVEPOINT sp_batch_scan;`. For each row $i$:
     - Creates `SAVEPOINT sp_expire_{i};`
     - Executes `UPDATE tool_approvals SET decision = 'expired', expired_at = ? WHERE operation_key = ?;`
     - On success: `RELEASE SAVEPOINT sp_expire_{i};` and appends to `expired_records`.
     - On error: `ROLLBACK TO SAVEPOINT sp_expire_{i};`, `RELEASE SAVEPOINT sp_expire_{i};`, sanitizes error,
       appends to `failed_records` / `partial_failures`, and logs a warning.
   - Pre-commit fencing validation: Re-validates `validate_scheduler_lease_ownership()`. If superseded,
     executes `ROLLBACK TO SAVEPOINT sp_batch_scan;` and `conn.rollback()`, ensuring zero uncommitted
     mutations are committed, and raises `SchedulerFencingError`.
   - On clean completion: Commits transaction and records audit events (`dedup=True`).
   - Returns structured dictionary: `scanned_at`, `expired_count`, `escalated_count`,
     `expired_operation_keys`, `escalated_operation_keys`, `failed_count`, `failed_operation_keys`,
     `partial_failures`, and `records`.
4. **Fenced Expiration & Escalation**:
   - `expire_tool_approval` and `escalate_tool_approval` accept `lease_name`, `expected_owner_id`, and
     `expected_fence_token`, validate ownership before applying changes, roll back on errors, and emit
     audit events with `dedup=True`.
5. **`FencedCheckpointStore` Wrapper**:
   - Forwards all fencing parameters in `expire_tool_approval`, `escalate_tool_approval`, and
     `check_and_expire_tool_approvals`.

### 2.2 Background Scheduler (`src/orchestrator/api/scheduler.py`)

1. **Fencing Parameter Injection**:
   - When `actor == "scheduler"`, passes `lease_name`, `expected_owner_id`, and `expected_fence_token` to
     `check_and_expire_tool_approvals` and `escalate_tool_approval`.
2. **SchedulerFencingError Handling**:
   - Catches `SchedulerFencingError`, gracefully marks `leadership_state = "lost"`, `is_leader = False`,
     `fence_token = None`, and returns `{"success": False, "reason": "Fenced out: ..."}`.
3. **Partial Failure Aggregation & Deduplication**:
   - Bypasses overdue escalation attempts for keys that failed during expiration in the same cycle.
   - Aggregates unique failed keys and partial failures across expiration and escalation phases.
   - Updates `_last_error = f"Partial scan failure on {len(unique_failed_keys)} approvals"` when partial
     failures occur without aborting future scan cycles.
4. **Diagnostics Telemetry**:
   - Exposes `last_scan_failed_count` in `get_status()`.

### 2.3 API Models & Routes

1. **`ApprovalSchedulerDiagnostics` (`src/orchestrator/api/models.py`)**:
   - Added `last_scan_failed_count: int = 0`.
2. **`ScanExpiredApprovalsResponse` (`src/orchestrator/api/models.py`)**:
   - Added `failed_count: int = 0`, `failed_operation_keys: list[str] = Field(default_factory=list)`.
3. **Route Integration (`src/orchestrator/api/routes.py`)**:
   - `scan_expired_approvals_endpoint` forwards `failed_count` and `failed_operation_keys` to
     `ScanExpiredApprovalsResponse`.

---

## 3. Modified Files

- `src/orchestrator/persistence/store.py`:
  Added `SchedulerFencingError`, `dedup=True` in `record_audit_event`, savepoint isolation and pre-commit
  fencing in `check_and_expire_tool_approvals`, fenced updates in `expire_tool_approval` and
  `escalate_tool_approval`, and forwarding in `FencedCheckpointStore`.
- `src/orchestrator/persistence/__init__.py`:
  Exported `SchedulerFencingError`.
- `src/orchestrator/api/models.py`:
  Added `last_scan_failed_count` to `ApprovalSchedulerDiagnostics`, added `failed_count` and
  `failed_operation_keys` to `ScanExpiredApprovalsResponse`.
- `src/orchestrator/api/routes.py`:
  Updated `scan_expired_approvals_endpoint` to populate `failed_count` and `failed_operation_keys`.
- `src/orchestrator/api/scheduler.py`:
  Handled `SchedulerFencingError`, forwarded fencing parameters, aggregated partial failures, and updated
  scheduler diagnostics telemetry.
- `docs/DEVELOPER_GUIDE.md`:
  Added Section 12.13 describing item-level savepoint atomicity, pre-commit fencing rollback, duplicate audit
  event prevention, and recovery guarantees.
- `tests/test_scheduler_scan_recovery.py`:
  Comprehensive test suite (10 tests) verifying partial failure, retries, audit deduplication, fatal error
  rollback, lease loss fencing, manual scans, and API telemetry.

---

## 4. Recovery Guarantees

1. **Item-Level Atomicity**:
   - An individual approval update failure rolls back only its nested savepoint (`sp_expire_{idx}`).
   - Progress made on other approvals in the batch is preserved and committed.
2. **Safe Idempotent Retries**:
   - Succeeded approvals transition to terminal `expired` state and are never re-processed.
   - Failed approvals remain in `pending` and are picked up cleanly on the next scan cycle.
3. **Audit Event Uniqueness**:
   - Exact deduplication via `operation_key` prevents duplicate logical `tool.approval_expired` and
     `tool.approval_escalated` audit events across repeated scans, manual executions, and retries.
4. **Pre-Commit Fencing Protection**:
   - Workers that lose their lease or are superseded during row processing are prevented from committing
     scheduled mutations. Transaction rollback guarantees zero dirty state.
5. **Operational Manual Scan Compatibility**:
   - Manual scans bypass fencing token requirements and operate safely on standby or leader workers,
     serialized cleanly via `_scan_lock`.

---

## 5. Verification Commands & Test Results

### 5.1 Focused Recovery Suite
Command:
```bash
python -m pytest multi-agent-orchestrator/tests/test_scheduler_scan_recovery.py -v
```
Output:
```
collected 10 items
test_partial_failure_item_level_rollback_and_progress PASSED               [ 10%]
test_retry_after_partial_failure_is_clean_and_idempotent PASSED            [ 20%]
test_prevention_of_duplicate_audit_events_across_scans_and_retries PASSED  [ 30%]
test_interrupted_scan_transaction_rollback_and_recovery PASSED             [ 40%]
test_stale_owner_fenced_out_during_batch_expiration PASSED                 [ 50%]
test_stale_owner_fenced_out_during_overdue_escalation PASSED               [ 60%]
test_scheduler_loop_handles_mid_scan_fence_loss PASSED                     [ 70%]
test_manual_scans_supported_on_standby_and_disabled_workers PASSED        [ 80%]
test_api_scan_expired_reports_partial_failures PASSED                     [ 90%]
test_scheduler_diagnostics_tracks_last_scan_failed_count PASSED           [100%]
============================== 10 passed in 2.94s ==============================
```

### 5.2 Full Approval & Scheduler Suite (80 Tests)
Command:
```bash
python -m pytest multi-agent-orchestrator/tests/test_approval_expiration.py \
                 multi-agent-orchestrator/tests/test_approval_scheduler.py \
                 multi-agent-orchestrator/tests/test_scheduler_lease.py \
                 multi-agent-orchestrator/tests/test_scheduler_scan_recovery.py \
                 multi-agent-orchestrator/tests/test_tool_approval_integrity.py \
                 multi-agent-orchestrator/tests/test_tool_approval_observability.py -v
```
Output:
```
============================= 80 passed in 11.64s =============================
```

### 5.3 Core API & Concurrency Regression Suite (64 Tests)
Command:
```bash
python -m pytest multi-agent-orchestrator/tests/test_api.py \
                 multi-agent-orchestrator/tests/test_api_security.py \
                 multi-agent-orchestrator/tests/test_admission_fencing.py \
                 multi-agent-orchestrator/tests/test_cross_process_admission.py \
                 multi-agent-orchestrator/tests/test_cancellation_deadlines.py -v
```
Output:
```
============================= 64 passed in 38.45s =============================
```

### 5.4 Ruff Linting
Command:
```bash
python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests
```
Output:
```
All checks passed!
```

---

## 6. Known Limitations

1. **SQLite Single-Writer Contention**:
   - While item-level savepoints and `BEGIN IMMEDIATE;` with retry backoff protect scan integrity,
     SQLite only supports one writer process at any instant. High write contention from external
     processes may delay scans until the busy timeout expires.
2. **External Side-Effect Boundaries**:
   - Database mutations are atomic, but external notifications (e.g. webhooks) do not participate
     in SQLite transactions. Systems triggering external actions must use downstream idempotency keys
     (as provided in Task 40) to guarantee end-to-end deduplication.
