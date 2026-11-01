# Task 49 Report: Atomic Approval State & Audit Persistence

## 1. Executive Summary

Task 49 eliminates split-brain inconsistencies between tool approval lifecycle state transitions and their
corresponding audit trail records. Previously, approval state updates (such as expirations, decisions,
revocations, and escalations) were committed in one database transaction while audit event records were
emitted through separate calls or subsequent loops under a swallow-on-failure logging policy. A crash,
lock timeout, or database error could leave a committed approval state change without its required audit
event or, conversely, create duplicate audit records upon scheduler retries.

Under Task 49:
- **Single-Connection Co-Located Atomicity**: Every tool approval state transition and its accompanying
  audit event record are executed on the same SQLite database connection within an atomic transaction
  boundary (`BEGIN IMMEDIATE` or nested `SAVEPOINT`).
- **Fail-Closed Rollback**: Approval lifecycle transitions invoke `record_audit_event()` with
  `raise_on_error=True`. If persisting the audit event fails (e.g., due to schema constraints or lock
  errors), the entire transaction/savepoint rolls back immediately, guaranteeing that no approval state
  transition commits without its audit record.
- **Savepoint Isolation in Batch Scans**: In `check_and_expire_tool_approvals`, each row update and its
  associated audit events (`tool.approval_expired`, `tool.approval_escalated`) execute inside a dedicated
  savepoint (`SAVEPOINT sp_expire_{idx}`). A failure on a single row rolls back only that row and records
  a partial failure, allowing valid sibling rows to commit cleanly.
- **Consistent Audit Deduplication**: Idempotent retries, manual scans, and concurrent scheduler cycles
  deduplicate audit events on `(event_type, outcome, operation_key)` within SQLite, preventing duplicated
  audit records.
- **Fenced Multi-Worker Safety**: Stale or superseded scheduled workers cannot commit approval mutations
  or audit events after losing lease leadership or failing admission fencing verification.
- **Legacy Safety Without Synthetic Backfills**: Historical rows lacking audit records remain valid and
  readable. The orchestrator inspects them cleanly via `verify_approval_audit_consistency()` without
  synthesizing fake historical audit events with artificial timestamps.

---

## 2. Modified Files & Component Changes

- `src/orchestrator/observability/audit.py`:
  - Added `conn: Any = None` and `raise_on_error: bool = False` to `record_audit_event()`.
  - When `raise_on_error=True`, store errors re-raise to caller for atomic rollback.
- `src/orchestrator/persistence/store.py`:
  - Extended `CheckpointStore.record_audit_event()` to accept `conn` and `raise_on_error`. Executes
    directly on caller connection without opening separate transactions.
  - Enhanced `(event_type, outcome, operation_key)` deduplication via SQLite
    `json_extract(metadata_json, '$.operation_key')` with fallback.
  - Rewrote approval transition methods (`record_tool_approval`, `revoke_tool_approval`,
    `recover_tool_approval`, `expire_tool_approval`, `escalate_tool_approval`) to atomically persist audit
    events on `conn` before `conn.commit()`, rolling back on audit failure.
  - Replaced post-commit audit loop in `check_and_expire_tool_approvals` by recording audit events inside
    each row's `SAVEPOINT sp_expire_{idx}`.
  - Added `verify_approval_audit_consistency(operation_key)` for non-destructive diagnostics.
  - Forwarded audit and verification methods in `FencedCheckpointStore`.
- `src/orchestrator/api/routes.py`:
  - Updated tool approval endpoints to pass `dedup=True` and prefer `record.get("audit_event_id")` from
    the atomic transition.
- `docs/DEVELOPER_GUIDE.md`:
  - Added Section 12.14: "Atomic Approval State & Audit Persistence", documenting ACID guarantees,
    failure rollback semantics, deduplication behavior, legacy compatibility, and diagnostics.
- `tests/test_atomic_approval_audit.py`:
  - Created 10 deterministic tests covering rollback on audit failure, batch row isolation, state
    transition atomicity, deduplication across retries, legacy record preservation, fencing validation,
    and concurrency.

---

## 3. Transaction Guarantees & ACID Semantics

### 3.1 Co-Located SQLite Atomicity
Because `tool_approvals` and `audit_events` reside in the identical SQLite database file, executing both
the `UPDATE tool_approvals` and `INSERT INTO audit_events` statements on a single SQLite connection `conn`
under write-ahead logging (WAL) mode guarantees full ACID atomicity:
1. **Atomicity**: Either both the state change and the audit event are committed, or neither is.
2. **Consistency**: Foreign keys and unique constraints are enforced prior to commit.
3. **Isolation**: Concurrent readers see the pre-transition state until commit; no dirty reads occur.
4. **Durability**: SQLite WAL guarantees fsync persistence of the single commit.

### 3.2 Single Transition Rollback
For single-approval mutations (`record_tool_approval`, `revoke_tool_approval`, `recover_tool_approval`,
`expire_tool_approval`, `escalate_tool_approval`):
```text
BEGIN IMMEDIATE;
UPDATE tool_approvals SET status = ?, ... WHERE approval_id = ?;
record_audit_event(conn=conn, raise_on_error=True, ...);
--> IF audit insert fails: conn.rollback() -> approval status reverts to prior state.
--> IF audit insert succeeds: conn.commit() -> both changes committed together.
```

### 3.3 Batch Scan Isolation via Nested Savepoints
In automated and manual lifecycle scans (`check_and_expire_tool_approvals`):
```text
BEGIN IMMEDIATE;
For each expired/escalated approval:
    SAVEPOINT sp_expire_{idx};
    UPDATE tool_approvals ...;
    record_audit_event(conn=conn, raise_on_error=True, event_type="tool.approval_expired");
    IF auto_escalate:
        record_audit_event(conn=conn, raise_on_error=True, event_type="tool.approval_escalated");
    IF ANY ERROR:
        ROLLBACK TO SAVEPOINT sp_expire_{idx};
        RELEASE SAVEPOINT sp_expire_{idx};
        record partial failure for approval_id;
        continue;
    RELEASE SAVEPOINT sp_expire_{idx};
Pre-commit fencing verification;
COMMIT;
```

---

## 4. Audit Deduplication & Fencing Protection

### 4.1 Deduplication Semantics
- Deduplication is keyed on `(event_type, outcome, operation_key)`.
- When `dedup=True` is passed, `record_audit_event` queries existing audit events in the current database
  connection.
- If a matching audit record already exists, `record_audit_event` returns the existing `event_id` and
  skips the `INSERT`, preserving idempotency across retried scans and concurrent API submissions.

### 4.2 Scheduler Lease Leadership & Fencing Safety
- Scheduled lifecycle scans execute under `FencedCheckpointStore` with an active scheduler lease.
- Before committing the batch transaction, `check_and_expire_tool_approvals` revalidates the caller's
  fencing token against `scheduler_leases`.
- If the lease expired or was taken over by another worker, the store raises `AdmissionFencingError`.
- The exception triggers `conn.rollback()`, ensuring no stale worker commits approval mutations or
  audit records.

---

## 5. Legacy Record Handling Strategy

Historical approval records created in Tasks 40–48 may exist in terminal states (`approved`, `rejected`,
`expired`, `revoked`) without corresponding audit event records in `audit_events`.
- **Read-Through Safety**: The system preserves all existing rows and treats them as valid. Historical
  approvals remain queryable through `get_tool_approvals` and API routes.
- **No Synthetic Backfilling**: The system strictly avoids fabricating retroactive audit events with fake
  timestamps, synthetic event IDs, or guessed caller identities.
- **Consistency Verification**: Operators can invoke `verify_approval_audit_consistency(operation_key)`
  to inspect records:
  - For atomic records, it returns `is_consistent=True` and links the corresponding audit event IDs.
  - For legacy records lacking audit events, it returns
    `is_consistent=True, is_legacy_uncommitted_audit=True` without raising errors or altering database
    state.

---

## 6. Deterministic Test Results

### 6.1 Focused Suite: `tests/test_atomic_approval_audit.py`
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_atomic_approval_audit.py -v
```
Results (10 passed in 2.36s):
- `test_atomic_expiration_rolls_back_on_audit_failure`: PASSED
- `test_batch_expiration_savepoint_rollback_on_single_failure`: PASSED
- `test_atomic_decision_rolls_back_on_audit_failure`: PASSED
- `test_atomic_revocation_rolls_back_on_audit_failure`: PASSED
- `test_atomic_recovery_rolls_back_on_audit_failure`: PASSED
- `test_atomic_successful_approval_and_audit_committed`: PASSED
- `test_audit_deduplication_on_retry`: PASSED
- `test_legacy_record_safety_no_fake_events`: PASSED
- `test_stale_worker_fencing_rolls_back_audit`: PASSED
- `test_concurrent_execution_audit_consistency`: PASSED

### 6.2 Approval Lifecycle & Scheduler Regression Suite (90 Tests)
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_atomic_approval_audit.py `
  multi-agent-orchestrator/tests/test_approval_expiration.py `
  multi-agent-orchestrator/tests/test_approval_scheduler.py `
  multi-agent-orchestrator/tests/test_scheduler_lease.py `
  multi-agent-orchestrator/tests/test_scheduler_scan_recovery.py `
  multi-agent-orchestrator/tests/test_tool_approval_integrity.py `
  multi-agent-orchestrator/tests/test_tool_approval_observability.py -v
```
Result: **90 passed in 11.32s**.

### 6.3 API, Security & Admission Regressions (64 Tests)
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_api.py `
  multi-agent-orchestrator/tests/test_api_security.py `
  multi-agent-orchestrator/tests/test_admission_fencing.py `
  multi-agent-orchestrator/tests/test_cancellation_deadlines.py `
  multi-agent-orchestrator/tests/test_cross_process_admission.py -v
```
Result: **64 passed in 41.11s** (55 passed in 36.59s + 9 passed in 4.52s).

### 6.4 Static Analysis & Linting
Command:
```powershell
python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests
```
Output:
```text
All checks passed!
```

---

## 7. Known Limitations & Remaining Boundaries

1. **Scope of Atomicity**:
   - Single-transaction atomicity is guaranteed for records residing within the SQLite database
     (`tool_approvals` and `audit_events`).
   - Atomicity does not extend across external systems, such as downstream HTTP webhooks, Slack
     notifications, or message brokers. External side effects remain at-least-once or best-effort.
2. **General Workflow Non-Approval Events**:
   - Non-approval workflow events (such as general run status logs or agent step traces) retain the
     safe-failure logging policy (`raise_on_error=False`) so non-critical observability hiccups do not
     abort active agent tasks.
3. **Cross-Database Audit Logging**:
   - If an operator deploys a separate, dedicated database connection exclusively for audit logs in the
     future, two-phase commit (2PC) or an outbox pattern would be required to maintain cross-database
     atomicity. SQLite does not support distributed two-phase commit.

---

## 8. Conclusion

Task 49 is complete. Approval lifecycle mutations and their audit event records are co-located in
atomic single-connection SQLite transactions. Rollback occurs automatically on audit write failures,
deduplication protects retries and concurrent operations, fencing prevents stale scheduled writes,
and legacy data integrity is preserved without synthetic backfilling.
