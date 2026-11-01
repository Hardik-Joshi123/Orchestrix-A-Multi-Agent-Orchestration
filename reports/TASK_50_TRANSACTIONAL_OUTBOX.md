# Task 50 Report: Transactional Outbox for Reliable External Notifications

## 1. Executive Summary

Task 50 introduces a durable transactional outbox pattern to ensure approval lifecycle events can be
delivered to external notification handlers reliably. In previous tasks, approval state transitions and
their audit events were committed atomically inside SQLite, but any external notification transmission
remained vulnerable to process interruption, worker crashes, and network partitions. If a worker died
immediately after committing an approval transition, notification intent was permanently lost.

Under Task 50:
- **ACID Co-Located Outbox Persistence**: Every tool approval lifecycle event (`tool.approval_requested`,
  `tool.approval_granted`, `tool.approval_rejected`, `tool.approval_revoked`, `tool.approval_expired`,
  `tool.approval_recovered`, `tool.approval_escalated`) is inserted into a durable `outbox_events` SQLite
  table within the exact same connection transaction as the approval state update and audit log event.
- **Fail-Closed Rollback**: If outbox record insertion fails, the entire transaction rolls back, ensuring
  zero un-notified approval state changes. In batch scans, each row's outbox write executes within an
  isolated savepoint (`SAVEPOINT sp_expire_{idx}`).
- **Bounded Cross-Process Dispatcher**: A background `OutboxDispatcher` safely claims pending and expired
  events in bounded batches using durable SQLite leases and monotonic fencing tokens (`fence_token`).
- **Fencing Protection Against Stale Dispatchers**: If a dispatcher's claim lease expires and is taken
  over by another worker, the stale dispatcher's subsequent delivery update is rejected with
  `StaleDispatcherError`, preventing stale or crashed workers from corrupting outbox delivery state.
- **Configurable Retries & Dead-Letter Handling**: Failed deliveries undergo bounded exponential backoff
  with configurable base/max intervals. Events exceeding `outbox_max_attempts` (or encountering terminal
  non-retryable errors) transition to durable `dead_letter` status with sanitized error metadata.
- **Notification Handler Interface**: A clean `NotificationHandler` interface and deterministic in-memory
  `TestNotificationHandler` allow comprehensive simulation without network calls or third-party dependencies.
- **Receiver-Side Idempotency**: Each outbox event includes a stable `idempotency_key` so external
  receivers can safely deduplicate repeated transmissions.
- **Operational Inspection & Safe Retry**: Authenticated REST API endpoints and SDK client methods enable
  operators to inspect outbox status, query diagnostics, trigger bounded dispatch cycles, and manually
  retry dead-letter events.

---

## 2. Modified & Created Files

- `src/orchestrator/config.py`:
  - Added outbox configuration settings: `outbox_enabled`, `outbox_dispatcher_enabled`,
    `outbox_poll_interval_seconds`, `outbox_batch_size`, `outbox_max_attempts`,
    `outbox_retry_backoff_base_seconds`, `outbox_retry_backoff_max_seconds`, `outbox_claim_ttl_seconds`.
  - Added field validators and cross-field validation rules.
  - Included outbox metrics in startup diagnostics dictionary.
- `src/orchestrator/notifications/handler.py` *(new)*:
  - Defined `NotificationDeliveryResult` dataclass.
  - Defined `NotificationHandler` abstract base class.
  - Implemented `TestNotificationHandler` with support for simulated transient errors, counted failures,
    non-retryable terminal errors, delivered event inspection, and receiver-side idempotency tracking.
- `src/orchestrator/notifications/dispatcher.py` *(new)*:
  - Implemented `OutboxDispatcher` background worker coordinating bounded dispatch cycles.
  - Integrated monotonic fencing tokens, safe exception handling, and graceful thread shutdown.
  - Implemented singleton factory `get_outbox_dispatcher()`.
- `src/orchestrator/notifications/__init__.py` *(new)*:
  - Module exports for notifications package.
- `src/orchestrator/persistence/store.py`:
  - Created `outbox_events` table and performance indices in `_init_db()` with schema migration checks.
  - Added `StaleDispatcherError` exception.
  - Added store methods: `insert_outbox_event`, `claim_outbox_events`, `update_outbox_event_delivery`,
    `retry_outbox_event`, `get_outbox_events`, `get_outbox_diagnostics`.
  - Bound outbox insertion atomically inside approval lifecycle transitions: `record_tool_approval_request`,
    `record_tool_approval`, `revoke_tool_approval`, `recover_tool_approval`, `expire_tool_approval`,
    `escalate_tool_approval`, and `check_and_expire_tool_approvals` (inside row savepoint).
  - Forwarded outbox methods in `FencedCheckpointStore`.
- `src/orchestrator/api/models.py`:
  - Added models: `OutboxEventModel`, `ListOutboxEventsResponse`, `OutboxDiagnosticsResponse`,
    `RetryOutboxEventResponse`, `DispatchOutboxResponse`.
- `src/orchestrator/api/routes.py`:
  - Added operational REST endpoints: `GET /approvals/outbox`, `GET /approvals/outbox/diagnostics`,
    `POST /approvals/outbox/{event_id}/retry`, `POST /approvals/outbox/dispatch`.
- `src/orchestrator/api/client.py`:
  - Added SDK methods: `list_outbox_events`, `get_outbox_diagnostics`, `retry_outbox_event`,
    `dispatch_outbox_events`.
- `src/orchestrator/api/server.py`:
  - Integrated `OutboxDispatcher` startup and graceful bounded shutdown into FastAPI `lifespan`.
- `docs/DEVELOPER_GUIDE.md`:
  - Added Section 12.15: "Transactional Outbox for Reliable External Notifications".
- `tests/test_transactional_outbox.py` *(new)*:
  - 12 deterministic unit and integration tests covering all requirements.

---

## 3. Transaction Guarantees & ACID Delivery Semantics

### 3.1 Single-Connection Atomicity
`tool_approvals`, `audit_events`, and `outbox_events` reside in the identical SQLite database file.
All three operations execute on a single connection `conn` within a single `BEGIN IMMEDIATE` or savepoint:
```text
BEGIN IMMEDIATE;
UPDATE tool_approvals SET ...;
INSERT INTO audit_events ...;
INSERT INTO outbox_events (event_id, event_type, payload, status='pending', ...) VALUES (...);
COMMIT;
```
If any error occurs during state mutation, audit logging, or outbox persistence:
- The entire transaction rolls back via `conn.rollback()` or savepoint rollback.
- An approval state transition is never committed without its matching audit event and outbox notification.

### 3.2 Delivery Guarantees & Receiver-Side Idempotency
- **At-Least-Once Delivery**: The outbox dispatcher guarantees that all committed lifecycle events are
  transmitted to registered external handlers at least once.
- **Non-Guarantee of External Exactly-Once**: Exactly-once delivery cannot be mathematically guaranteed
  to external endpoints without receiver-side deduplication. If a process terminates or network connectivity
  drops after external transmission but before SQLite marks the record `delivered`, the event will be
  re-delivered on recovery.
- **Stable Idempotency Keys**: Every outbox record includes a stable `idempotency_key` (e.g.
  `outbox:tool.approval_granted:{operation_key}:{approver}`) allowing receiving webhooks and notification
  gateways to deduplicate duplicate deliveries deterministically.

---

## 4. Bounded Dispatcher & Multi-Worker Fencing

### 4.1 Claim Protocol
- The dispatcher claims up to `outbox_batch_size` events where:
  `(status = 'pending' AND next_retry_at <= :now) OR (status = 'claimed' AND claim_expires_at <= :now)`
- Each claim atomically increments `fence_token = fence_token + 1` and updates `claimed_by = :owner_id`,
  `claimed_at = :now`, `claim_expires_at = :claim_expires_at`, and `attempt_count = attempt_count + 1`.

### 4.2 Stale-Dispatcher Fencing Protection
When a dispatcher finishes delivery and calls `update_outbox_event_delivery()` with its claim token:
- SQLite verifies `WHERE event_id = ? AND fence_token = ? AND claimed_by = ?`.
- If the lease expired while the dispatcher was slow and another worker claimed the event, `fence_token`
  has advanced.
- The update fails and the store raises `StaleDispatcherError`. The stale dispatcher is blocked from
  modifying outbox state or overwriting subsequent progress.

---

## 5. Retry Backoff & Dead-Letter State Machine

1. **Successful Delivery**:
   - `status = 'delivered'`, `delivered_at = :now`, `last_error = NULL`.
   - Claim fields cleared (`claimed_by = NULL`, `claim_expires_at = NULL`).
2. **Transient Delivery Failure** (`attempt_count < outbox_max_attempts` and `retryable=True`):
   - Computes exponential backoff:
     `backoff = min(outbox_retry_backoff_max_seconds, outbox_retry_backoff_base_seconds * 2^(attempts - 1))`
   - `status = 'pending'`, `next_retry_at = :now + backoff`, `last_error = sanitize_error(error)`.
   - Claim fields cleared, allowing scheduled retry.
3. **Terminal Failure** (`attempt_count >= outbox_max_attempts` or `retryable=False`):
   - `status = 'dead_letter'`, `last_error = sanitize_error(error)`.
   - Claim fields cleared. Event is held durably for operational inspection and manual retry.

---

## 6. Operational REST Endpoints & SDK Client

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/approvals/outbox` | `GET` | Paginated query of outbox events with `status` and `event_type` filtering. |
| `/approvals/outbox/diagnostics` | `GET` | Status counts, oldest pending timestamp, dispatcher metrics. |
| `/approvals/outbox/{event_id}/retry` | `POST` | Reset a failed/dead-letter event back to `pending`. |
| `/approvals/outbox/dispatch` | `POST` | Manually execute a bounded dispatch cycle. |

---

## 7. Deterministic Test Results

### 7.1 Focused Suite: `tests/test_transactional_outbox.py`
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_transactional_outbox.py -v
```
Results (12 passed in 2.31s):
- `test_outbox_atomic_insert_with_approval`: PASSED
- `test_outbox_rollback_on_outbox_failure`: PASSED
- `test_batch_expiration_savepoint_outbox_rollback`: PASSED
- `test_dispatcher_successful_delivery`: PASSED
- `test_dispatcher_retry_with_bounded_backoff`: PASSED
- `test_dispatcher_dead_letter_after_max_attempts`: PASSED
- `test_dispatcher_non_retryable_dead_letter`: PASSED
- `test_dispatcher_stale_owner_fencing`: PASSED
- `test_concurrent_dispatchers_claim_disjoint_events`: PASSED
- `test_handler_idempotency_deduplication`: PASSED
- `test_manual_retry_of_dead_letter`: PASSED
- `test_outbox_api_routes`: PASSED

### 7.2 Approval Lifecycle & Outbox Regression Suite (102 Tests)
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_transactional_outbox.py `
  multi-agent-orchestrator/tests/test_atomic_approval_audit.py `
  multi-agent-orchestrator/tests/test_approval_expiration.py `
  multi-agent-orchestrator/tests/test_approval_scheduler.py `
  multi-agent-orchestrator/tests/test_scheduler_lease.py `
  multi-agent-orchestrator/tests/test_scheduler_scan_recovery.py `
  multi-agent-orchestrator/tests/test_tool_approval_integrity.py `
  multi-agent-orchestrator/tests/test_tool_approval_observability.py -v
```
Result: **102 passed in 12.36s**.

### 7.3 API, Security & Admission Regressions (55 Tests)
Command:
```powershell
python -m pytest multi-agent-orchestrator/tests/test_api.py `
  multi-agent-orchestrator/tests/test_api_security.py `
  multi-agent-orchestrator/tests/test_admission_fencing.py `
  multi-agent-orchestrator/tests/test_cancellation_deadlines.py -v
```
Result: **55 passed in 37.06s**.

### 7.4 Static Analysis & Linting
Command:
```powershell
python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests
```
Output:
```text
All checks passed!
```

---

## 8. Known Limitations & Remaining Boundaries

1. **Single-Writer SQLite Lock Contention**:
   - Under heavy concurrent dispatching across numerous worker processes, SQLite transaction locks can
     experience contention. Dispatchers employ bounded batch claiming and exponential backoff to minimize
     transaction duration.
2. **External Notification Guarantee Boundary**:
   - At-least-once delivery is guaranteed up to the external notification handler boundary.
   - Downstream notification recipients must implement receiver-side deduplication based on `idempotency_key`
     or `event_id` to guarantee strictly single external user-facing notifications.
3. **No Automatic Tool Re-Execution**:
   - The outbox mechanism only delivers lifecycle notification intents; it never automatically approves
     tools or redispatches tool execution.

---

## 9. Conclusion

Task 50 is complete. Approval lifecycle state transitions, audit trail events, and outbox notification
records are persist co-located in atomic SQLite transactions. Bounded cross-process dispatching, monotonic
fencing protection against stale workers, configurable exponential backoff retries, durable dead-letter
handling, and authenticated inspection endpoints operate deterministically.
