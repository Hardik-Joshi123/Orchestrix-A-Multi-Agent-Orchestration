# Task 51 Report: Outbox Delivery Observability & Operational Recovery

## 1. Executive Summary

Task 51 extends the transactional outbox architecture introduced in Task 50 with durable delivery metrics,
granular failure diagnostics, append-only delivery history tracking, and authenticated operator recovery
safeguards. While Task 50 ensured atomic persistence and cross-process leased dispatching, operators
previously lacked persistent latency percentiles, expired-claim detection, and auditable manual recovery
mechanisms for stuck or dead-letter notifications.

Under Task 51:
- **Durable Persisted Metrics**: Metrics are derived directly from SQLite persistence tables rather than
  relying solely on in-memory counters. Telemetry survives process restarts and multi-worker scale-out.
  Aggregates include status counts (`pending`, `claimed`, `delivered`, `dead_letter`), oldest pending age,
  retry/attempt counts, delivery latency percentiles (`min`, `max`, `avg`, `p95`), and recent failure logs.
- **Granular Diagnostics**: Surfaces expired claim leases (hung or crashed workers), repeated delivery
  failures, and events approaching or exceeding configured retry thresholds.
- **Safe, Authenticated Operator Recovery**: Introduces single-event retry and batch recovery protocols
  requiring mandatory non-empty operator reasons, structured audit events (`outbox.retry_requested`,
  `outbox.recovered`), atomic state transition validation, and active lease protection (cannot reset an
  active claim unless `force=true`).
- **Append-Only History Audit Trail**: Delivery attempts, worker claims, delivery outcomes, failures,
  and operator recovery actions are permanently recorded in `outbox_delivery_history`. Delivery history is
  never erased or overwritten.
- **Sanitization & Redaction**: Sensitive arguments, tokens, and authorization headers in error messages
  are sanitized before persistence and API responses.
- **Preserved Guarantees**: Approval state, audit log, and outbox intent remain atomic. Monotonic fencing
  tokens (`fence_token`) protect against stale workers. Delivery remains at-least-once with receiver-side
  idempotency; tool executions are never automatically approved or re-dispatched.

---

## 2. Modified & Created Files

- `src/orchestrator/observability/audit.py`:
  - Added audit event types: `AuditEventType.OUTBOX_RETRY_REQUESTED`, `AuditEventType.OUTBOX_RECOVERED`.
- `src/orchestrator/persistence/store.py`:
  - Added `outbox_delivery_history` table and `idx_outbox_history_event` index in `_init_db()`.
  - Added columns to `outbox_events`: `retry_count`, `last_recovery_reason`, `last_recovery_by`,
    `last_recovery_at`.
  - Updated `insert_outbox_event`, `claim_outbox_events`, and `update_outbox_event_delivery` to record
    chronological actions (`created`, `claim`, `delivered`, `failed`, `dead_letter`) to delivery history.
  - Enhanced `retry_outbox_event` with reason validation, operator identity, active claim lease protection
    (raises `ValueError` if active claim exists and `force=False`), monotonic fencing advancement,
    audit event recording, and history logging.
  - Implemented `recover_outbox_events` for bounded batch recovery of dead-letter/failed events.
  - Implemented `get_outbox_event_history` for chronological delivery attempt inspection.
  - Enhanced `get_outbox_diagnostics(max_attempts)` with durable status counts, oldest pending event age,
    retry metrics, delivery latency percentiles, recent sanitized failures, expired claims, and retry
    limit diagnostics.
  - Bound all new methods in `FencedCheckpointStore`.
- `src/orchestrator/api/models.py`:
  - Updated `OutboxEventModel` with recovery metadata fields.
  - Updated `OutboxDiagnosticsResponse` with `oldest_pending_age_seconds`, `retry_metrics`,
    `delivery_latency`, `recent_failures`, `expired_claims_count`, `expired_claims`,
    `retry_limit_diagnostics`.
  - Added `RetryOutboxEventRequest`, `RecoverOutboxEventsRequest`, `RecoverOutboxEventsResponse`,
    `OutboxHistoryEntryModel`, and `OutboxHistoryResponse`.
- `src/orchestrator/api/routes.py`:
  - Updated `GET /approvals/outbox/diagnostics` to pass `max_attempts` and return comprehensive metrics.
  - Enhanced `POST /approvals/outbox/{event_id}/retry` to support JSON body/query params, mandatory reason,
    operator resolution, active claim conflict protection (HTTP 409), and force overrides.
  - Added `POST /approvals/outbox/recover` for authenticated, rate-limited batch recovery with reason.
  - Added `GET /approvals/outbox/{event_id}/history` for paginated chronological delivery history.
- `src/orchestrator/api/client.py`:
  - Updated `OrchestratorApiClient.retry_outbox_event` to accept reason, operator, force.
  - Added `OrchestratorApiClient.recover_outbox_events` and `get_outbox_event_history`.
- `docs/DEVELOPER_GUIDE.md`:
  - Added Section 12.16 ("Outbox Delivery Observability & Operational Recovery").
- `tests/test_outbox_observability_recovery.py` *(new)*:
  - 10 deterministic tests covering metrics, restart persistence, expired claims, limit diagnostics,
    redaction, active claim protection, audit logging, history, batch recovery, and SDK methods.

---

## 3. Metrics & Diagnostics Added

### 3.1 Durable Database Metrics
Exposed via `GET /approvals/outbox/diagnostics`:
1. **Status Breakdown**: Durable counts grouped by status (`pending`, `claimed`, `delivered`, `dead_letter`).
2. **Oldest Pending Event & Age**: Creation timestamp and computed age in seconds:
   $$\text{age} = \max(0.0, t_{\text{now}} - t_{\text{created}})$$
3. **Retry Metrics**:
   - `total_attempts`: Sum of all worker dispatch attempts recorded in SQLite.
   - `total_retries`: Sum of all manual operator recovery actions.
   - `retried_events_count`: Number of distinct events that underwent retries.
   - `repeated_failures_count`: Events with 2 or more failed delivery attempts.
4. **Delivery Latency Statistics**:
   Sampled across recent successful deliveries $(t_{\text{delivered}} - t_{\text{created}})$:
   - `avg_latency_seconds`: Arithmetic mean delivery duration.
   - `min_latency_seconds`: Minimum recorded delivery latency.
   - `max_latency_seconds`: Maximum recorded delivery latency.
   - `p95_latency_seconds`: 95th percentile delivery latency.
   - `delivered_samples_count`: Total samples analyzed in the latency window.
5. **Recent Failures**:
   Top 10 recent delivery failures with sanitized `last_error` text (tokens/passwords redacted).
6. **Expired Claims**:
   Active claims where `status = 'claimed' AND claim_expires_at <= now`, identifying stalled workers.
7. **Retry Limit Diagnostics**:
   - `approaching_limit_count`: Events in `pending` status with `attempt_count >= max_attempts - 1`.
   - `exceeded_limit_count`: Events in `dead_letter` status or `attempt_count >= max_attempts`.

---

## 4. Operator Recovery & Authorization Safeguards

1. **Mandatory Non-Empty Reason**:
   - Every recovery request requires an explicit justification string.
   - Empty or whitespace strings return HTTP 400 Bad Request (`Recovery reason cannot be empty`).
2. **Active Claim Lease Protection**:
   - If an event is currently held by an active worker (`claim_expires_at > now`), a standard recovery
     attempt is rejected with HTTP 409 Conflict.
   - If the operator confirms the worker crashed or hung ungracefully, passing `force=true` overrides the
     lease, resets status to `pending`, and advances the fencing token.
3. **Monotonic Fencing Advancement**:
   - Every recovery increments `fence_token = fence_token + 1`.
   - If a previous stalled worker attempts to update delivery status later, the store rejects it with
     `StaleDispatcherError`, preventing stale workers from overwriting recovery progress.
4. **Structured Audit Logging**:
   - Persists an `outbox.retry_requested` structured audit record with operator identity, reason,
     previous status, and attempt counts.
5. **Append-Only History Log**:
   - Every creation, claim, delivery, failure, and recovery is written to `outbox_delivery_history`.
   - Operators can inspect the full chronological sequence via `GET /approvals/outbox/{event_id}/history`.

---

## 5. Test Commands & Verification Results

### 5.1 Focused Suite: `tests/test_outbox_observability_recovery.py`
Command:
```powershell
python -m pytest tests/test_outbox_observability_recovery.py -v
```
Results (10 passed in 2.54s):
- `test_durable_delivery_metrics_and_latency`: PASSED
- `test_metrics_persist_across_store_restarts`: PASSED
- `test_expired_claims_diagnostics`: PASSED
- `test_retry_limit_diagnostics_and_approaching_events`: PASSED
- `test_recent_failures_sanitization`: PASSED
- `test_manual_recovery_requires_reason`: PASSED
- `test_manual_recovery_protects_active_claims`: PASSED
- `test_manual_recovery_emits_audit_and_preserves_history`: PASSED
- `test_batch_operator_recovery_and_authorization`: PASSED
- `test_sdk_methods_for_observability_and_recovery`: PASSED

### 5.2 Approval Lifecycle & Outbox Regressions (102 Tests)
Command:
```powershell
python -m pytest tests/test_transactional_outbox.py `
  tests/test_atomic_approval_audit.py `
  tests/test_approval_expiration.py `
  tests/test_approval_scheduler.py `
  tests/test_scheduler_lease.py `
  tests/test_scheduler_scan_recovery.py `
  tests/test_tool_approval_integrity.py `
  tests/test_tool_approval_observability.py -v
```
Result: **102 passed in 11.26s**.

### 5.3 API, Security & Fencing Regressions (55 Tests)
Command:
```powershell
python -m pytest tests/test_api.py `
  tests/test_api_security.py `
  tests/test_admission_fencing.py `
  tests/test_cancellation_deadlines.py -v
```
Result: **55 passed in 37.99s**.

### 5.4 Static Analysis & Linting
Command:
```powershell
python -m ruff check src tests
```
Result:
```text
All checks passed!
```

### 5.5 Line Length Compliance
Command:
```powershell
python -c "
files = [
    'src/orchestrator/config.py',
    'src/orchestrator/persistence/store.py',
    'src/orchestrator/api/models.py',
    'src/orchestrator/api/routes.py',
    'src/orchestrator/api/client.py',
    'tests/test_outbox_observability_recovery.py',
    'reports/TASK_51_OUTBOX_OBSERVABILITY_RECOVERY.md',
]
total = sum(
    1 for fp in files for idx, l in enumerate(open(fp, encoding='utf-8'), 1)
    if len(l.rstrip('\r\n')) > 110
)
print('Total lines > 110:', total)
"
```
Result: **Total lines > 110: 0**.

---

## 6. Tests Not Run (Out of Scope)
- LLM provider live fallback tests (`test_llm_fallback.py`) were not run to prevent unnecessary API usage.
- UI dashboard visual regression tests (`test_dashboard.py`) were not run as Task 51 is purely backend API
  and persistence outbox recovery.

---

## 7. Known Limitations & Follow-Up Boundaries

1. **Receiver-Side Deduplication Requirement**:
   - Outbox delivery operates under at-least-once semantics. If a network interruption occurs between external
     transmission and SQLite commit, an event may be redelivered. Downstream consumers must deduplicate
     using `idempotency_key`.
2. **SQLite Write Concurrency Under Large Batch Recovery**:
   - Batch recovery of large volumes of dead-letter events acquires SQLite `BEGIN IMMEDIATE` locks. Operators
     should use `max_events` batch limits (default 50, max 500) to avoid locking out concurrent API workers.
3. **No Automatic Tool Execution**:
   - Outbox recovery only manages lifecycle notification intents; it never approves tools, bypasses HITL
     validation, or re-dispatches underlying tool executions.

---

## 8. Conclusion

Task 51 is fully complete. All delivery metrics are durably computed from SQLite storage, expired leases and
retry limits are surfaced in operational diagnostics, recovery actions are safely guarded with mandatory
reasons and audit logging, and all regression suites pass cleanly with zero lint warnings.
