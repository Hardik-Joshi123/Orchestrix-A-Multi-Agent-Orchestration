# Task 41: Tool Execution Monitoring & Safe Reconciliation Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 41 establishes a secure operational monitoring and safe reconciliation workflow for external tool executions. While Task 40 introduced deterministic operation keys, downstream idempotency keys, double-gated fencing, and tracking of uncertain tool outcomes, operators previously lacked authenticated APIs to inspect tool execution state, scrub sensitive credentials, and safely reconcile uncertain operations without triggering unintended duplicate side effects.

Task 41 delivers an **end-to-end, privacy-preserving reconciliation system**:
- **Authenticated Tool Monitoring Endpoints**:
  - `GET /runs/{run_id}/tools`: Lists tool executions for a workflow run with filtering by `status` (e.g., `?status=uncertain`) and `tool_name`.
  - `GET /runs/{run_id}/tools/{operation_key}`: Fetches detailed status and reconciliation metadata for a specific operation key.
- **Strict Privacy Shielding & Credential Redaction**:
  - Automatically redacts sensitive keys (`api_key`, `token`, `password`, `secret`, `bearer`, `auth`, `credentials`) to `[REDACTED]`.
  - Scrubs `sk-...` API keys and Bearer tokens in string values.
  - Strips Python tracebacks and internal stack traces from error messages, retaining only the sanitized exception summary.
- **Explicit Operator Reconciliation Workflow**:
  - `POST /runs/{run_id}/tools/{operation_key}/reconcile`: Allows authorized operators to record a verified outcome (`completed` or `failed`) with a mandatory explanation (`reason`).
  - Reconciling to `completed`: Validates external side-effect success out-of-band and records verified results; resumed workflows short-circuit to this result without redispatching.
  - Reconciling to `failed`: Validates external failure; resumed workflows raise `ToolExecutionError` and do not redispatch.
- **Atomic Transition Engine & Idempotent Repetition**:
  - Governed by `BEGIN IMMEDIATE` exclusive write locks in SQLite.
  - In-flight (`pending`) executions cannot be reconciled (`HTTP 409 Conflict`).
  - Terminal states (`completed` or `failed`) cannot be transitioned to conflicting outcomes (`HTTP 409 Conflict`).
  - Repeated identical reconciliation requests return `HTTP 200 OK` with `idempotent=True`.
- **Structured Audit Logging**: Emits `tool.reconciled` audit events with actor, operation key, tool name, previous status, target status, and explanation.
- **Zero Automatic Redispatch**: The system never automatically redispatches uncertain operations during inspection, reconciliation, recovery, or resume.

---

## 2. Operational Workflow & State Transitions

```
                         [Uncertain Tool Execution]
                    (Network timeout, drop, or node crash)
                                   │
                                   ▼
                   [Operator Inspects via API]
              GET /runs/{run_id}/tools?status=uncertain
                   (Sensitive arguments redacted)
                                   │
                                   ▼
             [Operator Verifies External Provider State]
               (Stripe portal, Cloud console, MTA logs)
                                   │
            ┌──────────────────────┴──────────────────────┐
            ▼                                             ▼
 [Side Effect Occurred]                        [Side Effect Did NOT Occur]
POST .../reconcile                            POST .../reconcile
{ outcome: "completed",                       { outcome: "failed",
  reason: "Verified tx-123 in Stripe",          reason: "Carrier confirmed dropped",
  result_text: "tx-123:paid" }                  error_text: "Rejected by MTA" }
            │                                             │
            ▼                                             ▼
   [status='completed']                          [status='failed']
 (tool.reconciled audit event)                 (tool.reconciled audit event)
            │                                             │
            ▼                                             ▼
   [POST /runs/{id}/resume]                      [POST /runs/{id}/resume]
Returns cached result_text                    Raises ToolExecutionError
  (Zero Redispatch)                             (Zero Redispatch)
```

---

## 3. API Endpoints Specification

### 3.1 `GET /runs/{run_id}/tools`
- **Description**: List all tool execution records for a workflow run.
- **Authentication**: Requires valid Bearer token (`ORCHESTRATOR_API_TOKEN`).
- **Query Parameters**:
  - `status` (optional): Filter by status (`pending`, `completed`, `failed`, `uncertain`).
  - `tool_name` (optional): Filter by tool name.
- **Response Model**: `RunToolExecutionsResponse`
  - `run_id`: string
  - `total_executions`: integer
  - `executions`: list of `ToolExecutionModel` (with sanitized `args`, `result_text`, and `error_text`).

### 3.2 `GET /runs/{run_id}/tools/{operation_key}`
- **Description**: Inspect details for a single tool execution.
- **Authentication**: Requires valid Bearer token.
- **Response Model**: `ToolExecutionModel`
  - Includes `operation_key`, `tool_name`, `status`, `args` (redacted), `result_text`, `error_text`, `reconciled_by`, `reconciled_at`, `reconciliation_reason`.

### 3.3 `POST /runs/{run_id}/tools/{operation_key}/reconcile`
- **Description**: Atomically reconcile an uncertain execution to `completed` or `failed`.
- **Authentication**: Requires valid Bearer token; rate-limited.
- **Request Body**: `ReconcileToolExecutionRequest`
  - `outcome`: `"completed"` or `"failed"` (mandatory)
  - `reason`: Explanation of operator's decision (mandatory, 3–1000 characters)
  - `result_text`: Optional verified result string
  - `error_text`: Optional verified error string
  - `operator`: Optional operator identifier (defaults to client scope)
- **Response Model**: `ReconcileToolExecutionResponse`
  - `success`: boolean
  - `idempotent`: boolean
  - `previous_status`: string
  - `status`: string
  - `reconciled_by`: string
  - `reconciled_at`: string
  - `reconciliation_reason`: string
  - `audit_event_id`: string
  - `execution`: sanitized `ToolExecutionModel`

---

## 4. Summary of Codebase Modifications

| File | Purpose of Modifications |
|---|---|
| `src/orchestrator/persistence/store.py` | Added columns `reconciled_by`, `reconciled_at`, `reconciliation_reason` to `tool_executions` schema and migration; updated `get_tool_execution` and `list_tool_executions` to return reconciliation fields and support filtering; implemented atomic `reconcile_tool_execution` with `BEGIN IMMEDIATE` locks; defined `ToolExecutionNotFoundError` and `ToolReconciliationConflictError`. |
| `src/orchestrator/persistence/__init__.py` | Exported `ToolExecutionNotFoundError` and `ToolReconciliationConflictError`. |
| `src/orchestrator/observability/audit.py` | Added `AuditEventType.TOOL_RECONCILED = "tool.reconciled"`; implemented `sanitize_tool_arguments` and `sanitize_tool_execution` to scrub sensitive credentials and tracebacks. |
| `src/orchestrator/tools/registry.py` | Hardened `Tool.__call__` to verify that mutating non-idempotent or operator-reconciled failed tools raise `ToolExecutionError` and never redispatch during workflow resumes. |
| `src/orchestrator/api/models.py` | Added Pydantic schemas: `ToolExecutionModel`, `RunToolExecutionsResponse`, `ReconcileToolExecutionRequest`, `ReconcileToolExecutionResponse`. |
| `src/orchestrator/api/routes.py` | Implemented `GET /runs/{run_id}/tools`, `GET /runs/{run_id}/tools/{operation_key}`, and `POST /runs/{run_id}/tools/{operation_key}/reconcile` with auth, validation, and audit logging. |
| `src/orchestrator/api/client.py` | Added client methods: `get_run_tools`, `get_tool_execution`, `reconcile_tool_execution`. |
| `docs/DEVELOPER_GUIDE.md` | Added Section 12.6 detailing endpoint usage, reconciliation procedure, privacy shielding rules, and recovery limitations. |
| `tests/test_tool_execution_reconciliation.py` | Added 17 deterministic offline tests covering authentication, redaction, state transitions, idempotency, concurrent races, resume guarantees, and client SDK. |

---

## 5. Verification & Test Coverage Matrix

### 5.1 Dedicated Test Suite (`tests/test_tool_execution_reconciliation.py`)

| Test Function | Target Scenario | Result |
|---|---|---|
| `test_inspect_tool_executions_authentication` | Unauthenticated requests rejected (401); valid Bearer accepted (200) | **PASS** |
| `test_reconcile_authentication` | Reconciliation endpoint enforces API token authentication | **PASS** |
| `test_inspect_tool_executions_redaction` | Arguments redacted (`[REDACTED]`), tracebacks and secrets scrubbed | **PASS** |
| `test_inspect_invalid_run_not_found` | Non-existent run ID returns 404 | **PASS** |
| `test_inspect_single_tool_execution_not_found` | Non-existent operation key returns 404 | **PASS** |
| `test_reconcile_missing_reason_rejected` | Omitted or whitespace reconciliation reason rejected with 400 | **PASS** |
| `test_reconcile_invalid_outcome_rejected` | Unsupported outcome string rejected with 400 | **PASS** |
| `test_reconcile_uncertain_to_completed` | Uncertain transitions to completed, updates SQLite, records audit event | **PASS** |
| `test_reconcile_uncertain_to_failed` | Uncertain transitions to failed, updates SQLite, records audit event | **PASS** |
| `test_reconcile_pending_rejected` | Reconciling in-flight execution rejected with 409 Conflict | **PASS** |
| `test_reconcile_conflicting_terminal_outcomes_rejected` | Reconciling completed to failed rejected with 409 Conflict | **PASS** |
| `test_reconcile_idempotent_repeated_requests` | Repeated identical reconciliation returns 200 with `idempotent=True` | **PASS** |
| `test_concurrent_reconciliation_race` | Concurrent conflicting reconciliation requests resolve safely (one winner, rest 409) | **PASS** |
| `test_resumed_run_with_reconciled_completed_tool` | Resumed run short-circuits to verified result text without redispatching | **PASS** |
| `test_resumed_run_with_reconciled_failed_tool` | Resumed run raises `ToolExecutionError` without redispatching | **PASS** |
| `test_filter_by_status_and_tool_name` | Query filtering by `status` and `tool_name` works correctly | **PASS** |
| `test_client_sdk_tool_methods` | `OrchestratorApiClient` methods function as expected | **PASS** |

### 5.2 Suite Verification Metrics
- `pytest tests/test_tool_execution_reconciliation.py`: **17 passed** in 4.07s.
- `pytest tests/test_tool_execution_idempotency.py`: **12 passed** in 4.03s.
- Full regression suite across Tasks 31–41: **119 passed** in 37.33s.
- `ruff check src tests`: **0 errors, all clean**.

---

## 6. Operational Boundaries & Known Limitations

1. **No Exactly-Once Guarantees Across Network Partitions**:
   - The orchestrator cannot guarantee that a remote provider did not complete a mutation if the TCP connection was abruptly reset before an ACK was received.
   - Operators must verify downstream external systems (via cloud dashboards, provider portals, or financial logs) before reconciling an uncertain execution.
2. **Reconciliation Target Scope**:
   - Only tool executions in the `uncertain` state can be reconciled to a new outcome. Active (`pending`) executions cannot be modified while workers are actively executing them.
3. **Resumed Subtasks**:
   - Reconciling a tool execution to `completed` allows downstream specialist nodes to consume the recorded `result_text`. Reconciling to `failed` halts the subtask with a documented failure, allowing the supervisor to assess replanning or escalation according to workflow policy.
