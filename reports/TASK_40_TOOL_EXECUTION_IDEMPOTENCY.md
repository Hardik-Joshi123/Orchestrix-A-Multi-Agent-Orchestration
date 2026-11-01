# Task 40: Safe Tool Execution & External Side-Effect Idempotency Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 40 audits and hardens external tool execution across the orchestrator lifecycle. In distributed multi-agent workflows, tool calls often perform side effects on external services (such as provisioning infrastructure, updating CRM systems, sending communications, or executing payment transactions). When transient network errors, timeouts, worker crashes, workflow retries, or lease fencing events occur, blindly repeating tool invocations risks catastrophic duplicate side effects.

Task 40 introduces a **robust, end-to-end tool execution safety framework**:
- **Tool Safety Taxonomy (`IdempotencyMode`)**:
  - `READ_ONLY`: Naturally repeatable operations (e.g., searches, file reads, calculations) that permit automatic retries up to `max_retries` without persisting tracking records.
  - `MUTATING_IDEMPOTENT`: Mutating operations where the downstream provider accepts idempotency keys. Injected with a deterministic orchestrator key and short-circuited on subsequent identical calls.
  - `MUTATING_NON_IDEMPOTENT`: Mutating operations without provider-side idempotency support. Automatic retries are strictly disabled (`effective_retries = 0`). Concurrent duplicate dispatches are blocked with `ToolDuplicateExecutionError`. Timeouts and socket drops trigger `ToolUncertainOutcomeError`.
- **Deterministic Operation Key Derivation**: Stable keys computed via SHA-256 over canonicalized arguments:
  $$\text{op\_key} = \text{toolop:}\{\text{run\_id}\}:\{\text{subtask\_id}\}:\{\text{tool\_name}\}:\{\text{hash}_{16}\}$$
- **Downstream Key Propagation**: Seamless injection of `idempotency_key=op_key` into tool callables declaring `idempotency_key` or `**kwargs` parameters.
- **Double-Gated Admission Fencing**:
  - *Pre-Dispatch Gate*: Blocks tool execution if the run is cooperatively cancelled or the worker has lost admission lease ownership.
  - *Post-Execution Gate*: Re-validates admission ownership prior to committing results to prevent zombie workers from recording stale tool outputs after prolonged network I/O.
- **Lifecycle Tracking & Recovery**:
  - Tracks states (`pending`, `completed`, `failed`, `uncertain`) in a dedicated SQLite `tool_executions` table.
  - During node crash recovery (`recover_run_as_interrupted`) or administrative cancellation (`cancel_run_administratively`), any in-flight `pending` tool executions are atomically transitioned to `uncertain`.
- **Comprehensive Verification**: 12 deterministic unit tests in `tests/test_tool_execution_idempotency.py` and 135 total regression tests across Tasks 31-40 passing cleanly offline.

---

## 2. Tool Safety Taxonomy & Semantics

| Idempotency Mode | Side Effects? | Downstream Key Forwarding? | Retry Policy | Concurrent / Repeat Dispatch Behavior |
|---|---|---|---|---|
| **`READ_ONLY`** | No | Optional | Allowed (up to `max_retries`) | Always repeatable. Bypasses `tool_executions` table. |
| **`MUTATING_IDEMPOTENT`** | Yes | Yes (`idempotency_key`) | Safe with key | If `completed`, immediately returns cached `result_text`. If `pending`, blocks duplicate. |
| **`MUTATING_NON_IDEMPOTENT`** | Yes | No | **Strictly Disabled** (`retries = 0`) | Blocks concurrent duplicate dispatch (`ToolDuplicateExecutionError`). If outcome uncertain, raises `ToolUncertainOutcomeError`. |

### 2.1 Handling Uncertain Outcomes
When a mutating non-idempotent operation experiences a timeout or network disconnect, the orchestrator cannot verify whether the external system processed the request before the connection dropped. 
- Rather than assuming failure and retrying (which could double-charge or duplicate external records), the orchestrator marks the execution status as `uncertain` in SQLite.
- Raises `ToolUncertainOutcomeError`, halting automated retries and surfacing the issue for human-in-the-loop review or operator intervention.
- Subsequent calls with the same operation key are blocked unless the tool explicitly declares `retry_on_uncertain=True`.

---

## 3. Architecture & Execution Pipeline

```
           [Tool.__call__(args, context)]
                         │
                         ▼
             [Step 1: Schema Validation]
                         │
                         ▼
             [Step 2: Rate Limiting Check]
                         │
                         ▼
      [Step 3: Operation Key Derivation (SHA-256)]
                         │
                         ▼
      [Step 4: Pre-Dispatch Cancellation Check] ────► Cancelled? ──► Raise ToolExecutionError
                         │
                         ▼
      [Step 5: Pre-Dispatch Admission Fencing] ────► Lease Lost? ──► Raise ToolFencingError
                         │
                         ▼
      [Step 6: Idempotency Record Check]
             ├── Status == 'completed'? ──► Short-circuit & Return Cached result_text
             ├── Status == 'pending'?   ──► Raise ToolDuplicateExecutionError
             └── Status == 'uncertain'? ──► Raise ToolUncertainOutcomeError (unless retry_on_uncertain)
                         │
                         ▼
      [Step 7: Record Dispatch (status='pending')]
                         │
                         ▼
      [Step 8: Execute Tool Callable with Timeout]
             ├── Injects idempotency_key if supported
             ├── Retries enabled ONLY for READ_ONLY
             └── Timeout/Drop ──► Record status='uncertain' ──► Raise ToolUncertainOutcomeError
                         │
                         ▼
      [Step 9: Post-Execution Admission Fencing] ──► Lease Lost? ──► Discard & Raise ToolFencingError
                         │
                         ▼
      [Step 10: Record Completion (status='completed')]
                         │
                         ▼
      [Step 11: Return Result Text to Caller]
```

---

## 4. Database Persistence Schema

The `tool_executions` table in SQLite provides durable tracking of all mutating tool invocations:

```sql
CREATE TABLE IF NOT EXISTS tool_executions (
    operation_key   TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    subtask_id      TEXT NOT NULL,
    tool_name       TEXT NOT NULL,
    fence_token     INTEGER NOT NULL,
    status          TEXT NOT NULL, -- pending, completed, failed, uncertain
    args_json       TEXT NOT NULL,
    result_text     TEXT,
    error_text      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_executions_run_id ON tool_executions(run_id);
CREATE INDEX IF NOT EXISTS idx_tool_executions_tool_name ON tool_executions(tool_name);
```

### Store Methods:
- `get_tool_execution(operation_key)`: Retrieves record state for short-circuit checks.
- `record_tool_dispatch(operation_key, run_id, subtask_id, tool_name, fence_token, args)`: Records `pending` status without overwriting an existing `completed` outcome.
- `record_tool_completion(operation_key, result_text, owner_id, fence_token)`: Atomically transitions to `completed` while re-validating worker admission ownership under `BEGIN IMMEDIATE`.
- `record_tool_failure(operation_key, error_text, uncertain, owner_id, fence_token)`: Atomically transitions to `failed` or `uncertain`.
- `list_tool_executions(run_id)`: Diagnostic endpoint to audit all operations for a workflow run.
- `recover_uncertain_tool_executions(run_id)`: Administrative reset for remediated uncertain operations.

---

## 5. Summary of Codebase Modifications

| File | Changes Made |
|---|---|
| `src/orchestrator/persistence/store.py` | Added `tool_executions` DDL and indexes; implemented tool tracking queries and mutations; integrated uncertain state transition into `recover_run_as_interrupted` and `cancel_run_administratively`; bound worker identity in `FencedCheckpointStore`. |
| `src/orchestrator/tools/registry.py` | Defined `IdempotencyMode` enum, `ToolExecutionContext`, exceptions (`ToolUncertainOutcomeError`, `ToolFencingError`, `ToolDuplicateExecutionError`); implemented `derive_operation_key`; implemented 11-step double-gated execution pipeline in `Tool.__call__`. |
| `src/orchestrator/tools/__init__.py` | Exported `IdempotencyMode`, `ToolExecutionContext`, `ToolUncertainOutcomeError`, `ToolFencingError`, `ToolDuplicateExecutionError`, and `derive_operation_key`. |
| `src/orchestrator/graph/specialists.py` | Updated `_run_specialist` to instantiate `ToolExecutionContext` from graph state and pass to `REGISTRY.call`. |
| `src/orchestrator/graph/builder.py` | Attached `_store` and `_cancellation_token` to `state_snapshot` in `_run_fallback` so specialists inherit execution context. |
| `docs/DEVELOPER_GUIDE.md` | Added Section 12 detailing taxonomy, key derivation, double-gating, lifecycle transitions, and registration code examples. |
| `tests/test_tool_execution_idempotency.py` | Created 12 deterministic offline tests verifying all idempotency and safety requirements. |

---

## 6. Verification & Test Coverage

### 6.1 Dedicated Test Suite (`tests/test_tool_execution_idempotency.py`)

| Test Case | Scenario Tested | Outcome |
|---|---|---|
| `test_read_only_tool_retries` | Read-only tool retries on transient error and succeeds without writing tracking row | **PASS** |
| `test_mutating_idempotent_forwards_key` | Idempotent mutating tool receives derived `idempotency_key` downstream | **PASS** |
| `test_mutating_idempotent_short_circuits` | Repeated call with identical arguments returns cached result without re-executing | **PASS** |
| `test_mutating_non_idempotent_no_retries` | Non-idempotent tool fails immediately on error with zero automatic retries | **PASS** |
| `test_mutating_non_idempotent_uncertain_timeout` | Timeout marks status `uncertain` and raises `ToolUncertainOutcomeError` | **PASS** |
| `test_uncertain_reinvocation_blocked` | Subsequent invocation of uncertain operation is blocked | **PASS** |
| `test_uncertain_reinvocation_allowed_when_flagged` | Re-invocation succeeds when `retry_on_uncertain=True` | **PASS** |
| `test_concurrent_duplicate_pending_rejected` | Invoking tool while already `pending` raises `ToolDuplicateExecutionError` | **PASS** |
| `test_cancellation_token_blocks_dispatch` | Cooperative cancellation token aborts dispatch before execution begins | **PASS** |
| `test_fencing_rejection_before_dispatch` | Expired admission lease or stale fence token aborts pre-dispatch | **PASS** |
| `test_fencing_rejection_before_completion` | Superseded lease during execution blocks result commit | **PASS** |
| `test_crash_recovery_marks_pending_uncertain` | `recover_run_as_interrupted` marks in-flight tool executions `uncertain` | **PASS** |

### 6.2 Regression Test Results
- `tests/test_tool_execution_idempotency.py`: **12 passed** in 3.03s.
- Full regression suite for Tasks 31-40: **135 passed** in 37.00s.
- `ruff check src tests`: **0 errors, all clean**.

---

## 7. Operational Boundaries & Production Recommendations

1. **Provider Key Expiration**: External providers (such as Stripe or AWS) typically retain idempotency keys for 24 hours. Workflows resuming days later may experience provider-side key expiration; however, the orchestrator's local `tool_executions` cache retains completed results indefinitely, preventing repeat provider dispatches.
2. **Non-Idempotent External APIs**: For external services lacking idempotency support, configuring `IdempotencyMode.MUTATING_NON_IDEMPOTENT` guarantees single-execution semantics. In the event of network timeouts, human-in-the-loop review should be used to inspect external provider dashboards before unblocking runs.
3. **Cooperative Python Threads**: Cancellation and fencing checks occur before tool dispatch and immediately following execution. If a tool executes a blocking C-extension or un-timeoutable socket read, the thread will yield on the next checkpoint boundary once the blocking call returns.
