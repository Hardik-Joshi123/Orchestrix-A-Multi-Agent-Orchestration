# Task 44: Approval Workflow Observability & Operational Recovery Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 44 introduces complete operational visibility, granular diagnostics, and safe, privileged recovery mechanisms for the human-approval lifecycle implemented across Tasks 42–43:
1. **Approval Lifecycle Visibility**:
   - Structured status information across `pending`, `approved`, `rejected`, and `revoked` decisions.
   - Dedicated inspection endpoint `GET /runs/{run_id}/approvals` with query filtering by `status` and `tool_name`.
   - Automatic registration of `pending` approval requests in `tool_approvals` whenever an unapproved high-risk tool is invoked, enabling operators to immediately discover pending actions in the queue without manual workflow polling.
2. **Granular Root-Cause Diagnostics**:
   - Diagnostic engine (`diagnose_tool_approval` and `GET/POST /runs/{run_id}/tools/{operation_key}/diagnostics`) providing unambiguous, machine-readable diagnostic codes (`missing_approval`, `pending_approval`, `valid_approved`, `argument_fingerprint_mismatch`, `policy_risk_mismatch`, `policy_hash_mismatch`, `unauthorized_approver`, `revoked_approval`, `rejected_approval`, `workflow_cancelled`, `admission_fencing_lost`).
   - Evaluates argument parameter drift, policy configuration modifications, cooperative cancellation, and admission lease fencing loss.
   - Attaches up to 10 sanitized, relevant audit events to each diagnostic report.
3. **Privileged Operational Recovery**:
   - Dedicated endpoint `POST /runs/{run_id}/tools/{operation_key}/recover` and SDK client `recover_tool_approval`.
   - **Strict Safety Guarantees**:
     - **Never Auto-Approves**: Resets blocked/stale/rejected/revoked approval state strictly to `pending`.
     - **Never Redispatches**: Execution remains blocked until an authorized human explicitly reviews and approves the pending action via `POST /approve`.
     - **Verified Operator Identity**: Anonymous or generic credentials (`"bearer-token"`) are rejected (`HTTP 403 Forbidden`).
     - **Mandatory Justification**: Requires a non-empty, audited recovery justification (`HTTP 400 Bad Request` if empty).
     - **Structured Audit Trail**: Emits a `tool.approval_recovered` audit event.
4. **Data Privacy & Secrets Redaction**:
   - Central sanitization function `sanitize_tool_approval` scrubs passwords, API keys, bearer tokens, and execution tracebacks from reasons, revocation notes, approver strings, and error summaries before API responses are serialized.
5. **REST API & Python SDK Integration**:
   - Endpoints: `GET /runs/{run_id}/approvals`, `GET & POST /runs/{run_id}/tools/{operation_key}/diagnostics`, `POST /runs/{run_id}/tools/{operation_key}/recover`.
   - Client methods: `list_tool_approvals`, `get_tool_approval_diagnostics`, `recover_tool_approval`.

---

## 2. Architecture & Observability Lifecycle Flow

```
                      [Agent Invocation Boundary (Tool.__call__)]
                                         │
                                         ▼
                             [High-Risk Tool Invoked?]
                                         │
                    ┌────────────────────┴────────────────────┐
                    ▼ (Approved)                              ▼ (Needs Approval)
       [Pre-Dispatch Revalidation]               [Approval Request Auto-Recorded]
       - Check argument fingerprint              - Insert/ensure record in tool_approvals:
       - Check policy hash & risk level            * decision = 'pending'
       - Check store revocation status             * argument_fingerprint = SHA256(args)
       - Check admission lease fencing             * policy_hash = SHA256(policy)
                    │                            - Emit 'tool.approval_required' audit
                    ▼                                         │
         [Execute Tool Function]                              ▼
                                                 [ToolApprovalRequiredError Raised]
                                                              │
 ═════════════════════════════════════════════════════════════╪══════════════════════════════════════
                                                              │
                            [Operator Observability & Recovery Layer]
                                                              │
                    ┌─────────────────────────────────────────┼─────────────────────────────────────┐
                    ▼                                         ▼                                     ▼
      [GET /runs/{run_id}/approvals]           [GET/POST .../diagnostics]            [POST .../recover]
      - Filter: status=pending,revoked,...     - Evaluate exact failure code:        - Reject generic tokens (403)
      - Filter: tool_name=deploy_service       - missing_approval                    - Reject empty reason (400)
      - Redact passwords/tokens/traces         - pending_approval                    - Safely reset to 'pending'
                                               - argument_fingerprint_mismatch       - NEVER auto-approve
                                               - policy_risk/hash_mismatch           - NEVER redispatch
                                               - unauthorized_approver               - Emit 'tool.approval_recovered'
                                               - revoked_approval                                   │
                                               - rejected_approval                                  ▼
                                               - workflow_cancelled                  [Requires Explicit Human Approval]
                                               - admission_fencing_lost              (POST .../approve) before dispatch
                                               - valid_approved
```

---

## 3. Implementation Details

### 3.1 Observability & Audit Logging (`src/orchestrator/observability/audit.py`)
- Added `TOOL_APPROVAL_RECOVERED = "tool.approval_recovered"` and `TOOL_APPROVAL_REQUESTED = "tool.approval_requested"` to `AuditEventType`.
- Implemented `sanitize_tool_approval(record: dict[str, Any]) -> dict[str, Any]` to redact secret patterns (`sk-...`, bearer tokens, `password=...`, tracebacks) across `reason`, `revocation_reason`, `approver`, and `recovered_by`.

### 3.2 Durable Storage & Recovery (`src/orchestrator/persistence/store.py`)
- Added `recovered_by TEXT` column to `tool_approvals` schema and dynamic table migration check.
- Updated `record_tool_approval`: Allows existing `pending` records to transition to `approved` or `rejected` via `UPDATE`. Preserves conflict protection (`approved` <-> `rejected`, `revoked` -> `approved`).
- Implemented `record_tool_approval_request`: Records `decision = "pending"` for unapproved operations.
- Implemented `recover_tool_approval`: Resets `decision = "pending"`, clears `approver`, `revoked_by`, `revocation_reason`, sets `recovered_by`, updates `argument_fingerprint`/`policy_hash`, and records recovery timestamp.
- Updated `list_tool_approvals`: Supports SQL filtering by `status` (decision) and `tool_name`.
- Updated `get_tool_approval`: Supports both `(operation_key)` and `(run_id, operation_key)` call signatures.
- Delegated all updated signatures and new methods in `FencedCheckpointStore`.

### 3.3 Diagnostic Engine (`src/orchestrator/tools/registry.py`)
- In `Tool.__call__`: Automatically calls `store.record_tool_approval_request(...)` when `approval_record is None` before raising `ToolApprovalRequiredError`.
- If an existing approval record has `decision == "pending"`, raises `ToolApprovalRequiredError` stating pending status.
- Implemented `diagnose_tool_approval(...)`:
  - Evaluates cancellation tokens and store cancellation status (`workflow_cancelled`).
  - Evaluates admission fence tokens and active leases (`admission_fencing_lost`).
  - Evaluates missing records (`missing_approval`) and pending records (`pending_approval`).
  - Evaluates revoked records (`revoked_approval`) and rejected records (`rejected_approval`).
  - Evaluates approver authorization under current policy (`unauthorized_approver`).
  - Evaluates argument parameter drift (`argument_fingerprint_mismatch`).
  - Evaluates tool policy modifications (`policy_risk_mismatch`, `policy_hash_mismatch`).
  - Evaluates valid approvals ready for execution (`valid_approved`).
  - Attaches up to 10 relevant sanitized audit events.

### 3.4 REST API & Python SDK (`src/orchestrator/api/`)
- **Pydantic Models (`models.py`)**:
  - `RunToolApprovalsResponse` (with `total_approvals`, `total_count`, and `approvals`).
  - `DiagnoseToolApprovalRequest` and `ToolApprovalDiagnosticsResponse` (with `can_execute`, `can_proceed`, `state`, `status`, `diagnostic_code`, `message`, `is_blocked`, `is_stale`, and audit events).
  - `RecoverToolApprovalRequest` and `RecoverToolApprovalResponse`.
  - Added `recovered_by` to `ToolApprovalModel`.
- **API Routes (`routes.py`)**:
  - `GET /runs/{run_id}/approvals` (alias `/runs/{run_id}/tools/approvals`).
  - `GET & POST /runs/{run_id}/tools/{operation_key}/diagnostics` (alias `/runs/{run_id}/tools/{operation_key}/approval/diagnostics`).
  - `POST /runs/{run_id}/tools/{operation_key}/recover` (alias `/runs/{run_id}/tools/{operation_key}/reset-approval`).
- **Python Client SDK (`client.py`)**:
  - `list_tool_approvals(run_id, status=None, tool_name=None)`
  - `get_tool_approval_diagnostics(run_id, operation_key, tool_name=None, arguments=None)`
  - `recover_tool_approval(run_id, operation_key, tool_name, reason, operator=None, arguments=None)`

---

## 4. Verification & Test Execution Results

### 4.1 Dedicated Test Suite (`tests/test_tool_approval_observability.py`)
A comprehensive test suite of 15 tests was written and executed:
1. `test_tool_call_records_pending_approval_request`: Automatic pending request registration on unapproved invocation.
2. `test_list_tool_approvals_filtering`: Listing and filtering by `status` (pending, revoked, rejected) and `tool_name`.
3. `test_diagnose_tool_approval_missing`: Diagnostic code `missing_approval` for unregistered actions.
4. `test_diagnose_tool_approval_pending`: Diagnostic code `pending_approval` for actions under review.
5. `test_diagnose_tool_approval_valid`: Diagnostic code `valid_approved` when all constraints match.
6. `test_diagnose_tool_approval_rejected_and_revoked`: Diagnostic codes `rejected_approval` and `revoked_approval`.
7. `test_diagnose_tool_approval_fingerprint_and_policy_mismatches`: Diagnostic codes `argument_fingerprint_mismatch` and `policy_risk_mismatch`.
8. `test_diagnose_tool_approval_cancellation_and_fencing`: Diagnostic codes `workflow_cancelled` and `admission_fencing_lost`.
9. `test_recover_tool_approval_resets_to_pending`: Operational recovery resetting rejected state to pending with operator identity and audit note.
10. `test_recovery_never_auto_approves_or_redispatches`: Proves recovery strictly leaves the tool pending and blocked; zero external redispatches occur until explicit subsequent approval.
11. `test_sensitive_data_sanitization`: Verifies redaction of tokens, passwords, and tracebacks.
12. `test_api_list_tool_approvals`: HTTP API listing with authentication and query filtering.
13. `test_api_tool_approval_diagnostics`: HTTP API GET and POST diagnostics testing candidate parameters.
14. `test_api_tool_approval_recovery`: HTTP API recovery endpoint validating reasons, rejecting generic tokens (403), and emitting audit events.
15. `test_sdk_client_approval_observability_and_recovery`: OrchestratorApiClient methods end-to-end.

**Result**: 15 passed in 3.19s.

### 4.2 Comprehensive Tool Test Regression Suite
Ran all 5 tool safety and lifecycle suites:
- `test_tool_execution_idempotency.py` (Task 40)
- `test_tool_execution_reconciliation.py` (Task 41)
- `test_tool_permission_policies.py` (Task 42)
- `test_tool_approval_integrity.py` (Task 43)
- `test_tool_approval_observability.py` (Task 44)

**Result**: 77 passed in 13.42s.

### 4.3 Admission & Fencing Regression Suite
Ran Tasks 36–39 test suites:
- `test_admission_backpressure.py` (Task 36)
- `test_admission_fencing.py` (Task 37)
- `test_cross_process_admission.py` (Task 38)
- `test_fenced_state_transitions.py` (Task 39)

**Result**: 39 passed in 14.02s.

### 4.4 Static Analysis & Linter Verification
- Executed `python -m ruff check multi-agent-orchestrator/src multi-agent-orchestrator/tests`.
- **Result**: All checks passed (0 errors, 0 warnings).

---

## 5. Security & Safety Summary

| Control | Guarantee |
|---|---|
| **Recovery Status** | Recovery strictly resets state to `pending`. Direct transitions to `approved` are prohibited. |
| **No Auto-Redispatch** | Recovery never executes or redispatches external tool calls. Execution remains blocked. |
| **Operator Verification** | Generic API bearer tokens (`"bearer-token"`) cannot recover operations (`HTTP 403 Forbidden`). |
| **Mandatory Reason** | Empty recovery justifications are rejected (`HTTP 400 Bad Request`). |
| **Audit Trail** | Structured audit event `tool.approval_recovered` recorded with operator, justification, and operation key. |
| **Data Privacy** | All secrets, credentials, API keys, and stack traces are scrubbed via `sanitize_tool_approval`. |
| **Pre-Dispatch Checks** | Cancellation, admission fencing, and parameter fingerprints are revalidated immediately before execution. |

---

## 6. Conclusion

Task 44 has been fully implemented, documented, and verified. The human-in-the-loop approval workflow now provides full operational visibility, precise failure root-cause diagnostics, and safe, audited operational recovery without weakening existing security, admission fencing, or idempotency controls.
