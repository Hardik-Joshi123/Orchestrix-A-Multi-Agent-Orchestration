# Task 43: Approval Integrity, Authorization & Revocation Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 43 hardens the human-approval lifecycle introduced in Task 42 so that:
1. **Approvals are strictly bound to the exact intended tool operation**:
   - Approvals cannot be repurposed for altered arguments or stale policy configurations.
   - Deterministic SHA-256 fingerprinting (`argument_fingerprint`) binds approval directly to the canonicalized kwargs.
   - Policy hashes (`policy_hash`) and risk levels (`risk_level`) ensure that policy escalations or configuration changes invalidate stale approvals.
2. **Explicit approver authorization**:
   - Possession of a generic API bearer token is no longer sufficient to approve sensitive tool operations.
   - Generic/anonymous identities (`"bearer-token"`, `"authenticated-client"`, `"unknown"`, `"anonymous"`) are rejected with `HTTP 403 Forbidden` (`ToolApproverUnauthorizedError`).
   - Approvers are verified against tool-level policies (`authorized_approvers`) and global configuration (`tool_approval_allowed_approvers`).
3. **Safe approval state transitions and conflict protection**:
   - Conflicting state transitions (`approved` <-> `rejected`, `revoked` -> `approved`) raise `ToolApprovalConflictError` (`HTTP 409 Conflict`).
   - Identical repeated approval/rejection/revocation requests are strictly idempotent (`HTTP 200`, `idempotent: true`).
4. **Audited revocation mechanism**:
   - Dedicated authenticated endpoint `POST /runs/{run_id}/tools/{operation_key}/revoke` and SDK client `revoke_tool_approval`.
   - Structured audit event `tool.approval_revoked` emitted with actor, justification, and operation key.
5. **Pre-dispatch revalidation gate**:
   - Direct database check immediately before executing the underlying Python function (and between retry attempts) ensures that concurrent revocations or rejections halt execution before external side effects occur.
   - Cooperative cancellation and admission fencing are revalidated post-approval.

---

## 2. Architecture & Approval Integrity Flow

```
                     [Operator / Client Submits Approval]
                                       │
                                       ▼
                   [Step 1: Approver Authorization Check]
            (Is actor non-generic, in policy.authorized_approvers,
                  and in Settings.tool_approval_allowed_approvers?)
                                       │
                    ┌──────────────────┴──────────────────┐
                    ▼ (No)                                ▼ (Yes)
       [HTTP 403 Forbidden]               [Step 2: Fingerprint & Policy Hash]
     (tool.approver_denied audit)             - argument_fingerprint = SHA256(args)
                                              - policy_hash = SHA256(policy)
                                              - risk_level = policy.risk_level
                                                          │
                                                          ▼
                                      [Step 3: State Transition & DB Lock]
                                                          │
                    ┌─────────────────────────────────────┼─────────────────────────────────────┐
                    ▼ (Conflicting Transition)            ▼ (Identical Duplicate)               ▼ (Valid New Decision)
             [HTTP 409 Conflict]                  [HTTP 200 Idempotent]                 [HTTP 200 Success]
        (approved <-> rejected, etc.)             (idempotent: true)                    (tool.approved audit)
                                                          │
 ═════════════════════════════════════════════════════════╪═════════════════════════════════════════════════════════
                                                          │
                                     [Agent Invocation / Tool Dispatch]
                                                          │
                                                          ▼
                                          [Tool Boundary: Tool.__call__]
                                                          │
                                       [Step 4: Fingerprint & Policy Match]
                                       - current_fp == approval.argument_fingerprint?
                                       - current_risk == approval.risk_level?
                                       - current_phash == approval.policy_hash?
                                                          │
                    ┌─────────────────────────────────────┴─────────────────────────────────────┐
                    ▼ (Mismatch / Stale)                                                        ▼ (Valid)
        [ToolApprovalRequiredError]                                               [Pre-Dispatch Revalidation]
       (New approval cycle mandatory)                                             - Cancellation token check
                                                                                  - Store revocation re-check
                                                                                  - Admission fence re-check
                                                                                                │
                                                                                                ▼
                                                                                   [Execute Tool Function]
```

---

## 3. Database Schema & Migration

The `tool_approvals` table in `CheckpointStore` was extended to store cryptographic binding, policy freshness, and revocation metadata:

```sql
CREATE TABLE IF NOT EXISTS tool_approvals (
    operation_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    approver TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    argument_fingerprint TEXT,
    risk_level TEXT,
    policy_hash TEXT,
    revoked_by TEXT,
    revocation_reason TEXT,
    revoked_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_tool_approvals_run_id
    ON tool_approvals(run_id);
```

### Automatic Migration
On initialization of `CheckpointStore`, PRAGMA table info is inspected. If any of the 6 new columns (`argument_fingerprint`, `risk_level`, `policy_hash`, `revoked_by`, `revocation_reason`, `revoked_at`) are missing from pre-Task 43 databases, `ALTER TABLE tool_approvals ADD COLUMN ...` statements are applied seamlessly without data loss.

---

## 4. Key Implementation Details

### 4.1 Argument Fingerprinting & Policy Hashing
- `compute_argument_fingerprint(kwargs: dict[str, Any]) -> str`:
  Serializes arguments to sorted canonical JSON, stripping transient keys (`context`, `idempotency_key`), and computes a 64-character SHA-256 hex digest.
- `compute_policy_hash(policy: ToolPolicy | None) -> str`:
  Encodes `tool_name`, `risk_level`, sorted `allowed_agents`, `requires_approval`, and sorted `authorized_approvers` into a deterministic 16-character SHA-256 hash.

### 4.2 Approver Authorization Verification
- `verify_approver_authorization(approver: str | None, tool_name: str, policy: ToolPolicy | None = None) -> None`:
  1. Rejects blank or empty approvers.
  2. Rejects generic tokens (`"bearer-token"`, `"authenticated-client"`, `"unknown"`, `"anonymous"`).
  3. Validates against `policy.authorized_approvers` unless wildcard `*` is present.
  4. Validates against global `Settings.tool_approval_allowed_approvers` (configured via env var `ORCHESTRATOR_ALLOWED_APPROVERS` or `TOOL_APPROVAL_ALLOWED_APPROVERS`).
  5. Denials emit canonical audit event `tool.approver_denied` and raise `ToolApproverUnauthorizedError` (HTTP 403).

### 4.3 Safe State Machine & Revocation
- Transition rules inside SQLite transaction:
  - `None` -> `approved` | `rejected`
  - `approved` -> `approved` (idempotent, returns `idempotent=True`)
  - `rejected` -> `rejected` (idempotent, returns `idempotent=True`)
  - `approved` -> `revoked` (valid revocation via `revoke_tool_approval`)
  - `revoked` -> `revoked` (idempotent, returns `idempotent=True`)
  - `approved` -> `rejected`: `ToolApprovalConflictError` (HTTP 409)
  - `rejected` -> `approved`: `ToolApprovalConflictError` (HTTP 409)
  - `revoked` -> `approved`: `ToolApprovalConflictError` (HTTP 409)
  - `rejected` -> `revoked`: `ToolApprovalConflictError` (HTTP 409)

### 4.4 Pre-Dispatch Revalidation
- In `Tool.__call__`, immediately before calling the tool callable inside the execution loop:
  - Queries `store.get_tool_approval(op_key)`. If `decision == "revoked"`, aborts with `ToolPermissionDeniedError`.
  - Re-checks `context.cancellation_token.is_cancelled`. If cancelled, aborts with `ToolExecutionError`.
  - Re-checks `store.validate_admission_ownership(...)`. If superseded, aborts with `ToolFencingError`.
  - Re-evaluates before every retry attempt so revocations enacted during waiting or retrying stop execution.

---

## 5. REST API & Python SDK Updates

### REST Endpoints
- `POST /runs/{run_id}/tools/{operation_key}/approve`:
  - Request body: `tool_name`, `decision`, `reason`, `approver`, `argument_fingerprint`, `arguments`.
  - Response: `success`, `operation_key`, `run_id`, `tool_name`, `approver`, `decision`, `reason`, `created_at`, `idempotent`, `argument_fingerprint`, `risk_level`, `policy_hash`, `audit_event_id`.
  - Status codes: `200 OK`, `400 Bad Request`, `403 Forbidden`, `404 Not Found`, `409 Conflict`.
- `POST /runs/{run_id}/tools/{operation_key}/revoke`:
  - Request body: `tool_name`, `reason`, `revoked_by`.
  - Response: `success`, `operation_key`, `run_id`, `tool_name`, `revoked_by`, `decision`, `reason`, `revoked_at`, `idempotent`, `audit_event_id`.
  - Status codes: `200 OK`, `400 Bad Request`, `403 Forbidden`, `404 Not Found`, `409 Conflict`.
- `GET /runs/{run_id}/tools/{operation_key}/approval`:
  - Returns `ToolApprovalModel` containing all 13 approval and revocation fields.

### Python SDK (`OrchestratorApiClient`)
- `approve_tool_execution(run_id, operation_key, tool_name, decision="approved", reason="", approver=None, argument_fingerprint=None, arguments=None) -> ApproveToolExecutionResponse`
- `revoke_tool_approval(run_id, operation_key, tool_name, reason="", revoked_by=None) -> RevokeToolApprovalResponse`
- `get_tool_approval(run_id, operation_key) -> ToolApprovalModel`

---

## 6. Verification & Test Suite

A dedicated offline test suite was created in `tests/test_tool_approval_integrity.py` covering 15 test scenarios:
1. `test_argument_fingerprint_deterministic_and_filtered`: Argument hashing consistency and transient field filtering.
2. `test_compute_policy_hash_deterministic`: Policy hash sensitivity to risk levels and approvers.
3. `test_verify_approver_authorization_rejects_generic`: 403 rejection of generic approver tokens.
4. `test_verify_approver_authorization_tool_policy`: Enforcement of tool-level authorized approver whitelist.
5. `test_verify_approver_authorization_global_settings`: Enforcement of global configuration overrides.
6. `test_store_approval_idempotent_and_conflict`: Idempotent repeated approvals and 409 conflict on flip.
7. `test_store_revocation_lifecycle`: Revocation state machine, idempotency, and re-approval prevention.
8. `test_store_revocation_not_found`: 404 on revoking non-existent approval.
9. `test_tool_execution_fingerprint_mismatch_rejected`: Rejection when tool is invoked with altered arguments.
10. `test_tool_execution_policy_change_invalidates_approval`: Invalidation when policy is modified after approval.
11. `test_tool_execution_revoked_approval_rejected`: Execution denial on revoked approvals.
12. `test_tool_pre_dispatch_concurrent_revocation_during_retry`: Immediate halt during retry if revoked in store.
13. `test_tool_pre_dispatch_cancellation_and_fencing`: Cooperative cancellation and admission fencing gate.
14. `test_api_approval_authorization_and_revocation`: HTTP API workflow, conflict responses, and audit events.
15. `test_api_client_approve_and_revoke_methods`: Python SDK end-to-end integration.

### Full Test Execution Summary
- `pytest tests/test_tool_approval_integrity.py`: **15 passed** in 2.67s.
- `pytest tests/test_tool_*.py`: **73 passed** in 8.32s.
- `pytest tests/test_admission_*.py tests/test_cross_process_admission.py tests/test_fenced_state_transitions.py`: **39 passed** in 12.27s.
- Total regression verification: **112 passed** across all related test suites.
- Code style check: `ruff check src tests` returned **0 errors** (all checks passed).

---

## 7. Status & Sign-Off

All requirements for Task 43 are fully implemented, verified, documented in `docs/DEVELOPER_GUIDE.md` (Section 12.8), and accompanied by comprehensive regression tests. As instructed, work ceases strictly at Task 43 without git commits, git pushes, or initiating Task 44.
