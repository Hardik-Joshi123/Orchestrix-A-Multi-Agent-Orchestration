# Task 42: Tool Permission Policies & Risk-Based Approval Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 42 establishes a centralized, policy-driven security boundary for tool execution across the multi-agent orchestrator. While Tasks 40–41 introduced deterministic operation keys, downstream idempotency, double-gated fencing, and safe outcome reconciliation, specialist agents could still invoke any registered tool without explicit permission boundaries, and high-risk operations lacked a mandatory human-in-the-loop (HITL) approval gate.

Task 42 delivers a **centralized, enforceable tool policy and risk-based approval architecture**:
- **Tool Permission Policy Model (`ToolPolicy`)**:
  - **Risk Levels (`RiskLevel`)**: `LOW`, `MEDIUM`, `HIGH`, `CRITICAL`.
  - **Specialist Agent Scoping (`allowed_agents`)**: Enforces specialist-level access control (e.g. `["code", "analysis"]` vs `["*"]`). Unauthorized agents are rejected at the execution boundary before invoking tool functions.
  - **Approval Requirement (`requires_approval`)**: High and critical risk tools require explicit human approval by default before external side effects can be triggered.
- **Central Boundary Enforcement & Non-Bypassability**:
  - Enforced within `Tool.__call__`, which gates all tool invocations across direct calls, registry calls (`REGISTRY.call`, `REGISTRY.execute`), specialist execution (`_run_specialist`), custom plans, workflow resumes, and retry loops.
  - LLM prompts, agent reasoning, or custom plan sequences cannot bypass the Python execution gate.
- **Post-Approval Revalidation Pipeline**:
  - Revalidates three critical safety constraints immediately before dispatch after approval:
    1. **Cancellation**: Checks `cancellation_token.is_cancelled` to prevent dispatching approved actions on cancelled runs.
    2. **Agent Authorization**: Revalidates `context.agent_id in policy.allowed_agents`.
    3. **Admission Lease Ownership (Fencing)**: Validates `store.validate_admission_ownership(...)` to prevent stale or superseded workers from executing approved actions.
- **Durable Approval Persistence (`tool_approvals` SQLite table)**:
  - Persists approval records (`operation_key`, `run_id`, `tool_name`, `approver`, `decision`, `reason`, `created_at`).
  - Durable across worker restarts, workflow resume, and retry attempts. Rejections permanently block execution.
- **Authenticated Management APIs & Client SDK**:
  - `POST /runs/{run_id}/tools/{operation_key}/approve`: Authenticated endpoint to record `approved` or `rejected` decisions.
  - `GET /runs/{run_id}/tools/{operation_key}/approval`: Authenticated endpoint to retrieve recorded approval metadata.
  - Client SDK methods: `OrchestratorApiClient.approve_tool_execution` and `OrchestratorApiClient.get_tool_approval`.
- **Structured Audit Logging & Secrets Shielding**:
  - Emits canonical audit events: `tool.policy_denied`, `tool.approval_required`, and `tool.approved`.
  - All metadata is sanitized via `sanitize_audit_metadata` so secrets and sensitive arguments never appear in audit trails.

---

## 2. Policy & Approval Architecture

```
                  [Specialist / Node Invokes Tool]
                                │
                                ▼
               [Step 1: Schema & Rate Limit Check]
                                │
                                ▼
            [Step 2: Agent Permission Policy Gate]
         (Is context.agent_id in policy.allowed_agents?)
                                │
                 ┌──────────────┴──────────────┐
                 ▼ (No)                        ▼ (Yes)
       [ToolPermissionDeniedError]    [Step 3: Operation Key Derivation]
      (tool.policy_denied audit)               │
                                               ▼
                              [Step 4: Cancellation & Fencing Gate]
                                               │
                                               ▼
                                 [Step 5: Risk & Approval Gate]
                           (Risk HIGH/CRITICAL or requires_approval?)
                                               │
                 ┌─────────────────────────────┴─────────────────────────────┐
                 ▼ (Low/Medium Risk)                                         ▼ (High/Critical Risk)
          [Proceed to Step 6]                                     [Check In-Memory / Store Approval]
                                                                             │
                                           ┌─────────────────────────────────┼─────────────────────────────────┐
                                           ▼ (None)                          ▼ (Rejected)                      ▼ (Approved)
                            [ToolApprovalRequiredError]          [ToolPermissionDeniedError]            [Post-Approval Revalidation]
                           (tool.approval_required audit)         (tool.policy_denied audit)             1. Cancellation Token
                                                                                                         2. Agent Authorization
                                                                                                         3. Admission Ownership
                                                                                                                   │
                                                                                                                   ▼
                                                                                                          [Step 6: Idempotency & Dispatch]
                                                                                                          [Step 7: Downstream Execution]
                                                                                                          [Step 8: Post-Execution Fencing]
                                                                                                          [Step 9: Completion Record]
```

---

## 3. Database Schema Updates

The persistence layer (`CheckpointStore`) was extended with the `tool_approvals` table:

```sql
CREATE TABLE IF NOT EXISTS tool_approvals (
    operation_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    approver TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tool_approvals_run_id
    ON tool_approvals(run_id);
```

### Transition Methods:
- `record_tool_approval(run_id, operation_key, tool_name, approver, decision, reason)`: Records or updates an approval decision atomically (`ON CONFLICT(operation_key) DO UPDATE`).
- `get_tool_approval(operation_key)`: Fetches recorded decision by operation key.
- `list_tool_approvals(run_id)`: Lists all approvals for a specific workflow run.
- `FencedCheckpointStore` wraps and delegates these methods with admission fencing context.

---

## 4. API Specification & Client SDK

### Endpoints
1. `POST /runs/{run_id}/tools/{operation_key}/approve`
   - **Request**:
     ```json
     {
       "tool_name": "deploy_service",
       "decision": "approved",
       "reason": "Change ticket CHG-9021 approved by Security Officer",
       "approver": "secops-lead@company.com"
     }
     ```
   - **Response**: `200 OK`
     ```json
     {
       "success": true,
       "operation_key": "toolop:run-1:s1:deploy_service:89ab",
       "run_id": "run-1",
       "tool_name": "deploy_service",
       "approver": "secops-lead@company.com",
       "decision": "approved",
       "reason": "Change ticket CHG-9021 approved by Security Officer",
       "created_at": "2026-10-27T04:45:00.000000Z",
       "audit_event_id": "aud-1a2b3c4d5e6f"
     }
     ```

2. `GET /runs/{run_id}/tools/{operation_key}/approval`
   - **Response**: `200 OK`
     ```json
     {
       "operation_key": "toolop:run-1:s1:deploy_service:89ab",
       "run_id": "run-1",
       "tool_name": "deploy_service",
       "approver": "secops-lead@company.com",
       "decision": "approved",
       "reason": "Change ticket CHG-9021 approved by Security Officer",
       "created_at": "2026-10-27T04:45:00.000000Z"
     }
     ```

### Client SDK Methods (`OrchestratorApiClient`)
- `approve_tool_execution(run_id, operation_key, tool_name, decision="approved", reason="", approver=None)`
- `get_tool_approval(run_id, operation_key)`

---

## 5. Verification & Test Coverage Matrix

An independent offline test suite was implemented in `tests/test_tool_permission_policies.py` alongside the existing test suites.

| Test Case | Objective | Result |
|---|---|---|
| `test_allowed_agent_can_invoke_tool` | Specialist in `allowed_agents` executes tool cleanly | PASS |
| `test_unauthorized_agent_rejected_before_dispatch` | Unauthorized specialist blocked with `ToolPermissionDeniedError`, `fn` not called | PASS |
| `test_unspecified_agent_rejected_when_agents_restricted` | Context without `agent_id` blocked when tool has restricted `allowed_agents` | PASS |
| `test_wildcard_allowed_agents_permits_any_agent` | Default wildcard `allowed_agents=["*"]` allows any specialist | PASS |
| `test_high_and_critical_risk_tools_require_approval_by_default` | Tools with HIGH/CRITICAL risk require approval; block before dispatch | PASS |
| `test_explicit_in_memory_approval_satisfies_requirement` | Context carrying `ToolApproval(decision="approved")` executes successfully | PASS |
| `test_explicit_in_memory_rejection_raises_permission_denied` | Context carrying `ToolApproval(decision="rejected")` raises `ToolPermissionDeniedError` | PASS |
| `test_persisted_approval_in_store_satisfies_requirement` | Approval recorded via store permits execution across retries | PASS |
| `test_persisted_rejection_in_store_blocks_execution` | Rejection recorded via store permanently prevents execution | PASS |
| `test_post_approval_revalidation_detects_cancellation` | Cancelled run post-approval aborts before dispatch without executing `fn` | PASS |
| `test_post_approval_revalidation_detects_agent_auth_revocation` | Revoked or mismatch agent authorization post-approval blocks execution | PASS |
| `test_post_approval_revalidation_detects_admission_lease_loss` | Stale or superseded worker post-approval aborts with `ToolFencingError` | PASS |
| `test_cannot_bypass_approval_via_direct_registry_call` | `REGISTRY.call(...)` enforces risk-based approval identical to specialist flow | PASS |
| `test_unknown_tool_requires_approval_default_deny` | Unknown tools return `True` for `requires_approval` and raise `ToolNotFoundError` on call | PASS |
| `test_set_policy_dynamically_updates_tool_enforcement` | Updating policy via `ToolRegistry.set_policy` immediately changes boundary rules | PASS |
| `test_audit_logging_on_policy_denial_and_approval` | Verifies `tool.policy_denied` audit events and ensures secrets are scrubbed | PASS |
| `test_api_tool_approval_and_retrieval` | Validates `POST .../approve` and `GET .../approval` endpoints and SDK client | PASS |
| `test_specialist_execution_passes_agent_id_and_enforces_policy` | Specialists pass `agent_id=kind`; unauthorized tools yield error notes in prompt | PASS |

### Regression Test Suite Results
```
tests/test_tool_permission_policies.py: 18 passed
tests/test_tools_safety.py: 11 passed
tests/test_tool_execution_idempotency.py: 12 passed
tests/test_tool_execution_reconciliation.py: 17 passed
tests/test_security.py: 13 passed
Total: 71 passed, 0 failed in 8.77s
```

---

## 6. Operational Boundaries & Compliance

1. **Central Execution Boundary**: Policy and approval checks reside exclusively at the tool execution boundary in Python code (`Tool.__call__`). No agent prompt engineering, LLM output hallucination, or synthetic graph edge can bypass these checks.
2. **Post-Approval Revalidation**: Approvals do not constitute a permanent or unchecked license to execute. If a workflow is cancelled, an agent's permissions are modified, or the executing worker's admission lease expires after approval, execution is immediately aborted before dispatch.
3. **Immutability of Terminal Execution States**: Persisted tool approvals govern permission to execute; once executed, idempotency records in `tool_executions` maintain terminal-state immutability and duplicate dispatch prevention.
4. **Audit Privacy**: Audit events for policy denial and approval decisions record only actor identity, tool name, operation key, risk level, and rationale. Raw arguments and credentials are never exposed in audit records.
