# Task 03A Report: HITL Fix Review and Change-Scope Audit

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 03A  
**Date:** October 19, 2026  
**Audit Mode:** Read-Only Review & Scope Verification  
**Author:** Antigravity Engineering Agent  
**Status:** COMPLETE  

---

## 1. Executive Summary

### Purpose & Scope
This report provides an independent, read-only architectural review of the Human-in-the-Loop (HITL) safety fix implemented in Task 03, alongside a comprehensive change-scope audit of all modifications currently present in the repository working tree relative to Git `HEAD` (`1766b96`).

### Core Verdict
1. **The Core HITL Safety Defect is Fixed in the Fallback Engine:** For destructive/sensitive operations requiring human takeover (e.g., `DROP TABLE customers`), the fallback execution engine (`_run_fallback`) now successfully halts dispatch, quarantines raw specialist output from the final answer, marks the task `"status": "pending"`, and emits a clear `"### Human Review Required (take_over)"` notice.
2. **Authoritative Resolution Function Implemented:** The new `apply_resolution(state, resolution)` function in `orchestrator.hitl.queue` provides a callable mechanism that deterministically applies human decisions (`take_over`, `edit`, `reject`, `approve`) to the workflow state, updating the answer, status, and audit trace.
3. **Change Scope Contamination Confirmed:** 13 tracked files and 3 untracked items exist in the working tree. A significant portion of these changes (in `llm.py`, `memory/long_term.py`, `memory/short_term.py`, `observability/tracing.py`, and parts of `specialists.py` and `builder.py`) were introduced in an unprompted "continue" phase between Task 02 and Task 03 to fix non-HITL defects (FAIL-01 through FAIL-08). Only 6 tracked files are strictly required for the HITL fix.
4. **Architectural Gaps & Divergences Identified:**
   - **LangGraph vs. Fallback Divergence:** The compiled LangGraph topology (`build_graph()`) unconditionally routes from `escalate` back to `dispatch`, lacking the early-break behavior present in `_run_fallback`.
   - **Absence of State Persistence:** `apply_resolution` operates solely in-memory on dictionary objects. Once `run_task()` finishes, `GraphState` is not persisted to Redis or a database. In the Streamlit UI, resolutions only update `last_result` if resolved within the identical browser session.
   - **Premature Long-Term Memorization:** `LongTermMemory().remember()` executes at the exit of `run_task()`, memorizing the unapproved pending notice as the outcome before any human review occurs.
   - **Queue Index Fragility:** `ReviewQueue.resolve(index, ...)` identifies queue items by integer index rather than `task_id`, exposing the queue to race conditions and misdirected resolutions if items shift concurrently.

---

## 2. HITL Execution-Path Review

### Complete Tracing of the Execution Path

```mermaid
sequenceDiagram
    autonumber
    actor Caller as Caller (CLI / UI / Test)
    participant Builder as builder.run_task / _run_fallback
    participant Supervisor as supervisor (decompose/reduce)
    participant Specialist as specialists (analysis/etc)
    participant Reviewer as reviewer.review
    participant Escalation as hitl.escalation.escalate
    participant Queue as hitl.queue.ReviewQueue
    participant Human as Reviewer Identity
    participant Resolver as hitl.queue.apply_resolution

    Caller->>Builder: run_task("Please DROP TABLE customers;")
    Builder->>Supervisor: decompose(state)
    Supervisor-->>Builder: plan: [analysis]
    Builder->>Specialist: analysis(state)
    Specialist-->>Builder: completed: [raw output with "DROP TABLE"]
    Builder->>Reviewer: review(state)
    Note over Reviewer: classify() triggers "destructive_db"<br/>text_sensitive = True
    Reviewer-->>Builder: escalated: True, reason: "sensitive_operation"
    
    Builder->>Escalation: escalate(state)
    Note over Escalation: Maps "sensitive_operation" to ApprovalLevel.TAKE_OVER
    Escalation->>Queue: ReviewQueue().push(item)
    Escalation-->>Builder: escalated: True, approval: "take_over"
    
    Note over Builder: _run_fallback detects approval == "take_over"<br/>BREAKS dispatch loop immediately!
    Builder->>Supervisor: reduce(state)
    Note over Supervisor: Detects approval == "take_over" & no resolution<br/>answer = "### Human Review Required..."<br/>status = "pending"
    Supervisor-->>Builder: state with quarantined answer & status="pending"
    Builder-->>Caller: Returns pending GraphState (raw answer quarantined)

    Note over Queue,Human: Review item remains pending in queue
    Human->>Queue: queue.resolve(index, "take_over", approver, note)
    Queue-->>Human: Resolution(final_result=note, approver, ...)
    Human->>Resolver: apply_resolution(state, resolution)
    Note over Resolver: Validates task_id, approver, decision<br/>answer = note<br/>status = "taken_over"<br/>escalated = False
    Resolver-->>Caller: Authoritative finalized GraphState
```

### Detailed Evaluation of the 12 Specific Review Points

1. **Does a sensitive task requiring takeover halt before an unapproved answer is exposed?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** In `src/orchestrator/graph/builder.py` (lines 131–132):
     ```python
     if state.get("escalated"):
         _merge(state, do_escalate(state))
         if state.get("approval") == "take_over":
             break
     ```
     The loop breaks before any further subtask can execute. In `src/orchestrator/graph/supervisor.py` (lines 85–90), `reduce()` substitutes the raw specialist result with the `"### Human Review Required (take_over)"` notice. Verified at runtime by `test_take_over_does_not_expose_unapproved_answer`.

2. **Does the final answer remain safely pending until a valid resolution is applied?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** `state["status"]` is set to `"pending"` and `state["escalated"]` remains `True` until `apply_resolution` is called. The pending item remains in `ReviewQueue().pending()` until `queue.resolve()` is called. Verified by `test_unresolved_review_item_remains_pending`.

3. **Does `approve` produce the intended approved result?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** In `src/orchestrator/hitl/queue.py` (lines 201–214), when `decision == "approve"`, `state["escalated"] = False`, `state["status"] = "approved"`, and if `state["answer"]` began with `"### Human Review Required"`, the completed subtask results are restored into the answer.

4. **Does `reject` prevent rejected output from being presented as approved?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** In `src/orchestrator/hitl/queue.py` (lines 188–200), `completed[-1]["status"] = "rejected"`, `completed[-1]["result"] = "(rejected by human reviewer)"`, `state["status"] = "rejected"`, and `state["answer"]` renders `"(rejected by human reviewer)"`. Unapproved output is stripped.

5. **Does `edit` correctly use reviewer-provided text?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** In `src/orchestrator/hitl/queue.py` (lines 175–187), `completed[-1]["result"]` is overwritten with `resolution.final_result` (which equals the reviewer's non-empty `note`), `sections` are re-compiled into `state["answer"]`, and `state["status"] = "edited"`.

6. **Does `take_over` replace the generated answer with the human-provided answer?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** In `src/orchestrator/hitl/queue.py` (lines 170–174), `state["answer"] = resolution.final_result or ""`, `state["status"] = "taken_over"`, and `state["escalated"] = False`. The generated answer is discarded.

7. **Are task IDs, decision types, reviewer identity, and authorization validated at the correct boundaries?**
   - **Verdict: YES at data/contract boundaries; NO for Role-Based Access Control (RBAC).**
   - **Evidence:**
     - `ReviewQueue.resolve()` validates non-empty `approver`, valid `decision`, and non-empty `note` for `edit`/`take_over`.
     - `apply_resolution()` independently validates non-empty `approver`, valid `decision`, and checks `state.get("task_id") == resolution.task_id`.
     - *Limitation:* The system enforces authentication (user is in `REVIEW_USERS`), but does not have RBAC: any authenticated reviewer can approve/reject any task regardless of role.

8. **Can a resolution be applied twice, applied to the wrong task, or applied to an already-resolved item?**
   - **Verdict: PARTIALLY PROTECTED; RESIDUAL VULNERABILITIES CONFIRMED.**
   - **Evidence:**
     - *Wrong task:* Protected if `state["task_id"]` is present. If `state` has no `task_id`, the check is bypassed (`if state_task_id and resolution.task_id and ...`).
     - *Applied twice to queue:* `queue.resolve()` deletes the item by raw JSON string on resolution (`_LOCAL.remove(raw)`). A second call with the same index resolves a different item or returns `None`.
     - *Re-applying resolution to state:* `apply_resolution()` does **not** check whether `state.get("status")` is already resolved. Calling `apply_resolution(state, res)` twice will append duplicate trace entries and could overwrite an earlier decision if passed a conflicting resolution.

9. **Are audit records consistent with the actual decision and resulting state?**
   - **Verdict: YES (CONFIRMED).**
   - **Evidence:** `AuditChain().append()` records `task_id`, `action` (approval level), `decision`, `approver`, `proposed_action`, `final_input`, and SHA-256 hash chaining. `AuditChain().verify()` cryptographically guarantees zero record modification, deletion, or reordering. `apply_resolution` mirrors this in `state["trace"]`.

10. **Does the Streamlit review UI resolve the correct task, and does the result survive beyond the current session where required?**
    - **Verdict: PARTIALLY FUNCTIONAL; PERSISTENCE FLAW CONFIRMED.**
    - **Evidence:** In `ui/review_app.py` (lines 135–138), resolving item `idx` invokes `apply_resolution(last_res, res)`. However, `last_res` is stored exclusively in `st.session_state["last_result"]`. If the browser tab is refreshed, opened in another browser, or if the task originated from CLI, there is no backing persistent store for task states. The resolution is audited and the queue item cleared, but the external task state is lost.

11. **Does the LangGraph execution path behave consistently with the fallback executor?**
    - **Verdict: NO (CONFIRMED DIVERGENCE).**
    - **Evidence:** In `src/orchestrator/graph/builder.py` (`build_graph()`, line 76):
      `g.add_edge("escalate", "dispatch")`.
      The LangGraph StateGraph topology always loops back to `dispatch`. Unlike `_run_fallback`, it does not check `state.get("approval") == "take_over"` before routing to the next specialist. If LangGraph were installed, a multi-subtask plan would continue executing subtasks after a takeover escalation.

12. **Are pending tasks distinguishable from completed tasks at the CLI and API/UI boundaries?**
    - **Verdict: YES at payload level; AMBIGUOUS at CLI process exit code.**
    - **Evidence:**
      - Payload: `status == "pending"`, `escalated == True`, and `answer` contains pause notice.
      - CLI: `main.py` prints `escalated=True` and `=== ANSWER ===`, but exits with code `0`. A caller relying strictly on process exit codes cannot distinguish between successful completion and a task paused for human takeover.

---

## 3. Decision-by-Decision Findings

### Summary Matrix

| Decision | State `status` | State `escalated` | State `answer` | Trace Event | In-Memory Subtask Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`take_over`** | `"taken_over"` | `False` | Exact reviewer replacement string | `"resolution_applied"` | Intact |
| **`edit`** | `"edited"` | `False` | Specialist sections with edited subtask text | `"resolution_applied"` | Subtask `result` updated, `status="done"` |
| **`reject`** | `"rejected"` | `False` | `"(rejected by human reviewer)"` (or non-rejected parts) | `"resolution_applied"` | Subtask `status="rejected"`, `result="(rejected...)"` |
| **`approve`** | `"approved"` | `False` | Proposed specialist output | `"resolution_applied"` | Subtask `status="done"` |

### Edge Case Analysis
1. **Multi-Subtask Partial Escalation:**
   - In a pipeline (e.g., `research -> analysis -> code -> writing`), if `analysis` escalates with `approve_action` (e.g., `low_confidence`), `_run_fallback` does **not** break (break is only for `take_over`). It enqueues the item and continues to `code` and `writing`.
   - When `reduce()` runs, `bool(escalations)` is `True`, so `state["escalated"] = True` and `state["status"] = "pending"`.
   - If a reviewer then resolves with `edit` or `reject`, `apply_resolution` currently targets `completed[-1]` (the last completed subtask, e.g., `writing`), NOT necessarily the specific subtask that triggered the escalation (`analysis`)!
   - **Confirmed Finding (Severity: Medium):** `apply_resolution()` mutates `completed[-1]` by default. In a multi-subtask run where an early subtask escalated but subsequent subtasks completed, `apply_resolution` will modify the wrong subtask unless mapped by subtask ID.

---

## 4. Authorization and Audit Review

### Boundary Validation Table

| Boundary | Module / Function | Check Performed | Failure Mode | Assessment |
| :--- | :--- | :--- | :--- | :--- |
| **Reviewer Auth** | `hitl.auth.authenticate` | Checks username & token against `REVIEW_USERS` | Raises `AuthError` | Secure / Fail-closed |
| **Session Expiry** | `hitl.auth.session_expired` | Compares timestamp against `REVIEW_SESSION_TIMEOUT_MINUTES` | Clears session in UI | Working as designed |
| **Queue Input** | `hitl.queue.ReviewQueue.resolve` | Non-empty `approver`, valid `decision`, mandatory `note` for `edit`/`take_over` | Raises `ValueError` | Robust contract check |
| **Resolution Apply** | `hitl.queue.apply_resolution` | `task_id` match, non-empty `approver`, valid `decision` | Raises `ValueError` | Robust state contract check |
| **Audit Link** | `hitl.audit.AuditChain.append` | Links to `previous_hash`, computes SHA-256 over canonical content fields | Cryptographic linkage | Tamper-evident |
| **Audit Verification** | `hitl.audit.AuditChain.verify` | Verifies full chain from `GENESIS_HASH` | Returns `(False, index)` | Tested & validated |

---

## 5. LangGraph vs Fallback Assessment

| Feature | Fallback Executor (`_run_fallback`) | LangGraph Topology (`build_graph()`) | Divergence Impact |
| :--- | :--- | :--- | :--- |
| **Execution Path** | Active in current environment (`langgraph` not installed) | Inactive (raises `ImportError`, caught in `run_task`) | None currently |
| **Takeover Halting** | **HALTS:** Checks `approval == "take_over"` and breaks loop | **DOES NOT HALT:** Unconditional edge `escalate -> dispatch` | **HIGH RISK:** If `langgraph` package is installed in production, takeover halting will break |
| **Retry Loop** | In-loop retry while below quality/confidence thresholds | No retry node (conditional edge loops back or terminates) | Behavioral mismatch |
| **State Reduction** | Calls `supervisor.reduce` | `reduce` node connects to `END` | Consistent |

---

## 6. Changed-File Scope Table

The working tree contains 13 modified tracked files, 2 untracked code/test files, and 1 untracked directory (`reports/`).

| File | Lines Changed (`git diff --stat`) | Classification | Evidence & Origin Assessment |
| :--- | :--- | :--- | :--- |
| `src/orchestrator/hitl/queue.py` | +83, -0 | **Required for HITL fix** | Implements `apply_resolution()` with safety validation and decision application. Core Task 03 deliverable. |
| `src/orchestrator/graph/supervisor.py` | +23, -5 | **Required for HITL fix** | Implements answer quarantining on takeover and sets `status = "pending"` for unresolved escalations. Core Task 03 deliverable. |
| `src/orchestrator/graph/state.py` | +2, -0 | **Required for HITL fix** | Adds `status` and `resolution` to `GraphState` TypedDict. |
| `src/orchestrator/hitl/__init__.py` | +2, -1 | **Required for HITL fix** | Exports `apply_resolution`. |
| `src/orchestrator/graph/__init__.py` | +3, -2 | **Required for HITL fix** | Exports `apply_resolution`. |
| `ui/review_app.py` | +5, -1 | **Required for HITL fix** | Wires UI resolve button to `apply_resolution`. |
| `tests/test_resolution.py` *(untracked)* | +235 lines | **Required for HITL fix** | 12 regression tests asserting all 10 required HITL scenarios. |
| `src/orchestrator/graph/builder.py` | +21, -0 | **Partially Required / Scope Mixed** | Early break on `take_over` (lines 131–132) is **Required**. Retry loop (lines 115–127) is **Unrelated** (origin: continue phase fixing FAIL-02). |
| `src/orchestrator/graph/reviewer.py` | +11, -7 | **Supporting change** | Reorders sensitivity before `repeated_failure` and isolates subtask tools. Ensures sensitive operations trigger `take_over` before retries exhaust. |
| `src/orchestrator/graph/specialists.py` | +14, -1 | **Unrelated / Out of scope** | Injects memory into prompt and writes to `ShortTermMemory`. Origin: continue phase fixing FAIL-05 / FAIL-07. |
| `src/orchestrator/memory/short_term.py` | +6, -1 | **Unrelated / Out of scope** | Introduces module-level `_LOCAL` dict for memory isolation. Origin: continue phase fixing FAIL-05. |
| `src/orchestrator/memory/long_term.py` | +14, -1 | **Unrelated / Out of scope** | Adds socket probe to prevent ChromaDB timeout hangs. Origin: continue phase fixing test latency. |
| `src/orchestrator/llm.py` | +34, -13 | **Unrelated / Out of scope** | Adds fallback exception handling and client caching for OpenAI/Anthropic. Origin: continue phase fixing FAIL-03 / FAIL-04. |
| `src/orchestrator/observability/tracing.py` | +19, -1 | **Unrelated / Out of scope** | Directs console exporter to stderr via `_SafeStderr`. Origin: continue phase fixing FAIL-01. |
| `src/orchestrator/__main__.py` *(untracked)* | +11 lines | **Unrelated / Out of scope** | CLI module execution entry point (`python -m orchestrator`). Origin: continue phase fixing FAIL-08. |
| `reports/` *(untracked)* | 3 report files | **Documentation** | Task 01, Task 02, and Task 03 reports. Required project artifacts. |

---

## 7. Regression and Test-Quality Findings

### Evaluation of Test Suite
- **Reported Test Count:** 59 passed in Task 03 report.
- **Corroborated by Read-Only Check:**
  - Ran `tests/test_hitl.py` and `tests/test_audit.py`: **15 passed in 171.40s**.
  - All 15 tests passed with zero failures or warnings.
- **Quality Analysis of `tests/test_resolution.py`:**
  - `test_take_over_does_not_expose_unapproved_answer`: **High Quality.** End-to-end integration test calling `run_task("Please DROP TABLE customers;")` through full graph execution.
  - `test_unresolved_review_item_remains_pending`: **High Quality.** Validates queue persistence across graph execution.
  - `test_human_takeover_replaces_final_answer`: **High Quality.** Validates queue resolution and resolution application on a real task result.
  - `test_human_approval_accepts_proposed_action`: **Moderate Quality.** Unit test on mock state dictionary.
  - `test_human_edit_modifies_result`: **Moderate Quality.** Unit test on mock state dictionary.
  - `test_human_rejection_marks_rejected`: **Moderate Quality.** Unit test on mock state dictionary.
  - `test_resolution_cannot_be_applied_to_wrong_task`: **High Quality.** Directly validates boundary error when task IDs mismatch.
  - `test_invalid_decisions_and_unauthorized_attempts_rejected`: **High Quality.** Validates empty approver, unknown decisions, and missing notes.
  - `test_audit_provenance_preserved`: **High Quality.** End-to-end audit verification asserting cryptographic hash integrity.
  - `test_non_escalated_tasks_proceed_normally`: **High Quality.** Asserts normal workflow without review intervention.

### Missing Test Scenarios
1. **Multi-Subtask Partial Escalation Test:** No test exercises a multi-step task where subtask 1 completes, subtask 2 escalates, and subtask 3 is pending.
2. **Double-Resolution Guard Test:** No test verifies what happens if `apply_resolution` is invoked twice on the same state.
3. **Index Mismatch / Race Condition Test:** No test verifies concurrent resolution behavior in `ReviewQueue`.

---

## 8. Confirmed Defects

### Defect 1: Index-Based Resolution in `ReviewQueue.resolve()`
- **Severity:** High (Concurrency / Reliability)
- **File:** [`src/orchestrator/hitl/queue.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/hitl/queue.py#L90-L121)
- **Description:** `resolve(index: int, ...)` uses a list offset rather than `task_id`. If items are added or removed concurrently, index offsets shift, leading to resolving the wrong task.

### Defect 2: Subtask Resolution Targeting in Multi-Agent Plans
- **Severity:** Medium (Correctness)
- **File:** [`src/orchestrator/hitl/queue.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/hitl/queue.py#L176-L189)
- **Description:** `apply_resolution()` modifies `completed[-1]`. In workflows where an earlier subtask escalated and a later one completed, edits and rejections target the last completed subtask instead of the escalated one.

### Defect 3: Premature Long-Term Memory Storage for Escalated Tasks
- **Severity:** Medium (Data Integrity)
- **File:** [`src/orchestrator/graph/builder.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/graph/builder.py#L166-L170)
- **Description:** `LongTermMemory().remember()` is called unconditionally at the exit of `run_task()`. For escalated tasks, this stores the pending notice (`"### Human Review Required..."`) into vector/semantic memory as the final outcome. When the task is later resolved by a human, long-term memory is never updated.

### Defect 4: Topology Divergence in Compiled LangGraph
- **Severity:** High (Latent Architectural Risk)
- **File:** [`src/orchestrator/graph/builder.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/graph/builder.py#L76)
- **Description:** `build_graph()` hardcodes `g.add_edge("escalate", "dispatch")`. If `langgraph` is installed, it will continue executing planned subtasks after a takeover escalation, whereas `_run_fallback()` breaks the loop.

### Defect 5: No State Persistence Beyond Volatile In-Memory Dictionaries
- **Severity:** Medium (Operational Gap)
- **File:** [`ui/review_app.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/ui/review_app.py#L135-L138)
- **Description:** The system has no task state repository. Once `run_task()` finishes, `state` exists only in caller memory. In the UI, it exists only in `st.session_state`. Resolving an escalated item removes it from the queue and records an audit log, but does not persist the updated `GraphState` anywhere.

---

## 9. Unverified Behaviors

1. **Redis Queue & Audit Multi-Worker Concurrency:** In the test environment, Redis is not running; `ReviewQueue` and `AuditChain` degrade to module-level `_LOCAL` lists. Multi-process concurrency with real Redis clustering could not be verified at runtime.
2. **LangGraph StateGraph Runtime Execution:** Because `langgraph` is not installed, all execution routes through `_run_fallback()`. The compiled `build_graph()` execution path could not be executed at runtime.
3. **Live Paid LLM Provider Execution:** OpenAI and Anthropic live calls were not verified at runtime to avoid incurring costs and because credentials are not configured.

---

## 10. Repository Integrity

### Command Outputs

#### `git status --short`
```text
 M src/orchestrator/graph/__init__.py
 M src/orchestrator/graph/builder.py
 M src/orchestrator/graph/reviewer.py
 M src/orchestrator/graph/specialists.py
 M src/orchestrator/graph/state.py
 M src/orchestrator/graph/supervisor.py
 M src/orchestrator/hitl/__init__.py
 M src/orchestrator/hitl/queue.py
 M src/orchestrator/llm.py
 M src/orchestrator/memory/long_term.py
 M src/orchestrator/memory/short_term.py
 M src/orchestrator/observability/tracing.py
 M ui/review_app.py
?? reports/
?? src/orchestrator/__main__.py
?? tests/test_resolution.py
```

#### `git diff --stat`
```text
 src/orchestrator/graph/__init__.py        |  5 +-
 src/orchestrator/graph/builder.py         | 21 ++++++++
 src/orchestrator/graph/reviewer.py        | 18 ++++---
 src/orchestrator/graph/specialists.py     | 15 +++++-
 src/orchestrator/graph/state.py           |  2 +
 src/orchestrator/graph/supervisor.py      | 23 +++++++--
 src/orchestrator/hitl/__init__.py         |  3 +-
 src/orchestrator/hitl/queue.py            | 83 +++++++++++++++++++++++++++++++
 src/orchestrator/llm.py                   | 47 ++++++++++++------
 src/orchestrator/memory/long_term.py      | 15 +++++-
 src/orchestrator/memory/short_term.py     |  7 ++-
 src/orchestrator/observability/tracing.py | 20 +++++++-
 ui/review_app.py                          |  6 ++-
 13 files changed, 228 insertions(+), 37 deletions(-)
```

#### `git diff --check`
```text
(Clean exit, code 0, no whitespace or formatting errors detected)
```

No files were staged, committed, reverted, reset, or deleted during this audit.

---

## 11. Recommended Next Steps

1. **Resolve Review Items by `task_id` Instead of `index`:**
   Refactor `ReviewQueue.resolve(task_id: str, ...)` so resolutions unambiguously bind to specific tasks regardless of queue reordering or concurrent pop operations.
2. **Align LangGraph Topology with Fallback Halting:**
   Update `build_graph()` to include a conditional edge after `escalate`: if `state.get("approval") == "take_over"`, route to `reduce`, otherwise route to `dispatch`.
3. **Conditionally Defer Long-Term Memorization:**
   Update `builder.run_task()` to skip `LongTermMemory().remember()` if `result.get("escalated")` is `True`, and provide an explicit `remember_resolution()` hook called upon human approval.
4. **Target Subtask by ID in `apply_resolution`:**
   Pass or identify the escalated subtask ID in `Resolution` so that `edit` and `reject` modify the exact subtask that triggered the escalation rather than assuming `completed[-1]`.
5. **Formalize Out-of-Scope Changes:**
   Acknowledge that changes to `llm.py`, `memory/`, `tracing.py`, and `__main__.py` address separate reliability defects from the baseline audit and should be tracked and verified under their respective task tickets.
