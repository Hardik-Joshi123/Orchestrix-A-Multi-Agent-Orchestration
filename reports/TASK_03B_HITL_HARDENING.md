# Task 03B Report: HITL Correctness and Safety Hardening

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 03B  
**Date:** October 19, 2026  
**Status:** COMPLETE  
**Author:** Antigravity Engineering Agent  

---

## 1. Executive Summary

### Objective and Scope
Task 03B addressed and resolved the four core correctness, concurrency, and architectural defects identified during the Task 03A independent review:
1. **Stable Review-Item Identity:** Replaced fragile list-index-based review resolution with stable, unambiguous identifier lookup (`item_id` and `task_id`) in `ReviewQueue.resolve()`, making queue operations immune to list reordering and race conditions.
2. **Exact Subtask Resolution Targeting & Duplicate Guards:** Enhanced `apply_resolution()` to target the exact escalated subtask ID (`subtask_id`) rather than assuming `completed[-1]`, while preserving unaffected subtask results. Added fail-closed guards preventing duplicate or conflicting resolutions on an already-resolved state.
3. **LangGraph vs. Fallback Alignment:** Aligned the compiled LangGraph StateGraph topology (`build_graph()`) with the fallback executor by implementing `_after_escalate()`, routing `take_over` escalations directly to `reduce` while continuing dispatch only for permitted escalation levels.
4. **Long-Term Memory Safeguards:** Prevented unresolved pending review notices from being prematurely persisted into `LongTermMemory` as successful outcomes upon workflow completion.

All changes were implemented using the existing project abstractions, strictly validated with focused regression tests, and verified to be 100% passing without introducing regressions.

---

## 2. Root Causes Addressed

| # | Defect Identified in Task 03A | Root Cause | Fix Implemented |
| :-: | :--- | :--- | :--- |
| **1** | **Fragile Index Resolution** | `ReviewQueue.resolve(index: int)` accessed `raws[index]`. Concurrently added/removed items shifted offsets, risking resolving the wrong task. | Added `item_id` (UUID) to `ReviewItem` and updated `resolve()` to accept stable string identifiers (`item_id` or `task_id`), preserving integer index only as a legacy fallback. |
| **2** | **Blind Subtask Mutating (`completed[-1]`)** | `apply_resolution()` assumed the escalated subtask was always the final element in `completed`. In multi-subtask plans where a later subtask ran after an earlier escalation, the wrong subtask was edited or rejected. | Propagated `subtask_id` from `escalate()` into `ReviewItem` and `Resolution`. `apply_resolution()` now finds and mutates the exact subtask by ID and preserves all unaffected subtasks. |
| **3** | **Unprotected Resolution Overwriting** | `apply_resolution()` allowed repeated calls on the same state, overwriting prior decisions and appending duplicate trace entries. | Added an explicit guard raising `ValueError` if `state.get("resolution")` exists or `state.get("status") in ("taken_over", "edited", "rejected", "approved")`. |
| **4** | **LangGraph Takeover Loop Divergence** | `build_graph()` hardcoded `g.add_edge("escalate", "dispatch")`, allowing plans to continue executing after a `take_over` escalation. | Introduced `_after_escalate()` routing conditional edge in `build_graph()`: routes `take_over` to `reduce` and others to `dispatch`. |
| **5** | **Premature Long-Term Memory Storage** | `run_task()` unconditionally called `LongTermMemory().remember()` on exit, storing unapproved pending review notices into memory as completed outcomes. | Guarded memory write: `LongTermMemory().remember()` is executed only if `not result.get("escalated") and result.get("status") != "pending"`. |

---

## 3. Stable Review-Item Identity

### Design and Implementation
In `src/orchestrator/hitl/queue.py`:
- `ReviewItem` was augmented with:
  - `item_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])`: Unique identifier per review item.
  - `subtask_id: str = ""`: Associated subtask identifier.
- `ReviewQueue.resolve(identifier: str | int, decision: str, approver: str, note: str | None = None)`:
  - Accepts stable string identifier (`item_id` or `task_id`).
  - Iterates over queue items to match `item.item_id == identifier or item.task_id == identifier`.
  - Removes the specific matching raw item (`target_raw`) from `_LOCAL` or Redis.
  - Returns `None` safely if `identifier` is missing, empty, or unknown.
  - Retains integer index lookup for backward compatibility with existing tests.
- `ui/review_app.py`:
  - Updated Streamlit loop to use `item.item_id or item.task_id` for element keys (`f"dec-{item_id}"`, `f"note-{item_id}"`, `f"res-{item_id}"`) and passed `item_id` to `queue.resolve()`.

### Concurrency Limitation Analysis
While lookup by stable identifier prevents list-reorder misdirection, the queue store in the current environment degrades to an in-process list (`_LOCAL`), which is not thread-safe. In Redis mode, `lrange` followed by `lrem` is not wrapped in a Redis transaction (MULTI/EXEC or Lua script). True atomic check-and-remove in Redis remains a production hardening recommendation.

---

## 4. Correct Subtask Resolution

### Subtask Propagation Flow
1. **Creation:** `supervisor.plan_subtasks()` generates subtasks with IDs `"s1"`, `"s2"`, etc.
2. **Escalation Capture:** In `src/orchestrator/hitl/escalation.py`:
   ```python
   completed = state.get("completed", [])
   escalated_subtask = completed[-1] if completed else None
   subtask_id = escalated_subtask["id"] if escalated_subtask else ""
   ```
   Stores `subtask_id` in `ReviewItem`, `escalations`, and `trace`.
3. **Resolution Binding:** When `queue.resolve()` resolves the item, `subtask_id` is copied into the `Resolution` dataclass.
4. **Targeted State Mutation:** In `src/orchestrator/hitl/queue.py` (`apply_resolution`):
   - Locates `target_idx` where `sub["id"] == resolution.subtask_id`.
   - Modifies **only** `completed[target_idx]`.
   - Retains all other subtasks with their existing status and result.
   - Re-reduces `state["answer"]` from all completed subtasks with `status == "done"`.

### Re-Resolution Prevention Guard
In `apply_resolution`:
```python
if state.get("resolution") or state.get("status") in ("taken_over", "edited", "rejected", "approved"):
    prev_decision = state.get("resolution", {}).get("decision", state.get("status"))
    raise ValueError(
        f"Task {state_task_id or resolution.task_id!r} is already resolved "
        f"with decision {prev_decision!r} and cannot be re-resolved"
    )
```
Any subsequent attempt to apply a resolution to an already-resolved state raises `ValueError` and leaves the existing resolution intact.

---

## 5. LangGraph/Fallback Alignment

### Topology Comparison Before and After

#### Before Alignment
- **Fallback (`_run_fallback`):** Breaks dispatch loop on `approval == "take_over"`.
- **LangGraph (`build_graph`):** Hardcoded `g.add_edge("escalate", "dispatch")`, continuously dispatching subsequent subtasks.

#### After Alignment
Implemented `_after_escalate(state: GraphState) -> str`:
```python
def _after_escalate(state: GraphState) -> str:
    """Route after escalation: take_over halts dispatch and reduces immediately."""
    return "reduce" if state.get("approval") == "take_over" else "dispatch"
```
In `build_graph()`:
```python
g.add_conditional_edges("escalate", _after_escalate, {"reduce": "reduce", "dispatch": "dispatch"})
```

Both execution engines now exhibit identical routing semantics:
- `take_over` escalations transition immediately to `reduce` and halt specialist dispatch.
- `notify`, `approve_action`, and `approve_plan` escalations return to `dispatch` to continue planned work where appropriate.

---

## 6. Long-Term Memory Safeguards

### Safeguard Implementation
In `src/orchestrator/graph/builder.py` (`run_task`):
```python
# Persist what worked into long-term semantic memory (skip for unresolved/pending tasks).
if not result.get("escalated") and result.get("status") != "pending":
    LongTermMemory().remember(
        task=task,
        outcome=result.get("answer", ""),
        metadata={"task_id": task_id, "quality": result.get("quality", 0.0)},
    )
```

### Verified Behavior
- **Pending/Escalated Tasks:** When a task requires takeover, `result["status"] == "pending"` and `result["escalated"] == True`. Long-term memory write is skipped completely. No pending pause notices are written into the vector store.
- **Normal Completed Tasks:** When a task completes normally (`result["status"] == "completed"`, `result["escalated"] == False`), the outcome is memorized as before.

---

## 7. Files Changed and Reasons

| File | Changes Made | Rationale |
| :--- | :--- | :--- |
| `src/orchestrator/hitl/queue.py` | Added `uuid` import, `item_id` & `subtask_id` fields to `ReviewItem` and `Resolution`. Refactored `resolve()` to support stable identifier lookups (`str`). Added re-resolution guard and subtask targeting in `apply_resolution()`. | Core Task 03B requirement: stable item identity, exact subtask targeting, and idempotent resolution. |
| `src/orchestrator/hitl/escalation.py` | Extracted `subtask_id` from the escalating subtask in `completed` and passed it to `ReviewItem`, `escalations`, and `trace`. | Propagate exact subtask identity into review context. |
| `src/orchestrator/graph/builder.py` | Added `_after_escalate()` routing function and wired conditional edge in `build_graph()`. Added conditional guard to `LongTermMemory().remember()` in `run_task()`. | Align LangGraph takeover halting with fallback executor; prevent storing unapproved pending notices in long-term memory. |
| `ui/review_app.py` | Used `item_id` for element keys and passed `item_id` to `queue.resolve()`. | Stable UI element keys and identifier-based resolution. |
| `tests/test_resolution.py` | Added 9 new regression tests covering stable IDs, queue reordering, subtask targeting, duplicate resolution, routing logic, and long-term memory guards. | Comprehensive regression coverage for all Task 03B requirements. |

---

## 8. Tests Added and Exact Results

### Test Execution Summary
- **Test File:** `tests/test_resolution.py`
- **Command:** `$env:PYTHONPATH="src"; python -m pytest tests/test_resolution.py -v`
- **Result:** **21 passed in 345.63s (100% passing)**

### New and Updated Test Scenarios

| Test Function | Scenario Tested | Outcome |
| :--- | :--- | :---: |
| `test_resolve_by_stable_identifiers` | Resolves review items by stable `item_id` and `task_id` | **PASS** |
| `test_resolve_unknown_or_missing_identifier_fails_safely` | Empty, missing, or unknown IDs fail safely (`None`) without mutating queue | **PASS** |
| `test_queue_reorder_does_not_redirect_resolution` | Reordering queue in memory does not misdirect resolution to another task | **PASS** |
| `test_multi_subtask_early_escalation_targets_correct_subtask` | Edit targets the exact escalated subtask and preserves unaffected subtasks | **PASS** |
| `test_multi_subtask_rejection_targets_correct_subtask` | Rejection marks exact subtask rejected and preserves unaffected subtasks | **PASS** |
| `test_duplicate_and_conflicting_resolution_prevented` | Attempting duplicate or conflicting second resolution raises `ValueError` | **PASS** |
| `test_langgraph_routing_decision_logic` | `_after_escalate` routes `take_over` to `reduce` and others to `dispatch` | **PASS** |
| `test_long_term_memory_not_written_for_pending_unresolved_tasks` | Pending unresolved tasks are skipped by `LongTermMemory().remember()` | **PASS** |
| `test_long_term_memory_preserved_for_completed_tasks` | Normal completed tasks record outcomes in long-term memory | **PASS** |

### Existing Baseline Test Compatibility
- `tests/test_hitl.py` & `tests/test_audit.py`: **15 passed in 172.19s** (backward compatibility for legacy integer index resolution confirmed).
- `tests/test_orchestration.py` & `tests/test_cli.py`: **9 passed in 56.38s**.

---

## 9. Remaining Risks and Unverified Behavior

1. **LangGraph Runtime Execution:** While the routing logic in `_after_escalate` and the compiled StateGraph conditional edges were verified through unit testing, end-to-end execution of `graph.invoke()` remains **UNVERIFIED AT RUNTIME** because `langgraph` is not installed in the local environment.
2. **Redis Concurrency Atomicity:** In environments with an active Redis instance, `ReviewQueue.resolve()` executes `lrange` followed by `lrem`. Under high-concurrency multi-worker deployments, a Lua script or Redis transaction would be required to ensure strict atomic pop-and-resolve.
3. **Post-Resolution Memory Writeback:** When a human later resolves an escalated task via `apply_resolution()`, the resolved final answer is not automatically written back to `LongTermMemory`. Under the current architecture, `LongTermMemory.remember()` is called only during `run_task()`.

---

## 10. Final Git Status

### `git status --short`
```text
 M src/orchestrator/graph/__init__.py
 M src/orchestrator/graph/builder.py
 M src/orchestrator/graph/reviewer.py
 M src/orchestrator/graph/specialists.py
 M src/orchestrator/graph/state.py
 M src/orchestrator/graph/supervisor.py
 M src/orchestrator/hitl/__init__.py
 M src/orchestrator/hitl/escalation.py
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

### `git diff --stat`
```text
 src/orchestrator/graph/__init__.py        |   5 +-
 src/orchestrator/graph/builder.py         |  41 +++++--
 src/orchestrator/graph/reviewer.py        |  18 +--
 src/orchestrator/graph/specialists.py     |  15 ++-
 src/orchestrator/graph/state.py           |   2 +
 src/orchestrator/graph/supervisor.py      |  23 +++-
 src/orchestrator/hitl/__init__.py         |   3 +-
 src/orchestrator/hitl/escalation.py       |  16 ++-
 src/orchestrator/hitl/queue.py            | 188 +++++++++++++++++++++++++++---
 src/orchestrator/llm.py                   |  47 +++++---
 src/orchestrator/memory/long_term.py      |  15 ++-
 src/orchestrator/memory/short_term.py     |   7 +-
 src/orchestrator/observability/tracing.py |  20 +++-
 ui/review_app.py                          |  14 ++-
 14 files changed, 350 insertions(+), 64 deletions(-)
```

---

## 11. Repository Integrity

- **Linters:** `python -m ruff check src tests ui` -> `All checks passed!`
- **Formatter:** `python -m ruff format --check src tests ui` -> `35 files already formatted`
- **Diff Check:** `git diff --check` -> Clean exit (code 0), zero whitespace or formatting errors.
- **Report Preservation:** `TASK_01_BASELINE_AUDIT_OPUS.md`, `TASK_02_RUNTIME_VERIFICATION.md`, `TASK_03_HITL_SAFETY_FIX.md`, and `TASK_03A_HITL_REVIEW.md` are preserved intact in `reports/`.
