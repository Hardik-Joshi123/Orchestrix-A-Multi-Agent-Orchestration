# Task 03B1 Report: HITL Identity and Targeting Hardening

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 03B1  
**Date:** October 19, 2026  
**Status:** COMPLETE  
**Author:** Antigravity Engineering Agent  

---

## 1. Remaining Defects Verified

Following Task 03B, two critical correctness and identity vulnerabilities were verified in the codebase:

1. **Unsafe Subtask Inference from `completed[-1]`:**
   - In `src/orchestrator/graph/reviewer.py`, the reviewer scored the latest subtask but did not propagate the triggering subtask ID (`escalated_subtask_id`) into the returned state dictionary.
   - Consequently, `src/orchestrator/hitl/escalation.py` was forced to infer the escalated subtask solely by taking `completed[-1]`. In multi-agent plans where a subsequent subtask executed or completed before review was packaged, or in parallel branches, `completed[-1]` could point to an arbitrary or unaffected subtask.
   - Furthermore, in `src/orchestrator/hitl/queue.py` (`apply_resolution`), if `subtask_id` was missing or unresolved, `apply_resolution` silently defaulted back to `target_idx = len(completed) - 1`, mutating the wrong subtask.

2. **Review Identifier Ambiguity and Silent Index Hazards:**
   - In `ReviewQueue.resolve()`, when an identifier was a `task_id` string, it iterated over queue items and resolved the first item matching `item.task_id == identifier`. If multiple review items existed for the same task (e.g. multiple subtasks escalating in a pipeline), resolving by `task_id` silently and arbitrarily resolved whichever item appeared first in the list.
   - For legacy integer indices, passing an `int` (e.g. `0`) when multiple items were pending in the queue was vulnerable to silent resolution of the wrong item if the queue order shifted.

---

## 2. Root Cause Analysis

| Defect | Root Cause | Implemented Solution |
| :--- | :--- | :--- |
| **Subtask ID Blind Inference** | `reviewer.py` did not include `escalated_subtask_id` in its state update; `escalation.py` blindly took `completed[-1]`; `apply_resolution` fell back to `len(completed) - 1`. | (1) `reviewer.py` explicitly exports `escalated_subtask_id = latest["id"]`.<br>(2) `state.py` adds `escalated_subtask_id: str` to `GraphState`.<br>(3) `escalation.py` matches `subtask_id` explicitly and never infers from `completed[-1]` when multiple subtasks exist.<br>(4) `apply_resolution()` strictly fails safely (`ValueError`) if `subtask_id` is missing/invalid when multiple completed subtasks exist. |
| **Task ID Ambiguity in Queue** | `queue.resolve()` matched `item.task_id == identifier` without verifying uniqueness across pending items. | `resolve()` prioritizes unique `item_id`. If resolving by `task_id`, it counts matching pending items: if `len(matches) > 1`, it raises `ValueError` ("Ambiguous resolution") requiring the specific `item_id`. |
| **Silent Index Misdirection** | `queue.resolve()` accepted integer offsets regardless of queue length, vulnerable to index shifts. | `resolve()` disallows integer index resolution when `len(raws) > 1`, raising `ValueError` ("Ambiguous legacy index resolution"). Integer index `0` is allowed only when `len(raws) == 1` for safe backward compatibility. |

---

## 3. Files Changed and Reasons

### 1. `src/orchestrator/graph/state.py`
- Added `escalated_subtask_id: str` to the `GraphState` TypedDict definition so the triggering subtask ID is explicitly tracked across LangGraph nodes.

### 2. `src/orchestrator/graph/reviewer.py`
- Added `"escalated_subtask_id": latest["id"] if (escalate and latest) else ""` to the return dictionary of `review()`.
- Added `"subtask_id": latest["id"] if latest else ""` to the reviewer trace event.

### 3. `src/orchestrator/hitl/escalation.py`
- Looked up the escalated subtask explicitly using `state.get("escalated_subtask_id", "")`.
- Removed blind inference from `completed[-1]`. If multiple subtasks exist and `escalated_subtask_id` is not present, it leaves `subtask_id = ""` and `proposed_action = ""` rather than guessing.
- Propagated `subtask_id` into `ReviewItem`, `state["escalations"]`, and `state["trace"]`.

### 4. `src/orchestrator/hitl/queue.py`
- **Ambiguity-Safe Identifier Resolution (`resolve`):**
  - Unique match on `item.item_id` is prioritized.
  - Match on `item.task_id` counts pending items for that task: exactly 1 match resolves safely; multiple matches raise `ValueError` describing ambiguity and demanding `item_id`.
  - Integer index resolution is disallowed when `len(raws) > 1`, raising `ValueError` to prevent accidental misdirection. Only `identifier == 0` when `len(raws) == 1` is accepted for legacy compatibility.
- **Strict Subtask Targeting (`apply_resolution`):**
  - Removed blind fallback `target_idx = len(completed) - 1`.
  - When applying `edit` or `reject`: if `resolution.subtask_id` is specified, it must exist in `completed` (raises `ValueError` if not found).
  - If `resolution.subtask_id` is empty: allowed only when exactly 1 subtask exists in `completed`; if multiple subtasks exist, raises `ValueError` requiring `subtask_id`.

### 5. `tests/test_resolution.py`
- Added 4 new regression tests (Tests 20–23) verifying subtask ID propagation from review trigger, ambiguity rejection when multiple items exist for the same task, legacy index rejection when multiple items are pending, and safe failure when `subtask_id` is missing in multi-subtask states.

---

## 4. Tests Added and Exact Results

### Test Execution Commands and Results

#### Focused Task 03B1 Suite
- **Command:** `$env:PYTHONPATH="src"; python -m pytest tests/test_resolution.py -k "test_subtask_id_propagated or test_multiple_review_items or test_legacy_index or test_apply_resolution_rejects" -v`
- **Result:** **4 passed, 21 deselected in 76.82s (100% passing)**

#### Full Resolution Regression Suite (25 Tests)
- **Command:** `$env:PYTHONPATH="src"; python -m pytest tests/test_resolution.py -v`
- **Result:** **25 passed in 419.07s (100% passing)**

#### Baseline HITL Test Suite (10 Tests)
- **Command:** `$env:PYTHONPATH="src"; python -m pytest tests/test_hitl.py -v`
- **Result:** **10 passed in 151.54s (100% passing)**

### Breakdown of Task 03B1 Tests

| Test Function | Verification Scope | Status |
| :--- | :--- | :---: |
| `test_subtask_id_propagated_from_review_to_queue_item` | Confirms triggering subtask ID (`s1`) is explicitly propagated from reviewer output through `escalate()` into `ReviewItem.subtask_id` and trace events. | **PASS** |
| `test_multiple_review_items_same_task_ambiguity_fails_safely` | Pushes two items for the same `task_id`. Calling `queue.resolve(task_id)` raises `ValueError` ("Ambiguous resolution"). Resolving by unique `item_id` succeeds for each item. | **PASS** |
| `test_legacy_index_resolution_blocked_when_multiple_items_pending` | Pushes two items to queue. Calling `queue.resolve(0)` raises `ValueError` ("Ambiguous legacy index resolution"). When only one item remains, index `0` succeeds. | **PASS** |
| `test_apply_resolution_rejects_missing_or_invalid_subtask_in_multi_subtask_state` | In a multi-subtask plan (`s1`, `s2`), calling `apply_resolution` with `subtask_id=""` raises `ValueError` ("subtask_id is required"). Calling with unknown `s99` raises `ValueError`. | **PASS** |

---

## 5. Remaining Concurrency Limitations

1. **In-Memory Store Concurrency (`_LOCAL`):**
   - In development/test mode without an external Redis instance, `ReviewQueue` and `AuditChain` degrade to Python module-level lists (`_LOCAL`). These lists are not thread-safe and do not support multi-process synchronization.
2. **Redis Check-and-Remove Atomicity:**
   - When connected to Redis, `ReviewQueue.resolve()` executes `lrange` followed by `lrem`. This is two separate Redis commands. In a high-concurrency environment with multiple workers concurrently resolving queue items, a Redis Lua script or `WATCH`/`MULTI`/`EXEC` block would be required to guarantee strict transactional atomicity.

---

## 6. LangGraph Verification Status

- **Status:** **NOT VERIFIED AT RUNTIME (Dependency Absent)**
- **Details:** The `langgraph` package is not installed in the local Python environment. Graph construction in `build_graph()` is protected by an `ImportError` catch block in `run_task()`, which safely routes execution to `_run_fallback()`.
- **Static Verification:** Unit tests (`test_langgraph_routing_decision_logic`) verify that `_after_escalate()` routes `approval == "take_over"` to `"reduce"` and other approval levels to `"dispatch"`. However, end-to-end execution of a compiled LangGraph `StateGraph.invoke()` cannot be performed without installing the dependency.

---

## 7. Final Git Status and Integrity

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
 src/orchestrator/graph/builder.py         |  41 +++++-
 src/orchestrator/graph/reviewer.py        |  20 +--
 src/orchestrator/graph/specialists.py     |  15 +-
 src/orchestrator/graph/state.py           |   3 +
 src/orchestrator/graph/supervisor.py      |  23 ++-
 src/orchestrator/hitl/__init__.py         |   3 +-
 src/orchestrator/hitl/escalation.py       |  26 +++-
 src/orchestrator/hitl/queue.py            | 225 ++++++++++++++++++++++++++++--
 src/orchestrator/llm.py                   |  47 +++++--
 src/orchestrator/memory/long_term.py      |  15 +-
 src/orchestrator/memory/short_term.py     |   7 +-
 src/orchestrator/observability/tracing.py |  20 ++-
 ui/review_app.py                          |  14 +-
 14 files changed, 400 insertions(+), 64 deletions(-)
```

### Integrity Checks
- `git diff --check`: Clean exit (code 0), zero formatting or whitespace issues.
- `python -m ruff check src tests ui`: `All checks passed!`
- `python -m ruff format --check src tests ui`: `35 files already formatted`
- Existing reports preserved:
  - `reports/TASK_01_BASELINE_AUDIT_OPUS.md`
  - `reports/TASK_02_RUNTIME_VERIFICATION.md`
  - `reports/TASK_03_HITL_SAFETY_FIX.md`
  - `reports/TASK_03A_HITL_REVIEW.md`
  - `reports/TASK_03B_HITL_HARDENING.md`
