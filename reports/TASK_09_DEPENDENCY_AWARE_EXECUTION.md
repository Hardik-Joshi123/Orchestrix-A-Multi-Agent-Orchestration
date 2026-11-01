# Task 09 Report: Dependency-Aware Specialist Execution

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 09  
**Date:** October 19, 2026  
**Status:** COMPLETE  
**Author:** Antigravity Engineering Agent  

---

## 1. Existing Subtask Dependency Flow and Identified Gaps

Before Task 09, inspection of `src/orchestrator/graph/state.py`, `src/orchestrator/graph/supervisor.py`, `src/orchestrator/graph/builder.py`, `src/orchestrator/graph/specialists.py`, and existing test suites revealed the following architectural gaps:

1. **Absence of Subtask Dependency Representation:**
   - In `src/orchestrator/graph/state.py`, `Subtask` only possessed fields `id`, `kind`, `instruction`, `status`, `result`, `confidence`, `error`, `tools_used`.
   - There was no explicit field (`depends_on: list[str]`) to declare prerequisites between subtasks.
   - Decomposition in `supervisor.py` assumed either a flat list of independent subtasks or implicitly executed them sequentially without tracking prerequisites.

2. **Indiscriminate Specialist Dispatch on Failed Upstream Tasks:**
   - In `src/orchestrator/graph/builder.py`, `_next_specialist` simply took `pending[0]` from the plan:
     ```python
     done_ids = {c["id"] for c in state.get("completed", [])}
     pending = [s for s in state.get("plan", []) if s["id"] not in done_ids]
     ```
   - When an upstream subtask failed, downstream subtasks that strictly depended on its output were dispatched anyway, executing invalid computations on missing upstream data.

3. **Missing "Blocked" Lifecycle Status:**
   - Subtask statuses were constrained to `Literal["pending", "done", "rejected", "failed"]`.
   - Subtasks that could not execute because an upstream prerequisite failed had no accurate status representation and could neither be distinguished from subtasks that ran and failed nor from subtasks still waiting to run.

4. **Lack of Prerequisite Context Propagation:**
   - In `specialists.py`, specialist prompts only received memories and the subtask's own `instruction`.
   - Results produced by completed upstream prerequisite subtasks were never passed as context to downstream specialists.

5. **No Graph Reduction or Tracing for Blocked Subtasks:**
   - `supervisor.reduce` only inspected `done` and `failed` subtasks.
   - It did not recognize blocked subtasks, could not report them in the final answer or trace events, and did not transition final workflow status to `"partial"` or `"blocked"`.

6. **Unsafe Handling of Dependency Deadlocks and Cycles:**
   - Circular dependencies (`s1 <-> s2`) or references to missing subtask IDs had no detection mechanism, risking execution deadlocks or infinite loops.

---

## 2. Root Cause Analysis

| Defect | Root Cause | Implemented Solution |
| :--- | :--- | :--- |
| **No Dependency Declaration Field** | `Subtask` schema lacked dependency metadata. | Added `depends_on: list[str]` to `Subtask` and updated `status` to include `"blocked"`. Added `blocked: Annotated[list[Subtask], operator.add]` to `GraphState`. |
| **Execution of Blocked Downstream Subtasks** | Dispatch logic routed to `pending[0]` regardless of prerequisite success or failure. | Implemented `check_dependencies(state)` in `_dispatch` and topological ready-check in `_next_specialist`. Subtasks with failed/missing dependencies transition to `"blocked"`. |
| **Cascading Failures Unhandled** | No transitive dependency resolution. | Implemented fixpoint iteration in `check_dependencies` so failure of `s1` cascades to block `s2`, which transitively blocks `s3`. |
| **Missing Dependency Output Context** | Specialists only received static instruction text. | Updated `_run_specialist` in `specialists.py` to inject outputs from completed prerequisite subtasks directly into the downstream prompt. |
| **Reducer Blind to Blocked State** | `supervisor.reduce` only processed `done` and `failed`. | Enhanced `reduce` to extract `blocked` subtasks, report them in trace telemetry and ShortTermMemory, and assign final status `"partial"` or `"blocked"`. |
| **Circular / Deadlock Vulnerability** | No cycle detection among pending subtasks. | Added cycle and missing dependency detection in `check_dependencies` that safely transitions unresolvable subtasks to `"blocked"` with clear diagnostic reasons. |

---

## 3. Implementation and Files Changed

### 1. `src/orchestrator/graph/state.py`
- Updated `Subtask`:
  - Added `status: Literal["pending", "done", "rejected", "failed", "blocked"]`.
  - Added `depends_on: list[str]`.
- Updated `GraphState`:
  - Added `blocked: Annotated[list[Subtask], operator.add]`.
  - Updated status documentation to include `"blocked"`.

### 2. `src/orchestrator/graph/supervisor.py`
- Updated `plan_subtasks(task)`:
  - Initializes each planned subtask with `depends_on: []` (ensures sequential subtask IDs are not falsely assumed to be dependent).
- Updated `parse_plan_payload(raw, default_instruction)`:
  - Extracts `depends_on`, `dependencies`, and `prerequisites` (supporting both string IDs and lists of string IDs).
  - Preserves user-provided `id` if present (e.g. `step_a`), falling back to `f"s{i + 1}"`.
- Updated `supervisor.reduce`:
  - Filters and deduplicates `blocked` subtasks from `state["completed"]` and `state["blocked"]`.
  - Determines terminal workflow status:
    - `"partial"` if some subtasks succeeded (`done`) while others failed or were blocked.
    - `"failed"` if no subtasks succeeded and failures occurred.
    - `"blocked"` if all subtasks were blocked by unmet dependencies.
    - `"completed"` if all subtasks succeeded without unresolved failures or blocks.
  - Updates trace event with `blocked: len(blocked)`.
  - Stores `short.set("blocked", [b["id"] for b in blocked])` in `ShortTermMemory`.

### 3. `src/orchestrator/graph/builder.py`
- Updated `_ACCUMULATING`:
  - Added `"blocked"` to `_ACCUMULATING = ("trace", "completed", "failed", "blocked", "tools_used", "escalations")`.
- Implemented `check_dependencies(state: GraphState) -> GraphState`:
  - Detects missing dependencies not present in the plan.
  - Iteratively identifies pending subtasks whose prerequisites are in `failed` or `blocked` sets and transitions them to `status: "blocked"` with error metadata (e.g. `"Upstream dependency 's1' failed or was blocked"`).
  - Detects circular dependencies or unresolvable deadlocks among pending subtasks, transitioning deadlocked subtasks to `"blocked"`.
  - Records blocked subtask records into `ShortTermMemory` and appends `"event": "blocked"` trace events.
- Updated `_dispatch(state)`:
  - Calls `check_dependencies(state)` to transition newly blocked subtasks before dispatch routing.
- Updated `_next_specialist(state)`:
  - Filters subtasks whose prerequisites are strictly satisfied (`status == "done"`).
  - Routes to the first ready subtask, or returns `"reduce"` if all subtasks are finished or blocked.
- Updated `_run_fallback(state)`:
  - Merges `_dispatch(state)` at each iteration and after the loop to ensure parity with LangGraph's node topology.
- Updated `run_task`:
  - Initializes `"blocked": []` in initial state.
  - Excludes `"blocked"` outcomes from being stored in `LongTermMemory`.

### 4. `src/orchestrator/graph/specialists.py`
- Updated subtask selection in `_run_specialist`:
  - Selects the next pending subtask whose prerequisites are all completed with `status == "done"`.
- Enhanced prompt construction:
  - Collects outputs from completed prerequisite subtasks specified in `depends_on` and injects them into the downstream specialist's prompt context.

### 5. `tests/test_dependencies.py`
- Created comprehensive test suite verifying:
  - Linear prerequisite failure blocking (`s1 -> s2`).
  - Independent branches executing when another branch fails (`s1` fails, `s2` blocked, `s3` and `s4` succeed).
  - Cascading transitive blocking (`s1 -> s2 -> s3`).
  - Diamond dependency resolution order (`s1 -> (s2, s3) -> s4`).
  - Recovery on retry: downstream subtask unblocked and executed when prerequisite succeeds on retry.
  - Recovery via HITL resolution: human approval/edit unblocks downstream subtask; human rejection blocks downstream subtask.
  - Safe handling of missing dependencies not in the plan.
  - Deadlock detection for circular dependencies (`s1 <-> s2`).
  - Dependency alias parsing in `parse_plan_payload`.
  - Memory isolation: `ShortTermMemory` records blocked subtasks, `LongTermMemory` omits blocked task outcomes.
  - Direct reducer unit tests for partial and blocked statuses.

---

## 4. Verification and Exact Test Results

### 1. Focused Dependency Test Suite (`tests/test_dependencies.py`)
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_dependencies.py -v
```
**Output:**
```text
============================= test session starts =============================
platform win32 -- Python 3.11.3, pytest-7.4.3, pluggy-1.6.0
rootdir: C:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator
configfile: pyproject.toml
plugins: anyio-3.7.1, dash-2.14.2, Faker-38.2.0, asyncio-0.21.1, flask-1.2.0
asyncio: mode=Mode.STRICT
collected 12 items

tests\test_dependencies.py::test_subtask_blocked_when_linear_prerequisite_fails PASSED [  8%]
tests\test_dependencies.py::test_independent_subtasks_execute_when_another_branch_fails PASSED [ 16%]
tests\test_dependencies.py::test_cascading_dependencies_blocked_transitively PASSED [ 25%]
tests\test_dependencies.py::test_diamond_dependencies_execute_in_correct_order PASSED [ 33%]
tests\test_dependencies.py::test_prerequisite_recovery_on_retry_unblocks_downstream PASSED [ 41%]
tests\test_dependencies.py::test_prerequisite_recovery_via_hitl_resolution PASSED [ 50%]
tests\test_dependencies.py::test_prerequisite_rejection_via_hitl_blocks_downstream PASSED [ 58%]
tests\test_dependencies.py::test_missing_dependency_in_plan_is_blocked_safely PASSED [ 66%]
tests\test_dependencies.py::test_circular_dependency_deadlock_detected_safely PASSED [ 75%]
tests\test_dependencies.py::test_parse_plan_payload_handles_dependency_aliases PASSED [ 83%]
tests\test_dependencies.py::test_short_term_and_long_term_memory_isolation_with_blocked PASSED [ 91%]
tests\test_dependencies.py::test_reducer_partial_and_blocked_status PASSED [100%]

============================= 12 passed in 35.03s =============================
```

### 2. Core Orchestration and Reliability Regression Suites
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_dependencies.py tests/test_planning.py tests/test_orchestration.py tests/test_specialist_reliability.py tests/test_tools_safety.py -v
```
**Output:**
```text
======================== 46 passed in 80.10s (0:01:20) ========================
```

### 3. Full Repository Regression Run
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_resolution.py tests/test_hitl.py tests/test_memory.py tests/test_cli.py tests/test_security.py tests/test_smoke.py tests/test_builder_errors.py tests/test_audit.py -v
```
**Output:**
```text
======================= 86 passed in 563.18s (0:09:23) ========================
```
**Total Repository Tests:** 132 passed, 0 failed.

### 4. Code Quality and Linter Verification
```powershell
python -m ruff check src tests
```
**Output:**
```text
All checks passed!
```

---

## 5. Architectural Invariants Preserved

1. **Independent Subtasks Execute Safely:**
   - Failure of one subtask does NOT halt or abort parallel/independent branches unless an explicit dependency exists.
   - Sequential subtask IDs do not implicitly enforce dependencies unless declared in `depends_on`.

2. **Blocked Work is Never Marked Completed:**
   - Blocked subtasks receive `status: "blocked"` with an informative error stating the specific failed prerequisite.
   - They are never marked `"done"` and never execute specialist or tool calls.

3. **Retries and HITL Recovery Work Seamlessly:**
   - If an upstream subtask recovers via retry or is approved/edited by a human reviewer, its dependent subtasks immediately become ready and execute to completion.
   - If an upstream subtask is rejected by human review, downstream subtasks are blocked safely.

4. **Accurate Memory and Telemetry Isolation:**
   - Blocked subtasks are recorded in `ShortTermMemory` under `subtask:<id>` and tracked under `blocked`.
   - Workflows with `"blocked"` status are never written to `LongTermMemory`.
   - Dispatch node emits `"event": "blocked"` trace spans with subtask identity and failure reasons.
