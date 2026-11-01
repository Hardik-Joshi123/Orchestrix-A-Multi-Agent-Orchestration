# Task 06 Report: Make Supervisor Planning Drive Execution

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 06  
**Date:** October 19, 2026  
**Status:** COMPLETE  
**Author:** Antigravity Engineering Agent  

---

## 1. Executive Summary & Objective

In previous tasks (Tasks 01–05), the multi-agent orchestrator was audited, verified, and hardened for Human-in-the-Loop (HITL) safety, CLI JSON hygiene, and short/long-term memory integration.

The objective of **Task 06** was to ensure that the supervisor's decomposition and planning stage meaningfully dictates which specialist agents execute and in what order, replacing rigid hardcoded keyword matching while maintaining safe fallback behaviors, registered specialist validation, HITL controls, and memory persistence.

---

## 2. Current Behavior and Root Cause Analysis

### Identified Gaps Before Task 06

1. **LLM Decomposition Call Discarded:**
   - In `src/orchestrator/graph/supervisor.py:decompose()`, the code previously called:
     ```python
     llm = LLMClient()
     llm.complete(
         system="You are a supervisor that decomposes tasks...",
         prompt=f"Decompose this task into ordered subtasks: {task}",
         json_mode=True,
     )
     plan = plan_subtasks(task)
     ```
   - The completion object returned by `llm.complete()` was completely ignored and discarded.
   - Immediately following the call, `plan = plan_subtasks(task)` was invoked unconditionally.

2. **Hardcoded Keyword Routing and Canonical Ordering:**
   - `plan_subtasks(task)` evaluated regex keyword patterns against the task string (`_KEYWORD_ROUTES`).
   - It always sorted the resulting subtasks according to a static canonical tuple:
     ```python
     _ORDER = ("research", "analysis", "code", "writing")
     ```
   - Consequently, dynamic plan generation had zero effect on specialist selection or execution order. Tasks requiring non-standard sequences (e.g. "writing" before "research") or multi-step execution by the same specialist kind were impossible.

3. **Missing Specialist Validation and Normalization:**
   - There was no validation layer for planned specialist names against registered specialist nodes (`"research"`, `"analysis"`, `"code"`, `"writing"`).
   - If an LLM proposed a role alias (e.g., `"coder"`, `"developer"`, `"writer"`) or an unknown role (e.g., `"summarizer"`, `"qa"`), routing would fail or crash the graph dispatcher.

---

## 3. Implementation Details

### 1. Registered Specialists Validation and Normalization
In `src/orchestrator/graph/supervisor.py`:
- Defined `VALID_SPECIALISTS = ("research", "analysis", "code", "writing")`.
- Defined `SPECIALIST_ALIASES` mapping common synonyms:
  - `"coder"`, `"coding"`, `"developer"`, `"programmer"`, `"software"` -> `"code"`
  - `"writer"`, `"write"`, `"documentation"`, `"summary"` -> `"writing"`
  - `"researcher"`, `"investigate"`, `"search"` -> `"research"`
  - `"analyst"`, `"analyze"`, `"evaluation"`, `"evaluator"` -> `"analysis"`
- Implemented `normalize_specialist_kind(kind: str) -> str`:
  - Validates input string against `VALID_SPECIALISTS`.
  - Checks `SPECIALIST_ALIASES`.
  - Gracefully defaults unknown or malformed kinds to `"analysis"`, ensuring that graph dispatch never encounters an unregistered destination node.

### 2. Structured Plan Payload Parsing
In `src/orchestrator/graph/supervisor.py`:
- Implemented `parse_plan_payload(raw: str | dict | list | None, default_instruction: str = "") -> list[Subtask] | None`:
  - Handles Python dictionaries (with keys `"plan"` or `"subtasks"`), Python lists of subtask dicts or strings, and JSON strings.
  - Strips markdown code fences (```json ... ```) commonly emitted by LLMs.
  - Normalizes specialist kinds using `normalize_specialist_kind()`.
  - Emits well-formed `Subtask` items with unique IDs (`s1`, `s2`, ...), status `"pending"`, initial confidence `0.0`, and assigned instructions.
  - Returns `None` if parsing fails or no valid subtasks are present, cleanly signaling the need for fallback.

### 3. Dynamic Plan Execution in `decompose()`
In `src/orchestrator/graph/supervisor.py`:
- Updated `decompose(state)`:
  1. Checks if `state.get("plan")` was supplied by the caller (allowing pre-defined plan injection).
  2. If not, requests decomposition from `LLMClient.complete(..., json_mode=True)` and parses the output via `parse_plan_payload()`.
  3. If LLM completion is unavailable, invalid, or returns an unparseable payload, safely falls back to keyword-based `plan_subtasks(task)`.
  4. Records the decomposition stage and subtask plan IDs into `ShortTermMemory`.

### 4. Graph Dispatch and Injected Plan Support
In `src/orchestrator/graph/builder.py`:
- In `_next_specialist(state)`: Added a safety check validating that `pending[0].get("kind")` is normalized via `normalize_specialist_kind` before returning to conditional edges.
- In `run_task(task, plan=None)`: Added optional `plan` parameter so callers and unit tests can provide explicit execution plans.
- Preserved loop termination in both LangGraph and fallback executors (`_run_fallback`), where pending subtasks are processed strictly by ID sequence (`s["id"] not in done_ids`).

---

## 4. Verification and Test Results

### 1. Dedicated Planning Test Suite (`tests/test_planning.py`)
Created 10 comprehensive tests covering all requirements:
1. `test_normalize_specialist_kind`: Canonical validation, alias normalization, and safe analysis default.
2. `test_parse_plan_payload`: Dict, list, markdown-fenced JSON, and invalid payload handling.
3. `test_plan_drives_specialist_selection`: Verified custom plan selects only specified specialists (`["writing", "code"]`) ignoring keyword defaults.
4. `test_plan_drives_execution_order`: Verified non-canonical execution order (`["writing", "research"]`) executes writing first, then research.
5. `test_plan_with_repeated_specialists`: Verified repeated specialist kinds (`["code", "code"]`) execute sequentially as distinct subtasks.
6. `test_unknown_specialist_normalizes_safely_during_execution`: Verified unknown kind (`"quantum_compiler"`) normalizes to `"analysis"` and runs to completion.
7. `test_llm_completion_drives_decomposition`: Mocked LLM JSON output parses and drives specialist execution end-to-end.
8. `test_invalid_llm_output_falls_back_safely`: Unparseable text from LLM safely falls back to keyword routing.
9. `test_llm_exception_falls_back_safely`: Exceptions during LLM completion fall back to keyword routing without crashing workflow.
10. `test_decompose_state_preservation`: Verified state tracking and trace accumulation.

**Command & Output:**
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_planning.py | Select-String -Pattern "passed"
```
```text
10 passed in 3.07s
```

### 2. Regression Testing on Existing Test Suites
- **Smoke, CLI, Orchestration, Builder Errors, Audit, Security:**
  ```powershell
  $env:PYTHONPATH="src"; python -m pytest tests/test_orchestration.py tests/test_smoke.py tests/test_cli.py tests/test_builder_errors.py tests/test_audit.py tests/test_security.py | Select-String -Pattern "passed"
  ```
  ```text
  44 passed in 26.51s
  ```

- **Memory Integration Suite (`tests/test_memory.py`):**
  ```powershell
  $env:PYTHONPATH="src"; python -m pytest tests/test_memory.py | Select-String -Pattern "passed"
  ```
  ```text
  12 passed in 41.33s
  ```

### 3. Code Style and Linting
- `python -m ruff check src tests`:
  ```text
  All checks passed!
  ```
- `python -m ruff format --check src/orchestrator/graph/supervisor.py src/orchestrator/graph/builder.py tests/test_planning.py`:
  ```text
  3 files already formatted
  ```

---

## 5. Mock Provider and Live LLM Behavior

- In `src/orchestrator/llm.py`, the default mock provider returns:
  `{"summary": "[mock] handled: ...", "confidence": 0.72, "quality": 0.71}`
- Because this payload lacks a `"plan"` or `"subtasks"` key, `parse_plan_payload` safely returns `None`, seamlessly engaging the keyword fallback `plan_subtasks(task)`.
- This ensures 100% backward compatibility for all existing tests running in offline deterministic mock mode.
- When live LLMs (OpenAI / Anthropic) or test mocks return structured plans conforming to `{"plan": [{"kind": "...", "instruction": "..."}]}`, the supervisor dynamically adopts and executes the plan.

---

## 6. Git Status and Change Footprint

### Tracked Modified Files:
- `src/orchestrator/graph/supervisor.py`: Plan payload parsing, specialist normalization, and dynamic decomposition.
- `src/orchestrator/graph/builder.py`: Plan parameter injection and specialist normalization guard in router.

### New Test Files:
- `tests/test_planning.py`: 10 focused unit and integration tests.

### Reports:
- `reports/TASK_06_DYNAMIC_PLANNING.md`: This report.

---

## 7. Conclusion

Task 06 successfully connected supervisor plan generation to the actual execution path. The orchestrator now respects dynamically generated specialist selections and custom execution sequences, safely normalizes specialist names, handles unparseable or failed LLM responses via graceful fallback, and preserves all HITL and memory guarantees.
