# Task 16 Implementation Report: Persistent Workflow State and Resume Support

## Executive Summary
This report documents the implementation of local durable checkpointing and resume support for the Agent Orchestration System. The multi-agent orchestrator can now durably record workflow state snapshots at key execution boundaries using Python standard-library SQLite (`sqlite3`) and resume interrupted, failed, or escalated workflows without re-executing already completed subtasks (`status == "done"`).

---

## 1. Architecture & Design

### 1.1 Local SQLite Checkpoint Storage Engine
The persistence backend is implemented in `src/orchestrator/persistence/store.py` (`CheckpointStore`) without requiring external database servers, background daemon services, or paid dependencies.

- **Storage Engine:** Standard-library `sqlite3` with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`), busy timeout (`PRAGMA busy_timeout=5000;`), and foreign key enforcement (`PRAGMA foreign_keys=ON;`).
- **Database Schema:**
  ```sql
  CREATE TABLE IF NOT EXISTS runs (
      run_id TEXT PRIMARY KEY,
      task TEXT,
      status TEXT,
      latest_step INTEGER DEFAULT 0,
      schema_version INTEGER,
      created_at TEXT,
      updated_at TEXT
  );

  CREATE TABLE IF NOT EXISTS checkpoints (
      run_id TEXT,
      step INTEGER,
      transition TEXT,
      state_json TEXT,
      schema_version INTEGER,
      created_at TEXT,
      PRIMARY KEY (run_id, step)
  );

  CREATE INDEX IF NOT EXISTS idx_checkpoints_run_id ON checkpoints(run_id);
  ```
- **Schema Versioning:** Checkpoints write `schema_version = 1` (`SCHEMA_VERSION = 1`). Attempting to load a checkpoint with an unsupported future schema version raises `CheckpointIncompatibleError`.
- **Integrity Validation:** If checkpoint JSON cannot be decoded or is corrupted, `CheckpointCorruptError` is raised. Unknown run IDs raise `CheckpointNotFoundError`.

### 1.2 Sensitive Attribute Sanitization
To prevent credential or secret leakage into local persistence files, `sanitize_state_for_persistence` recursively inspects and sanitizes state dictionaries and strings prior to JSON serialization:
- Keys matching sensitive patterns (`password`, `secret`, `api[_-]?key`, `auth`, `token`, `credential`) are replaced with `"[REDACTED]"`.
- Bearer tokens (`Bearer ...`) and OpenAI/Anthropic key formats (`sk-...`) embedded in strings are redacted (`Bearer [REDACTED_TOKEN]`, `[REDACTED_KEY]`).

### 1.3 Concurrency & Coordinator Synchronization
All database writes within the orchestrator graph executor (`_run_fallback`) take place strictly on the main coordinator thread. Specialist worker threads running concurrently via `ThreadPoolExecutor` execute specialist functions in isolation and return state updates to the coordinator thread. The coordinator merges each worker's result and writes checkpoints sequentially. This architectural separation guarantees zero SQLite lock contention and completely eliminates multi-threaded race conditions during persistence.

---

## 2. Safe Execution Boundary Writes

Checkpoints are captured synchronously at clean execution boundaries:
1. **`init`**: Written at the start of `run_task`, saving the initial task instruction, recalled context, and initial run correlation ID.
2. **`plan`**: Written immediately after `supervisor.decompose` finishes decomposing and validating the plan.
3. **`blocked`**: Written whenever `check_dependencies` detects that upstream prerequisites failed or were blocked.
4. **`subtask_complete`**: Written when a specialist subtask successfully completes execution and passes review.
5. **`subtask_failed`**: Written when a specialist execution fails or exhausts its retry budget.
6. **`escalate`**: Written when reviewer quality checks or sensitive tools trigger Human-in-the-Loop escalation.
7. **`resolution`**: Written in `apply_resolution` immediately upon recording a human review decision (`approve`, `edit`, `take_over`, `reject`).
8. **`resume`**: Written at the start of `resume_task` recording resumption metadata.
9. **`reduce`**: Written after `supervisor.reduce` synthesizes the final assembled answer.

---

## 3. Workflow Resumption Mechanics (`resume_task`)

### 3.1 Loading & State Reconstruction
`resume_task(run_id: str, max_concurrency: int | None = None, store: CheckpointStore | None = None) -> GraphState`:
1. Loads the latest valid checkpoint record for `run_id` from `store`.
2. Reconstructs `GraphState`, preserving all accumulated attributes (`run_id`, `completed`, `failed`, `tools_used`, `retries`, `escalations`, `trace`).
3. If all planned subtasks are already done and the terminal status is completed or resolved, returns the cached state immediately without scheduling worker threads.

### 3.2 Bypassing Completed Subtasks
Subtasks already present in `completed` with `status == "done"` are preserved in `done_ids` and skipped during dispatch. Only pending subtasks or subtasks needing re-execution are submitted to the thread pool.

### 3.3 Dependency Re-evaluation
Any subtasks that were marked `blocked` in earlier checkpoints are cleared from `blocked` and re-evaluated by `check_dependencies`:
- If upstream subtasks are now satisfied (e.g. after a human reviewer approved or edited an escalated dependency), downstream subtasks are automatically unblocked and executed.
- If upstream dependencies remain failed, downstream subtasks are re-blocked with updated diagnostics.

### 3.4 Trace Correlation
Resumed executions preserve the original `run_id` throughout all downstream spans, events, and checkpoints. A structured lifecycle event `workflow.resume` is appended to the execution trace:
```json
{
  "node": "workflow",
  "event": "resume",
  "run_id": "<run_id>",
  "completed_count": 1,
  "total_subtasks": 2
}
```

### 3.5 External Tool Execution Limitation (Important Boundary)
> [!IMPORTANT]
> External tool execution cannot guarantee exactly-once side effects across process restarts. If an external API call or remote mutation (e.g., HTTP POST, database update) was initiated right before a crash, resuming execution cannot verify whether the remote side effect was committed unless the remote service is idempotent. This limitation is explicitly documented in code docstrings and system architecture specifications.

---

## 4. Configuration and CLI Integration

### 4.1 Settings (`src/orchestrator/config.py`)
- `checkpoint_db_path: str = "orchestrator_checkpoints.db"` (configurable via environment variable `CHECKPOINT_DB_PATH`).
- `checkpoint_enabled: bool = True` (configurable via environment variable `CHECKPOINT_ENABLED`).

### 4.2 CLI Integration (`src/orchestrator/main.py`)
Added command-line options:
- `--resume <RUN_ID>`: Resumes execution of an interrupted or escalated workflow by run ID.
- `--status <RUN_ID>`: Queries the current execution status and step count from checkpoints without executing.
- `--concurrency <INT>`: Configures maximum concurrent specialists during execution or resume.

Output discipline:
- With `--json`, pure valid JSON is emitted on `stdout` without any logging or diagnostic text.
- Diagnostic information (`› provider=...`, `› resuming run=...`) is strictly routed to `sys.stderr`.
- Non-existent run IDs return a controlled error message to `stderr` and exit with code 1.

---

## 5. Verification & Test Coverage

### 5.1 Test Suite: `tests/test_persistence_resume.py`
A comprehensive, deterministic test suite was added to verify all aspects of local persistence and resume support:
1. `test_checkpoint_store_basic_crud`: Verifies step sequencing, latest checkpoint retrieval, historical listing, and run indexing.
2. `test_incompatible_schema_version_raises`: Verifies `CheckpointIncompatibleError` on future schema versions.
3. `test_corrupt_checkpoint_data_raises`: Verifies `CheckpointCorruptError` on malformed JSON records.
4. `test_checkpoint_not_found_raises`: Verifies `CheckpointNotFoundError` on missing or empty run IDs.
5. `test_secret_sanitization_for_persistence`: Verifies redaction of API keys, passwords, and authorization tokens.
6. `test_boundary_writes_recorded_during_workflow`: Verifies synchronous checkpoint writes across `init`, `plan`, `subtask_complete`, and `reduce`.
7. `test_resume_bypasses_completed_subtasks`: Verifies that previously completed subtasks (`status == "done"`) are not re-executed on resume.
8. `test_resume_already_completed_task`: Verifies short-circuit return for already completed tasks.
9. `test_resume_after_hitl_resolution_unblocks_dependent_subtasks`: Verifies downstream dependency unblocking after reviewer approval.
10. `test_resume_trace_correlation`: Verifies `workflow.resume` event creation and stable `run_id` correlation.
11. `test_concurrency_and_synchronized_writes`: Verifies coordinator synchronization during parallel subtask execution (`max_concurrency=4`).
12. `test_cli_status_and_resume_options`: Verifies CLI `--status` and `--resume` flags in text and pure JSON modes.

### 5.2 Test Results
- `tests/test_persistence_resume.py`: 12 passed in 11.50s.
- `tests/test_cli.py`: 11 passed in 16.15s.
- `tests/test_concurrency.py`: 14 passed in 30.31s.
- `tests/test_dependencies.py`, `tests/test_planning.py`, `tests/test_specialist_reliability.py`: 39 passed in 99.11s.
- `tests/test_result_assembly.py`, `tests/test_token_cost.py`, `tests/test_observability.py`: 39 passed in 48.27s.
- Linting (`ruff check`): 0 errors across all modified and newly created files.
