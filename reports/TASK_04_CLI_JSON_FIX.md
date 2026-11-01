# Task 04 Report: CLI JSON Output Hardening and Stream Cleanliness

**Project:** `multi-agent-orchestrator`  
**Task:** Project 15 — Task 04  
**Date:** October 19, 2026  
**Status:** COMPLETE  
**Author:** Antigravity Engineering Agent  

---

## 1. Root Cause Analysis

In Task 02 runtime verification, running the CLI with `--json` mode (`python -m orchestrator.main "Summarize the quarterly report" --json`) resulted in a parse failure when consumed as machine-readable JSON:
```text
json.decoder.JSONDecodeError: Extra data: line 31 column 1 (char 794)
```

Inspection traced three distinct mechanisms causing stdout corruption:

1. **OpenTelemetry Default Console Exporter on `sys.stdout`:**
   - In `src/orchestrator/observability/tracing.py`, when no external OTLP endpoint was specified (`settings.otel_exporter_otlp_endpoint` was empty/unset), `setup_tracing()` configured:
     ```python
     exporter = ConsoleSpanExporter()
     ```
   - In the OpenTelemetry SDK, `ConsoleSpanExporter` defaults its output stream to `sys.stdout`.
   - As workflow nodes (`supervisor.decompose`, `specialist.writing`, `reviewer.review`, `supervisor.reduce`) finished, `BatchSpanProcessor` buffered their JSON span definitions.
   - When the process exited, Python's `atexit` lifecycle automatically flushed `BatchSpanProcessor`, causing `ConsoleSpanExporter` to dump multiple raw JSON span definitions directly to `sys.stdout`.
   - Because `main.py` had already emitted the task result JSON to `sys.stdout`, stdout contained concatenated, un-delimited JSON documents, violating the requirement for a single valid JSON document.

2. **CLI Argument and Help Output Leaking into `sys.stdout`:**
   - In `src/orchestrator/main.py`, if no task was passed (`if not args.task:`), the CLI called `parser.print_help()`.
   - In Python's `argparse`, `print_help()` writes to `sys.stdout` unless `file=sys.stderr` is explicitly provided.
   - If a caller invoked `orchestrator --json` without a task argument, the usage text was written to `stdout`, producing invalid JSON.

3. **Unhandled Exception Tracebacks on Domain Validation Errors:**
   - When input validation failed in `run_task` (such as empty whitespace or tasks exceeding `max_task_chars`), `run_task` raised `ValueError`.
   - `main.py` did not catch exceptions from `run_task`, allowing tracebacks to dump unformatted diagnostic text rather than structured error reporting to `sys.stderr`.

---

## 2. Implementation

To ensure that stdout emits strictly one valid JSON document without string filtering or disabling observability:

1. **Redirected OpenTelemetry Console Exporter to `_SafeStderr`:**
   - In `src/orchestrator/observability/tracing.py`, defined `_SafeStderr` which implements `.write()`, `.flush()`, and `.isatty()` delegating dynamically to `sys.stderr`.
   - Handled `closed` stream attributes and suppressed exceptions during interpreter shutdown (`atexit`), preventing crashes if stderr is closed prematurely.
   - Passed `out=_SafeStderr()` to `ConsoleSpanExporter(out=_SafeStderr())`.
   - Observability remains completely functional; spans and traces continue to be recorded and exported, but to `sys.stderr` rather than contaminating `sys.stdout`.

2. **Redirected CLI Help to `sys.stderr` on Missing Arguments:**
   - In `src/orchestrator/main.py`, updated `parser.print_help()` to `parser.print_help(file=sys.stderr)`.
   - An invocation lacking required task arguments emits usage information to `sys.stderr` and exits with return code `1`, leaving `sys.stdout` completely clean.

3. **Graceful Error Handling in CLI Execution:**
   - In `src/orchestrator/main.py`, wrapped `run_task(args.task)` in a `try...except Exception as exc:` block.
   - Any runtime or validation errors (e.g., `ValueError: task must not be empty`, `ValueError: task exceeds maximum length`) emit `Error: <reason>` to `sys.stderr` and return code `1`, leaving `sys.stdout` empty and unpolluted.

4. **Preserved Human-Readable Output and Tracing Modes:**
   - Human-readable text mode (when `--json` is omitted) continues to display `=== ANSWER ===` and `=== REVIEW ===` on `sys.stdout`.
   - Trace mode (`--trace`) in text mode continues to print `=== TRACE ===` sections with event JSON entries to `sys.stdout`.
   - When `--json` is selected, `result` already contains the full execution trace in `result["trace"]`, and stdout contains strictly the single JSON serialization of `result`.

---

## 3. Files Changed

### 1. `src/orchestrator/main.py`
- Changed line 22: `parser.print_help()` -> `parser.print_help(file=sys.stderr)`.
- Wrapped `result = run_task(args.task)` in `try...except Exception as exc:` to print `Error: {exc}` to `sys.stderr` and return `1`.

### 2. `src/orchestrator/observability/tracing.py`
- Added `_SafeStderr` wrapper providing `.write()`, `.flush()`, and `.isatty()` with exception suppression and dynamic `sys.stderr` lookup.
- Configured `ConsoleSpanExporter(out=_SafeStderr())`.

### 3. `tests/test_cli.py`
- Preserved all 4 existing smoke tests.
- Added 7 regression tests verifying:
  - Exact single JSON document decoding without preambles or postambles.
  - Diagnostic messages (`› provider=...`) and OTel spans routed to `stderr`.
  - Missing task with `--json` leaves `stdout` empty.
  - Empty/whitespace task with `--json` emits error to `stderr` and leaves `stdout` empty.
  - Task exceeding length limit with `--json` emits error to `stderr` and leaves `stdout` empty.
  - `--trace` with `--json` produces valid JSON containing embedded trace list without human headings.
  - Real subprocess invocation (`python -m orchestrator.main ... --json`) parses with `json.loads` without `JSONDecodeError: Extra data`.

---

## 4. Regression Tests Added

| Test Function | Target Verification | Status |
| :--- | :--- | :--- |
| `test_no_task_prints_help_and_returns_1` | Preserved smoke test: missing task prints usage and returns 1. | PASS |
| `test_runs_task_in_text_mode` | Preserved smoke test: default text mode outputs answer and review headers. | PASS |
| `test_json_mode_emits_valid_json` | Preserved smoke test: basic in-process JSON parse. | PASS |
| `test_trace_flag_prints_trace` | Preserved smoke test: text mode with `--trace` prints trace section. | PASS |
| `test_json_mode_stdout_strictly_valid_json_no_extra_content` | Asserts stdout starts and ends strictly with JSON boundaries, `raw_decode` has 0 trailing characters, and stdout lacks section headers. | PASS |
| `test_json_mode_diagnostic_output_sent_to_stderr` | Asserts `provider=` and `task=` diagnostics appear in `stderr` and are absent from `stdout`. | PASS |
| `test_no_task_with_json_flag_leaves_stdout_clean` | Asserts `--json` without task returns 1, emits usage to `stderr`, and leaves `stdout` empty. | PASS |
| `test_controlled_error_empty_task_leaves_stdout_clean` | Asserts `--json` with whitespace task returns 1, emits `error: task must not be empty` to `stderr`, and leaves `stdout` empty. | PASS |
| `test_controlled_error_task_too_long_leaves_stdout_clean` | Asserts `--json` with task exceeding length returns 1, emits length error to `stderr`, and leaves `stdout` empty. | PASS |
| `test_json_mode_with_trace_flag_preserves_valid_json` | Asserts `--json --trace` produces valid JSON payload with `trace` list and no human headings. | PASS |
| `test_subprocess_json_mode_end_to_end_no_span_pollution` | Runs real external Python process, asserts clean `json.loads(p.stdout)` with no extra span data on process shutdown. | PASS |

---

## 5. Exact Test Commands and Results

### 1. Focused CLI Regression Suite
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_cli.py -v
```
**Output:**
```text
============================= test session starts =============================
platform win32 -- Python 3.11.3, pytest-7.4.3, pluggy-1.6.0
rootdir: C:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator
configfile: pyproject.toml
plugins: anyio-3.7.1, dash-2.14.2, Faker-38.2.0, asyncio-0.21.1, flask-1.2.0
asyncio: mode=Mode.STRICT
collected 11 items

tests\test_cli.py ...........                                            [100%]

============================= 11 passed in 49.52s =============================
```

### 2. Smoke Test Suite
```powershell
$env:PYTHONPATH="src"; python -m pytest tests/test_smoke.py -v
```
**Output:**
```text
============================= test session starts =============================
platform win32 -- Python 3.11.3, pytest-7.4.3, pluggy-1.6.0
rootdir: C:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator
configfile: pyproject.toml
plugins: anyio-3.7.1, dash-2.14.2, Faker-38.2.0, asyncio-0.21.1, flask-1.2.0
asyncio: mode=Mode.STRICT
collected 7 items

tests\test_smoke.py .......                                              [100%]

============================= 7 passed in 14.49s ==============================
```

### 3. Code Linter Check
```powershell
python -m ruff check src tests ui
```
**Output:**
```text
All checks passed!
```

### 4. Code Formatter Check
```powershell
python -m ruff format --check src/orchestrator/main.py src/orchestrator/observability/tracing.py tests/test_cli.py
```
**Output:**
```text
3 files already formatted
```

---

## 6. Remaining Limitations

1. **Combined Stream Invocations (`2>&1`):**
   - If an external caller explicitly merges file descriptor 2 into file descriptor 1 (e.g. `python -m orchestrator.main ... --json 2>&1`), standard Unix stream semantics dictate that stderr diagnostics and span logs will interleave with stdout. Consumers expecting pure JSON must read stdout (FD 1) exclusively.
2. **ChromaDB Connection Probe Latency:**
   - In environments without an active ChromaDB service, `LongTermMemory` socket probes incur a ~2.7s initial TCP timeout. This latency affects invocation duration, though it does not impact output stream correctness.

---

## 7. Final Git Status

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
 M src/orchestrator/main.py
 M src/orchestrator/memory/long_term.py
 M src/orchestrator/memory/short_term.py
 M src/orchestrator/observability/tracing.py
 M tests/test_cli.py
 M ui/review_app.py
?? reports/
?? src/orchestrator/__main__.py
?? tests/test_resolution.py
```
