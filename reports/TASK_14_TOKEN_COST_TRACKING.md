# Task 14: LLM Token Usage and Estimated Cost Tracking Report

## 1. Implementation Summary

Task 14 introduced lightweight, reliable token-usage accounting and configurable estimated-cost tracking to the multi-agent orchestrator. The implementation builds directly upon the existing `LLMClient`, OpenTelemetry tracing context, and run correlation model without redesigning the orchestration pipeline or introducing paid dependencies.

### Key Architectural Enhancements

1. **Lightweight Token Usage Capture (`Usage`)**:
   - Added dataclass [`Usage`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/observability/usage.py) capturing `input_tokens`, `output_tokens`, `total_tokens`, and `is_estimated`.
   - Extracted actual provider-reported usage:
     - **OpenAI**: Extracted from `resp.usage` (`prompt_tokens`, `completion_tokens`, `total_tokens`).
     - **Anthropic**: Extracted from `resp.usage` (`input_tokens`, `output_tokens`).
     - **Mock**: Deterministic token accounting (`in_tokens = max(1, len(prompt) // 4)`, `out_tokens = max(1, len(text) // 4)`).
   - Safe metadata handling: if a provider returns missing or malformed usage, `usage` is left as `None` or zeroes without inventing estimated token numbers as actual counts.
   - Backward compatibility: [`Completion`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/llm.py) accepts `usage: Usage | None = None` with default value, preserving full compatibility for existing callers and positional arguments.

2. **Configurable Estimated Cost Accounting (`ModelPricing`)**:
   - Implemented documented pricing rates in USD per 1M tokens, keeping input-token and output-token rates separate.
   - Default rates documented from official provider tables:
     - `openai:gpt-4o-mini`: $0.15 / 1M input, $0.60 / 1M output
     - `openai:gpt-4o`: $2.50 / 1M input, $10.00 / 1M output
     - `anthropic:claude-opus-4-8` / `claude-3-opus`: $15.00 / 1M input, $75.00 / 1M output
     - `anthropic:claude-3-5-sonnet`: $3.00 / 1M input, $15.00 / 1M output
     - `anthropic:claude-3-haiku`: $0.25 / 1M input, $1.25 / 1M output
     - `mock:mock-1`: $0.00 / 1M input, $0.00 / 1M output
   - Extensibility: Custom pricing can be registered programmatically via `register_model_pricing` or configured via `Settings.llm_custom_pricing`.
   - **Unknown pricing discipline**: Unconfigured models are flagged with `is_cost_known=False` and report estimated cost as `None` (unknown) rather than a misleading `$0.00`.

3. **Isolated Per-Run Usage Aggregation (`RunUsageSummary`)**:
   - Keyed by `run_id` (correlated with `get_current_run_id()` from `trace_run`).
   - Thread-safe registry (`_RUN_REGISTRY`) isolates concurrent and sequential runs to prevent cross-run leakage.
   - Compact summary fields:
     - `run_id`: Active workflow run ID
     - `total_input_tokens`, `total_output_tokens`, `total_tokens`: Total tokens across successful requests
     - `successful_requests`: Count of successful completions
     - `failed_requests`: Count of failed attempts
     - `estimated_cost_usd`: Sum of known estimated costs (or `None` if all costs unknown)
     - `has_unknown_cost`: Boolean flag indicating if any requests had unconfigured pricing
     - `unknown_cost_requests`: Count of requests with unknown pricing
     - `breakdown`: Per `provider:model` dictionary breakdown
   - **Failed Attempt Separation**: Failed attempts are recorded in `failed_requests` and separate `failed_attempts_tokens`, never counted as successful completions.
   - **Fallback Attribution**: When fallback occurs, the failed primary attempt and the successful fallback attempt are attributed to their respective provider and model.

4. **Integration with Workflow and CLI**:
   - In [`run_task`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/graph/builder.py), aggregated usage is attached to `result["usage"]` as optional metadata without modifying existing result contracts.
   - In [`GraphState`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/graph/state.py), added optional `usage: dict[str, Any]` field.
   - In [`cli`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/src/orchestrator/main.py):
     - `--json` mode emits exactly one valid JSON document containing `result["usage"]`.
     - Text mode outputs a dedicated `=== USAGE ===` summary block.
   - In [`ui/review_app.py`](file:///c:/Users/Dell/OneDrive/Desktop/Project/multi-agent-orchestrator/ui/review_app.py), added optional token and cost caption under "Last run result".

5. **Observability & Fault Tolerance**:
   - Emits structured `token_usage` trace events via `create_trace_event`.
   - Never logs secrets, authorization tokens, or raw prompts.
   - All usage tracking is wrapped in `try...except Exception: pass`, ensuring telemetry failures cannot disrupt workflow execution.

---

## 2. Files Changed

1. `src/orchestrator/observability/usage.py` (New):
   - `Usage`, `ModelPricing`, `LLMUsageRecord`, `RunUsageSummary`, `DEFAULT_PRICING`
   - `register_model_pricing`, `get_model_pricing`, `calculate_estimated_cost`
   - `record_llm_usage`, `get_run_usage`, `clear_run_usage`
2. `src/orchestrator/llm.py`:
   - Updated `Completion` dataclass with `usage: Usage | None = None`.
   - Extracted provider usage in `_openai`, `_anthropic`, `_mock`.
   - Integrated usage recording in `_try_provider` on success and failure.
   - Re-exported usage accounting classes and functions.
3. `src/orchestrator/config.py`:
   - Added `llm_custom_pricing: str = ""` to `Settings`.
4. `src/orchestrator/graph/state.py`:
   - Added optional `usage: dict[str, Any]` field to `GraphState`.
5. `src/orchestrator/graph/builder.py`:
   - Integrated `result["usage"] = get_run_usage(task_id).to_dict()` in `run_task`.
6. `src/orchestrator/main.py`:
   - Added `=== USAGE ===` display section in text CLI mode; preserved JSON purity in `--json` mode.
7. `ui/review_app.py`:
   - Added usage summary caption for the last run result.
8. `tests/test_token_cost.py` (New):
   - 12 comprehensive deterministic offline tests covering all task requirements.

---

## 3. Exact Test Commands and Results

### Focused Token Usage and Cost Tracking Suite
```powershell
$env:PYTHONPATH="src"
python -m pytest tests/test_token_cost.py -v
```
**Result:** 12 passed in 2.78s:
- `test_token_usage_extraction_and_dataclass`: PASSED
- `test_missing_or_malformed_usage_metadata_handled_safely`: PASSED
- `test_cost_calculation_from_configured_rates`: PASSED
- `test_unknown_pricing_produces_unknown_cost_not_zero`: PASSED
- `test_primary_and_fallback_provider_attribution`: PASSED
- `test_multiple_llm_calls_aggregate_into_one_run_summary`: PASSED
- `test_different_runs_remain_isolated`: PASSED
- `test_failed_attempts_not_counted_as_successful_completions`: PASSED
- `test_usage_tracking_failures_do_not_break_workflow`: PASSED
- `test_trace_events_do_not_expose_secrets`: PASSED
- `test_completion_and_callers_compatibility`: PASSED
- `test_cli_json_and_workflow_usage_integration`: PASSED

### Code Quality (Ruff)
```powershell
python -m ruff check src/orchestrator/observability/usage.py src/orchestrator/llm.py src/orchestrator/config.py src/orchestrator/graph/state.py src/orchestrator/graph/builder.py src/orchestrator/main.py ui/review_app.py tests/test_token_cost.py
```
**Result:** `All checks passed!` (0 errors).

### Regression Suites
- `python -m pytest tests/test_llm_fallback.py tests/test_cli.py tests/test_observability.py -v`: 36 passed in 18.25s
- `python -m pytest tests/test_planning.py tests/test_specialist_reliability.py -v`: Passed

---

## 4. Supported Usage Fields and Pricing Configuration

### Usage Fields
- `input_tokens`: Tokens sent in system instruction and prompt.
- `output_tokens`: Tokens generated by the model.
- `total_tokens`: Sum of input and output tokens.
- `is_estimated`: Boolean indicating whether counts were reported by the provider (`False`) or approximated (`True`).
- `successful_requests`: Count of successful completions.
- `failed_requests`: Count of failed attempts.
- `estimated_cost_usd`: Known estimated cost rounded to 6 decimal places.
- `has_unknown_cost`: True if any model in the run lacked pricing configuration.
- `unknown_cost_requests`: Count of requests with unknown pricing.
- `breakdown`: Sub-summary per `provider:model`.

### Pricing Configuration
Pricing can be configured via:
1. Built-in defaults in `DEFAULT_PRICING` (OpenAI models, Anthropic models, Mock).
2. Python API: `register_model_pricing(provider, model, input_cost_per_1m, output_cost_per_1m)`.
3. Configuration / environment variable: `LLM_CUSTOM_PRICING='{"my_prov:my_model": {"input_cost_per_1m": 1.5, "output_cost_per_1m": 4.5}}'`.

---

## 5. Limitations and Unverified Live-Provider Behavior

1. **Live Provider API Calls**:
   - Automated tests were executed using mocks and deterministic provider responses. Real OpenAI and Anthropic API endpoints were not invoked live during testing due to sandbox restrictions and absence of live credentials.
2. **Cached Token Pricing**:
   - Prompt caching discounts (e.g. OpenAI prompt cache read discounts or Anthropic prompt cache 10% rates) are not modeled separately; standard input rates are applied.
3. **Currency**:
   - All rates and estimates are denominated in USD.

---

## 6. Final Git Status

Distinction between Task 14 changes and pre-existing workspace modifications:

### Modified / Created in Task 14:
- `src/orchestrator/observability/usage.py` (New file)
- `src/orchestrator/llm.py`
- `src/orchestrator/config.py`
- `src/orchestrator/graph/state.py`
- `src/orchestrator/graph/builder.py`
- `src/orchestrator/main.py`
- `ui/review_app.py`
- `tests/test_token_cost.py` (New file)
- `reports/TASK_14_TOKEN_COST_TRACKING.md` (This report)

### Pre-existing from Earlier Tasks (Preserved):
- `reports/TASK_01` through `reports/TASK_13`
- `src/orchestrator/__main__.py`
- `src/orchestrator/graph/*` (except state and builder additions)
- `src/orchestrator/hitl/*`
- `src/orchestrator/memory/*`
- `src/orchestrator/observability/tracing.py`
- `src/orchestrator/tools/*`
- `tests/test_audit.py`
- `tests/test_builder_errors.py`
- `tests/test_cli.py`
- `tests/test_dependencies.py`
- `tests/test_hitl.py`
- `tests/test_llm_fallback.py`
- `tests/test_memory.py`
- `tests/test_observability.py`
- `tests/test_orchestration.py`
- `tests/test_planning.py`
- `tests/test_resolution.py`
- `tests/test_result_assembly.py`
- `tests/test_security.py`
- `tests/test_smoke.py`
- `tests/test_specialist_reliability.py`
- `tests/test_tools_safety.py`
