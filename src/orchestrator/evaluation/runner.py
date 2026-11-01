"""Reproducible offline workflow evaluation runner and CLI entry point."""

from __future__ import annotations

import argparse
import copy
import datetime
import json
import logging
import random
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

from ..config import Settings
from ..graph.builder import resume_task, run_task
from ..graph.state import GraphState, Subtask
from ..hitl.queue import _LOCAL as hitl_local_queue
from ..hitl.queue import ReviewQueue, apply_resolution
from ..memory.long_term import reset_local_store as reset_ltm
from ..memory.short_term import reset_local_store as reset_stm
from ..observability.usage import clear_run_usage
from ..persistence.store import CheckpointStore
from .cases import DATASET_VERSION, EvaluationCase, get_evaluation_cases
from .metrics import CaseEvaluationResult, MetricResult, calculate_metrics

logger = logging.getLogger(__name__)


@dataclass
class EvaluationReport:
    """Comprehensive evaluation report containing dataset metadata, metrics, and case outcomes."""

    version: str
    timestamp: str
    seed: int
    total_cases: int
    passed_cases: int
    failed_cases: int
    pass_rate: float
    metrics: dict[str, MetricResult]
    case_results: list[CaseEvaluationResult]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "timestamp": self.timestamp,
            "seed": self.seed,
            "total_cases": self.total_cases,
            "passed_cases": self.passed_cases,
            "failed_cases": self.failed_cases,
            "pass_rate": round(self.pass_rate, 4),
            "metrics": {k: v.to_dict() for k, v in self.metrics.items()},
            "case_results": [r.to_dict() for r in self.case_results],
            "metadata": self.metadata,
        }


class EvaluationRunner:
    """Deterministic evaluation runner that executes cases in isolated environments."""

    def __init__(
        self,
        settings: Settings | None = None,
        seed: int = 42,
        temp_dir: Path | str | None = None,
    ) -> None:
        self.settings = settings or Settings()
        self.seed = seed
        self.temp_dir = Path(temp_dir) if temp_dir else None

    def _isolate_state(self) -> None:
        """Reset all in-memory stores and usage accounting for clean case isolation."""
        reset_stm()
        reset_ltm()
        clear_run_usage()
        # Clear in-memory HITL review queue
        hitl_local_queue.clear()

    def run_case(self, case: EvaluationCase, store: CheckpointStore) -> CaseEvaluationResult:
        """Execute a single evaluation case under deterministic mocks and isolated storage."""
        start_time = time.perf_counter()
        self._isolate_state()
        random.seed(self.seed)

        run_id = f"eval-{case.case_id}"
        specialist_attempts: dict[str, int] = {}
        subtask_invocations: list[str] = []

        def mock_invoke_specialist(kind: str, state: GraphState, subtask: Subtask) -> dict[str, Any]:
            sid = subtask.get("id", "s1")
            subtask_invocations.append(sid)
            specialist_attempts[sid] = specialist_attempts.get(sid, 0) + 1
            attempt_idx = specialist_attempts[sid] - 1

            if sid in case.mock_specialist_results:
                mock_def = case.mock_specialist_results[sid]
                if isinstance(mock_def, list):
                    res_spec = mock_def[min(attempt_idx, len(mock_def) - 1)]
                elif isinstance(mock_def, dict):
                    res_spec = mock_def
                else:
                    res_spec = {"status": "done", "result": str(mock_def), "confidence": 0.85}

                st_status = res_spec.get("status", "done")
                confidence = float(res_spec.get("confidence", 0.85))
                result_text = res_spec.get("result", "")
                if st_status == "done" and not result_text:
                    result_text = (
                        f"Successfully executed {kind} specialist subtask {sid} with verified analysis."
                    )
                err_text = res_spec.get("error", "")

                return {
                    "completed": [
                        {
                            **subtask,
                            "status": st_status,
                            "result": result_text,
                            "error": err_text,
                            "confidence": confidence,
                            "tools_used": res_spec.get("tools_used", []),
                        }
                    ],
                    "tools_used": res_spec.get("tools_used", []),
                    "trace": [],
                }

            # Standard deterministic mock specialist output
            default_text = (
                f"Thorough analytical assessment and findings from {kind} specialist for subtask {sid}."
            )
            return {
                "completed": [
                    {
                        **subtask,
                        "status": "done",
                        "result": default_text,
                        "confidence": 0.85,
                        "tools_used": [],
                    }
                ],
                "tools_used": [],
                "trace": [],
            }

        # Safe isolated execution context
        eval_settings = Settings(
            default_llm_provider="mock",
            fallback_llm_provider="mock",
            openai_api_key="",
            anthropic_api_key="",
            checkpoint_enabled=True,
            checkpoint_db_path=str(store.db_path),
            confidence_threshold=0.6,
            quality_threshold=0.6,
            max_retries=2,
            max_specialist_concurrency=case.max_concurrency or 4,
        )

        error_msg: str | None = None
        state: GraphState = {}
        llm_patcher = None

        try:
            if getattr(case, "mock_llm_response", None):
                from orchestrator.llm import Completion

                llm_patcher = patch(
                    "orchestrator.graph.supervisor.LLMClient.complete",
                    return_value=Completion(case.mock_llm_response, "mock", "mock-1"),
                )
                llm_patcher.start()

            with patch("orchestrator.graph.builder._invoke_specialist", side_effect=mock_invoke_specialist):
                with patch("orchestrator.config.get_settings", return_value=eval_settings):
                    if case.is_resume_case:
                        # Pre-populate initial checkpoint
                        init_state = copy.deepcopy(case.initial_checkpoint_state or {})
                        init_state.setdefault("run_id", run_id)
                        init_state.setdefault("task_id", run_id)
                        store.save_checkpoint(run_id, init_state, transition="init")

                        state = resume_task(run_id, store=store, max_concurrency=case.max_concurrency)
                    else:
                        state = run_task(
                            task=case.task,
                            plan=case.plan,
                            run_id=run_id,
                            max_concurrency=case.max_concurrency,
                        )

                        # If HITL action is defined and workflow escalated, resolve and resume
                        if case.hitl_action and state.get("escalated"):
                            q = ReviewQueue()
                            action = case.hitl_action
                            res_obj = q.resolve(
                                identifier=run_id,
                                decision=action.get("decision", "approve"),
                                approver=action.get("approver", "evaluator"),
                                note=action.get("note", "Approved during evaluation"),
                            )
                            if res_obj:
                                state = apply_resolution(state, res_obj)
                                if store and run_id:
                                    try:
                                        store.save_checkpoint(run_id, state, transition="hitl_resolution")
                                    except Exception:
                                        pass
                                # If approved and subsequent planned subtasks remain unexecuted, resume
                                if action.get("decision") == "approve":
                                    executed_ids = {c["id"] for c in state.get("completed", [])}
                                    plan_ids = {s["id"] for s in state.get("plan", [])}
                                    if not plan_ids.issubset(executed_ids):
                                        state = resume_task(
                                            run_id, store=store, max_concurrency=case.max_concurrency
                                        )

        except Exception as exc:
            logger.exception("Case %s raised unexpected exception: %s", case.case_id, exc)
            error_msg = f"{type(exc).__name__}: {exc}"
            state = {"status": "failed", "answer": f"Error: {exc}", "completed": []}
        finally:
            if llm_patcher is not None:
                llm_patcher.stop()

        duration = time.perf_counter() - start_time

        # Extract outcomes
        actual_status = state.get("status", "unknown")
        if isinstance(case.expected.expected_status, (list, tuple, set)):
            status_match = actual_status in case.expected.expected_status
        else:
            status_match = actual_status == case.expected.expected_status

        completed_all = state.get("completed", [])
        completed_ids = [c["id"] for c in completed_all if c.get("status") in ("done", "approved", "edited")]
        failed_ids = [
            c["id"]
            for c in (completed_all + state.get("failed", []))
            if c.get("status") in ("failed", "rejected")
        ]
        blocked_ids = [
            c["id"]
            for c in (completed_all + state.get("blocked", []))
            if c.get("status") == "blocked"
        ]

        # Duplicate detection: identify any subtask executed more times than retries + 1
        retries_total = state.get("retries", 0)
        duplicates: list[str] = []
        counts: dict[str, int] = {}
        for sid in subtask_invocations:
            counts[sid] = counts.get(sid, 0) + 1
        for sid, cnt in counts.items():
            if cnt > (retries_total + 1):
                duplicates.append(f"{sid} (invoked {cnt} times)")

        # Section coverage check
        answer_text = state.get("answer", "")
        matched_sections = [
            sec for sec in case.expected.expected_sections if sec in answer_text
        ]
        missing_sections = [
            sec for sec in case.expected.expected_sections if sec not in answer_text
        ]

        has_escalated = (
            bool(state.get("escalated"))
            or bool(state.get("escalations"))
            or bool(state.get("resolution"))
        )

        # Determine pass/fail
        passed = bool(
            status_match
            and error_msg is None
            and not duplicates
            and not missing_sections
            and set(case.expected.expected_completed).issubset(set(completed_ids))
            and set(case.expected.expected_failed).issubset(set(failed_ids))
            and set(case.expected.expected_blocked).issubset(set(blocked_ids))
            and (
                case.expected.expected_escalated is None
                or has_escalated == case.expected.expected_escalated
            )
            and retries_total >= case.expected.expected_retries_min
        )

        return CaseEvaluationResult(
            case_id=case.case_id,
            name=case.name,
            passed=passed,
            status=actual_status,
            expected_status=case.expected.expected_status,
            status_match=status_match,
            completed_subtasks=list(dict.fromkeys(completed_ids)),
            failed_subtasks=list(dict.fromkeys(failed_ids)),
            blocked_subtasks=list(dict.fromkeys(blocked_ids)),
            retries=retries_total,
            escalated=has_escalated,
            sections_matched=matched_sections,
            missing_sections=missing_sections,
            duplicates_detected=duplicates,
            execution_record=state,
            error_message=error_msg,
            duration_seconds=duration,
        )

    def run_all(
        self,
        tags: list[str] | None = None,
        case_ids: list[str] | None = None,
    ) -> EvaluationReport:
        """Run all matching evaluation cases and compute comprehensive metrics."""
        cases = get_evaluation_cases(tags=tags, case_ids=case_ids)
        if not cases:
            raise ValueError("No evaluation cases matched the requested filters.")

        case_results: list[CaseEvaluationResult] = []
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with tempfile.TemporaryDirectory() as tmp_root:
            db_dir = Path(tmp_root)
            for i, case in enumerate(cases):
                case_db = db_dir / f"eval_case_{case.case_id}_{i}.db"
                store = CheckpointStore(db_path=case_db, enabled=True)
                try:
                    res = self.run_case(case, store)
                    case_results.append(res)
                finally:
                    store.close()

        total = len(case_results)
        passed = sum(1 for r in case_results if r.passed)
        failed = total - passed
        pass_rate = passed / total if total > 0 else 0.0

        metrics = calculate_metrics(cases, case_results)

        return EvaluationReport(
            version=DATASET_VERSION,
            timestamp=now,
            seed=self.seed,
            total_cases=total,
            passed_cases=passed,
            failed_cases=failed,
            pass_rate=pass_rate,
            metrics=metrics,
            case_results=case_results,
            metadata={
                "tag_filters": tags or [],
                "case_id_filters": case_ids or [],
                "dataset_version": DATASET_VERSION,
            },
        )

    def format_text_summary(self, report: EvaluationReport) -> str:
        """Render a readable text summary of the evaluation report."""
        lines = [
            f"=== MULTI-AGENT WORKFLOW EVALUATION (v{report.version}) ===",
            f"Timestamp: {report.timestamp} | Seed: {report.seed}",
            f"Overall Pass Rate: {report.pass_rate * 100:.1f}% "
            f"({report.passed_cases}/{report.total_cases} passed)\n",
            "--- METRICS SUMMARY ---",
        ]

        for _k, m in report.metrics.items():
            if m.applicable and m.value is not None:
                val_str = f"{m.value * 100:.1f}% ({m.numerator}/{m.denominator})"
            else:
                val_str = f"N/A ({m.numerator}/{m.denominator})"
            lines.append(f"  • {m.name:32} : {val_str}")

        lines.append("\n--- CASE OUTCOMES ---")
        for r in report.case_results:
            mark = "PASS" if r.passed else "FAIL"
            lines.append(f"  [{mark}] {r.case_id:36} - {r.status} (expected: {r.expected_status})")
            if not r.passed:
                if r.error_message:
                    lines.append(f"        Error: {r.error_message}")
                if r.missing_sections:
                    lines.append(f"        Missing sections: {r.missing_sections}")
                if r.duplicates_detected:
                    lines.append(f"        Duplicates: {r.duplicates_detected}")
        return "\n".join(lines)

    def save_report(self, report: EvaluationReport, path: str | Path) -> None:
        """Serialize evaluation report to a JSON file."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)


def eval_cli(argv: list[str] | None = None) -> int:
    """CLI runner entry point for offline evaluation."""
    parser = argparse.ArgumentParser(description="Run offline multi-agent workflow evaluation suite.")
    parser.add_argument("--cases", metavar="CASE_IDS", help="Comma-separated case IDs to evaluate.")
    parser.add_argument("--tag", metavar="TAG", help="Filter evaluation cases by tag.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=1.0,
        help="Minimum overall pass rate threshold required for exit code 0 (default: 1.0).",
    )
    parser.add_argument("--output", metavar="PATH", help="Write evaluation report JSON to file.")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed.")
    parser.add_argument("--json", action="store_true", help="Emit JSON output to stdout.")
    args = parser.parse_args(argv)

    tags = [t.strip() for t in args.tag.split(",")] if args.tag else None
    case_ids = [c.strip() for c in args.cases.split(",")] if args.cases else None

    print("› Starting offline workflow evaluation suite...", file=sys.stderr)
    runner = EvaluationRunner(seed=args.seed)

    try:
        report = runner.run_all(tags=tags, case_ids=case_ids)
    except Exception as exc:
        print(f"Error during evaluation run: {exc}", file=sys.stderr)
        return 1

    if args.output:
        try:
            runner.save_report(report, args.output)
            print(f"› Saved report JSON to: {args.output}", file=sys.stderr)
        except Exception as exc:
            print(f"Error writing output file: {exc}", file=sys.stderr)
            return 1

    if args.json:
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(runner.format_text_summary(report))

    # Threshold check
    if report.pass_rate < args.min_score or report.failed_cases > 0:
        print(
            f"› Evaluation failed: pass rate {report.pass_rate * 100:.1f}% "
            f"below min_score {args.min_score * 100:.1f}%",
            file=sys.stderr,
        )
        return 1

    print("› Evaluation completed successfully: 100% pass rate.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(eval_cli())
