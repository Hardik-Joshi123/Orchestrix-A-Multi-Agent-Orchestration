"""Comprehensive metrics calculation engine for workflow evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .cases import EvaluationCase


@dataclass
class MetricResult:
    """The computed score, numerator, and denominator for a single metric."""

    name: str
    numerator: int
    denominator: int
    value: float | None
    applicable: bool
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "numerator": self.numerator,
            "denominator": self.denominator,
            "value": round(self.value, 4) if self.value is not None else None,
            "applicable": self.applicable,
            "description": self.description,
        }


@dataclass
class CaseEvaluationResult:
    """Per-case evaluation telemetry and correctness outcome."""

    case_id: str
    name: str
    passed: bool
    status: str
    expected_status: str
    status_match: bool
    completed_subtasks: list[str]
    failed_subtasks: list[str]
    blocked_subtasks: list[str]
    retries: int
    escalated: bool
    sections_matched: list[str]
    missing_sections: list[str]
    duplicates_detected: list[str]
    execution_record: dict[str, Any]
    error_message: str | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        # Execution record can be large; exclude raw state if serializing summary
        data.pop("execution_record", None)
        return data


def compute_metric(
    name: str,
    numerator: int,
    denominator: int,
    description: str,
) -> MetricResult:
    """Safely compute a metric, properly handling non-applicable (zero denominator) cases.

    Never reports missing measurements or zero-denominator cases as successful results (1.0).
    """
    if denominator <= 0:
        return MetricResult(
            name=name,
            numerator=numerator,
            denominator=0,
            value=None,
            applicable=False,
            description=description,
        )

    val = float(numerator) / float(denominator)
    return MetricResult(
        name=name,
        numerator=numerator,
        denominator=denominator,
        value=min(1.0, max(0.0, val)),
        applicable=True,
        description=description,
    )


def calculate_metrics(
    cases: list[EvaluationCase],
    case_results: list[CaseEvaluationResult],
) -> dict[str, MetricResult]:
    """Compute the 10 defined evaluation metrics across all executed cases."""
    case_map = {c.case_id: c for c in cases}
    total_cases = len(case_results)

    # 1. Task completion rate: terminal status in ('completed', 'approved', 'edited')
    successful_completions = sum(
        1 for r in case_results if r.status in ("completed", "approved", "edited")
    )
    metric_completion = compute_metric(
        name="task_completion_rate",
        numerator=successful_completions,
        denominator=total_cases,
        description="Fraction of evaluated workflows that completed (completed, approved, or edited).",
    )

    # 2. Correct terminal-status rate: matches case expected_status
    status_matches = sum(1 for r in case_results if r.status_match)
    metric_status = compute_metric(
        name="correct_terminal_status_rate",
        numerator=status_matches,
        denominator=total_cases,
        description="Fraction of evaluated workflows whose terminal status strictly matched expectation.",
    )

    # 3. Plan validity rate: evaluated plans that are valid without cycles or invalid kinds
    plans_evaluated = 0
    plans_valid = 0
    for r in case_results:
        raw_plan = r.execution_record.get("plan")
        if raw_plan:
            plans_evaluated += 1
            from ..graph.supervisor import validate_plan

            is_valid, _ = validate_plan(raw_plan, strict_specialists=True)
            if is_valid:
                plans_valid += 1
    metric_plan_validity = compute_metric(
        name="plan_validity_rate",
        numerator=plans_valid,
        denominator=plans_evaluated,
        description="Fraction of execution plans that were structurally and semantically valid.",
    )

    # 4. Dependency satisfaction rate: executed subtasks with prerequisites whose prereqs were all done
    dep_subtasks_evaluated = 0
    dep_subtasks_satisfied = 0
    for r in case_results:
        plan = r.execution_record.get("plan") or []
        completed = r.execution_record.get("completed") or []
        done_ids = {c["id"] for c in completed if c.get("status") == "done"}

        for st in completed:
            sid = st.get("id")
            plan_item = next((s for s in plan if s.get("id") == sid), None)
            deps = (plan_item.get("depends_on") if plan_item else None) or st.get("depends_on") or []
            if deps:
                dep_subtasks_evaluated += 1
                if st.get("status") in ("done", "approved", "edited"):
                    if all(d in done_ids for d in deps):
                        dep_subtasks_satisfied += 1
                elif st.get("status") == "blocked":
                    # Blocked subtasks did not execute with unsatisfied deps
                    dep_subtasks_satisfied += 1

    metric_deps = compute_metric(
        name="dependency_satisfaction_rate",
        numerator=dep_subtasks_satisfied,
        denominator=dep_subtasks_evaluated,
        description="Fraction of dependent subtasks where all prerequisites were satisfied before execution.",
    )

    # 5. Correct blocked-task rate: subtasks expected to be blocked that actually blocked
    total_expected_blocked = sum(len(case_map[r.case_id].expected.expected_blocked) for r in case_results)
    actual_correctly_blocked = sum(
        len(set(r.blocked_subtasks) & set(case_map[r.case_id].expected.expected_blocked))
        for r in case_results
    )
    metric_blocked = compute_metric(
        name="correct_blocked_rate",
        numerator=actual_correctly_blocked,
        denominator=total_expected_blocked,
        description="Fraction of expected-blocked subtasks that were correctly transitioned to 'blocked'.",
    )

    # 6. Retry recovery rate: subtasks that retried and subsequently succeeded
    retried_subtasks_count = 0
    recovered_subtasks_count = 0
    for r in case_results:
        if r.retries > 0:
            retried_subtasks_count += 1
            if r.status in ("completed", "approved", "edited"):
                recovered_subtasks_count += 1
    metric_retry = compute_metric(
        name="retry_recovery_rate",
        numerator=recovered_subtasks_count,
        denominator=retried_subtasks_count,
        description="Fraction of retried subtasks that recovered to a successful status.",
    )

    # 7. HITL decision-handling accuracy: cases with HITL action correctly recorded and resolved
    hitl_cases_total = sum(1 for r in case_results if case_map[r.case_id].hitl_action is not None)
    hitl_cases_passed = 0
    for r in case_results:
        c = case_map[r.case_id]
        if c.hitl_action:
            res_meta = r.execution_record.get("resolution")
            status = r.status
            expected_status = c.expected.expected_status
            if res_meta and status == expected_status and not r.execution_record.get("escalated"):
                if res_meta.get("approver") == c.hitl_action.get("approver"):
                    hitl_cases_passed += 1
    metric_hitl = compute_metric(
        name="hitl_decision_handling_accuracy",
        numerator=hitl_cases_passed,
        denominator=hitl_cases_total,
        description=(
            "Fraction of escalated workflows whose human resolutions were accurately recorded and applied."
        ),
    )

    # 8. Resume correctness: resume cases where completed subtasks were bypassed and results assembled
    resume_cases_total = sum(1 for r in case_results if case_map[r.case_id].is_resume_case)
    resume_cases_passed = 0
    for r in case_results:
        c = case_map[r.case_id]
        if c.is_resume_case:
            trace = r.execution_record.get("trace") or []
            has_resume_event = any(e.get("event") == "resume" for e in trace)
            if r.passed and has_resume_event and not r.duplicates_detected:
                resume_cases_passed += 1
    metric_resume = compute_metric(
        name="resume_correctness",
        numerator=resume_cases_passed,
        denominator=resume_cases_total,
        description=(
            "Fraction of resume cases where completed work was bypassed and execution completed cleanly."
        ),
    )

    # 9. Output completeness and required-section coverage
    total_sections_expected = sum(len(case_map[r.case_id].expected.expected_sections) for r in case_results)
    total_sections_matched = sum(len(r.sections_matched) for r in case_results)
    metric_output = compute_metric(
        name="output_completeness_rate",
        numerator=total_sections_matched,
        denominator=total_sections_expected,
        description="Fraction of declared expected specialist sections present in the assembled output.",
    )

    # 10. Duplicate execution or duplicate result detection: fraction of cases free from duplicate executions
    cases_without_duplicates = sum(1 for r in case_results if not r.duplicates_detected)
    metric_duplicates = compute_metric(
        name="no_duplicates_rate",
        numerator=cases_without_duplicates,
        denominator=total_cases,
        description="Fraction of evaluated workflows completely free of duplicate executions or results.",
    )

    return {
        "task_completion_rate": metric_completion,
        "correct_terminal_status_rate": metric_status,
        "plan_validity_rate": metric_plan_validity,
        "dependency_satisfaction_rate": metric_deps,
        "correct_blocked_rate": metric_blocked,
        "retry_recovery_rate": metric_retry,
        "hitl_decision_handling_accuracy": metric_hitl,
        "resume_correctness": metric_resume,
        "output_completeness_rate": metric_output,
        "no_duplicates_rate": metric_duplicates,
    }
