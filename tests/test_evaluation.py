"""Comprehensive tests for offline workflow evaluation framework and metrics."""

from __future__ import annotations

import io
import json
from unittest.mock import patch

import pytest

from orchestrator.evaluation.cases import (
    DATASET_VERSION,
    EvaluationCase,
    ExpectedBehavior,
    get_evaluation_cases,
)
from orchestrator.evaluation.metrics import (
    CaseEvaluationResult,
    calculate_metrics,
    compute_metric,
)
from orchestrator.evaluation.runner import EvaluationRunner, eval_cli
from orchestrator.memory.long_term import reset_local_store as reset_ltm
from orchestrator.memory.short_term import reset_local_store as reset_stm


@pytest.fixture(autouse=True)
def clean_memory():
    """Reset in-process memory stores before and after each test."""
    reset_stm()
    reset_ltm()
    yield
    reset_stm()
    reset_ltm()


# -----------------------------------------------------------------------------
# 1. Metric Calculation and Denominator Safety Tests
# -----------------------------------------------------------------------------


def test_metric_zero_denominator_safe_handling():
    """Metrics with zero denominators must yield applicable=False and value=None."""
    res = compute_metric(
        name="test_zero_denom",
        numerator=0,
        denominator=0,
        description="Zero denominator test metric",
    )
    assert not res.applicable
    assert res.value is None
    assert res.denominator == 0

    d = res.to_dict()
    assert d["applicable"] is False
    assert d["value"] is None
    assert d["denominator"] == 0


def test_metric_valid_calculation_and_clamping():
    """Metrics with valid denominators must compute accurate ratios clamped to [0.0, 1.0]."""
    res_half = compute_metric("half", 5, 10, "50% test")
    assert res_half.applicable is True
    assert res_half.value == 0.5
    assert res_half.to_dict()["value"] == 0.5

    # Clamping test
    res_overflow = compute_metric("overflow", 12, 10, "overflow test")
    assert res_overflow.value == 1.0

    res_negative = compute_metric("negative", -2, 10, "negative test")
    assert res_negative.value == 0.0


def test_all_ten_metrics_computed_correctly():
    """Verify that calculate_metrics computes all 10 required evaluation metrics."""
    cases = [
        EvaluationCase(
            case_id="c1",
            name="C1",
            description="desc",
            task="Task 1",
            expected=ExpectedBehavior(
                expected_status="completed",
                expected_completed=["s1"],
                expected_sections=["### Analysis specialist"],
            ),
        ),
        EvaluationCase(
            case_id="c2",
            name="C2",
            description="desc",
            task="Task 2",
            expected=ExpectedBehavior(
                expected_status="rejected",
                expected_failed=["s1"],
                expected_blocked=["s2"],
            ),
            hitl_action={"decision": "reject", "approver": "sec_lead"},
        ),
    ]

    results = [
        CaseEvaluationResult(
            case_id="c1",
            name="C1",
            passed=True,
            status="completed",
            expected_status="completed",
            status_match=True,
            completed_subtasks=["s1"],
            failed_subtasks=[],
            blocked_subtasks=[],
            retries=0,
            escalated=False,
            sections_matched=["### Analysis specialist"],
            missing_sections=[],
            duplicates_detected=[],
            execution_record={
                "plan": [{"id": "s1", "kind": "analysis", "instruction": "Task 1", "depends_on": []}],
                "completed": [{"id": "s1", "kind": "analysis", "status": "done", "result": "done"}],
            },
        ),
        CaseEvaluationResult(
            case_id="c2",
            name="C2",
            passed=True,
            status="rejected",
            expected_status="rejected",
            status_match=True,
            completed_subtasks=[],
            failed_subtasks=["s1"],
            blocked_subtasks=["s2"],
            retries=2,
            escalated=True,
            sections_matched=[],
            missing_sections=[],
            duplicates_detected=[],
            execution_record={
                "plan": [
                    {"id": "s1", "kind": "research", "instruction": "Task 2a", "depends_on": []},
                    {"id": "s2", "kind": "code", "instruction": "Task 2b", "depends_on": ["s1"]},
                ],
                "completed": [
                    {"id": "s1", "kind": "research", "status": "rejected", "result": "failed"},
                    {"id": "s2", "kind": "code", "status": "blocked", "result": ""},
                ],
                "resolution": {"approver": "sec_lead", "decision": "reject"},
                "escalated": False,
            },
        ),
    ]

    metrics = calculate_metrics(cases, results)

    expected_metric_keys = {
        "task_completion_rate",
        "correct_terminal_status_rate",
        "plan_validity_rate",
        "dependency_satisfaction_rate",
        "correct_blocked_rate",
        "retry_recovery_rate",
        "hitl_decision_handling_accuracy",
        "resume_correctness",
        "output_completeness_rate",
        "no_duplicates_rate",
    }

    assert set(metrics.keys()) == expected_metric_keys

    # Check values
    assert metrics["task_completion_rate"].value == 0.5  # 1 out of 2 completed/approved/edited
    assert metrics["correct_terminal_status_rate"].value == 1.0  # 2 out of 2 matched
    assert metrics["plan_validity_rate"].value == 1.0  # both valid
    assert metrics["correct_blocked_rate"].value == 1.0  # s2 correctly blocked
    assert metrics["retry_recovery_rate"].value == 0.0  # c2 retried but did not recover
    assert metrics["hitl_decision_handling_accuracy"].value == 1.0  # c2 resolution matched
    assert not metrics["resume_correctness"].applicable  # 0 resume cases -> N/A
    assert metrics["resume_correctness"].value is None
    assert metrics["no_duplicates_rate"].value == 1.0


# -----------------------------------------------------------------------------
# 2. Dataset Integrity and Case Validation Tests
# -----------------------------------------------------------------------------


def test_dataset_version_and_case_integrity():
    """Verify evaluation dataset versioning, uniqueness, and validity of all 9 cases."""
    assert DATASET_VERSION == "1.0.0"
    all_cases = get_evaluation_cases()
    assert len(all_cases) == 9

    case_ids = [c.case_id for c in all_cases]
    assert len(case_ids) == len(set(case_ids)), "Evaluation case IDs must be strictly unique"

    valid_statuses = ("completed", "partial", "failed", "pending", "approved", "edited", "rejected")
    for case in all_cases:
        case.validate()
        assert case.expected.expected_status in valid_statuses
        assert case.name
        assert case.task
        assert case.tags


def test_case_validation_rejects_malformed_definitions():
    """EvaluationCase.validate() must raise ValueError for missing or invalid attributes."""
    # Empty case_id
    with pytest.raises(ValueError, match="non-empty 'case_id'"):
        EvaluationCase(case_id="", name="N", description="D", task="T").validate()

    # Empty name
    with pytest.raises(ValueError, match="'name' must not be empty"):
        EvaluationCase(case_id="c_bad", name="", description="D", task="T").validate()

    # Empty task
    with pytest.raises(ValueError, match="'task' must not be empty"):
        EvaluationCase(case_id="c_bad", name="N", description="D", task="").validate()

    # Invalid expected status
    with pytest.raises(ValueError, match="unknown expected_status"):
        EvaluationCase(
            case_id="c_bad",
            name="N",
            description="D",
            task="T",
            expected=ExpectedBehavior(expected_status="invalid_status"),
        ).validate()


# -----------------------------------------------------------------------------
# 3. Filtering and Tag Selection Tests
# -----------------------------------------------------------------------------


def test_get_evaluation_cases_filtering():
    """Verify filtering evaluation cases by tags and case IDs."""
    # Tag filter
    hitl_cases = get_evaluation_cases(tags=["hitl"])
    assert len(hitl_cases) >= 1
    assert all("hitl" in c.tags for c in hitl_cases)

    # Case ID filter
    single = get_evaluation_cases(case_ids=["case_01_single_specialist"])
    assert len(single) == 1
    assert single[0].case_id == "case_01_single_specialist"

    # Non-existent filter returns empty list; runner.run_all raises ValueError
    assert get_evaluation_cases(tags=["non_existent_tag_xyz"]) == []
    runner = EvaluationRunner()
    with pytest.raises(ValueError, match="No evaluation cases matched"):
        runner.run_all(tags=["non_existent_tag_xyz"])


# -----------------------------------------------------------------------------
# 4. Deterministic Runner Execution and Offline Safety
# -----------------------------------------------------------------------------


def test_evaluation_runner_isolated_and_offline():
    """Verify that runner executes deterministically offline with mock LLM providers."""
    runner = EvaluationRunner(seed=123)

    # Active assertion: OpenAI and Anthropic API keys must not be accessed
    with patch("orchestrator.llm.LLMClient._openai") as mock_openai:
        with patch("orchestrator.llm.LLMClient._anthropic") as mock_anthropic:
            report = runner.run_all(case_ids=["case_01_single_specialist"])

            assert not mock_openai.called, "Evaluation must not call live OpenAI API"
            assert not mock_anthropic.called, "Evaluation must not call live Anthropic API"

    assert report.total_cases == 1
    assert report.passed_cases == 1
    assert report.pass_rate == 1.0
    assert report.case_results[0].passed is True
    assert report.case_results[0].status == "completed"


def test_evaluation_runner_min_score_and_exit_code():
    """Verify min_score threshold enforcement and CLI exit codes."""
    # Passing run with min_score=0.9
    code = eval_cli(["--cases", "case_01_single_specialist", "--min-score", "0.9"])
    assert code == 0

    # Failing threshold: min_score > 1.0 (impossible to satisfy)
    code_failing = eval_cli(["--cases", "case_01_single_specialist", "--min-score", "1.1"])
    assert code_failing == 1


def test_evaluation_cli_json_mode_clean_output():
    """Verify that --json emits clean, valid JSON on stdout."""
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    with patch("sys.stdout", stdout_buf):
        with patch("sys.stderr", stderr_buf):
            code = eval_cli(["--cases", "case_01_single_specialist", "--json"])

    assert code == 0
    raw_stdout = stdout_buf.getvalue().strip()
    assert raw_stdout, "stdout must contain JSON payload"

    parsed = json.loads(raw_stdout)
    assert parsed["version"] == "1.0.0"
    assert parsed["total_cases"] == 1
    assert parsed["passed_cases"] == 1
    assert "metrics" in parsed
    assert "case_results" in parsed


def test_evaluation_reproducibility():
    """Verify that running the evaluation twice with the same seed produces identical results."""
    runner1 = EvaluationRunner(seed=42)
    report1 = runner1.run_all(case_ids=["case_01_single_specialist", "case_03_concurrent_independent"])

    runner2 = EvaluationRunner(seed=42)
    report2 = runner2.run_all(case_ids=["case_01_single_specialist", "case_03_concurrent_independent"])

    assert report1.total_cases == report2.total_cases
    assert report1.passed_cases == report2.passed_cases
    assert report1.pass_rate == report2.pass_rate

    for r1, r2 in zip(report1.case_results, report2.case_results, strict=True):
        assert r1.case_id == r2.case_id
        assert r1.passed == r2.passed
        assert r1.status == r2.status
        assert r1.completed_subtasks == r2.completed_subtasks
