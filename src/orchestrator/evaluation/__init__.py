"""Offline workflow evaluation framework."""

from __future__ import annotations

from .cases import DATASET_VERSION, EvaluationCase, ExpectedBehavior, get_evaluation_cases
from .metrics import CaseEvaluationResult, MetricResult, calculate_metrics, compute_metric
from .runner import EvaluationReport, EvaluationRunner, eval_cli

__all__ = [
    "DATASET_VERSION",
    "CaseEvaluationResult",
    "EvaluationCase",
    "EvaluationReport",
    "EvaluationRunner",
    "ExpectedBehavior",
    "MetricResult",
    "calculate_metrics",
    "compute_metric",
    "eval_cli",
    "get_evaluation_cases",
]
