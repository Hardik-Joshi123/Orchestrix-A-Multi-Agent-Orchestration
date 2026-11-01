"""Versioned evaluation dataset and case definitions for offline workflow evaluation."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from typing import Any

DATASET_VERSION: str = "1.0.0"


@dataclass
class ExpectedBehavior:
    """Expected orchestration behavior and assertions for an evaluation case."""

    expected_status: str
    expected_completed: list[str] = field(default_factory=list)
    expected_failed: list[str] = field(default_factory=list)
    expected_blocked: list[str] = field(default_factory=list)
    expected_retries_min: int = 0
    expected_escalated: bool | None = None
    expected_sections: list[str] = field(default_factory=list)
    expected_tools_used: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "expected_status": self.expected_status,
            "expected_completed": self.expected_completed,
            "expected_failed": self.expected_failed,
            "expected_blocked": self.expected_blocked,
            "expected_retries_min": self.expected_retries_min,
            "expected_escalated": self.expected_escalated,
            "expected_sections": self.expected_sections,
            "expected_tools_used": self.expected_tools_used,
        }


@dataclass
class EvaluationCase:
    """A deterministic, isolated workflow evaluation test case."""

    case_id: str
    name: str
    description: str
    task: str
    tags: list[str] = field(default_factory=list)
    plan: list[dict[str, Any]] | None = None
    mock_llm_response: str | None = None
    mock_specialist_results: dict[str, Any] = field(default_factory=dict)
    hitl_action: dict[str, Any] | None = None
    is_resume_case: bool = False
    initial_checkpoint_state: dict[str, Any] | None = None
    expected: ExpectedBehavior = field(default_factory=lambda: ExpectedBehavior(expected_status="completed"))
    max_concurrency: int | None = None

    def validate(self) -> None:
        """Validate structural integrity of the evaluation case."""
        if not self.case_id or not str(self.case_id).strip():
            raise ValueError("EvaluationCase must have a non-empty 'case_id'")
        if not self.name or not str(self.name).strip():
            raise ValueError(f"Case {self.case_id}: 'name' must not be empty")
        if not self.task or not str(self.task).strip():
            raise ValueError(f"Case {self.case_id}: 'task' must not be empty")
        if not self.expected.expected_status:
            raise ValueError(f"Case {self.case_id}: 'expected_status' must not be empty")

        valid_statuses = ("completed", "partial", "failed", "pending", "approved", "edited", "rejected")
        if self.expected.expected_status not in valid_statuses:
            raise ValueError(
                f"Case {self.case_id}: unknown expected_status '{self.expected.expected_status}'. "
                f"Must be one of {valid_statuses}"
            )

        if self.is_resume_case:
            if not self.initial_checkpoint_state:
                raise ValueError(
                    f"Case {self.case_id}: resume cases must specify 'initial_checkpoint_state'"
                )

    def to_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "name": self.name,
            "description": self.description,
            "task": self.task,
            "tags": self.tags,
            "plan": self.plan,
            "mock_specialist_results": self.mock_specialist_results,
            "hitl_action": self.hitl_action,
            "is_resume_case": self.is_resume_case,
            "initial_checkpoint_state": self.initial_checkpoint_state,
            "expected": self.expected.to_dict(),
            "max_concurrency": self.max_concurrency,
        }


# ============================================================================
# Representative Evaluation Cases (Version 1.0.0)
# ============================================================================

_RAW_EVALUATION_CASES: list[EvaluationCase] = [
    # Case 1: Simple single-specialist task
    EvaluationCase(
        case_id="case_01_single_specialist",
        name="Single Specialist Workflow",
        description=(
            "Single specialist workflow testing end-to-end decomposition, specialist review, "
            "and result reduction."
        ),
        task="Analyze latency and throughput bottlenecks in database queries.",
        tags=["single", "analysis", "smoke"],
        plan=[
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Analyze latency and throughput bottlenecks in database queries thoroughly.",
                "status": "pending",
                "depends_on": [],
            }
        ],
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_completed=["s1"],
            expected_sections=["### Analysis specialist"],
        ),
    ),
    # Case 2: Multi-specialist task with dependencies
    EvaluationCase(
        case_id="case_02_multi_specialist_dependencies",
        name="Multi-Specialist Sequential Dependencies",
        description=(
            "Sequential pipeline where downstream subtasks depend strictly on upstream results "
            "(research -> analysis -> writing)."
        ),
        task="Research vector databases, analyze latency trade-offs, and draft technical documentation.",
        tags=["multi", "dependencies", "pipeline"],
        plan=[
            {
                "id": "s1",
                "kind": "research",
                "instruction": (
                    "Research vector database indexing algorithms and performance characteristics."
                ),
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "analysis",
                "instruction": "Analyze performance trade-offs based on indexing research findings.",
                "status": "pending",
                "depends_on": ["s1"],
            },
            {
                "id": "s3",
                "kind": "writing",
                "instruction": "Draft technical documentation summarizing trade-offs and recommendations.",
                "status": "pending",
                "depends_on": ["s2"],
            },
        ],
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_completed=["s1", "s2", "s3"],
            expected_sections=[
                "### Research specialist",
                "### Analysis specialist",
                "### Writing specialist",
            ],
        ),
    ),
    # Case 3: Independent tasks eligible for concurrent execution
    EvaluationCase(
        case_id="case_03_concurrent_independent",
        name="Concurrent Independent Subtasks",
        description=(
            "Multiple independent specialist subtasks running concurrently without dependencies "
            "under bounded concurrency."
        ),
        task="Perform parallel performance benchmarking on CPU, memory, and network subsystems.",
        tags=["concurrency", "parallel", "independent"],
        max_concurrency=3,
        plan=[
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": "Benchmark CPU profile and saturation points across compute nodes.",
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "analysis",
                "instruction": (
                    "Benchmark memory allocation and garbage collection overhead across compute nodes."
                ),
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s3",
                "kind": "research",
                "instruction": (
                    "Gather network packet loss and round-trip latency metrics across compute nodes."
                ),
                "status": "pending",
                "depends_on": [],
            },
        ],
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_completed=["s1", "s2", "s3"],
            expected_sections=["### Analysis specialist", "### Research specialist"],
        ),
    ),
    # Case 4: Invalid LLM plan requiring fallback
    EvaluationCase(
        case_id="case_04_invalid_plan_fallback",
        name="Invalid LLM Plan Fallback",
        description=(
            "Supervisor detects an unresolvable circular dependency in an LLM plan "
            "and falls back to a valid keyword plan."
        ),
        task="Write an automated integration test script to validate API responses.",
        tags=["planning", "fallback", "validation"],
        plan=None,
        mock_llm_response=json.dumps(
            {
                "plan": [
                    {
                        "id": "s1",
                        "kind": "code",
                        "instruction": "Write test runner script",
                        "depends_on": ["s2"],
                    },
                    {
                        "id": "s2",
                        "kind": "code",
                        "instruction": "Setup test environment fixtures",
                        "depends_on": ["s1"],
                    },
                ]
            }
        ),
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_sections=["### Code specialist"],
        ),
    ),
    # Case 5: Specialist failure with successful retry
    EvaluationCase(
        case_id="case_05_specialist_retry_recovery",
        name="Specialist Retry and Recovery",
        description=(
            "Specialist encounters a temporary failure on the first attempt but succeeds "
            "upon retry within configured limits."
        ),
        task="Analyze system logs to diagnose transient socket timeouts.",
        tags=["retry", "resilience", "recovery"],
        plan=[
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": (
                    "Analyze system logs for transient socket timeouts and error codes in detail."
                ),
                "status": "pending",
                "depends_on": [],
            }
        ],
        mock_specialist_results={
            "s1": [
                {
                    "status": "failed",
                    "error": "Transient socket timeout during log ingestion attempt",
                    "confidence": 0.0,
                },
                {
                    "status": "done",
                    "result": (
                        "Detailed log analysis completed successfully with identified timeout root causes."
                    ),
                    "confidence": 0.85,
                },
            ]
        },
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_completed=["s1"],
            expected_retries_min=1,
            expected_sections=["### Analysis specialist"],
        ),
    ),
    # Case 6: Failed prerequisite and blocked dependent task
    EvaluationCase(
        case_id="case_06_dependency_failure_blocked",
        name="Prerequisite Failure and Blocked Dependent Task",
        description=(
            "Upstream prerequisite fails permanently, and dependent subtask is correctly marked blocked "
            "without executing."
        ),
        task="Research third-party auth vulnerabilities and implement security patch.",
        tags=["dependencies", "failure", "blocked"],
        plan=[
            {
                "id": "s1",
                "kind": "research",
                "instruction": "Research auth vulnerability specifications and CVE disclosures.",
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "code",
                "instruction": "Implement security patch for auth vulnerability according to research.",
                "status": "pending",
                "depends_on": ["s1"],
            },
        ],
        mock_specialist_results={
            "s1": {
                "status": "failed",
                "error": "Upstream CVE database unreachable (permanent network partition)",
                "confidence": 0.0,
            }
        },
        hitl_action={
            "decision": "reject",
            "approver": "security_lead",
            "note": "Upstream CVE database permanently unavailable",
        },
        expected=ExpectedBehavior(
            expected_status="rejected",
            expected_failed=["s1"],
            expected_blocked=["s2"],
        ),
    ),
    # Case 7: HITL escalation and resolution
    EvaluationCase(
        case_id="case_07_hitl_escalation_resolution",
        name="HITL Escalation and Reviewer Resolution",
        description=(
            "Low confidence specialist subtask triggers HITL escalation; "
            "human reviewer approves action and workflow completes."
        ),
        task="Analyze corporate treasury allocation policy under shifting interest rates.",
        tags=["hitl", "escalation", "approval"],
        plan=[
            {
                "id": "s1",
                "kind": "analysis",
                "instruction": (
                    "Recommend treasury allocation policy adjustments under shifting rate regime."
                ),
                "status": "pending",
                "depends_on": [],
            }
        ],
        mock_specialist_results={
            "s1": {
                "status": "pending",
                "result": (
                    "Proposed rebalancing 40% of liquid reserves into short-duration government securities."
                ),
                "confidence": 0.35,  # triggers low-confidence escalation
            }
        },
        hitl_action={
            "decision": "approve",
            "approver": "chief_risk_officer",
            "note": "Approved rebalancing allocation after risk committee review.",
        },
        expected=ExpectedBehavior(
            expected_status="approved",
            expected_completed=["s1"],
            expected_escalated=True,
            expected_sections=["### Analysis specialist"],
        ),
    ),
    # Case 8: Partial result and final result assembly
    EvaluationCase(
        case_id="case_08_partial_result_assembly",
        name="Partial Result and Incomplete Work Assembly",
        description=(
            "One independent subtask succeeds while another fails, verifying partial status "
            "and assembly of completed work alongside failure diagnostics."
        ),
        task="Research distributed consensus protocols and implement consensus module.",
        tags=["partial", "reduction", "assembly"],
        plan=[
            {
                "id": "s1",
                "kind": "research",
                "instruction": (
                    "Research Raft consensus guarantees and leader election mechanics in detail."
                ),
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "code",
                "instruction": "Implement Raft consensus state machine in Python.",
                "status": "pending",
                "depends_on": [],
            },
        ],
        mock_specialist_results={
            "s1": {
                "status": "done",
                "result": (
                    "Thorough research report on Raft consensus protocol guarantees and leader election."
                ),
                "confidence": 0.90,
            },
            "s2": {
                "status": "failed",
                "error": "Syntax generator failure: invalid syntax in protocol handler",
                "confidence": 0.0,
            },
        },
        hitl_action={
            "decision": "reject",
            "approver": "tech_lead",
            "note": "Skip failing consensus implementation and assemble verified research",
        },
        expected=ExpectedBehavior(
            expected_status="rejected",
            expected_completed=["s1"],
            expected_failed=["s2"],
            expected_sections=["### Research specialist", "### Incomplete Work"],
        ),
    ),
    # Case 9: Resume from a checkpoint without repeating completed work
    EvaluationCase(
        case_id="case_09_checkpoint_resume_bypass",
        name="Durable Checkpoint Resumption Without Duplicate Execution",
        description=(
            "Resume an interrupted workflow from a durable checkpoint where subtask 1 completed; "
            "verify subtask 1 is bypassed and subtask 2 completes."
        ),
        task="Research high-performance caching algorithms and draft implementation guide.",
        tags=["resume", "persistence", "checkpoint"],
        is_resume_case=True,
        plan=[
            {
                "id": "s1",
                "kind": "research",
                "instruction": "Research cache eviction policies and cache hit optimizations.",
                "status": "pending",
                "depends_on": [],
            },
            {
                "id": "s2",
                "kind": "writing",
                "instruction": (
                    "Draft cache implementation guide with verified operational configurations."
                ),
                "status": "pending",
                "depends_on": ["s1"],
            },
        ],
        initial_checkpoint_state={
            "run_id": "eval-resume-case-09",
            "task_id": "eval-resume-case-09",
            "task": "Research high-performance caching algorithms and draft implementation guide.",
            "plan": [
                {
                    "id": "s1",
                    "kind": "research",
                    "instruction": "Research cache eviction policies and cache hit optimizations.",
                    "status": "done",
                    "depends_on": [],
                },
                {
                    "id": "s2",
                    "kind": "writing",
                    "instruction": (
                        "Draft cache implementation guide with verified operational configurations."
                    ),
                    "status": "pending",
                    "depends_on": ["s1"],
                },
            ],
            "completed": [
                {
                    "id": "s1",
                    "kind": "research",
                    "instruction": "Research cache eviction policies and cache hit optimizations.",
                    "status": "done",
                    "result": (
                        "Detailed research on LRU, LFU, and ARC cache eviction algorithms "
                        "with benchmark comparisons."
                    ),
                    "confidence": 0.95,
                    "tools_used": ["web_search"],
                }
            ],
            "failed": [],
            "blocked": [],
            "tools_used": ["web_search"],
            "retries": 0,
            "status": "pending",
            "trace": [],
        },
        expected=ExpectedBehavior(
            expected_status="completed",
            expected_completed=["s1", "s2"],
            expected_sections=["### Research specialist", "### Writing specialist"],
        ),
    ),
]


def get_evaluation_cases(
    tags: list[str] | None = None,
    case_ids: list[str] | None = None,
) -> list[EvaluationCase]:
    """Retrieve and validate evaluation cases, optionally filtered by tags or case IDs."""
    cases = copy.deepcopy(_RAW_EVALUATION_CASES)

    # Validate all cases at load time
    seen_ids = set()
    for c in cases:
        c.validate()
        if c.case_id in seen_ids:
            raise ValueError(f"Duplicate evaluation case_id found: '{c.case_id}'")
        seen_ids.add(c.case_id)

    if case_ids:
        filter_ids = {cid.strip() for cid in case_ids if cid.strip()}
        cases = [c for c in cases if c.case_id in filter_ids]

    if tags:
        filter_tags = {t.strip().lower() for t in tags if t.strip()}
        cases = [c for c in cases if any(t.lower() in filter_tags for t in c.tags)]

    return cases


EVALUATION_CASES: list[EvaluationCase] = _RAW_EVALUATION_CASES
