"""Tests for supervisor plan generation, validation, ordering, and execution."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from orchestrator.graph.builder import run_task
from orchestrator.graph.supervisor import (
    MAX_PLAN_SIZE,
    VALID_SPECIALISTS,
    decompose,
    is_supported_specialist,
    normalize_specialist_kind,
    parse_plan_payload,
    plan_subtasks,
    validate_plan,
)
from orchestrator.llm import Completion
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


def test_normalize_specialist_kind():
    """Verify validation and normalization of specialist kinds and aliases."""
    for valid in VALID_SPECIALISTS:
        assert normalize_specialist_kind(valid) == valid
        assert normalize_specialist_kind(valid.upper()) == valid

    # Aliases
    assert normalize_specialist_kind("coder") == "code"
    assert normalize_specialist_kind("developer") == "code"
    assert normalize_specialist_kind("writer") == "writing"
    assert normalize_specialist_kind("documentation") == "writing"
    assert normalize_specialist_kind("investigate") == "research"
    assert normalize_specialist_kind("evaluator") == "analysis"
    assert normalize_specialist_kind("evaluation") == "analysis"

    # Unknown or malformed inputs safely default to analysis
    assert normalize_specialist_kind("astrophysics") == "analysis"
    assert normalize_specialist_kind("database_admin") == "analysis"
    assert normalize_specialist_kind("") == "analysis"
    assert normalize_specialist_kind(None) == "analysis"  # type: ignore[arg-type]


def test_parse_plan_payload():
    """Verify structured plan parsing from dicts, lists, and markdown JSON strings."""
    assert parse_plan_payload(None) is None
    assert parse_plan_payload("") is None
    assert parse_plan_payload("   ") is None
    assert parse_plan_payload("not valid json") is None
    assert parse_plan_payload({}) is None
    assert parse_plan_payload({"random": 123}) is None

    # Markdown codeblock
    md_payload = '```json\n{"plan": [{"kind": "code", "instruction": "implement feature"}]}\n```'
    parsed_md = parse_plan_payload(md_payload, default_instruction="default")
    assert parsed_md is not None
    assert len(parsed_md) == 1
    assert parsed_md[0]["kind"] == "code"
    assert parsed_md[0]["instruction"] == "implement feature"
    assert parsed_md[0]["status"] == "pending"

    # List of strings
    parsed_list = parse_plan_payload(["writing", "coder", "unknown_spec"], default_instruction="do task")
    assert parsed_list is not None
    assert len(parsed_list) == 3
    assert [s["kind"] for s in parsed_list] == ["writing", "code", "analysis"]
    assert all(s["instruction"] == "do task" for s in parsed_list)

    # Dict with 'subtasks' and alternative keys ('specialist', 'task')
    alt_dict = {
        "subtasks": [
            {"specialist": "researcher", "task": "gather requirements"},
            {"role": "writer", "instruction": "document design"},
        ]
    }
    parsed_alt = parse_plan_payload(alt_dict)
    assert parsed_alt is not None
    assert [s["kind"] for s in parsed_alt] == ["research", "writing"]
    assert parsed_alt[0]["instruction"] == "gather requirements"
    assert parsed_alt[1]["instruction"] == "document design"


def test_plan_drives_specialist_selection():
    """Verify that a custom plan selects only the specified specialists, ignoring keyword defaults."""
    # A task containing 'research', 'analyze', and 'code' keywords would normally trigger all 3.
    # By injecting a plan with only 'writing' and 'code', only those two should execute.
    custom_plan = [
        {"kind": "writing", "instruction": "Draft documentation"},
        {"kind": "code", "instruction": "Write unit tests"},
    ]
    result = run_task("Research the architecture, analyze safety, and implement code", plan=custom_plan)
    executed_kinds = [c["kind"] for c in result["completed"]]
    assert executed_kinds == ["writing", "code"]
    assert "### Writing specialist" in result["answer"]
    assert "### Code specialist" in result["answer"]
    assert "### Research specialist" not in result["answer"]
    assert "### Analysis specialist" not in result["answer"]


def test_plan_drives_execution_order():
    """Verify that execution order matches the plan order, not canonical pipeline order."""
    # Canonical pipeline order is: research -> analysis -> code -> writing.
    # We specify reverse order: writing -> research.
    reverse_plan = [
        {"kind": "writing", "instruction": "Write outline first"},
        {"kind": "research", "instruction": "Research references second"},
    ]
    result = run_task("Plan with custom order", plan=reverse_plan)
    executed_kinds = [c["kind"] for c in result["completed"]]
    assert executed_kinds == ["writing", "research"]


def test_plan_with_repeated_specialists():
    """Verify that a plan with repeated specialists executes each subtask distinctly."""
    repeated_plan = [
        {"kind": "code", "instruction": "Step 1: Write backend logic"},
        {"kind": "code", "instruction": "Step 2: Write test assertions"},
    ]
    result = run_task("Multi-step coding task", plan=repeated_plan)
    assert len(result["completed"]) == 2
    assert result["completed"][0]["id"] == "s1"
    assert result["completed"][0]["kind"] == "code"
    assert result["completed"][1]["id"] == "s2"
    assert result["completed"][1]["kind"] == "code"


def test_unknown_specialist_normalizes_safely_during_execution():
    """Verify that an unknown specialist in the plan normalizes safely without crashing."""
    plan_with_unknown = [
        {"kind": "quantum_compiler", "instruction": "Compile quantum circuit"},
    ]
    result = run_task("Run quantum task", plan=plan_with_unknown)
    executed_kinds = [c["kind"] for c in result["completed"]]
    assert executed_kinds == ["analysis"]
    assert result["status"] == "completed"
    assert "### Analysis specialist" in result["answer"]


def test_llm_completion_drives_decomposition():
    """Verify that an LLM returning a valid structured plan drives specialist execution."""
    dynamic_plan_json = json.dumps(
        {
            "plan": [
                {"kind": "code", "instruction": "Implement algorithm"},
                {"kind": "writing", "instruction": "Document usage"},
            ]
        }
    )

    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(dynamic_plan_json, "mock", "mock-model"),
    ):
        result = run_task("Build and explain an algorithm")
        executed_kinds = [c["kind"] for c in result["completed"]]
        assert executed_kinds == ["code", "writing"]


def test_invalid_llm_output_falls_back_safely():
    """Verify that invalid or unparseable LLM output falls back to keyword planning."""
    # LLM returns plain text or invalid JSON
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion("Sorry, I cannot decompose this into JSON.", "mock", "mock-model"),
    ):
        # Task has 'research' and 'code' keywords
        result = run_task("Research the library and write code")
        executed_kinds = [c["kind"] for c in result["completed"]]
        assert "research" in executed_kinds
        assert "code" in executed_kinds
        assert result["status"] == "completed"


def test_llm_exception_falls_back_safely():
    """Verify that an exception in LLM completion falls back to keyword planning."""

    def side_effect(system="", prompt="", *args, **kwargs):
        if "supervisor" in system.lower():
            raise RuntimeError("LLM API timeout")
        return Completion(
            "This is a detailed mock output produced by the specialist that "
            "easily exceeds the length requirement for quality scoring.",
            "mock",
            "mock-model",
        )

    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        side_effect=side_effect,
    ):
        result = run_task("Write a draft report")
        executed_kinds = [c["kind"] for c in result["completed"]]
        assert executed_kinds == ["writing"]
        assert result["status"] == "completed"

    # Also test decompose() directly with unconditional exception
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        side_effect=RuntimeError("Decomposition error"),
    ):
        state = {"task": "Write a report", "retries": 0, "completed": [], "trace": []}
        update = decompose(state)
        assert [s["kind"] for s in update["plan"]] == ["writing"]


def test_decompose_state_preservation():
    """Verify supervisor.decompose preserves state fields and records trace."""
    state = {
        "task_id": "test-task-123",
        "task": "Research quantum computing",
        "retries": 1,
        "completed": [],
        "trace": [],
    }
    update = decompose(state)
    assert update["retries"] == 1
    assert len(update["trace"]) == 1
    assert update["trace"][0]["node"] == "supervisor"
    assert update["trace"][0]["event"] == "decompose"
    assert isinstance(update["plan"], list)


# -----------------------------------------------------------------------------
# Additional Task 12 Planning Quality & Validation Tests
# -----------------------------------------------------------------------------


def test_valid_llm_plan_accepted_and_dependencies_preserved():
    """A valid LLM plan is accepted and its dependencies are preserved during execution."""
    llm_plan_json = json.dumps(
        {
            "plan": [
                {
                    "id": "s1",
                    "kind": "research",
                    "instruction": "Gather security guidelines",
                    "depends_on": [],
                },
                {
                    "id": "s2",
                    "kind": "code",
                    "instruction": "Implement authentication based on guidelines",
                    "depends_on": ["s1"],
                },
            ]
        }
    )

    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(llm_plan_json, "mock", "mock-model"),
    ):
        result = run_task("Implement secure auth")
        assert result["status"] == "completed"
        completed = result["completed"]
        assert len(completed) == 2
        assert completed[0]["id"] == "s1"
        assert completed[0]["kind"] == "research"
        assert completed[1]["id"] == "s2"
        assert completed[1]["kind"] == "code"
        assert completed[1]["depends_on"] == ["s1"]


def test_malformed_or_empty_llm_output_falls_back_safely():
    """Malformed or empty LLM output falls back to keyword planning."""
    for malformed in ["", "   ", "not json at all", "{}", '{"random_key": 42}', '{"plan": []}']:
        with patch(
            "orchestrator.graph.supervisor.LLMClient.complete",
            return_value=Completion(malformed, "mock", "mock-model"),
        ):
            result = run_task("Research the market and analyze competitors")
            assert result["status"] in ("completed", "pending")
            executed_kinds = [c["kind"] for c in result["completed"]]
            assert "research" in executed_kinds
            assert "analysis" in executed_kinds


def test_unsupported_specialist_kinds_rejected_from_llm():
    """Unsupported specialist kinds in LLM plan trigger safe fallback to keyword planning."""
    hallucinated_plan = json.dumps(
        {
            "plan": [
                {"id": "s1", "kind": "quantum_compiler", "instruction": "Compile quantum code"},
            ]
        }
    )

    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(hallucinated_plan, "mock", "mock-model"),
    ):
        result = run_task("Research the new features")
        assert result["status"] == "completed"
        executed_kinds = [c["kind"] for c in result["completed"]]
        assert executed_kinds == ["research"]

    assert not is_supported_specialist("quantum_compiler")
    assert is_supported_specialist("code")
    assert is_supported_specialist("coder")

    is_valid, reason = validate_plan(
        [{"id": "s1", "kind": "quantum_compiler", "instruction": "do work"}],
        strict_specialists=True,
    )
    assert not is_valid
    assert "unsupported specialist kind" in reason


def test_duplicate_ids_and_missing_dependencies_rejected():
    """Duplicate IDs and missing dependencies are rejected and fall back safely."""
    # 1. Duplicate IDs
    duplicate_plan = json.dumps(
        {
            "plan": [
                {"id": "s1", "kind": "code", "instruction": "Step 1"},
                {"id": "s1", "kind": "writing", "instruction": "Step 2"},
            ]
        }
    )
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(duplicate_plan, "mock", "mock-model"),
    ):
        result = run_task("Write some code and draft docs")
        assert result["status"] == "completed"
        assert any(c["kind"] in ("code", "writing") for c in result["completed"])

    is_valid, reason = validate_plan(
        [
            {"id": "s1", "kind": "code", "instruction": "Step 1"},
            {"id": "s1", "kind": "writing", "instruction": "Step 2"},
        ]
    )
    assert not is_valid
    assert "Duplicate subtask ID" in reason

    # 2. Missing dependency
    missing_dep_plan = json.dumps(
        {
            "plan": [
                {"id": "s1", "kind": "code", "instruction": "Step 1", "depends_on": ["s999"]},
            ]
        }
    )
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(missing_dep_plan, "mock", "mock-model"),
    ):
        result = run_task("Write code")
        assert result["status"] == "completed"

    is_valid, reason = validate_plan(
        [{"id": "s1", "kind": "code", "instruction": "Step 1", "depends_on": ["s999"]}]
    )
    assert not is_valid
    assert "references non-existent dependency" in reason


def test_self_dependencies_and_dependency_cycles_rejected():
    """Self-dependencies and cyclic dependencies are rejected by validation."""
    # 1. Self-dependency
    self_dep_plan = json.dumps(
        {
            "plan": [
                {"id": "s1", "kind": "code", "instruction": "Self dep", "depends_on": ["s1"]},
            ]
        }
    )
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(self_dep_plan, "mock", "mock-model"),
    ):
        result = run_task("Write self-referential code")
        assert result["status"] == "completed"

    is_valid, reason = validate_plan(
        [{"id": "s1", "kind": "code", "instruction": "Self dep", "depends_on": ["s1"]}]
    )
    assert not is_valid
    assert "cannot depend on itself" in reason

    # 2. Cyclic dependency
    cyclic_plan = json.dumps(
        {
            "plan": [
                {"id": "s1", "kind": "research", "instruction": "Research", "depends_on": ["s2"]},
                {"id": "s2", "kind": "analysis", "instruction": "Analyze", "depends_on": ["s1"]},
            ]
        }
    )
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        return_value=Completion(cyclic_plan, "mock", "mock-model"),
    ):
        result = run_task("Research and analyze")
        assert result["status"] == "completed"

    is_valid, reason = validate_plan(
        [
            {"id": "s1", "kind": "research", "instruction": "Research", "depends_on": ["s2"]},
            {"id": "s2", "kind": "analysis", "instruction": "Analyze", "depends_on": ["s1"]},
        ]
    )
    assert not is_valid
    assert "Dependency cycle detected" in reason


def test_oversized_plans_bounded():
    """Oversized plans with more than MAX_PLAN_SIZE items are bounded safely."""
    oversized_items = [
        {"id": f"s{i + 1}", "kind": "code", "instruction": f"Implement step {i + 1}"}
        for i in range(15)
    ]
    parsed = parse_plan_payload(oversized_items, max_size=MAX_PLAN_SIZE)
    assert parsed is not None
    assert len(parsed) == MAX_PLAN_SIZE

    is_valid, reason = validate_plan(oversized_items, max_size=MAX_PLAN_SIZE)
    assert not is_valid
    assert "exceeds maximum allowed size" in reason


def test_invalid_fallback_plan_produces_controlled_failure():
    """When both LLM plan and keyword fallback plan are invalid, a controlled failure occurs."""
    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        side_effect=RuntimeError("LLM offline"),
    ):
        with patch("orchestrator.graph.supervisor.plan_subtasks", return_value=[]):
            result = run_task("Analyze financial data")
            assert result["status"] == "failed"
            assert "Plan validation failed" in result["answer"]
            assert len(result.get("failed", [])) == 1


def test_independent_subtasks_have_no_unnecessary_dependencies():
    """Fallback plan and independent subtasks do not have unnecessary dependencies."""
    plan = plan_subtasks("Research market trends, write report, and implement prototype")
    assert len(plan) == 3
    for s in plan:
        assert s.get("depends_on") == []


def test_planning_failure_preserves_cli_json_contract(capsys):
    """When planning fails, CLI --json still outputs strictly valid single JSON document."""
    from orchestrator.main import cli

    with patch(
        "orchestrator.graph.supervisor.LLMClient.complete",
        side_effect=RuntimeError("LLM offline"),
    ):
        with patch("orchestrator.graph.supervisor.plan_subtasks", return_value=[]):
            rc = cli(["Analyze financial data", "--json"])
            assert rc == 0
            captured = capsys.readouterr()
            raw = captured.out.strip()
            assert raw.startswith("{")
            assert raw.endswith("}")

            decoder = json.JSONDecoder()
            obj, idx = decoder.raw_decode(raw)
            assert raw[idx:].strip() == ""
            assert obj.get("status") == "failed"
            assert "Plan validation failed" in obj.get("answer", "")

