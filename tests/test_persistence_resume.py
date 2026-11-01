"""Deterministic tests for Task 16: Persistent Workflow State and Resume Support."""

from __future__ import annotations

import json
import sqlite3

import pytest

from orchestrator.config import Settings
from orchestrator.graph.builder import _run_fallback, resume_task, run_task
from orchestrator.graph.state import GraphState, Subtask
from orchestrator.hitl.queue import ReviewItem, ReviewQueue, apply_resolution
from orchestrator.main import cli
from orchestrator.persistence.store import (
    SCHEMA_VERSION,
    CheckpointCorruptError,
    CheckpointIncompatibleError,
    CheckpointNotFoundError,
    CheckpointStore,
    sanitize_state_for_persistence,
)


@pytest.fixture
def temp_db(tmp_path):
    """Provide a path to a temporary SQLite database file."""
    return tmp_path / "test_checkpoints.db"


@pytest.fixture
def store(temp_db):
    """Provide an initialized CheckpointStore instance with a temp file database."""
    return CheckpointStore(db_path=temp_db)


# ============================================================================
# 1. Store CRUD & Queries
# ============================================================================


def test_checkpoint_store_basic_crud(store, temp_db):
    state1: GraphState = {
        "run_id": "run-101",
        "task_id": "run-101",
        "task": "Build feature X",
        "status": "pending",
        "plan": [{"id": "s1", "kind": "code", "instruction": "Write code", "status": "pending"}],
        "completed": [],
    }

    step1 = store.save_checkpoint("run-101", state1, transition="init")
    assert step1 == 1

    state2: GraphState = {
        **state1,
        "completed": [
            {
                "id": "s1",
                "kind": "code",
                "instruction": "Write code",
                "status": "done",
                "result": "code ready",
            }
        ],
        "status": "completed",
    }
    step2 = store.save_checkpoint("run-101", state2, transition="subtask_complete")
    assert step2 == 2

    # Load latest checkpoint
    latest = store.load_latest_checkpoint("run-101")
    assert latest.run_id == "run-101"
    assert latest.step == 2
    assert latest.transition == "subtask_complete"
    assert latest.state["status"] == "completed"
    assert latest.schema_version == SCHEMA_VERSION

    # Load specific step
    s1_rec = store.load_checkpoint_step("run-101", 1)
    assert s1_rec.step == 1
    assert s1_rec.transition == "init"
    assert s1_rec.state["status"] == "pending"

    # List checkpoints
    all_ckpts = store.list_checkpoints("run-101")
    assert len(all_ckpts) == 2
    assert [c.step for c in all_ckpts] == [1, 2]

    # Run summary
    summary = store.get_run_summary("run-101")
    assert summary is not None
    assert summary["run_id"] == "run-101"
    assert summary["task"] == "Build feature X"
    assert summary["status"] == "completed"
    assert summary["latest_step"] == 2

    # List runs
    runs = store.list_runs()
    assert len(runs) == 1
    assert runs[0]["run_id"] == "run-101"


# ============================================================================
# 2. Schema Versioning & Compatibility
# ============================================================================


def test_incompatible_schema_version_raises(store, temp_db):
    state: GraphState = {"run_id": "run-v2", "task": "Future task", "status": "pending"}
    store.save_checkpoint("run-v2", state, transition="init")

    # Manually tamper with schema_version to simulate future version
    conn = sqlite3.connect(temp_db)
    with conn:
        conn.execute("UPDATE checkpoints SET schema_version = 999 WHERE run_id = 'run-v2'")
    conn.close()

    with pytest.raises(CheckpointIncompatibleError) as exc_info:
        store.load_latest_checkpoint("run-v2")
    assert "newer than supported version" in str(exc_info.value)


# ============================================================================
# 3. Corrupt Data Handling
# ============================================================================


def test_corrupt_checkpoint_data_raises(store, temp_db):
    state: GraphState = {"run_id": "run-corrupt", "task": "Crash task", "status": "pending"}
    store.save_checkpoint("run-corrupt", state, transition="init")

    # Tamper with JSON payload
    conn = sqlite3.connect(temp_db)
    with conn:
        conn.execute("UPDATE checkpoints SET state_json = '{invalid-json: ' WHERE run_id = 'run-corrupt'")
    conn.close()

    with pytest.raises(CheckpointCorruptError) as exc_info:
        store.load_latest_checkpoint("run-corrupt")
    assert "Corrupt checkpoint data" in str(exc_info.value)


# ============================================================================
# 4. Missing Checkpoint Handling
# ============================================================================


def test_checkpoint_not_found_raises(store):
    with pytest.raises(CheckpointNotFoundError) as exc_info:
        store.load_latest_checkpoint("non_existent_run_id")
    assert "No checkpoint found" in str(exc_info.value)

    with pytest.raises(CheckpointNotFoundError):
        store.load_latest_checkpoint("")


# ============================================================================
# 5. Secret Sanitization in Persistence
# ============================================================================


def test_secret_sanitization_for_persistence():
    dirty_state = {
        "api_key": "secret-12345",
        "nested": {
            "password": "my_password",
            "openai_secret": "sk-1234567890abcdef123456",
            "auth_header": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.xyz",
            "regular": "safe content",
        },
        "list_items": [
            {"token": "token-abc"},
            "Look at password=supersecret in this string",
        ],
    }

    clean = sanitize_state_for_persistence(dirty_state)
    assert clean["api_key"] == "[REDACTED]"
    assert clean["nested"]["password"] == "[REDACTED]"
    assert clean["nested"]["openai_secret"] == "[REDACTED]"
    assert clean["nested"]["auth_header"] == "[REDACTED]"
    assert clean["nested"]["regular"] == "safe content"
    assert clean["list_items"][0]["token"] == "[REDACTED]"
    assert "[REDACTED]" in clean["list_items"][1]


# ============================================================================
# 6. Safe Boundary Writes Recorded in Workflow Run
# ============================================================================


def test_boundary_writes_recorded_during_workflow(store, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    plan: list[Subtask] = [
        {
            "id": "s1",
            "kind": "analysis",
            "instruction": "Perform comprehensive analysis of quarterly operational metrics and system logs",
            "status": "pending",
            "depends_on": [],
        },
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Draft a thorough summary report detailing findings and actionable next steps",
            "status": "pending",
            "depends_on": ["s1"],
        },
    ]

    result = run_task("Analyze data and write report", plan=plan, run_id="run-boundary-test")
    assert result["status"] == "completed"

    records = store.list_checkpoints("run-boundary-test")
    transitions = [r.transition for r in records]

    # Verify key boundary transitions were captured
    assert "init" in transitions
    assert "plan" in transitions
    assert "subtask_complete" in transitions
    assert "reduce" in transitions

    # Steps must be sequential
    steps = [r.step for r in records]
    assert steps == list(range(1, len(records) + 1))


# ============================================================================
# 7. Resume Bypasses Completed Subtasks (status == "done")
# ============================================================================


def test_resume_bypasses_completed_subtasks(store, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    plan: list[Subtask] = [
        {"id": "s1", "kind": "research", "instruction": "Gather facts", "status": "pending"},
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Write draft",
            "status": "pending",
            "depends_on": ["s1"],
        },
    ]

    # Simulate an interrupted run where s1 already completed but s2 never started
    s1_done: Subtask = {
        "id": "s1",
        "kind": "research",
        "instruction": "Gather facts",
        "status": "done",
        "result": "Facts gathered in phase 1 with comprehensive evidence and thorough documentation",
        "confidence": 0.95,
        "tools_used": ["web_search"],
    }

    initial_interrupted_state: GraphState = {
        "run_id": "run-resume-bypass",
        "task_id": "run-resume-bypass",
        "task": "Research and write draft",
        "plan": plan,
        "completed": [s1_done],
        "failed": [],
        "blocked": [],
        "tools_used": ["web_search"],
        "retries": 0,
        "status": "pending",
        "trace": [],
    }

    store.save_checkpoint("run-resume-bypass", initial_interrupted_state, transition="subtask_complete")

    # Track specialist invocations to verify s1 is never re-run
    invoked_subtasks = []

    def mock_invoke_specialist(kind, state, subtask):
        invoked_subtasks.append(subtask["id"])
        return {
            "completed": [{
                **subtask,
                "status": "done",
                "result": f"Executed {subtask['id']} with comprehensive documentation and verified results",
                "confidence": 0.95,
            }],
            "tools_used": [],
            "trace": [],
        }

    monkeypatch.setattr("orchestrator.graph.builder._invoke_specialist", mock_invoke_specialist)

    # Resume the task
    resumed_result = resume_task("run-resume-bypass", store=store)

    assert resumed_result["status"] == "completed"
    # s1 must NOT have been invoked; only s2 should execute
    assert "s1" not in invoked_subtasks
    assert "s2" in invoked_subtasks

    # Result answer should contain both s1 and s2 results
    assert "Facts gathered in phase 1" in resumed_result["answer"]
    assert "Executed s2" in resumed_result["answer"]


# ============================================================================
# 8. Resume Already Completed Task Returns Cached State
# ============================================================================


def test_resume_already_completed_task(store):
    completed_state: GraphState = {
        "run_id": "run-already-done",
        "task_id": "run-already-done",
        "task": "Completed task",
        "plan": [
            {"id": "s1", "kind": "analysis", "instruction": "Do it", "status": "pending", "depends_on": []}
        ],
        "completed": [
            {"id": "s1", "kind": "analysis", "instruction": "Do it", "status": "done", "result": "All good"}
        ],
        "status": "completed",
        "answer": "All good",
        "trace": [],
    }
    store.save_checkpoint("run-already-done", completed_state, transition="reduce")

    res = resume_task("run-already-done", store=store)
    assert res["status"] == "completed"
    assert res["answer"] == "All good"


# ============================================================================
# 9. Resume After HITL Resolution Unblocks Dependent Work
# ============================================================================


def test_resume_after_hitl_resolution_unblocks_dependent_subtasks(store, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    plan: list[Subtask] = [
        {"id": "s1", "kind": "code", "instruction": "Critical code", "status": "pending", "depends_on": []},
        {
            "id": "s2",
            "kind": "writing",
            "instruction": "Summarize verified code with thorough documentation and analysis",
            "status": "pending",
            "depends_on": ["s1"],
        },
    ]

    # Simulate s1 escalating to HITL
    queue = ReviewQueue()
    item = ReviewItem(
        task_id="run-hitl-resume",
        task="Critical code and summary",
        reason="low_confidence",
        level="high",
        proposed_action="Uncertain code",
        confidence=0.3,
        quality=0.3,
        subtask_id="s1",
    )
    queue.push(item)

    escalated_state: GraphState = {
        "run_id": "run-hitl-resume",
        "task_id": "run-hitl-resume",
        "task": "Critical code and summary",
        "plan": plan,
        "completed": [{
            "id": "s1",
            "kind": "code",
            "instruction": "Critical code",
            "status": "pending",
            "result": "Uncertain code",
            "confidence": 0.3,
        }],
        "failed": [],
        "blocked": [{
            "id": "s2",
            "kind": "writing",
            "instruction": "Summarize code",
            "status": "blocked",
            "result": "",
            "confidence": 0.0,
            "error": "Upstream subtask 's1' was escalated",
        }],
        "escalated": True,
        "escalation_reason": "low_confidence",
        "escalated_subtask_id": "s1",
        "status": "pending",
        "trace": [],
    }
    store.save_checkpoint("run-hitl-resume", escalated_state, transition="escalate")

    # Reviewer approves/edits subtask 1
    resolution = queue.resolve(
        identifier=item.item_id,
        decision="approve",
        approver="senior_engineer",
        note="Approved safe code",
    )
    assert resolution is not None
    resolved_state = apply_resolution(escalated_state, resolution)
    assert resolved_state["escalated"] is False

    # Resume the workflow
    resumed = resume_task("run-hitl-resume", store=store)

    # Subtask 2 should have been unblocked and executed successfully
    done_ids = {c["id"] for c in resumed["completed"] if c.get("status") == "done"}
    assert "s1" in done_ids
    assert "s2" in done_ids
    assert resumed["status"] in ("completed", "approved")


# ============================================================================
# 10. Trace Correlation and Resume Event
# ============================================================================


def test_resume_trace_correlation(store, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    plan: list[Subtask] = [
        {
            "id": "s1",
            "kind": "analysis",
            "instruction": "Perform thorough analysis on system observability trace correlation",
            "status": "pending",
            "depends_on": [],
        },
    ]

    interrupted_state: GraphState = {
        "run_id": "run-trace-correlate",
        "task_id": "run-trace-correlate",
        "task": "Test trace correlation",
        "plan": plan,
        "completed": [],
        "status": "pending",
        "trace": [{"node": "workflow", "event": "start", "run_id": "run-trace-correlate"}],
    }
    store.save_checkpoint("run-trace-correlate", interrupted_state, transition="init")

    res = resume_task("run-trace-correlate", store=store)

    events = res.get("trace", [])
    resume_events = [e for e in events if e.get("event") == "resume"]
    assert len(resume_events) == 1
    assert resume_events[0]["run_id"] == "run-trace-correlate"
    # All trace events must carry the same run_id
    for evt in events:
        if "run_id" in evt:
            assert evt["run_id"] == "run-trace-correlate"


# ============================================================================
# 11. Concurrency and Synchronized Writes
# ============================================================================


def test_concurrency_and_synchronized_writes(store, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    # 4 independent subtasks running concurrently
    plan: list[Subtask] = [
        {
            "id": f"s{i}",
            "kind": "analysis",
            "instruction": f"Parallel job {i}",
            "status": "pending",
            "depends_on": [],
        }
        for i in range(1, 5)
    ]

    state: GraphState = {
        "run_id": "run-concurrent-ckpts",
        "task_id": "run-concurrent-ckpts",
        "task": "Parallel tasks",
        "plan": plan,
        "completed": [],
        "failed": [],
        "blocked": [],
        "tools_used": [],
        "retries": 0,
        "trace": [],
    }

    result = _run_fallback(state, max_concurrency=4, store=store)
    assert result["status"] == "completed"

    records = store.list_checkpoints("run-concurrent-ckpts")
    complete_events = [r for r in records if r.transition == "subtask_complete"]
    assert len(complete_events) == 4


# ============================================================================
# 12. CLI Status and Resume Options
# ============================================================================


def test_cli_status_and_resume_options(store, capsys, monkeypatch):
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(checkpoint_enabled=True, checkpoint_db_path=str(store.db_path)),
    )

    # 1. Run task and capture output
    rc = cli(["Analyze tech stack", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    run_id = payload["run_id"]
    assert run_id

    # 2. Query status via CLI in text mode
    rc_status = cli(["--status", run_id])
    assert rc_status == 0
    text_out = capsys.readouterr().out
    assert "=== STATUS:" in text_out
    assert run_id in text_out

    # 3. Query status via CLI in JSON mode
    rc_status_json = cli(["--status", run_id, "--json"])
    assert rc_status_json == 0
    json_status = json.loads(capsys.readouterr().out)
    assert json_status["run_id"] == run_id
    assert json_status["status"] == "completed"

    # 4. Resume via CLI
    rc_resume = cli(["--resume", run_id, "--json"])
    assert rc_resume == 0
    resumed_payload = json.loads(capsys.readouterr().out)
    assert resumed_payload["run_id"] == run_id

    # 5. Non-existent status query returns 1
    rc_err = cli(["--status", "missing-run-id"])
    assert rc_err == 1
    err_out = capsys.readouterr().err
    assert "No run found" in err_out
