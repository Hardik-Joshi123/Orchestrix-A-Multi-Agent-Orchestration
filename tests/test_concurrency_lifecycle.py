"""Deterministic Concurrency & Lifecycle Race Safety Test Suite.

Validates concurrency safety, state isolation, and race-free lifecycle transitions:
- Distinct runs executing concurrently without state leakage
- Cancellation of one run not affecting another
- Duplicate start/resume requests raising RunConflictError
- Cancellation racing with successful completion
- Timeout racing with cancellation
- Late worker completion after cancellation
- Concurrent checkpoint writes and terminal-state preservation
- Cancellation registry cleanup after all terminal outcomes
- Resumed run receives fresh execution-control state without stale cancellation
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
from unittest.mock import patch

import pytest

from orchestrator.api.manager import RunConflictError, RunManager
from orchestrator.config import Settings
from orchestrator.graph.builder import resume_task, run_task
from orchestrator.graph.cancellation import (
    CancellationToken,
    get_cancellation_registry,
)
from orchestrator.llm import Completion
from orchestrator.persistence.store import CheckpointStore


@pytest.fixture(autouse=True)
def clean_registry():
    """Ensure cancellation registry is clean before and after each test."""
    reg = get_cancellation_registry()
    reg.clear()
    yield
    reg.clear()


@pytest.fixture
def tmp_store(tmp_path):
    """Provide a thread-safe isolated CheckpointStore backed by a temporary file."""
    db_file = tmp_path / "test_concurrency.db"
    return CheckpointStore(db_path=str(db_file), enabled=True)


# ---------------------------------------------------------------------------
# 1. Distinct runs executing concurrently without state leakage
# ---------------------------------------------------------------------------

def test_concurrent_distinct_runs_isolation(tmp_store, monkeypatch):
    """Two distinct runs execute simultaneously with overlapping specialists and zero cross-leakage."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    barrier = threading.Barrier(2, timeout=5.0)

    def mock_complete(system="", prompt="", *args, **kwargs):
        # Synchronize both runs inside specialist execution
        barrier.wait()
        if "Alpha" in prompt or "Alpha" in system:
            return Completion(
                "Alpha result computed successfully with comprehensive verification and thorough analysis.",
                "mock",
                "mock-1",
            )
        return Completion(
            "Beta result computed successfully with comprehensive verification and thorough analysis.",
            "mock",
            "mock-1",
        )

    plan_alpha = [{"id": "s1", "kind": "analysis", "instruction": "Analyze Alpha", "depends_on": []}]
    plan_beta = [{"id": "s1", "kind": "writing", "instruction": "Write Beta", "depends_on": []}]

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut_alpha = executor.submit(
                run_task,
                task="Task Alpha",
                plan=plan_alpha,
                run_id="run-alpha",
                store=tmp_store,
            )
            fut_beta = executor.submit(
                run_task,
                task="Task Beta",
                plan=plan_beta,
                run_id="run-beta",
                store=tmp_store,
            )

            res_alpha = fut_alpha.result(timeout=10.0)
            res_beta = fut_beta.result(timeout=10.0)

    # Verify run isolation
    assert res_alpha["run_id"] == "run-alpha"
    assert res_alpha["status"] == "completed"
    assert "Alpha result" in res_alpha["answer"]
    assert "Beta" not in res_alpha["answer"]

    assert res_beta["run_id"] == "run-beta"
    assert res_beta["status"] == "completed"
    assert "Beta result" in res_beta["answer"]
    assert "Alpha" not in res_beta["answer"]

    # Verify persistence isolation
    summary_alpha = tmp_store.get_run_summary("run-alpha")
    summary_beta = tmp_store.get_run_summary("run-beta")
    assert summary_alpha["status"] == "completed"
    assert summary_beta["status"] == "completed"
    assert summary_alpha["task"] == "Task Alpha"
    assert summary_beta["task"] == "Task Beta"


# ---------------------------------------------------------------------------
# 2. Cancellation of one run does not affect another
# ---------------------------------------------------------------------------

def test_concurrent_cancellation_isolation(tmp_store, monkeypatch):
    """Cancelling Run 1 does not affect concurrent Run 2, which completes successfully."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    manager = RunManager()
    started_event = threading.Event()
    cancel_signal = threading.Event()

    def mock_complete(system="", prompt="", *args, **kwargs):
        if "slow" in prompt.lower() or "slow" in system.lower():
            started_event.set()
            # Wait until cancellation is signaled for run-1
            cancel_signal.wait(timeout=5.0)
        return Completion(
            "Specialist output completed with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        # Start Run 1 (slow) in background
        resp_1 = manager.start_run(
            task="Slow task run-1",
            plan=[
                {"id": "s1", "kind": "analysis", "instruction": "Slow task", "depends_on": []},
                {"id": "s2", "kind": "writing", "instruction": "Next step", "depends_on": []},
            ],
            run_id="run-cancel-target",
            background=True,
            store=tmp_store,
        )
        assert resp_1.status == "running"

        # Wait until Run 1 starts executing
        assert started_event.wait(timeout=5.0)

        # Start Run 2 in background
        resp_2 = manager.start_run(
            task="Independent fast task run-2",
            plan=[{"id": "s1", "kind": "research", "instruction": "Fast task", "depends_on": []}],
            run_id="run-independent",
            background=True,
            store=tmp_store,
        )
        assert resp_2.status == "running"

        # Cancel Run 1 ONLY
        cancel_resp = manager.cancel_run("run-cancel-target", reason="Operator cancel")
        assert cancel_resp.status == "cancelled"

        # Release worker thread
        cancel_signal.set()

        # Wait for both runs to finalize
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline:
            if not manager.is_active("run-cancel-target") and not manager.is_active("run-independent"):
                break
            time.sleep(0.05)

    # Assert Run 1 is cancelled
    final_1 = manager.get_run("run-cancel-target", store=tmp_store)
    assert final_1.status == "cancelled"

    # Assert Run 2 completed successfully without interference
    final_2 = manager.get_run("run-independent", store=tmp_store)
    assert final_2.status == "completed"


# ---------------------------------------------------------------------------
# 3. Duplicate start/resume requests raising RunConflictError
# ---------------------------------------------------------------------------

def test_duplicate_start_run_conflict(tmp_store, monkeypatch):
    """Submitting two start requests with the same run ID raises RunConflictError."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    manager = RunManager()
    run_started = threading.Event()
    release_run = threading.Event()

    def mock_complete(*args, **kwargs):
        run_started.set()
        release_run.wait(timeout=5.0)
        return Completion(
            "Result completed with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        # 1. Start initial run in background
        manager.start_run(
            task="Initial run",
            run_id="duplicate-test-01",
            background=True,
            store=tmp_store,
        )
        assert run_started.wait(timeout=5.0)

        # 2. Concurrent duplicate start_run must raise RunConflictError
        with pytest.raises(RunConflictError) as exc_info:
            manager.start_run(
                task="Duplicate attempt",
                run_id="duplicate-test-01",
                background=True,
                store=tmp_store,
            )
        assert "already executing in-process" in str(exc_info.value)

        # Release worker
        release_run.set()

        # Wait for run to finish
        deadline = time.monotonic() + 5.0
        while manager.is_active("duplicate-test-01") and time.monotonic() < deadline:
            time.sleep(0.05)

        # 3. Once finished, another start_run with same ID raises RunConflictError due to terminal status
        with pytest.raises(RunConflictError) as exc_info_2:
            manager.start_run(
                task="New attempt on finished ID",
                run_id="duplicate-test-01",
                background=False,
                store=tmp_store,
            )
        assert "terminal status" in str(exc_info_2.value)


def test_duplicate_resume_run_conflict(tmp_store, monkeypatch):
    """Attempting to resume a run that is already actively running raises RunConflictError."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    # Seed an interrupted run with step 1 done, step 2 pending
    plan = [
        {
            "id": "s1",
            "kind": "analysis",
            "instruction": "Step 1",
            "status": "done",
            "result": "Done 1 completed with high quality, rigorous accuracy, and detailed results.",
        },
        {"id": "s2", "kind": "writing", "instruction": "Step 2", "status": "pending", "depends_on": ["s1"]},
    ]
    initial_state = {
        "run_id": "resume-dup-run",
        "task_id": "resume-dup-run",
        "task": "Test task",
        "plan": plan,
        "completed": [plan[0]],
        "status": "partial",
    }
    tmp_store.save_checkpoint("resume-dup-run", initial_state, transition="init")

    manager = RunManager()
    resume_started = threading.Event()
    resume_release = threading.Event()

    def mock_complete(*args, **kwargs):
        resume_started.set()
        resume_release.wait(timeout=5.0)
        return Completion(
            "Step 2 output completed with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        # Start resume in background
        manager.resume_run("resume-dup-run", background=True, store=tmp_store)
        assert resume_started.wait(timeout=5.0)

        # Duplicate resume while active raises RunConflictError
        with pytest.raises(RunConflictError) as exc_info:
            manager.resume_run("resume-dup-run", background=True, store=tmp_store)
        assert "already executing in-process" in str(exc_info.value)

        # Release resume worker
        resume_release.set()

        deadline = time.monotonic() + 5.0
        while manager.is_active("resume-dup-run") and time.monotonic() < deadline:
            time.sleep(0.05)


def test_engine_duplicate_run_conflict(tmp_store, monkeypatch):
    """Calling run_task directly with the same run_id from two threads raises ValueError."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    in_specialist = threading.Event()
    release_specialist = threading.Event()

    def mock_complete(*args, **kwargs):
        in_specialist.set()
        release_specialist.wait(timeout=5.0)
        return Completion(
            "Worker output completed with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            fut1 = executor.submit(run_task, task="Task 1", run_id="engine-dup-01", store=tmp_store)
            assert in_specialist.wait(timeout=5.0)

            # Thread 2 calls run_task with the exact same run_id
            with pytest.raises(ValueError) as exc_info:
                run_task(task="Task 2", run_id="engine-dup-01", store=tmp_store)
            assert "already executing concurrently" in str(exc_info.value)

            release_specialist.set()
            res1 = fut1.result(timeout=5.0)
            assert res1["status"] == "completed"


# ---------------------------------------------------------------------------
# 4. Cancellation racing with successful completion
# ---------------------------------------------------------------------------

def test_cancellation_racing_with_completion(tmp_store, monkeypatch):
    """When a run completes, concurrent cancel_run cannot overwrite completion and returns 409 Conflict."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    manager = RunManager()
    plan = [{"id": "s1", "kind": "analysis", "instruction": "Analyze data", "depends_on": []}]

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(
            "Final analysis complete with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        ),
    ):
        # Run synchronously to completion
        resp = manager.start_run(
            task="Completing workflow",
            plan=plan,
            run_id="run-complete-race",
            background=False,
            store=tmp_store,
        )
        assert resp.status == "completed"

        # Cancellation arrived after completion
        with pytest.raises(RunConflictError) as exc_info:
            manager.cancel_run("run-complete-race", store=tmp_store)
        assert "terminal status 'completed'" in str(exc_info.value)

        # Checkpoint store must preserve status 'completed'
        summary = tmp_store.get_run_summary("run-complete-race")
        assert summary["status"] == "completed"


# ---------------------------------------------------------------------------
# 5. Timeout racing with cancellation
# ---------------------------------------------------------------------------

def test_timeout_racing_with_cancellation(tmp_store, monkeypatch):
    """When cancellation and deadline occur concurrently, terminal status is deterministic."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    token = CancellationToken(run_id="race-cancel-timeout")
    token.cancel(reason="Explicit cancellation")

    plan = [
        {"id": "s1", "kind": "analysis", "instruction": "Subtask 1", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Subtask 2", "depends_on": []},
    ]

    # Pre-expired timeout + pre-cancelled token: token was cancelled first
    result = run_task(
        task="Race test",
        plan=plan,
        run_id="race-cancel-timeout",
        timeout=0.001,
        cancellation_token=token,
        store=tmp_store,
    )

    assert result["status"] == "cancelled"
    assert result.get("cancellation_reason") == "Explicit cancellation"

    # Verify persistent status
    summary = tmp_store.get_run_summary("race-cancel-timeout")
    assert summary["status"] == "cancelled"


# ---------------------------------------------------------------------------
# 6. Late worker completion after cancellation
# ---------------------------------------------------------------------------

def test_late_worker_completion_after_cancellation(tmp_store, monkeypatch):
    """Late worker finishing after cancellation cannot overwrite cancelled status."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    manager = RunManager()
    worker_started = threading.Event()
    worker_release = threading.Event()

    def mock_complete(*args, **kwargs):
        worker_started.set()
        worker_release.wait(timeout=5.0)
        return Completion(
            "Late completed output with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        )

    plan = [
        {"id": "s1", "kind": "analysis", "instruction": "In-flight subtask", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Next subtask", "depends_on": []},
    ]

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_complete):
        # 1. Start run in background
        manager.start_run(
            task="Late worker test",
            plan=plan,
            run_id="run-late-worker",
            background=True,
            store=tmp_store,
        )
        assert worker_started.wait(timeout=5.0)

        # 2. Cancel run while worker is executing
        cancel_resp = manager.cancel_run("run-late-worker", reason="User stopped task", store=tmp_store)
        assert cancel_resp.status == "cancelled"

        # 3. Allow worker to finish late
        worker_release.set()

        deadline = time.monotonic() + 5.0
        while manager.is_active("run-late-worker") and time.monotonic() < deadline:
            time.sleep(0.05)

    # 4. Verify terminal status in manager and store is cancelled, NOT completed
    final_resp = manager.get_run("run-late-worker", store=tmp_store)
    assert final_resp.status == "cancelled"

    summary = tmp_store.get_run_summary("run-late-worker")
    assert summary["status"] == "cancelled"


# ---------------------------------------------------------------------------
# 7. Concurrent checkpoint writes and terminal-state preservation
# ---------------------------------------------------------------------------

def test_concurrent_checkpoint_writes_safety(tmp_store):
    """Multiple threads writing checkpoints concurrently do not collide or corrupt terminal status."""
    run_id = "concurrent-writes-run"

    # Step 1: Save initial state
    tmp_store.save_checkpoint(
        run_id,
        {"run_id": run_id, "task": "Concurrent task", "status": "running"},
        transition="init",
    )

    num_threads = 10
    barrier = threading.Barrier(num_threads)
    errors: list[Exception] = []

    def writer(idx: int):
        try:
            barrier.wait(timeout=5.0)
            tmp_store.save_checkpoint(
                run_id,
                {"run_id": run_id, "task": "Concurrent task", "status": "running", "worker": idx},
                transition=f"subtask_{idx}",
            )
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10.0)

    assert not errors, f"Concurrent checkpoint write errors: {errors}"

    checkpoints = tmp_store.list_checkpoints(run_id)
    # 1 init + 10 worker checkpoints = 11 total checkpoints
    assert len(checkpoints) == 11

    # Verify step numbers are strictly monotonic: 1..11
    steps = [c.step for c in checkpoints]
    assert steps == list(range(1, 12))

    # Now verify terminal status preservation:
    # 1. Write terminal checkpoint 'completed'
    tmp_store.save_checkpoint(
        run_id,
        {"run_id": run_id, "task": "Concurrent task", "status": "completed"},
        transition="complete",
    )
    assert tmp_store.get_run_summary(run_id)["status"] == "completed"

    # 2. Simulate a late stale checkpoint update with status 'running'
    try:
        tmp_store.save_checkpoint(
            run_id,
            {"run_id": run_id, "task": "Concurrent task", "status": "running"},
            transition="stale_update",
        )
    except Exception:
        pass

    # Status in runs table MUST remain 'completed'
    assert tmp_store.get_run_summary(run_id)["status"] == "completed"


# ---------------------------------------------------------------------------
# 8. Cancellation registry cleanup after all terminal outcomes
# ---------------------------------------------------------------------------

def test_cancellation_registry_cleanup_after_outcomes(tmp_store, monkeypatch):
    """Registry unregisters tokens after normal completion, failure, cancellation, and timeout."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    reg = get_cancellation_registry()

    # Case A: Normal completion
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(
            "Done with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        ),
    ):
        run_task("Task normal", run_id="reg-clean-normal", store=tmp_store)
    assert reg.get("reg-clean-normal") is None

    # Case B: Execution failure
    def mock_fail(*args, **kwargs):
        raise RuntimeError("Fatal worker error")

    with patch("orchestrator.graph.specialists.LLMClient.complete", side_effect=mock_fail):
        run_task("Task fail", run_id="reg-clean-fail", store=tmp_store)
    assert reg.get("reg-clean-fail") is None

    # Case C: Pre-cancelled
    tok = CancellationToken(run_id="reg-clean-cancel")
    tok.cancel()
    run_task("Task cancel", run_id="reg-clean-cancel", cancellation_token=tok, store=tmp_store)
    assert reg.get("reg-clean-cancel") is None

    # Case D: Timeout
    tok_timeout = CancellationToken(run_id="reg-clean-timeout", deadline=time.monotonic() - 1.0)
    run_task(
        "Task timeout",
        run_id="reg-clean-timeout",
        timeout=0.001,
        cancellation_token=tok_timeout,
        store=tmp_store,
    )
    assert reg.get("reg-clean-timeout") is None


# ---------------------------------------------------------------------------
# 9. Resumed run receives fresh execution-control state
# ---------------------------------------------------------------------------

def test_resumed_run_gets_fresh_cancellation_token(tmp_store, monkeypatch):
    """Resuming an earlier cancelled run creates a fresh, uncancelled token and executes to completion."""
    monkeypatch.setattr(
        "orchestrator.config.get_settings",
        lambda: Settings(
            checkpoint_enabled=True,
            checkpoint_db_path=str(tmp_store.db_path),
            default_llm_provider="mock",
        ),
    )

    plan = [
        {"id": "s1", "kind": "analysis", "instruction": "Step 1", "depends_on": []},
        {"id": "s2", "kind": "writing", "instruction": "Step 2", "depends_on": ["s1"]},
    ]

    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(
            "Done 1 with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        ),
    ):
        # 1. Run step 1, but cancel before step 2
        tok = get_cancellation_registry().get_or_create("fresh-token-run")
        tok.cancel("Cancelled before step 2")

        res_cancelled = run_task(
            task="Two step task",
            plan=plan,
            run_id="fresh-token-run",
            cancellation_token=tok,
            store=tmp_store,
        )
        assert res_cancelled["status"] == "cancelled"

    # 2. Resume execution
    with patch(
        "orchestrator.graph.specialists.LLMClient.complete",
        return_value=Completion(
            "Done 2 with high quality, rigorous accuracy, and detailed results.",
            "mock",
            "mock-1",
        ),
    ):
        res_resumed = resume_task(
            run_id="fresh-token-run",
            store=tmp_store,
        )

    # 3. Resumed run must complete successfully without inheriting cancelled status
    assert res_resumed["status"] == "completed"
    assert "Done 2" in res_resumed["answer"]

    # Verify cancellation registry cleaned up
    assert get_cancellation_registry().get("fresh-token-run") is None
