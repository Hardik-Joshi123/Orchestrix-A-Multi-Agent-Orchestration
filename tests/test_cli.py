"""CLI smoke tests: the `orchestrator` entry point runs end-to-end on the mock."""

from __future__ import annotations

import json

from orchestrator.main import cli


def test_no_task_prints_help_and_returns_1(capsys):
    rc = cli([])
    assert rc == 1
    out = capsys.readouterr()
    assert "usage" in (out.out + out.err).lower()


def test_runs_task_in_text_mode(capsys):
    rc = cli(["Summarize the quarterly report"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "=== ANSWER ===" in out
    assert "=== REVIEW ===" in out


def test_json_mode_emits_valid_json(capsys):
    rc = cli(["Summarize the quarterly report", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload.get("answer")


def test_trace_flag_prints_trace(capsys):
    rc = cli(["Research vector DBs and write a summary", "--trace"])
    assert rc == 0
    assert "=== TRACE ===" in capsys.readouterr().out


def test_json_mode_stdout_strictly_valid_json_no_extra_content(capsys):
    rc = cli(["Summarize the quarterly report", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    raw = captured.out
    assert raw.startswith("{\n") or raw.startswith("{")
    assert raw.rstrip().endswith("}")
    # Verify exact single JSON document parse without any extra data
    decoder = json.JSONDecoder()
    obj, idx = decoder.raw_decode(raw.lstrip())
    assert raw.lstrip()[idx:].strip() == ""
    assert obj.get("status") in ("completed", "pending")
    # Verify diagnostic and section text is absent from stdout
    assert "provider=" not in raw
    assert "=== ANSWER ===" not in raw
    assert "=== REVIEW ===" not in raw


def test_json_mode_diagnostic_output_sent_to_stderr(capsys, monkeypatch):
    dummy_result = {
        "task_id": "test_id",
        "task": "Test task",
        "answer": "dummy answer",
        "status": "completed",
        "trace": [],
    }
    monkeypatch.setattr("orchestrator.main.run_task", lambda task: dummy_result)
    rc = cli(["Test task", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    # Diagnostics must be in stderr, never in stdout
    assert "provider=" in captured.err
    assert "task=" in captured.err
    assert "provider=" not in captured.out


def test_no_task_with_json_flag_leaves_stdout_clean(capsys):
    rc = cli(["--json"])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "usage" in captured.err.lower()


def test_controlled_error_empty_task_leaves_stdout_clean(capsys):
    rc = cli(["   ", "--json"])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error: task must not be empty" in captured.err.lower()


def test_controlled_error_task_too_long_leaves_stdout_clean(capsys, monkeypatch):
    from orchestrator.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "max_task_chars", 10)
    rc = cli(["This task is definitely longer than ten chars", "--json"])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "error: task exceeds maximum length" in captured.err.lower()


def test_json_mode_with_trace_flag_preserves_valid_json(capsys, monkeypatch):
    dummy_result = {
        "task_id": "test_id",
        "task": "Test task",
        "answer": "dummy answer",
        "status": "completed",
        "trace": [{"node": "supervisor", "event": "decompose"}],
    }
    monkeypatch.setattr("orchestrator.main.run_task", lambda task: dummy_result)
    rc = cli(["Test task", "--json", "--trace"])
    assert rc == 0
    captured = capsys.readouterr()
    # Output must still parse as single valid JSON object
    payload = json.loads(captured.out)
    assert isinstance(payload.get("trace"), list)
    assert "=== TRACE ===" not in captured.out


def test_subprocess_json_mode_end_to_end_no_span_pollution():
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    p = subprocess.run(
        [sys.executable, "-m", "orchestrator.main", "Summarize the quarterly report", "--json"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert p.returncode == 0
    # Must parse cleanly without JSONDecodeError: Extra data
    payload = json.loads(p.stdout)
    assert payload.get("status") in ("completed", "pending")
    assert payload.get("answer")
    assert "provider=" not in p.stdout
    # Spans and diagnostic messages must have gone to stderr
    assert "provider=" in p.stderr
