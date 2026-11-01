"""Smoke tests for package setup, CLI entry points, environment templates, and offline readiness."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest
import ui.api_client as api_client_mod
import ui.dashboard_helpers as dashboard_helpers_mod
from fastapi.testclient import TestClient

import orchestrator
from orchestrator.api.server import create_app
from orchestrator.config import get_settings
from orchestrator.llm import LLMClient
from orchestrator.main import cli

_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_package_metadata_and_version():
    """Verify package version is defined and follows semantic versioning."""
    version = getattr(orchestrator, "__version__", None)
    assert version is not None, "orchestrator.__version__ must be defined"
    assert re.match(r"^\d+\.\d+\.\d+", version), f"Invalid version string: {version}"


def test_cli_help_smoke(capsys):
    """Verify CLI --help displays usage and exits cleanly with 0."""
    with pytest.raises(SystemExit) as exc:
        cli(["--help"])
    assert exc.value.code == 0
    captured = capsys.readouterr()
    assert "Run a task through the multi-agent orchestrator" in captured.out
    assert "--diagnostics" in captured.out
    assert "--serve" in captured.out
    assert "--eval" in captured.out


def test_cli_diagnostics_smoke(capsys):
    """Verify CLI --diagnostics outputs startup summary without leaking secrets."""
    # Standard text diagnostics emit to stderr to keep stdout clean
    rc = cli(["--diagnostics"])
    assert rc == 0
    captured = capsys.readouterr()
    err_out = captured.err
    assert "Multi-Agent Orchestrator Configuration Diagnostics" in err_out
    assert "LLM Provider:" in err_out
    assert "Concurrency Limit:" in err_out
    assert "Persistence:" in err_out
    assert "API Server:" in err_out

    # JSON diagnostics emit to stdout for machine parsability
    rc_json = cli(["--diagnostics", "--json"])
    assert rc_json == 0
    captured_json = capsys.readouterr()
    assert '"default_provider": "mock"' in captured_json.out
    assert '"version": "0.1.0"' in captured_json.out


def test_python_module_invocation_smoke():
    """Verify python -m orchestrator --diagnostics executes successfully."""
    proc = subprocess.run(
        [sys.executable, "-m", "orchestrator", "--diagnostics"],
        cwd=str(_PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    output = proc.stdout + proc.stderr
    assert "Multi-Agent Orchestrator Configuration Diagnostics" in output


def test_env_example_template_validity():
    """Verify .env.example exists, documents critical variables, and contains no real secrets."""
    env_example = _PROJECT_ROOT / ".env.example"
    assert env_example.exists(), ".env.example must exist in repository root"
    content = env_example.read_text(encoding="utf-8")

    # Critical variables must be documented
    required_keys = [
        "DEFAULT_LLM_PROVIDER",
        "FALLBACK_LLM_PROVIDER",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "MAX_SPECIALIST_CONCURRENCY",
        "CHECKPOINT_ENABLED",
        "CHECKPOINT_DB_PATH",
        "CONFIDENCE_THRESHOLD",
        "QUALITY_THRESHOLD",
        "REVIEW_AUTH_ENABLED",
        "REVIEW_USERS",
        "API_HOST",
        "API_PORT",
        "ORCHESTRATOR_API_TOKEN",
        "API_MAX_BODY_BYTES",
        "API_RATE_LIMIT_ENABLED",
        "REDIS_URL",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
    ]
    for key in required_keys:
        assert f"{key}=" in content, f".env.example must document {key}"

    # Verify no real secrets or live tokens are committed
    lines = content.splitlines()
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("OPENAI_API_KEY="):
            val = stripped.split("=", 1)[1].strip()
            assert val == "", f"OPENAI_API_KEY must be empty in template, found: {val}"
        elif stripped.startswith("ANTHROPIC_API_KEY="):
            val = stripped.split("=", 1)[1].strip()
            assert val == "", f"ANTHROPIC_API_KEY must be empty in template, found: {val}"
        elif stripped.startswith("ORCHESTRATOR_API_TOKEN="):
            val = stripped.split("=", 1)[1].strip()
            assert val == "", f"ORCHESTRATOR_API_TOKEN must be empty in template, found: {val}"


def test_api_app_routes_and_offline_health():
    """Verify FastAPI application instantiates offline and serves /health route."""
    app = create_app()
    route_paths = set()
    for route in app.routes:
        if hasattr(route, "path"):
            route_paths.add(route.path)
        elif hasattr(route, "original_router"):
            for sub_route in getattr(route.original_router, "routes", []):
                if hasattr(sub_route, "path"):
                    route_paths.add(sub_route.path)

    expected_routes = {
        "/health",
        "/runs",
        "/runs/{run_id}",
        "/runs/{run_id}/resume",
        "/runs/{run_id}/checkpoints",
        "/evaluation",
    }
    for expected in expected_routes:
        assert expected in route_paths, f"Expected route {expected} not found in API routes"

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert "version" in data
    assert data["checkpoint_backend"] == "sqlite"


def test_dashboard_modules_importable_and_functional():
    """Verify UI helper and API client modules are importable and execute basic utilities."""
    badge = dashboard_helpers_mod.format_status_badge("completed")
    assert "Completed" in str(badge)

    classified = dashboard_helpers_mod.classify_subtasks(
        {"completed_subtasks": ["s1"], "failed_subtasks": [], "blocked_subtasks": []}
    )
    assert classified["completed"] == ["s1"]

    client = api_client_mod.OrchestratorApiClient("http://127.0.0.1:8000")
    assert client.base_url == "http://127.0.0.1:8000"


def test_zero_api_key_graceful_offline_fallback():
    """Verify graph and LLM components degrade gracefully to mock provider without credentials."""
    settings = get_settings()
    assert settings.default_llm_provider in ("mock", "openai", "anthropic")

    llm = LLMClient()
    out = llm.complete(system="System instructions", prompt="Perform offline analysis")
    assert out.provider == "mock"
    assert len(out.text) > 0
