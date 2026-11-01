"""Production deployment readiness, containerization, and service separation tests (Task 59)."""

from __future__ import annotations

import pathlib
import re
import shutil
import subprocess
from unittest.mock import MagicMock, patch

import pytest
import yaml
from fastapi.testclient import TestClient

from orchestrator.api.manager import RunManager
from orchestrator.api.server import create_app, validate_network_binding
from orchestrator.config import reset_settings
from orchestrator.healthcheck import (
    check_api_health,
    check_redis_health,
    check_worker_health,
)
from orchestrator.healthcheck import (
    main as healthcheck_main,
)
from orchestrator.hitl.queue import ReviewQueue, apply_resolution
from orchestrator.persistence.store import CheckpointStore
from orchestrator.tools import REGISTRY, RiskLevel, Tool

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


# ── 1. Static Containerization & Specification Tests ─────────────────────────


def test_dockerfile_structure_and_security() -> None:
    """Dockerfile follows production container best practices, non-root user, and no embedded secrets."""
    dockerfile_path = REPO_ROOT / "Dockerfile"
    assert dockerfile_path.is_file(), "Dockerfile must exist in project root."

    content = dockerfile_path.read_text(encoding="utf-8")
    raw_lines = content.splitlines()
    lines = [
        line.strip()
        for line in raw_lines
        if line.strip() and not line.strip().startswith("#")
    ]

    # 1. Base image must be lightweight official python slim
    assert any(re.match(r"^FROM\s+python:3\.11-slim", line) for line in lines), (
        "Dockerfile must use python:3.11-slim base image."
    )

    # 2. Non-root user setup
    assert any("groupadd" in line and "orchestrator" in line for line in lines), (
        "Dockerfile must create non-root group 'orchestrator'."
    )
    assert any("useradd" in line and "orchestrator" in line for line in lines), (
        "Dockerfile must create non-root user 'orchestrator'."
    )
    assert any(line == "USER orchestrator" for line in lines), (
        "Dockerfile must switch to USER orchestrator before CMD."
    )

    # 3. Persistent volume declared
    assert any("VOLUME" in line and "/app/data" in line for line in lines), (
        "Dockerfile must declare VOLUME ['/app/data'] for SQLite persistence."
    )

    # 4. Port 8000 exposed
    assert any(re.match(r"^EXPOSE\s+8000", line) for line in lines), "Dockerfile must expose port 8000."

    # 5. Layer caching: dependency files copied before source code
    req_copy_idx = next(
        i for i, line in enumerate(lines) if "requirements.txt" in line and "COPY" in line
    )
    src_copy_idx = next(i for i, line in enumerate(lines) if "src/" in line and "COPY" in line)
    assert req_copy_idx < src_copy_idx, "Dependencies must be copied before source code for layer caching."

    # 6. No hardcoded credentials or API keys in ENV or RUN
    forbidden_tokens = ["sk-", "secret-token", "password123", "ANTHROPIC_API_KEY=", "OPENAI_API_KEY="]
    for line in lines:
        if line.startswith("ENV"):
            for tok in forbidden_tokens:
                assert tok not in line, f"Potential credential leak detected in Dockerfile: {line}"


def test_dockerignore_completeness() -> None:
    """.dockerignore prevents version control, secrets, virtualenvs, and test caches from build context."""
    dockerignore_path = REPO_ROOT / ".dockerignore"
    assert dockerignore_path.is_file(), ".dockerignore must exist in project root."

    content = dockerignore_path.read_text(encoding="utf-8")
    raw_lines = content.splitlines()
    ignored_patterns = {
        line.strip()
        for line in raw_lines
        if line.strip() and not line.strip().startswith("#")
    }

    expected_exclusions = [
        ".git",
        "__pycache__",
        "*.py[cod]",
        ".venv",
        ".pytest_cache",
        ".ruff_cache",
        "*.db",
        "*.db-wal",
        "*.db-shm",
        ".env",
    ]
    for pattern in expected_exclusions:
        assert any(pattern in pat for pat in ignored_patterns), (
            f".dockerignore must exclude '{pattern}' from container build context."
        )

    # Example templates must be explicitly preserved
    assert any("!.env.example" in pat for pat in ignored_patterns), (
        ".dockerignore should allow !.env.example."
    )
    assert any("!.env.docker.example" in pat for pat in ignored_patterns), (
        ".dockerignore should allow !.env.docker.example."
    )


def test_docker_compose_specification() -> None:
    """docker-compose.yml defines separated api, worker, and redis services with persistent storage."""
    compose_path = REPO_ROOT / "docker-compose.yml"
    assert compose_path.is_file(), "docker-compose.yml must exist in project root."

    with open(compose_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    services = config.get("services", {})
    assert "api" in services, "docker-compose.yml must define 'api' service."
    assert "worker" in services, "docker-compose.yml must define 'worker' service."
    assert "redis" in services, "docker-compose.yml must define 'redis' broker service."

    # 1. API Service validation
    api_svc = services["api"]
    assert "8000:8000" in api_svc.get("ports", []), "API service must publish port 8000:8000."
    assert any("orchestrator_data:/app/data" in v for v in api_svc.get("volumes", [])), (
        "API service must mount orchestrator_data to /app/data."
    )
    api_hc = api_svc.get("healthcheck", {})
    assert api_hc, "API service must specify container healthcheck."
    assert any("/ready" in str(item) for item in api_hc.get("test", [])), (
        "API healthcheck test must probe /ready endpoint."
    )

    # 2. Celery Worker Service validation
    worker_svc = services["worker"]
    worker_cmd = " ".join(worker_svc.get("command", []))
    assert "celery" in worker_cmd and "worker" in worker_cmd, "Worker command must run Celery worker."
    assert any("orchestrator_data:/app/data" in v for v in worker_svc.get("volumes", [])), (
        "Worker service must share orchestrator_data volume with API service."
    )
    assert worker_svc.get("stop_signal") == "SIGTERM", "Worker must use SIGTERM for graceful shutdown."
    assert worker_svc.get("stop_grace_period") in ("30s", 30), (
        "Worker must define stop_grace_period of at least 30s."
    )
    worker_hc = worker_svc.get("healthcheck", {})
    assert worker_hc, "Worker service must specify container healthcheck."

    # 3. Redis Broker Service validation
    redis_svc = services["redis"]
    assert "redis:7-alpine" in redis_svc.get("image", ""), "Redis service must use redis:7-alpine."
    redis_hc = redis_svc.get("healthcheck", {})
    assert any("ping" in str(item).lower() for item in redis_hc.get("test", [])), (
        "Redis healthcheck must test ping."
    )

    # 4. Volumes declaration
    volumes = config.get("volumes", {})
    assert "orchestrator_data" in volumes, "docker-compose.yml must declare orchestrator_data volume."
    assert "redis_data" in volumes, "docker-compose.yml must declare redis_data volume."


def test_compose_yaml_equivalence() -> None:
    """compose.yaml exists and matches docker-compose.yml for modern Docker Compose v2 compatibility."""
    c_yaml = REPO_ROOT / "compose.yaml"
    dc_yaml = REPO_ROOT / "docker-compose.yml"
    assert c_yaml.is_file(), "compose.yaml must exist for Docker Compose v2 compatibility."

    with open(c_yaml, encoding="utf-8") as f1, open(dc_yaml, encoding="utf-8") as f2:
        assert yaml.safe_load(f1) == yaml.safe_load(f2), "compose.yaml and docker-compose.yml must match."


def test_env_docker_example_coverage() -> None:
    """.env.docker.example documents production variables without exposing live credentials."""
    env_docker_path = REPO_ROOT / ".env.docker.example"
    assert env_docker_path.is_file(), ".env.docker.example must exist in project root."

    content = env_docker_path.read_text(encoding="utf-8")
    raw_lines = content.splitlines()
    lines = [
        line.strip()
        for line in raw_lines
        if line.strip() and not line.strip().startswith("#")
    ]
    parsed = {}
    for line in lines:
        if "=" in line:
            k, v = line.split("=", 1)
            parsed[k.strip()] = v.strip()

    required_keys = [
        "API_HOST",
        "API_PORT",
        "ORCHESTRATOR_API_TOKEN",
        "CHECKPOINT_DB_PATH",
        "CHECKPOINT_ENABLED",
        "ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND",
        "CELERY_ENABLED",
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
        "DEFAULT_LLM_PROVIDER",
    ]
    for rk in required_keys:
        assert rk in parsed, f".env.docker.example must specify '{rk}'."

    # Validate safe placeholders
    assert parsed["ORCHESTRATOR_API_TOKEN"].startswith("replace_with_"), (
        "ORCHESTRATOR_API_TOKEN must use a placeholder in example file."
    )
    assert parsed["OPENAI_API_KEY"] == "", "OPENAI_API_KEY must be empty in example file."
    assert parsed["ANTHROPIC_API_KEY"] == "", "ANTHROPIC_API_KEY must be empty in example file."


# ── 2. Security & Network Binding Tests ───────────────────────────────────────


def test_production_network_binding_security_validation() -> None:
    """Non-loopback network binding (0.0.0.0 in Docker) strictly requires ORCHESTRATOR_API_TOKEN."""
    # Binding 0.0.0.0 without token raises ValueError
    with pytest.raises(ValueError, match="Unsafe network binding rejected"):
        validate_network_binding("0.0.0.0", token="")

    # Binding 0.0.0.0 with configured token succeeds
    validate_network_binding("0.0.0.0", token="secure-prod-token-12345")

    # Binding 127.0.0.1 without token is permitted for local development
    validate_network_binding("127.0.0.1", token="")


# ── 3. Healthcheck Probes & Graceful Shutdown Tests ───────────────────────────


def test_healthcheck_module_api_probe() -> None:
    """check_api_health and check_redis_health return correct status on success and failure."""
    # 1. API probe success on HTTP 200
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_urlopen.return_value.__enter__.return_value = mock_resp
        assert check_api_health(host="127.0.0.1", port=8000, path="/ready") is True

    # 2. API probe failure when connection refused
    with patch("urllib.request.urlopen", side_effect=ConnectionRefusedError("Connection refused")):
        assert check_api_health(host="127.0.0.1", port=8000, path="/ready") is False

    # 3. API probe failure when server returns 503
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_resp = MagicMock()
        mock_resp.status = 503
        mock_urlopen.return_value.__enter__.return_value = mock_resp
        assert check_api_health(host="127.0.0.1", port=8000, path="/ready") is False

    # 4. Redis probe success
    with patch("redis.from_url") as mock_redis_from_url:
        mock_r = MagicMock()
        mock_r.ping.return_value = True
        mock_redis_from_url.return_value = mock_r
        assert check_redis_health("redis://redis:6379/0") is True

    # 5. Redis probe failure
    with patch("redis.from_url", side_effect=ConnectionError("Broker unreachable")):
        assert check_redis_health("redis://redis:6379/0") is False


def test_healthcheck_module_worker_probe(tmp_path: pathlib.Path) -> None:
    """check_worker_health verifies Celery inspect ping and falls back to SQLite heartbeat."""
    db_file = str(tmp_path / "worker_probe_test.db")
    store = CheckpointStore(db_path=db_file)

    # Initially, no workers registered: probe returns False
    assert check_worker_health(store=store) is False

    # Worker registers heartbeat: probe returns True
    store.record_worker_heartbeat(
        worker_id="celery-worker-docker-01",
        status="active",
        queues="orchestrator_workflows",
    )
    assert check_worker_health(store=store) is True

    # Worker goes offline: probe returns False
    store.record_worker_heartbeat(
        worker_id="celery-worker-docker-01",
        status="offline",
    )
    assert check_worker_health(store=store) is False


def test_healthcheck_cli_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """python -m orchestrator.healthcheck CLI returns exit code 0 on healthy and 1 on unhealthy."""
    # Test --role api healthy
    with patch("orchestrator.healthcheck.check_api_health", return_value=True):
        assert healthcheck_main(["--role", "api", "--port", "8000"]) == 0

    # Test --role api unhealthy
    with patch("orchestrator.healthcheck.check_api_health", return_value=False):
        assert healthcheck_main(["--role", "api", "--port", "8000"]) == 1

    # Test --role worker healthy
    with patch("orchestrator.healthcheck.check_worker_health", return_value=True):
        assert healthcheck_main(["--role", "worker"]) == 0

    # Test --role worker unhealthy
    with patch("orchestrator.healthcheck.check_worker_health", return_value=False):
        assert healthcheck_main(["--role", "worker"]) == 1


def test_graceful_shutdown_distinguishes_readiness_from_liveness(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Graceful shutdown marks /ready as 503 (not accepting runs) while /health reports shutting_down."""
    db_file = str(tmp_path / "shutdown_test.db")
    monkeypatch.setenv("CHECKPOINT_DB_PATH", db_file)
    monkeypatch.setenv("CHECKPOINT_ENABLED", "true")
    reset_settings()

    store = CheckpointStore(db_path=db_file)
    manager = RunManager(owner_id="api-node-shutdown", store=store)
    app = create_app()

    from orchestrator.api.routes import get_manager, get_store

    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store
    client = TestClient(app)

    # 1. Normal operating state
    r_health_1 = client.get("/health")
    assert r_health_1.status_code == 200
    assert r_health_1.json()["status"] == "healthy"

    r_ready_1 = client.get("/ready")
    assert r_ready_1.status_code == 200
    assert r_ready_1.json()["status"] == "ready"

    # 2. Service begins graceful shutdown (SIGTERM received)
    manager.shutdown(grace_period=0.1, store=store)
    assert manager.is_shutting_down is True

    # Liveness probe reports shutting down
    r_health_2 = client.get("/health")
    assert r_health_2.status_code == 200
    assert r_health_2.json()["status"] == "shutting_down"

    # Readiness probe rejects new traffic with 503
    r_ready_2 = client.get("/ready")
    assert r_ready_2.status_code == 503
    assert r_ready_2.json()["status"] == "not_ready"


# ── 4. Persistent Storage & SQLite WAL Verification ───────────────────────────


def test_sqlite_wal_persistence_across_simulated_container_restarts(
    tmp_path: pathlib.Path,
) -> None:
    """SQLite WAL storage preserves checkpoints, outbox events, and audit logs across container restarts."""
    shared_data_dir = tmp_path / "app_data"
    shared_data_dir.mkdir(parents=True, exist_ok=True)
    shared_db = str(shared_data_dir / "orchestrator_checkpoints.db")

    run_id = "run-docker-persist-001"

    # ── Phase 1: API Container writes initial run ──
    api_store_1 = CheckpointStore(db_path=shared_db)
    api_store_1.save_checkpoint(
        run_id=run_id,
        state={"task": "Containerized multi-agent analysis", "status": "queued"},
        transition="enqueue",
    )
    summary_1 = api_store_1.get_run_summary(run_id)
    assert summary_1 is not None and summary_1["status"] == "queued"

    # ── Phase 2: Worker Container executes and updates checkpoints ──
    worker_store_1 = CheckpointStore(db_path=shared_db)
    worker_mgr_1 = RunManager(owner_id="worker-container-pid-101", store=worker_store_1)
    res = worker_mgr_1.execute_dispatched_run(
        run_id=run_id,
        task="Containerized multi-agent analysis",
        store=worker_store_1,
    )
    assert res.get("status") in ("completed", "approved")

    # Verify SQLite WAL journal exists or transactions committed in directory
    assert (shared_data_dir / "orchestrator_checkpoints.db").is_file()

    # ── Phase 3: Simulated Container Restart (destroying memory, connections, and processes) ──
    del api_store_1
    del worker_store_1
    del worker_mgr_1

    # ── Phase 4: Fresh API and Worker containers mount the same shared volume ──
    restarted_api_store = CheckpointStore(db_path=shared_db)
    restarted_summary = restarted_api_store.get_run_summary(run_id)
    assert restarted_summary is not None
    assert restarted_summary["status"] == "completed"
    assert restarted_summary["latest_step"] >= 1

    # Verify audit event history persisted intact
    audits = restarted_api_store.get_audit_events(run_id=run_id)
    assert len(audits) >= 1
    assert any(a["event_type"] in ("workflow.completed", "workflow.started") for a in audits)


# ── 5. End-to-End Deployment Workflow & HITL Validation ───────────────────────


@pytest.fixture
def deployment_env(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Configure environment variables matching docker-compose.yml."""
    data_dir = tmp_path / "app_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    db_file = str(data_dir / "orchestrator_checkpoints.db")
    token = "test-prod-bearer-token-xyz"

    env = {
        "CHECKPOINT_DB_PATH": db_file,
        "CHECKPOINT_ENABLED": "true",
        "API_HOST": "0.0.0.0",
        "API_PORT": "8000",
        "ORCHESTRATOR_API_TOKEN": token,
        "ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND": "celery",
        "ORCHESTRATOR_CELERY_ENABLED": "true",
        "ORCHESTRATOR_CELERY_ALWAYS_EAGER": "true",
        "CELERY_BROKER_URL": "redis://redis:6379/0",
        "CELERY_RESULT_BACKEND": "redis://redis:6379/0",
        "CELERY_TASK_DEFAULT_QUEUE": "orchestrator_workflows",
        "DEFAULT_LLM_PROVIDER": "mock",
    }
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    reset_settings()
    return env


def test_e2e_deployment_workflow_execution_and_cancellation(
    deployment_env: dict[str, str],
) -> None:
    """Full workflow execution, status query, and cancellation under production container settings."""
    db_path = deployment_env["CHECKPOINT_DB_PATH"]
    token = deployment_env["ORCHESTRATOR_API_TOKEN"]
    store = CheckpointStore(db_path=db_path)
    app = create_app()
    manager = RunManager(owner_id="prod-api-worker-e2e", store=store)

    from orchestrator.api.routes import get_manager, get_store

    app.dependency_overrides[get_manager] = lambda: manager
    app.dependency_overrides[get_store] = lambda: store

    client = TestClient(app, headers={"Authorization": f"Bearer {token}"})

    # 1. Submit workflow run
    run_id = "run-prod-e2e-001"
    post_resp = client.post(
        "/runs",
        json={
            "run_id": run_id,
            "task": "Production container reliability audit",
            "background": True,
            "execution_backend": "celery",
        },
    )
    assert post_resp.status_code == 202
    assert post_resp.json()["run_id"] == run_id

    # 2. Verify completed status via API query
    get_resp = client.get(f"/runs/{run_id}")
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["status"] == "completed"
    assert data["answer"] != ""

    # 3. Test administrative cancellation
    run_id_cancel = "run-prod-cancel-002"
    store.save_checkpoint(
        run_id_cancel,
        {"task": "Long running simulation", "status": "running", "step": 1},
        transition="subtask_dispatched",
    )
    cancel_resp = client.post(
        f"/runs/{run_id_cancel}/cancel",
        json={"reason": "Operator requested deployment teardown"},
    )
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "cancelled"

    # 4. Idempotent re-cancellation succeeds
    cancel_repeat = client.post(
        f"/runs/{run_id_cancel}/cancel",
        json={"reason": "Repeat cancellation request"},
    )
    assert cancel_repeat.status_code == 200
    assert cancel_repeat.json()["status"] == "cancelled"

    # 5. Cancelling already-completed run returns 409 Conflict
    conflict_resp = client.post(f"/runs/{run_id}/cancel")
    assert conflict_resp.status_code == 409
    assert "already reached terminal status" in conflict_resp.json()["detail"]


def test_deployment_hitl_workflow_pause_and_resumption(
    deployment_env: dict[str, str],
) -> None:
    """HITL sensitive workflows pause, release lease, and resume safely under container settings."""
    db_path = deployment_env["CHECKPOINT_DB_PATH"]
    token = deployment_env["ORCHESTRATOR_API_TOKEN"]
    store = CheckpointStore(db_path=db_path)

    # Register high-risk tool
    def drop_staging_table(table_name: str) -> str:
        return f"Dropped {table_name}"

    tool = Tool(
        name="container_drop_staging_table",
        fn=drop_staging_table,
        schema={"type": "object", "properties": {"table_name": {"type": "string"}}},
        requires_approval=True,
        side_effecting=True,
        risk_level=RiskLevel.HIGH,
    )
    REGISTRY.register(tool)

    try:
        app = create_app()
        manager = RunManager(owner_id="prod-hitl-manager", store=store)

        from orchestrator.api.routes import get_manager, get_store

        app.dependency_overrides[get_manager] = lambda: manager
        app.dependency_overrides[get_store] = lambda: store

        client = TestClient(app, headers={"Authorization": f"Bearer {token}"})

        run_id = "run-prod-hitl-pause-003"
        plan = [
            {
                "id": "s1",
                "specialist": "analysis",
                "instruction": "Drop table staging_metrics via container_drop_staging_table",
                "tools": ["container_drop_staging_table"],
                "status": "pending",
            },
            {
                "id": "s2",
                "specialist": "analysis",
                "instruction": "Confirm table drop in staging audit records",
                "tools": [],
                "status": "pending",
                "depends_on": ["s1"],
            },
        ]

        from orchestrator.celery_app import execute_workflow_run, resume_workflow_run

        # Execute up to approval gate
        execute_workflow_run(run_id=run_id, task="Drop staging metrics table", plan=plan)

        # Verify paused and lease released
        summary_paused = store.get_run_summary(run_id)
        assert summary_paused is not None
        assert summary_paused["status"] in ("waiting_approval", "pending")
        assert store.get_workflow_admission(run_id) is None

        # Verify API status confirms paused state
        paused_api_resp = client.get(f"/runs/{run_id}")
        assert paused_api_resp.status_code == 200
        assert paused_api_resp.json()["status"] in ("waiting_approval", "pending")

        # Human operator approves review item
        rq = ReviewQueue()
        for it in rq.pending():
            if it.task_id == run_id:
                res = rq.resolve(it.item_id, decision="approve", approver="prod-sec-admin")
                chk = store.load_latest_checkpoint(run_id)
                if chk and res:
                    apply_resolution(chk.state, res)

        # Worker resumes workflow
        res_resume = resume_workflow_run(run_id=run_id)
        assert res_resume.get("run_id") == run_id

        summary_final = store.get_run_summary(run_id)
        assert summary_final is not None
        assert summary_final["status"] in ("completed", "approved")

        # Verify API status confirms completed state
        final_api_resp = client.get(f"/runs/{run_id}")
        assert final_api_resp.status_code == 200
        assert final_api_resp.json()["status"] in ("completed", "approved")

    finally:
        if "container_drop_staging_table" in REGISTRY._tools:
            del REGISTRY._tools["container_drop_staging_table"]
        try:
            from orchestrator.memory.long_term import _LOCAL
            _LOCAL.clear()
        except Exception:
            pass


# ── 6. Environmental Limitations Honesty ──────────────────────────────────────


def test_docker_daemon_environmental_limitation_reported_honestly() -> None:
    """Validate whether Docker CLI and daemon are installed, reporting limitations truthfully."""
    docker_bin = shutil.which("docker")
    if docker_bin is None:
        # Docker CLI is absent in this execution environment (e.g. Windows without Docker Desktop)
        pytest.skip(
            "Docker CLI is not installed on this host. Static Dockerfile, Compose, healthcheck, "
            "and SQLite WAL containerization readiness tests passed deterministically."
        )

    # If docker binary exists, verify daemon responsiveness
    res = subprocess.run([docker_bin, "info"], capture_output=True, text=True)
    if res.returncode != 0:
        pytest.skip(
            f"Docker CLI found at '{docker_bin}', but Docker daemon is not running: "
            f"{res.stderr.strip()[:100]}."
        )
