"""Production Configuration Validation and Secrets Management Tests (Task 61).

Validates:
- Comprehensive environment variable parsing, aliases, and precedence.
- Strict startup validation (URL schemes, ports, timeouts, concurrency, and compatibility).
- Safe network binding and secret token requirements.
- Zero secret leakage in logs, diagnostics, and error formatters.
- Zero-downtime secret rotation for API bearer tokens and webhook signing.
- Strict production configuration rules (persistence, mandatory keys, TLS, and broker safety).
- Configuration consistency across API and Celery worker processes.
"""

from __future__ import annotations

import pathlib
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from orchestrator.api.routes import verify_api_token
from orchestrator.api.server import create_app
from orchestrator.config import (
    Settings,
    format_config_error,
    get_settings,
    get_startup_diagnostics,
    mask_secret,
    reset_settings,
    sanitize_broker_url,
    sanitize_text,
    validate_configuration,
    validate_production_configuration,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clean_config_environment(monkeypatch: pytest.MonkeyPatch):
    """Ensure clean configuration state across tests."""
    reset_settings()
    env_keys = [
        "ORCHESTRATOR_API_TOKEN",
        "ORCHESTRATOR_API_TOKEN_PREVIOUS",
        "API_TOKEN",
        "API_TOKEN_PREVIOUS",
        "ORCHESTRATOR_API_HOST",
        "API_HOST",
        "ORCHESTRATOR_API_PORT",
        "API_PORT",
        "ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND",
        "WORKFLOW_EXECUTION_BACKEND",
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
        "REDIS_URL",
        "CHECKPOINT_DB_PATH",
        "CHECKPOINT_ENABLED",
        "DEFAULT_LLM_PROVIDER",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "NOTIFICATION_PROVIDER",
        "NOTIFICATION_WEBHOOK_URL",
        "NOTIFICATION_WEBHOOK_SIGNING_SECRET",
        "NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS",
        "NOTIFICATION_WEBHOOK_AUTH_TOKEN",
    ]
    for k in env_keys:
        monkeypatch.delenv(k, raising=False)
    yield
    reset_settings()
    for k in env_keys:
        monkeypatch.delenv(k, raising=False)


# ── 1. Startup Validation & Rejection of Invalid Configuration ────────────────


def test_non_loopback_binding_requires_strong_token() -> None:
    """validate_configuration rejects non-loopback host (e.g. 0.0.0.0) without configured token."""
    s_no_token = Settings(api_host="0.0.0.0", api_token="")
    with pytest.raises(ValueError, match="Unsafe network binding rejected"):
        validate_configuration(s_no_token)

    # Rejects placeholder token
    s_placeholder = Settings(api_host="0.0.0.0", api_token="replace_with_prod_secret_token_12345")
    with pytest.raises(ValueError, match="contains an unreplaced placeholder"):
        validate_configuration(s_placeholder)

    # Rejects weak token (< 16 chars)
    s_weak = Settings(api_host="0.0.0.0", api_token="short_token")
    with pytest.raises(ValueError, match="Insecure API token.*at least 16 characters"):
        validate_configuration(s_weak)

    # Accepts strong token (>= 16 chars)
    s_valid = Settings(api_host="0.0.0.0", api_token="production-secret-token-32chars-xyz")
    assert validate_configuration(s_valid).api_token == "production-secret-token-32chars-xyz"


def test_invalid_celery_broker_scheme_rejected() -> None:
    """validate_configuration rejects unsupported or malformed Celery broker URL schemes."""
    invalid_schemes = ["http://redis:6379/0", "ftp://broker.internal", "not-a-url", "sqlite:///test.db"]
    for url in invalid_schemes:
        s = Settings(celery_broker_url=url)
        with pytest.raises(ValueError, match="Invalid Celery broker URL"):
            validate_configuration(s)

    # Valid schemes are accepted
    valid_schemes = [
        "redis://redis:6379/0",
        "rediss://secure-redis:6379/0",
        "amqp://guest:guest@rabbitmq:5672//",
        "memory://",
    ]
    for url in valid_schemes:
        s = Settings(celery_broker_url=url)
        assert validate_configuration(s).celery_broker_url == url


def test_invalid_celery_result_backend_scheme_rejected() -> None:
    """validate_configuration rejects unsupported Celery result backend schemes."""
    with pytest.raises(ValueError, match="Invalid Celery result backend URL"):
        validate_configuration(Settings(celery_result_backend="ftp://backend:6379/0"))

    # Valid result backend schemes pass
    s_redis = Settings(celery_result_backend="redis://redis:6379/1")
    assert validate_configuration(s_redis).celery_result_backend == "redis://redis:6379/1"


def test_invalid_redis_url_scheme_rejected() -> None:
    """validate_configuration rejects non-redis schemes for redis_url."""
    with pytest.raises(ValueError, match="Invalid redis_url"):
        validate_configuration(Settings(redis_url="http://localhost:6379"))


def test_distributed_celery_rejects_in_memory_sqlite() -> None:
    """Distributed Celery execution rejects in-memory SQLite storage (:memory:)."""
    s = Settings(
        workflow_execution_backend="celery",
        celery_enabled=True,
        celery_always_eager=False,
        checkpoint_db_path=":memory:",
    )
    with pytest.raises(ValueError, match="Incompatible configuration.*Distributed Celery execution"):
        validate_configuration(s)


def test_unreplaced_secret_placeholders_rejected() -> None:
    """Configuring placeholder values in production secrets raises actionable validation error."""
    with pytest.raises(ValueError, match="openai_api_key contains an unreplaced placeholder"):
        validate_configuration(Settings(openai_api_key="replace_with_openai_key"))

    with pytest.raises(ValueError, match="anthropic_api_key contains an unreplaced placeholder"):
        validate_configuration(Settings(anthropic_api_key="replace_with_anthropic_key"))

    with pytest.raises(ValueError, match="notification_webhook_signing_secret contains an unreplaced"):
        validate_configuration(
            Settings(
                notification_provider="webhook",
                notification_webhook_url="https://example.com/webhook",
                notification_webhook_signing_secret="replace_with_signing_secret",
            )
        )


# ── 2. Secrets Management & Redaction ─────────────────────────────────────────


def test_broker_url_password_redaction() -> None:
    """sanitize_broker_url masks passwords and sensitive query params in connection strings."""
    # 1. Password in authority
    url_with_pass = "redis://:super_secret_redis_pass@redis.internal:6379/0"
    sanitized = sanitize_broker_url(url_with_pass)
    assert sanitized is not None
    assert "super_secret_redis_pass" not in sanitized
    assert ":***@redis.internal:6379/0" in sanitized

    # 2. Username and password
    pg_url = "postgresql://app_user:secret_pg_pass@pg.internal:5432/orchestrator"
    sanitized_pg = sanitize_broker_url(pg_url)
    assert sanitized_pg is not None
    assert "secret_pg_pass" not in sanitized_pg
    assert "app_user:***@pg.internal:5432/orchestrator" in sanitized_pg

    # 3. Query parameter with secret
    query_url = "https://hooks.example.com/events?token=super_confidential_token&format=json"
    sanitized_q = sanitize_broker_url(query_url)
    assert sanitized_q is not None
    assert "super_confidential_token" not in sanitized_q
    assert "token=%2A%2A%2A" in sanitized_q or "token=***" in sanitized_q


def test_sanitized_postgres_dsn_property() -> None:
    """Settings.sanitized_postgres_dsn masks database password in formatted DSN."""
    s = Settings(
        postgres_user="orchestrator_admin",
        postgres_password="ultra-secret-password-123",
        postgres_host="db.prod.internal",
        postgres_port=5432,
        postgres_db="orchestrator_prod",
    )
    dsn = s.sanitized_postgres_dsn
    assert "ultra-secret-password" not in dsn
    assert "orchestrator_admin:***@db.prod.internal:5432/orchestrator_prod" in dsn


def test_mask_secret_utility() -> None:
    """mask_secret utility redacts short tokens completely and preserves boundaries for audit."""
    assert mask_secret("") == ""
    assert mask_secret(None) == ""
    assert mask_secret("short") == "[REDACTED]"
    assert mask_secret("1234567890") == "[REDACTED]"
    masked = mask_secret("sk-ant-api03-abcdefghijklmnop12345678")
    assert masked.startswith("sk-a...")
    assert masked.endswith("5678")
    assert "abcdefghijklmnop" not in masked


def test_sanitize_text_redacts_credentials_and_patterns() -> None:
    """sanitize_text replaces known secrets and raw API key patterns with [REDACTED]."""
    text = (
        "Execution failed for provider with key sk-proj-abcdef1234567890abcdef12345 and "
        "Bearer super_secret_bearer_token_123456"
    )
    cleaned = sanitize_text(text)
    assert "sk-proj-abcdef1234567890abcdef12345" not in cleaned
    assert "[REDACTED_API_KEY]" in cleaned
    assert "Bearer [REDACTED_TOKEN]" in cleaned


def test_format_config_error_redacts_secrets() -> None:
    """format_config_error suppresses raw secret tokens from exception strings."""
    secret_token = "ultra_confidential_bearer_token_999"
    s = Settings(api_token=secret_token)
    exc = ValueError(f"Invalid API token '{secret_token}': connection rejected.")
    formatted = format_config_error(exc, settings=s)
    assert secret_token not in formatted
    assert "[REDACTED]" in formatted

    # Also test regex sanitization when settings object is not passed
    bearer_exc = ValueError("Failed connection with Bearer live_super_secret_token_12345")
    formatted_bearer = format_config_error(bearer_exc)
    assert "live_super_secret_token_12345" not in formatted_bearer
    assert "Bearer [REDACTED_TOKEN]" in formatted_bearer


def test_get_startup_diagnostics_privacy_audit() -> None:
    """get_startup_diagnostics emits structured metadata without exposing credentials."""
    s = Settings(
        openai_api_key="sk-live-openai-secret-key-0001",
        anthropic_api_key="sk-ant-live-anthropic-secret-0002",
        api_token="prod-secret-bearer-token-0003",
        api_token_previous="old-secret-bearer-token-0004",
        postgres_password="secret-postgres-database-password-0005",
        celery_broker_url="redis://:broker_password@redis:6379/0",
        notification_webhook_url="https://hooks.example.com/alerts",
        notification_webhook_auth_token="webhook-bearer-secret-0006",
        notification_webhook_signing_secret="webhook-signing-secret-0007",
    )
    diag = get_startup_diagnostics(s)

    # Diagnostics contain safe flags and metadata
    assert diag["llm"]["has_openai_key"] is True
    assert diag["llm"]["has_anthropic_key"] is True
    assert diag["api"]["token_rotation_active"] is True
    assert "token rotation active" in diag["api"]["auth"]
    assert diag["notifications"]["signing_enabled"] is True
    assert diag["notifications"]["has_auth_token"] is True

    # Check that ZERO secrets appear in diagnostic serialization
    diag_str = str(diag)
    forbidden_tokens = [
        "sk-live-openai",
        "sk-ant-live-anthropic",
        "prod-secret-bearer",
        "old-secret-bearer",
        "secret-postgres-database",
        "broker_password",
        "webhook-bearer-secret",
        "webhook-signing-secret",
    ]
    for forbidden in forbidden_tokens:
        assert forbidden not in diag_str, f"Credential leaked in diagnostics: {forbidden}"


# ── 3. Zero-Downtime API Bearer Token Rotation ────────────────────────────────


def test_api_token_rotation_accepts_primary_and_previous(monkeypatch: pytest.MonkeyPatch) -> None:
    """verify_api_token accepts both current and previous tokens during rotation window."""
    primary_token = "primary-strong-token-12345678"
    previous_token = "previous-strong-token-87654321"

    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", primary_token)
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN_PREVIOUS", previous_token)
    reset_settings()

    mock_req = MagicMock()
    mock_req.client.host = "127.0.0.1"
    mock_req.url.path = "/runs"
    mock_req.headers = {}

    # 1. Primary token succeeds
    cred_primary = MagicMock()
    cred_primary.credentials = primary_token
    verify_api_token(cred_primary, mock_req)

    # 2. Previous token succeeds
    cred_previous = MagicMock()
    cred_previous.credentials = previous_token
    verify_api_token(cred_previous, mock_req)

    # 3. Invalid token fails with 401
    cred_invalid = MagicMock()
    cred_invalid.credentials = "wrong-bearer-token-xyz"
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc_info:
        verify_api_token(cred_invalid, mock_req)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail == "Invalid authentication token."


def test_api_token_rotation_via_comma_separated_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """verify_api_token accepts comma-separated token values within ORCHESTRATOR_API_TOKEN."""
    tok1 = "token-rotation-phase-a-12345"
    tok2 = "token-rotation-phase-b-67890"

    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", f"{tok1},{tok2}")
    reset_settings()

    mock_req = MagicMock()
    mock_req.client.host = "127.0.0.1"
    mock_req.url.path = "/runs"
    mock_req.headers = {}

    for tok in (tok1, tok2):
        cred = MagicMock()
        cred.credentials = tok
        verify_api_token(cred, mock_req)


def test_api_server_routes_with_token_rotation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Live FastAPI TestClient authenticates with primary and previous tokens."""
    token_new = "live-test-new-bearer-token-123"
    token_old = "live-test-old-bearer-token-456"

    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token_new)
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN_PREVIOUS", token_old)
    reset_settings()

    app = create_app()
    client = TestClient(app)

    # Request without token fails 401
    r_unauth = client.get("/diagnostics")
    assert r_unauth.status_code == 401

    # Request with old token succeeds 200
    r_old = client.get("/diagnostics", headers={"Authorization": f"Bearer {token_old}"})
    assert r_old.status_code == 200

    # Request with new token succeeds 200
    r_new = client.get("/diagnostics", headers={"Authorization": f"Bearer {token_new}"})
    assert r_new.status_code == 200


# ── 4. Production Startup Validation (validate_production_configuration) ─────


def test_production_validation_succeeds_on_valid_stack(tmp_path: pathlib.Path) -> None:
    """validate_production_configuration succeeds when all production criteria are satisfied."""
    db_file = str(tmp_path / "prod_checkpoints.db")
    s = Settings(
        api_host="0.0.0.0",
        api_port=8000,
        api_token="production-grade-secret-token-32chars",
        checkpoint_db_path=db_file,
        checkpoint_enabled=True,
        default_llm_provider="openai",
        openai_api_key="sk-proj-valid-production-openai-key-12345",
        workflow_execution_backend="celery",
        celery_enabled=True,
        celery_always_eager=False,
        celery_broker_url="redis://redis:6379/0",
        notification_provider="webhook",
        notification_webhook_url="https://hooks.company.com/alerts",
        notification_webhook_signing_secret="super-secure-webhook-signing-key-32chars",
        notification_webhook_tls_verify=True,
        notification_webhook_allow_insecure_http=False,
        review_auth_enabled=True,
    )
    validated = validate_production_configuration(s)
    assert validated.api_host == "0.0.0.0"
    assert validated.checkpoint_enabled is True


def test_production_validation_rejects_missing_live_llm_credentials(tmp_path: pathlib.Path) -> None:
    """validate_production_configuration rejects live LLM provider without configured API key."""
    db_file = str(tmp_path / "prod_checkpoints.db")
    # Missing OpenAI key
    s_openai = Settings(
        checkpoint_db_path=db_file,
        default_llm_provider="openai",
        openai_api_key="",
    )
    with pytest.raises(ValueError, match="openai_api_key is required when default_llm_provider is 'openai'"):
        validate_production_configuration(s_openai)

    # Missing Anthropic key
    s_anthropic = Settings(
        checkpoint_db_path=db_file,
        default_llm_provider="anthropic",
        anthropic_api_key="",
    )
    with pytest.raises(
        ValueError, match="anthropic_api_key is required when default_llm_provider is 'anthropic'"
    ):
        validate_production_configuration(s_anthropic)


def test_production_validation_rejects_celery_eager_mode(tmp_path: pathlib.Path) -> None:
    """validate_production_configuration rejects celery_always_eager=True in production."""
    db_file = str(tmp_path / "prod_checkpoints.db")
    s = Settings(
        checkpoint_db_path=db_file,
        workflow_execution_backend="celery",
        celery_always_eager=True,
    )
    with pytest.raises(ValueError, match="celery_always_eager must be False in production"):
        validate_production_configuration(s)


def test_production_validation_rejects_insecure_webhooks(tmp_path: pathlib.Path) -> None:
    """validate_production_configuration rejects webhooks without signing secret or with insecure HTTP."""
    db_file = str(tmp_path / "prod_checkpoints.db")
    # Missing webhook signing secret
    s_unsigned = Settings(
        checkpoint_db_path=db_file,
        notification_provider="webhook",
        notification_webhook_url="https://hooks.company.com/alerts",
        notification_webhook_signing_secret="",
    )
    with pytest.raises(ValueError, match="notification_webhook_signing_secret is required in production"):
        validate_production_configuration(s_unsigned)

    # Insecure HTTP allowed
    s_insecure = Settings(
        checkpoint_db_path=db_file,
        notification_provider="webhook",
        notification_webhook_url="https://hooks.company.com/alerts",
        notification_webhook_signing_secret="valid-signing-secret-32chars",
        notification_webhook_allow_insecure_http=True,
    )
    with pytest.raises(
        ValueError, match="notification_webhook_allow_insecure_http must be False in production"
    ):
        validate_production_configuration(s_insecure)


# ── 5. Environment Variable Precedence & Consistency ─────────────────────────


def test_environment_variable_precedence_over_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    """Environment variables take precedence over code defaults and .env entries."""
    monkeypatch.setenv("ORCHESTRATOR_API_PORT", "9990")
    monkeypatch.setenv("ORCHESTRATOR_API_HOST", "127.0.0.2")
    monkeypatch.setenv("MAX_SPECIALIST_CONCURRENCY", "12")
    reset_settings()

    s = Settings()
    assert s.api_port == 9990
    assert s.api_host == "127.0.0.2"
    assert s.max_specialist_concurrency == 12


def test_api_and_worker_shared_configuration_consistency(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """API and Celery worker resolve compatible settings when sharing an execution environment."""
    shared_db = str(tmp_path / "shared_data" / "orchestrator_checkpoints.db")
    token = "shared-production-bearer-token-12345"

    monkeypatch.setenv("CHECKPOINT_DB_PATH", shared_db)
    monkeypatch.setenv("CHECKPOINT_ENABLED", "true")
    monkeypatch.setenv("ORCHESTRATOR_API_TOKEN", token)
    monkeypatch.setenv("ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND", "celery")
    monkeypatch.setenv("CELERY_ENABLED", "true")
    monkeypatch.setenv("CELERY_BROKER_URL", "redis://redis:6379/0")
    reset_settings()

    api_settings = get_settings()
    worker_settings = Settings()

    # Shared properties must match exactly
    assert api_settings.checkpoint_db_path == worker_settings.checkpoint_db_path
    assert api_settings.checkpoint_enabled == worker_settings.checkpoint_enabled
    assert api_settings.workflow_execution_backend == worker_settings.workflow_execution_backend
    assert api_settings.celery_broker_url == worker_settings.celery_broker_url

    # Both validate cleanly
    validate_configuration(api_settings)
    validate_configuration(worker_settings)
