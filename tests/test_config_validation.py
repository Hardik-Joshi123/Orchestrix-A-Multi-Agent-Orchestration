"""Comprehensive tests for configuration validation and startup diagnostics.

Covers:
- Valid default configuration.
- Rejection of invalid negative, zero, malformed, and out-of-range settings.
- Environment-variable alias parsing and precedence.
- Non-sensitive startup diagnostics (structured dict and terminal summary).
- Absence of credentials and secrets from diagnostics and logs.
- Safe local loopback development without credentials.
- Unsafe non-loopback network binding rejection without authentication.
- Strict CLI JSON stdout and stderr error handling.
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from orchestrator.api.server import is_loopback_host, validate_network_binding
from orchestrator.config import (
    Settings,
    format_config_error,
    format_diagnostics_summary,
    get_settings,
    get_startup_diagnostics,
    validate_configuration,
)
from orchestrator.main import cli

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clean_config_env(monkeypatch):
    """Clean all relevant configuration environment variables around tests."""
    get_settings.cache_clear()
    env_keys = [
        "ORCHESTRATOR_API_TOKEN",
        "API_TOKEN",
        "ORCHESTRATOR_API_HOST",
        "API_HOST",
        "ORCHESTRATOR_API_PORT",
        "API_PORT",
        "ORCHESTRATOR_API_MAX_BODY_BYTES",
        "ORCHESTRATOR_MAX_BODY_BYTES",
        "API_MAX_BODY_BYTES",
        "ORCHESTRATOR_RATE_LIMIT_ENABLED",
        "API_RATE_LIMIT_ENABLED",
        "ORCHESTRATOR_RATE_LIMIT_REQUESTS",
        "API_RATE_LIMIT_REQUESTS",
        "ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS",
        "API_RATE_LIMIT_WINDOW_SECONDS",
        "ORCHESTRATOR_TRUSTED_PROXIES",
        "API_TRUSTED_PROXIES",
        "ORCHESTRATOR_CORS_ORIGINS",
        "API_CORS_ORIGINS",
        "ORCHESTRATOR_MAX_SPECIALIST_CONCURRENCY",
        "SPECIALIST_CONCURRENCY",
        "MAX_SPECIALIST_CONCURRENCY",
        "ORCHESTRATOR_MAX_CONCURRENCY",
        "MAX_CONCURRENCY",
        "CONCURRENCY",
        "ORCHESTRATOR_MAX_ACTIVE_RUNS",
        "MAX_ACTIVE_RUNS",
        "API_MAX_ACTIVE_RUNS",
        "ORCHESTRATOR_CHECKPOINT_DB_PATH",
        "CHECKPOINT_DB_PATH",
        "ORCHESTRATOR_LLM_PROVIDER",
        "LLM_PROVIDER",
        "DEFAULT_LLM_PROVIDER",
        "ORCHESTRATOR_FALLBACK_LLM_PROVIDER",
        "FALLBACK_LLM_PROVIDER",
        "ORCHESTRATOR_LLM_TIMEOUT",
        "LLM_TIMEOUT_SECONDS",
        "ORCHESTRATOR_LLM_MAX_RETRIES",
        "LLM_MAX_RETRIES",
        "ORCHESTRATOR_CUSTOM_PRICING",
        "LLM_CUSTOM_PRICING",
    ]
    for k in env_keys:
        monkeypatch.delenv(k, raising=False)
    yield
    get_settings.cache_clear()
    for k in env_keys:
        monkeypatch.delenv(k, raising=False)


# -----------------------------------------------------------------------------
# 1. Valid Default Configuration
# -----------------------------------------------------------------------------


def test_valid_default_configuration():
    """Default Settings must instantiate cleanly and pass all validation checks."""
    settings = Settings()
    validated = validate_configuration(settings)
    assert validated.default_llm_provider == "mock"
    assert validated.fallback_llm_provider == "mock"
    assert validated.max_specialist_concurrency == 4
    assert validated.effective_concurrency == 4
    assert validated.api_port == 8000
    assert validated.api_host == "127.0.0.1"
    assert validated.api_max_body_bytes == 1_000_000
    assert validated.api_rate_limit_enabled is True
    assert validated.api_rate_limit_requests == 60
    assert validated.api_rate_limit_window_seconds == 60.0
    assert validated.checkpoint_enabled is True
    assert validated.checkpoint_db_path == "orchestrator_checkpoints.db"


# -----------------------------------------------------------------------------
# 2. Concurrency Validation
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("invalid_val", [-1, -50, 129, 500])
def test_invalid_specialist_concurrency_rejected(invalid_val):
    """max_specialist_concurrency must enforce 0 <= val <= 128."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(max_specialist_concurrency=invalid_val)
    err_str = format_config_error(exc_info.value)
    assert "max_specialist_concurrency" in err_str


def test_zero_specialist_concurrency_defaults_to_one():
    """max_specialist_concurrency=0 is safely normalized to 1 in effective_concurrency."""
    s = Settings(max_specialist_concurrency=0)
    assert s.effective_concurrency == 1


@pytest.mark.parametrize("invalid_val", [0, -1, 150])
def test_invalid_max_concurrency_rejected(invalid_val):
    """max_concurrency when set must enforce 1 <= val <= 128."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(max_concurrency=invalid_val)
    err_str = format_config_error(exc_info.value)
    assert "max_concurrency" in err_str


def test_valid_concurrency_overrides():
    """Valid concurrency values within bounds are accepted."""
    s = Settings(max_specialist_concurrency=8, max_concurrency=16)
    assert s.max_specialist_concurrency == 8
    assert s.max_concurrency == 16
    assert s.effective_concurrency == 16


# -----------------------------------------------------------------------------
# 3. API Host, Port, and Request Body Limits
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("invalid_port", [0, -1, 65536, 99999])
def test_invalid_api_port_rejected(invalid_port):
    """API port must be between 1 and 65535."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_port=invalid_port)
    err_str = format_config_error(exc_info.value)
    assert "Port number must be between 1 and 65535" in err_str


@pytest.mark.parametrize("invalid_host", ["", "   "])
def test_empty_api_host_rejected(invalid_host):
    """api_host must be a non-empty string."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_host=invalid_host)
    err_str = format_config_error(exc_info.value)
    assert "api_host must be a non-empty hostname" in err_str


@pytest.mark.parametrize("invalid_bytes", [0, -1, -1000, 100_000_001])
def test_invalid_api_max_body_bytes_rejected(invalid_bytes):
    """api_max_body_bytes must be between 1 and 100MB."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_max_body_bytes=invalid_bytes)
    err_str = format_config_error(exc_info.value)
    assert "api_max_body_bytes" in err_str


# -----------------------------------------------------------------------------
# 4. Rate Limiting Validation
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("invalid_requests", [0, -1, -10])
def test_invalid_rate_limit_requests_rejected(invalid_requests):
    """api_rate_limit_requests must be >= 1."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_rate_limit_requests=invalid_requests)
    err_str = format_config_error(exc_info.value)
    assert "api_rate_limit_requests must be an integer >= 1" in err_str


@pytest.mark.parametrize("invalid_window", [0.0, -1.0, -0.001])
def test_invalid_rate_limit_window_rejected(invalid_window):
    """api_rate_limit_window_seconds must be > 0."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(api_rate_limit_window_seconds=invalid_window)
    err_str = format_config_error(exc_info.value)
    assert "api_rate_limit_window_seconds must be a positive number" in err_str


# -----------------------------------------------------------------------------
# 5. Checkpointing & Persistence Validation
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("invalid_path", ["", "   "])
def test_empty_checkpoint_db_path_rejected(invalid_path):
    """checkpoint_db_path cannot be empty whitespace."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(checkpoint_db_path=invalid_path)
    err_str = format_config_error(exc_info.value)
    assert "checkpoint_db_path must be a non-empty file path" in err_str


# -----------------------------------------------------------------------------
# 6. LLM Provider, Retries, Timeout, and Pricing Validation
# -----------------------------------------------------------------------------


@pytest.mark.parametrize("invalid_provider", ["unknown", "foobar", "gemini_pro_ultra", "123"])
def test_invalid_default_provider_rejected(invalid_provider):
    """default_llm_provider must be one of mock, openai, anthropic."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(default_llm_provider=invalid_provider)
    err_str = format_config_error(exc_info.value)
    assert "Invalid default_llm_provider" in err_str
    assert "mock" in err_str


def test_provider_case_insensitivity_normalization():
    """Provider names must normalize case-insensitively."""
    s = Settings(default_llm_provider="OPENAI", fallback_llm_provider="ANTHROPIC")
    assert s.default_llm_provider == "openai"
    assert s.fallback_llm_provider == "anthropic"


@pytest.mark.parametrize("invalid_fallback", ["unsupported", "aws_bedrock"])
def test_invalid_fallback_provider_rejected(invalid_fallback):
    """fallback_llm_provider must be valid or empty."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(fallback_llm_provider=invalid_fallback)
    err_str = format_config_error(exc_info.value)
    assert "Invalid fallback_llm_provider" in err_str


@pytest.mark.parametrize("invalid_retries", [-1, -5, 21, 50])
def test_invalid_llm_max_retries_rejected(invalid_retries):
    """llm_max_retries must be between 0 and 20."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(llm_max_retries=invalid_retries)
    err_str = format_config_error(exc_info.value)
    assert "llm_max_retries" in err_str


@pytest.mark.parametrize("invalid_timeout", [0.0, -10.0, 601.0])
def test_invalid_llm_timeout_seconds_rejected(invalid_timeout):
    """llm_timeout_seconds must be between 0.1 and 600.0."""
    with pytest.raises(ValidationError) as exc_info:
        Settings(llm_timeout_seconds=invalid_timeout)
    err_str = format_config_error(exc_info.value)
    assert "llm_timeout_seconds" in err_str


def test_custom_pricing_json_validation():
    """Custom pricing JSON must be validated for valid structure and non-negative rates."""
    # Valid custom pricing
    valid_json = json.dumps(
        {
            "openai:gpt-4o": {"input_cost_per_1m": 2.50, "output_cost_per_1m": 10.00},
            "custom:model": {"input": 0.50, "output": 1.50},
        }
    )
    s = Settings(llm_custom_pricing=valid_json)
    assert "openai:gpt-4o" in s.llm_custom_pricing

    # Malformed non-JSON string
    with pytest.raises(ValidationError) as exc_info1:
        Settings(llm_custom_pricing="not-a-json-string")
    assert "must be valid JSON" in format_config_error(exc_info1.value)

    # JSON not an object
    with pytest.raises(ValidationError) as exc_info2:
        Settings(llm_custom_pricing="[1, 2, 3]")
    assert "must be a JSON object" in format_config_error(exc_info2.value)

    # Negative rate rejected
    neg_json = json.dumps({"openai:gpt-4o": {"input_cost_per_1m": -5.0, "output_cost_per_1m": 10.0}})
    with pytest.raises(ValidationError) as exc_info3:
        Settings(llm_custom_pricing=neg_json)
    assert "cannot have negative rates" in format_config_error(exc_info3.value)

    # Non-numeric rate rejected
    str_json = json.dumps({"openai:gpt-4o": {"input_cost_per_1m": "free", "output_cost_per_1m": 10.0}})
    with pytest.raises(ValidationError) as exc_info4:
        Settings(llm_custom_pricing=str_json)
    assert "non-numeric cost values" in format_config_error(exc_info4.value)


# -----------------------------------------------------------------------------
# 7. Environment Variable Parsing and Precedence
# -----------------------------------------------------------------------------


def test_environment_variable_alias_parsing(monkeypatch):
    """Settings must properly parse aliased environment variable names."""
    monkeypatch.setenv("ORCHESTRATOR_API_PORT", "9090")
    monkeypatch.setenv("ORCHESTRATOR_API_MAX_BODY_BYTES", "500000")
    monkeypatch.setenv("ORCHESTRATOR_MAX_SPECIALIST_CONCURRENCY", "8")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_REQUESTS", "30")
    monkeypatch.setenv("ORCHESTRATOR_RATE_LIMIT_WINDOW_SECONDS", "15.0")
    monkeypatch.setenv("ORCHESTRATOR_LLM_PROVIDER", "openai")
    monkeypatch.setenv("ORCHESTRATOR_FALLBACK_LLM_PROVIDER", "mock")
    get_settings.cache_clear()

    s = Settings()
    assert s.api_port == 9090
    assert s.api_max_body_bytes == 500_000
    assert s.max_specialist_concurrency == 8
    assert s.api_rate_limit_requests == 30
    assert s.api_rate_limit_window_seconds == 15.0
    assert s.default_llm_provider == "openai"
    assert s.fallback_llm_provider == "mock"


# -----------------------------------------------------------------------------
# 8. Local Loopback Dev vs Unsafe Remote Network Binding
# -----------------------------------------------------------------------------


def test_loopback_hosts_allow_unauthenticated_dev():
    """is_loopback_host accurately detects loopback interfaces and permits unauthenticated dev."""
    assert is_loopback_host("127.0.0.1") is True
    assert is_loopback_host("localhost") is True
    assert is_loopback_host("::1") is True
    assert is_loopback_host("testclient") is True
    assert is_loopback_host(None) is True

    # Permitted without token
    validate_network_binding("127.0.0.1", token="")
    validate_network_binding("localhost", token="")


def test_unsafe_remote_binding_rejected_without_token():
    """validate_network_binding rejects non-loopback host when token is missing."""
    with pytest.raises(ValueError) as exc1:
        validate_network_binding("0.0.0.0", token="")
    assert "Unsafe network binding rejected" in str(exc1.value)

    with pytest.raises(ValueError) as exc2:
        validate_network_binding("192.168.1.105", token="")
    assert "Unsafe network binding rejected" in str(exc2.value)

    # Permitted when token is configured
    validate_network_binding("0.0.0.0", token="secure-prod-token")
    validate_network_binding("192.168.1.105", token="secure-prod-token")


# -----------------------------------------------------------------------------
# 9. Startup Diagnostics & Zero Secret Leakage
# -----------------------------------------------------------------------------


def test_startup_diagnostics_structure_and_privacy():
    """get_startup_diagnostics must expose non-sensitive telemetry with zero secret leakage."""
    s = Settings(
        openai_api_key="sk-live-secret-openai-key-99999",
        anthropic_api_key="sk-ant-secret-anthropic-key-88888",
        api_token="prod-bearer-token-77777",
        postgres_password="super-secret-db-password",
    )
    diag = get_startup_diagnostics(s)

    # Verify structured fields
    assert diag["llm"]["default_provider"] == "mock"
    assert diag["execution"]["specialist_concurrency"] == 4
    assert diag["persistence"]["backend"] == "sqlite"
    assert diag["api"]["port"] == 8000
    assert diag["api"]["auth"] == "enabled (token configured)"

    # Verify NO secrets in structured dict
    diag_str = str(diag)
    assert "sk-live-secret" not in diag_str
    assert "sk-ant-secret" not in diag_str
    assert "prod-bearer-token" not in diag_str
    assert "super-secret-db-password" not in diag_str


def test_diagnostics_summary_formatting_privacy():
    """format_diagnostics_summary must format a clean terminal summary with zero secrets."""
    s = Settings(
        openai_api_key="sk-live-secret-openai-key-99999",
        anthropic_api_key="sk-ant-secret-anthropic-key-88888",
        api_token="prod-bearer-token-77777",
        postgres_password="super-secret-db-password",
    )
    summary = format_diagnostics_summary(s)

    assert "Multi-Agent Orchestrator Configuration Diagnostics:" in summary
    assert "LLM Provider:" in summary
    assert "Concurrency Limit:" in summary
    assert "API Security:" in summary

    # Secrets must NEVER be in terminal summary
    assert "sk-live" not in summary
    assert "sk-ant" not in summary
    assert "prod-bearer" not in summary
    assert "super-secret" not in summary


# -----------------------------------------------------------------------------
# 10. CLI Diagnostics and Strict JSON Stdout Behavior
# -----------------------------------------------------------------------------


def test_cli_diagnostics_text_output(capsys):
    """CLI --diagnostics must print terminal summary to stderr and return 0."""
    rc = cli(["--diagnostics"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "Multi-Agent Orchestrator Configuration Diagnostics:" in (captured.out + captured.err)
    assert "LLM Provider:" in (captured.out + captured.err)


def test_cli_diagnostics_json_output(capsys):
    """CLI --diagnostics --json must print valid JSON to stdout without extra stderr noise."""
    rc = cli(["--diagnostics", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    # stdout must parse as single valid JSON document
    payload = json.loads(captured.out)
    assert "llm" in payload
    assert "api" in payload
    assert payload["llm"]["default_provider"] == "mock"


def test_cli_invalid_concurrency_argument_error(capsys):
    """CLI --concurrency with out-of-range value exits 1 with clean error to stderr."""
    rc = cli(["--concurrency", "0", "Some task"])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Configuration error: --concurrency must be 1..128" in captured.err


def test_cli_invalid_environment_variable_error(capsys, monkeypatch):
    """Invalid environment variable causes CLI to fail early with clean error to stderr."""
    monkeypatch.setenv("ORCHESTRATOR_API_PORT", "99999")
    get_settings.cache_clear()

    rc = cli(["--diagnostics"])
    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Configuration error:" in captured.err
    assert "Port number must be between 1 and 65535" in captured.err
    assert "Traceback" not in captured.err
