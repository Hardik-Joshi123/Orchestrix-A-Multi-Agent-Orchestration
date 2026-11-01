# Task 61 Report: Production Configuration Validation & Secrets Management

**Repository**: `c:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator`  
**Execution Date**: November 1, 2026  
**Status**: Completed  
**Test Suite**: 21/21 new tests passing; 178/181 regression tests passing (3 skipped without Docker)

---

## 1. Executive Summary

Task 61 audited and fortified configuration management, secret handling, and startup validation
across the Multi-Agent Orchestrator. Prior to this task, the orchestrator featured extensive
configuration settings across subsystems (LLMs, API, persistence, tool approvals, admission leases,
transactional outbox, webhook signing, durable replay store, and Celery background workers), but
lacked unified production startup validation, consistent credential masking in error outputs,
zero-downtime API token rotation, and strict incompatibility detection between distributed execution
backends and storage layers.

### Key Accomplishments
1. **Comprehensive Configuration Audit & Inventory**: Audited all 101 settings declared in
   `src/orchestrator/config.py` and environment variable overrides across the codebase.
   Documented default values, environment variable aliases, required/optional statuses, and
   security sensitivity classifications.
2. **Strict Startup Validation (`validate_configuration` & `validate_production_configuration`)**:
   - Rejects unauthenticated API bindings to non-loopback addresses (`0.0.0.0`), enforcing a
     minimum token length of 16 characters.
   - Automatically detects and rejects template placeholders starting with `replace_with_` on
     production interfaces and required secret fields.
   - Rejects incompatible in-memory SQLite storage (`:memory:`) when running under distributed
     Celery execution.
   - Rejects invalid broker and result backend URL schemes (restricting to `redis://`, `rediss://`,
     `amqp://`, `rpc://`, `sqs://`).
   - Enforces production webhook transport safety (requiring `https://`, TLS verification, and
     HMAC signing).
3. **Multi-Layer Secret Redaction & URL Masking**:
   - Implemented `mask_secret()`, `sanitize_broker_url()`, and `sanitize_text()`.
   - Masks authority passwords (`user:***@host:port`) and query credentials (`?token=***`,
     `?auth=***`, `?secret=***`) in broker URLs and webhook endpoints.
   - Automatically redacts raw secrets from configuration exceptions via `format_config_error()`.
   - Upgraded startup diagnostics (`/diagnostics` and CLI `--diagnostics`) to expose structured
     operational metadata without returning plain-text credentials.
4. **Zero-Downtime Secret Rotation**:
   - Added support for dual-token acceptance in API Bearer authentication via
     `ORCHESTRATOR_API_TOKEN_PREVIOUS` and comma-delimited tokens in `ORCHESTRATOR_API_TOKEN`,
     verified with constant-time `hmac.compare_digest`.
   - Preserved and documented outbound webhook HMAC signing secret rotation via
     `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS`.
5. **Production Templates & Documentation**:
   - Updated `.env.example` with complete, classified inventory of all 101 configuration settings.
   - Updated `.env.docker.example` with safe `replace_with_` placeholders and rotation variables.
   - Updated `docs/DEPLOYMENT_GUIDE.md` with Section 12: Production Configuration & Runbooks.
6. **Testing & Quality Assurance**:
   - Added 21 automated tests in `tests/test_production_configuration_validation.py`.
   - Verified 178 passing tests across the complete Tasks 50–61 regression suite.
   - Zero lines exceeding 110 characters; clean `ruff check` verification.

---

## 2. Configuration Inventory & Precedence

### 2.1 Configuration Precedence Hierarchy
Configuration settings are resolved through a deterministic 4-tier precedence hierarchy:
1. **Explicit In-Code Arguments**: Parameters passed directly into constructors or functions.
2. **Environment Variables**: Values injected via Docker, Kubernetes, or host environment.
3. **Environment Files (`.env`)**: Values loaded from `.env` via `python-dotenv`.
4. **Pydantic Model Defaults**: Safe, offline-ready fallback values defined in `Settings`.

### 2.2 Configuration Inventory by Subsystem

#### LLM Provider & Inference Limits
- `DEFAULT_LLM_PROVIDER`: Default `mock`. Valid: `mock`, `openai`, `anthropic`.
- `FALLBACK_LLM_PROVIDER`: Default `mock`. Valid: `mock`, `openai`, `anthropic`.
- `OPENAI_API_KEY` (Secret): Default `""`. Required if provider is `openai`. Rejects placeholders.
- `ANTHROPIC_API_KEY` (Secret): Default `""`. Required if provider is `anthropic`. Rejects placeholders.
- `LLM_CUSTOM_PRICING` (Sensitive): Default `""`. Optional JSON provider rate override.
- `LLM_TIMEOUT_SECONDS`: Default `30.0`. Request timeout float $> 0.0$.
- `LLM_MAX_RETRIES`: Default `2`. Retry count integer $\ge 0$.
- `MAX_TASK_CHARS`: Default `8000`. Maximum prompt length integer $\ge 100$.

#### Specialist Concurrency & HITL Review
- `MAX_SPECIALIST_CONCURRENCY`: Default `4`. Parallel specialist worker bound (1 to 128).
- `CONFIDENCE_THRESHOLD`: Default `0.6`. Escalation score threshold (0.0 to 1.0).
- `QUALITY_THRESHOLD`: Default `0.6`. Escalation quality score threshold (0.0 to 1.0).
- `MAX_RETRIES`: Default `2`. Intra-workflow retry attempts before review escalation.
- `REVIEW_AUTH_ENABLED`: Default `true`. Requires reviewer authentication (must be `true` in prod).
- `REVIEW_USERS` (Secret): Default `dev_admin:...`. Comma-delimited `user:token` credentials.
- `REVIEW_SESSION_TIMEOUT_MINUTES`: Default `15`. Reviewer session expiration.

#### Tool Approvals & Admission Leases
- `TOOL_APPROVAL_TIMEOUT_SECONDS`: Default `3600.0`. Pending tool approval timeout (1s to 86400s).
- `TOOL_APPROVAL_MAX_EXPIRATION_SECONDS`: Default `86400.0`. Max permitted approval horizon.
- `TOOL_APPROVAL_SWEEPER_INTERVAL_SECONDS`: Default `60.0`. Interval between expired approval sweeps.
- `ADMISSION_LEASE_DURATION_SECONDS`: Default `30.0`. Specialist lease duration $> 0.0$.
- `ADMISSION_LEASE_RENEWAL_INTERVAL_SECONDS`: Default `10.0`. Lease renewal period $<$ duration.
- `ADMISSION_STALE_LEASE_THRESHOLD_SECONDS`: Default `60.0`. Stale lease detection threshold.

#### HTTP API Server & Authentication
- `API_HOST`: Default `127.0.0.1`. Non-loopback (`0.0.0.0`) strictly requires `ORCHESTRATOR_API_TOKEN`.
- `API_PORT`: Default `8000`. TCP listening port (1 to 65535).
- `ORCHESTRATOR_API_TOKEN` (Secret): Default `""`. Min 16 chars on non-loopback; rejects placeholders.
- `ORCHESTRATOR_API_TOKEN_PREVIOUS` (Secret): Default `""`. Secondary token for zero-downtime rotation.
- `API_MAX_BODY_BYTES`: Default `1000000` (1MB). Maximum HTTP request payload body size.
- `API_RATE_LIMIT_ENABLED`: Default `true`. Enables sliding-window client rate limiting.
- `API_RATE_LIMIT_REQUESTS`: Default `60`. Requests allowed per window integer $\ge 1$.
- `API_RATE_LIMIT_WINDOW_SECONDS`: Default `60.0`. Rate limit evaluation window.
- `API_CORS_ORIGINS`: Default `http://localhost:8501,http://127.0.0.1:8501`. CORS whitelist.
- `API_TRUSTED_PROXIES`: Default `127.0.0.1,::1,localhost`. Trusted reverse proxy IPs/CIDRs.
- `API_RECOVERY_ON_STARTUP`: Default `true`. Recovers interrupted runs on service launch.
- `API_SHUTDOWN_GRACE_SECONDS`: Default `5.0`. Max drain seconds during SIGTERM.

#### Persistence, Storage & Audit Logs
- `CHECKPOINT_ENABLED`: Default `true`. Enables SQLite checkpoint persistence.
- `CHECKPOINT_DB_PATH`: Default `orchestrator_checkpoints.db`. Rejects `:memory:` in Celery mode.
- `AUDIT_RETENTION_DAYS`: Default `90`. Retention period (1 to 3650 days).
- `AUDIT_CLEANUP_ON_STARTUP`: Default `false`. Purges expired audit events during startup.

#### Distributed Background Execution (Celery + Redis)
- `ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND`: Default `sync`. Valid: `sync` or `celery`.
- `CELERY_ENABLED`: Default `false`. Explicit activation boolean.
- `CELERY_BROKER_URL` (Sensitive): Default `redis://localhost:6379/0`. Valid schemes: redis, amqp, sqs.
- `CELERY_RESULT_BACKEND` (Sensitive): Default `redis://localhost:6379/0`. Valid schemes: redis, db+sqlite.
- `CELERY_TASK_DEFAULT_QUEUE`: Default `orchestrator_workflows`. Task queue identifier.
- `CELERY_WORKER_CONCURRENCY`: Default `4`. Concurrency slots per worker (1 to 64).
- `CELERY_ALWAYS_EAGER`: Default `false`. Rejected as `true` in production mode.
- `CELERY_WORKER_HEARTBEAT_INTERVAL_SECONDS`: Default `15.0`. Heartbeat frequency.
- `CELERY_WORKER_STALE_THRESHOLD_SECONDS`: Default `60.0`. Stale worker threshold.
- `CELERY_WORKFLOW_STUCK_THRESHOLD_SECONDS`: Default `300.0`. Stuck run detection timeout.
- `CELERY_RECOVERY_BATCH_SIZE`: Default `50`. Max runs recovered per reconciliation cycle.

#### Transactional Outbox & Outbound Webhook Delivery
- `OUTBOX_ENABLED`: Default `true`. Reliable transactional event publishing.
- `OUTBOX_DISPATCH_INTERVAL_SECONDS`: Default `5.0`. Polling interval for unpublished events.
- `OUTBOX_BATCH_SIZE`: Default `20`. Maximum events dispatched per batch.
- `OUTBOX_MAX_ATTEMPTS`: Default `5`. Max delivery retry attempts before failure marking.
- `OUTBOX_RETRY_BACKOFF_SECONDS`: Default `2.0`. Exponential backoff base multiplier.
- `OUTBOX_MAX_BACKOFF_SECONDS`: Default `60.0`. Maximum retry backoff cap.
- `OUTBOX_RETENTION_DAYS`: Default `14`. Age after which delivered outbox events are purged.
- `OUTBOX_CLEANUP_INTERVAL_SECONDS`: Default `3600.0`. Periodic background purger frequency.
- `OUTBOX_DRAIN_ON_SHUTDOWN`: Default `true`. Flushes pending outbox events during shutdown.
- `NOTIFICATION_PROVIDER`: Default `none`. Valid: `none`, `log`, `webhook`.
- `NOTIFICATION_WEBHOOK_URL` (Sensitive): Default `""`. HTTPS destination URL in production.
- `NOTIFICATION_WEBHOOK_AUTH_TOKEN` (Secret): Default `""`. Bearer token for webhook target.
- `NOTIFICATION_WEBHOOK_SIGNING_SECRET` (Secret): Default `""`. HMAC key; required in prod webhook mode.
- `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS` (Secret): Default `""`. Key for zero-downtime rotation.
- `NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS`: Default `10.0`. Outbound HTTP client timeout.
- `NOTIFICATION_WEBHOOK_MAX_RETRIES`: Default `3`. Delivery retry count.
- `NOTIFICATION_WEBHOOK_TLS_VERIFY`: Default `true`. Must be `true` in production mode.
- `NOTIFICATION_WEBHOOK_ALLOW_INSECURE_HTTP`: Default `false`. Must be `false` in production mode.
- `WEBHOOK_REPLAY_STORE_BACKEND`: Default `sqlite`. Valid: `sqlite`, `memory`.
- `WEBHOOK_REPLAY_RETENTION_SECONDS`: Default `86400.0`. Retention horizon for event signatures.
- `WEBHOOK_REPLAY_CLEANUP_INTERVAL_SECONDS`: Default `3600.0`. Periodic replay sweep frequency.
- `WEBHOOK_REPLAY_CLEANUP_BATCH_SIZE`: Default `1000`. Max replay rows purged per transaction.

---

## 3. Startup Validation Architecture

Startup validation runs synchronously during application startup in `run_server()`
(`src/orchestrator/api/server.py`) and CLI invocations (`src/orchestrator/main.py`), ensuring that
invalid or insecure configurations fail immediately before opening network ports or attempting to
process workflows.

### 3.1 Network Binding Security
When binding to non-loopback host interfaces (e.g. `0.0.0.0` or public IP):
- Requires an explicit `ORCHESTRATOR_API_TOKEN`.
- Requires the token length to be at least 16 characters.
- Forbids placeholder values beginning with `replace_with_`.
- When bound to loopback interfaces (`127.0.0.1`, `localhost`, `::1`), token authentication remains
  optional to support seamless zero-setup offline development.

### 3.2 Incompatible Settings Detection
- **In-Memory SQLite with Distributed Celery**: Running `workflow_execution_backend="celery"` with
  `checkpoint_db_path=":memory:"` is rejected. Separate OS processes cannot share in-memory SQLite tables,
  leading to divergent workflow states. Persistent storage is strictly required.
- **Eager Celery in Production**: `validate_production_configuration()` rejects
  `celery_always_eager=True` to prevent silent synchronous task processing when distributed background
  scaling is expected.
- **Insecure Production Webhooks**: In production, `notification_provider="webhook"` requires:
  - `notification_webhook_signing_secret` configured and not a placeholder.
  - `notification_webhook_tls_verify=True`.
  - `notification_webhook_allow_insecure_http=False`.
  - Endpoint URL scheme must be `https://`.

### 3.3 Broker and Backend URL Validation
Connection strings for `celery_broker_url`, `celery_result_backend`, and `redis_url` are checked against
whitelisted URI schemes:
- Celery Broker: `redis://`, `rediss://`, `amqp://`, `rpc://`, `sqs://`.
- Celery Result Backend: `redis://`, `rediss://`, `rpc://`, `db+sqlite://`.
- Redis Memory URL: `redis://`, `rediss://`.

---

## 4. Secrets Management & Privacy Safeguards

The orchestrator guarantees that credentials and sensitive material are protected throughout the system
lifecycle:

### 4.1 URL Credential Sanitization (`sanitize_broker_url`)
Connection strings frequently embed passwords in the URI authority (`user:pass@host:port`) or in query
parameters (`?token=secret`, `?auth=key`). `sanitize_broker_url()` masks both dimensions:
- `redis://:secret_pass@redis:6379/0` $\rightarrow$ `redis://:***@redis:6379/0`
- `amqp://user:pass@rabbit:5672//` $\rightarrow$ `amqp://user:***@rabbit:5672//`
- `redis://redis:6379/0?auth=supersecret&token=abc` $\rightarrow$ `redis://redis:6379/0?auth=***&token=***`

### 4.2 Error Formatting Redaction (`format_config_error`)
When configuration validation fails, exception details are filtered through `sanitize_text()`:
- Specific known secrets from `Settings` are replaced with `[REDACTED]`.
- Regex patterns detect and redact OpenAI API keys (`sk-...` $\rightarrow$ `[REDACTED_API_KEY]`) and Bearer
  authorization tokens (`Bearer ...` $\rightarrow$ `Bearer [REDACTED_TOKEN]`).

### 4.3 Safe Startup Diagnostics (`get_startup_diagnostics`)
The runtime `/diagnostics` route and CLI `--diagnostics` flag expose operational telemetry while masking
credentials:
- API token status reports boolean presence and rotation state:
  `"enabled (token rotation active: primary + previous configured)"`
- Webhook URLs are stripped of sensitive query parameters.
- LLM API keys report presence flags (`has_openai_key: True`, `has_anthropic_key: False`) without printing.
- Broker URLs report masked hosts and paths (`redis://:***@redis:6379/0`).

---

## 5. Zero-Downtime Secret Rotation

The orchestrator provides dual-secret verification allowing credentials to be rotated seamlessly without
service interruptions or rejected requests.

### 5.1 API Bearer Token Rotation
In `src/orchestrator/api/routes.py`, `verify_api_token()` extracts:
- Primary tokens from `ORCHESTRATOR_API_TOKEN` (supports single token or comma-separated tokens).
- Previous tokens from `ORCHESTRATOR_API_TOKEN_PREVIOUS`.
- Compares incoming Bearer tokens against the list of valid tokens using constant-time comparison
  (`hmac.compare_digest`) to protect against timing side-channel attacks.

### 5.2 Webhook HMAC Signing Key Rotation
In `src/orchestrator/notifications/signing.py` and `webhook.py`:
- Outbound webhooks are signed using `notification_webhook_signing_secret`.
- Downstream verification functions evaluate both `notification_webhook_signing_secret` and
  `notification_webhook_signing_secret_previous` before rejecting payloads.

### 5.3 Operational Rotation Runbook (Rolling Deployment)
1. **Stage 1: Generate New Credential**
   ```bash
   NEW_TOKEN=$(openssl rand -hex 32)
   ```
2. **Stage 2: Configure Dual Acceptance**
   Update `.env` on orchestrator nodes:
   ```env
   ORCHESTRATOR_API_TOKEN=<NEW_TOKEN>
   ORCHESTRATOR_API_TOKEN_PREVIOUS=<OLD_TOKEN>
   ```
   Perform a rolling restart: `docker compose up -d api`
   Both `<NEW_TOKEN>` and `<OLD_TOKEN>` are now accepted by the API.
3. **Stage 3: Update Upstream Clients**
   Distribute `<NEW_TOKEN>` to all upstream API consumers and client applications.
4. **Stage 4: Retire Old Credential**
   Remove `ORCHESTRATOR_API_TOKEN_PREVIOUS` from `.env` and redeploy:
   ```env
   ORCHESTRATOR_API_TOKEN=<NEW_TOKEN>
   ORCHESTRATOR_API_TOKEN_PREVIOUS=
   ```
   The old credential is now completely revoked.

---

## 6. Verification & Test Suite Results

### 6.1 Dedicated Configuration & Secrets Test Suite
`tests/test_production_configuration_validation.py` contains 21 focused unit and integration tests:
- `test_loopback_binding_permits_empty_token`: Validates unauthenticated local development.
- `test_non_loopback_binding_requires_token`: Validates token requirement on `0.0.0.0`.
- `test_non_loopback_binding_rejects_placeholder_token`: Rejects `replace_with_` token templates.
- `test_non_loopback_binding_rejects_short_token`: Rejects tokens shorter than 16 characters.
- `test_non_loopback_binding_accepts_strong_token`: Accepts valid $\ge 16$-char tokens.
- `test_inmemory_sqlite_rejected_in_celery_mode`: Rejects `:memory:` SQLite with Celery execution.
- `test_valid_sqlite_path_accepted_in_celery_mode`: Accepts valid SQLite path with Celery execution.
- `test_invalid_celery_broker_scheme_rejected`: Rejects unsupported broker URI schemes (e.g. `ftp://`).
- `test_invalid_celery_backend_scheme_rejected`: Rejects unsupported result backend schemes.
- `test_valid_celery_urls_accepted`: Validates standard `redis://` and `amqp://` schemes.
- `test_placeholder_secrets_rejected`: Rejects `replace_with_` across LLM and webhook secret fields.
- `test_sanitize_broker_url_masks_password`: Masks authority passwords in connection strings.
- `test_sanitize_broker_url_masks_query_credentials`: Masks sensitive query parameters.
- `test_sanitize_text_redacts_credentials`: Redacts known secrets and regex matches.
- `test_format_config_error_redacts_secrets`: Validates non-leaking exception formatting.
- `test_get_startup_diagnostics_privacy_audit`: Verifies that diagnostics omit raw secrets.
- `test_api_token_rotation_via_previous_field`: Validates dual token matching via `api_token_previous`.
- `test_api_token_rotation_via_comma_separated_tokens`: Validates comma-separated token list.
- `test_api_server_routes_with_token_rotation`: End-to-end FastAPI test with `TestClient` and `/diagnostics`.
- `test_production_validation_succeeds_on_valid_stack`: Verifies production stack validation.
- `test_production_validation_catches_all_production_violations`: Verifies each production invariant.

**Result**: 21 passed in 2.78s.

### 6.2 Full Regression Test Suite (Tasks 50–61)
Executed the complete regression suite across all preceding tasks:
- `tests/test_transactional_outbox.py` (12 tests)
- `tests/test_webhook_signing.py` (15 tests)
- `tests/test_durable_replay_protection.py` (11 tests)
- `tests/test_replay_store_maintenance.py` (11 tests)
- `tests/test_celery_execution.py` (12 tests)
- `tests/test_celery_observability_recovery.py` (12 tests)
- `tests/test_celery_failure_injection.py` (16 tests)
- `tests/test_deployment_readiness.py` (14 tests, 1 skipped)
- `tests/test_docker_runtime_validation.py` (5 tests, 2 skipped)
- `tests/test_config_validation.py` (52 tests)
- `tests/test_production_configuration_validation.py` (21 tests)

**Regression Result**: **178 passed, 3 skipped in 72.79s**.

---

## 7. Environmental Honesty & Compliance

1. **Environmental Honesty**: The host machine (Windows 11 with Python 3.11.3) lacks a running Docker daemon.
   The 3 skipped tests in `test_deployment_readiness.py` and `test_docker_runtime_validation.py` were
   gracefully bypassed via pytest skip marks. No tests were mocked to falsify container runtime execution.
2. **Line Length Limit ($\le 110$ chars)**: All modified and newly created files (`config.py`, `routes.py`,
   `server.py`, `main.py`, `.env.example`, `.env.docker.example`, `DEPLOYMENT_GUIDE.md`, test files, and this
   report) strictly adhere to the $\le 110$ character line length limit.
3. **No Commits/Pushes**: No git commits or remote pushes were executed, preserving repository state for user
   inspection.
