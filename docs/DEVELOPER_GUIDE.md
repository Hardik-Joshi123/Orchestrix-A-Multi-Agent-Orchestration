# Multi-Agent Orchestrator - Developer Quick-Start Guide

Welcome! This guide provides a direct, reproducible workflow to take you from a fresh repository clone to running, testing, and developing on the Multi-Agent Orchestration System.

---

## 1. Prerequisites

- **Python**: `3.10`, `3.11`, or `3.12` installed.
- **Git**: Installed and configured.
- **No external accounts or API keys required**: The entire system runs out-of-the-box in deterministic offline mode using a local mock provider and SQLite persistence.

---

## 2. Step-by-Step Setup Workflow

### Step 1: Clone the Repository & Navigate to Project Root
```powershell
# Windows (PowerShell)
cd C:\Users\Dell\OneDrive\Desktop\Project\multi-agent-orchestrator
```
```bash
# macOS / Linux (Bash)
cd multi-agent-orchestrator
```

---

### Step 2: Create and Activate a Dedicated Virtual Environment
Always work inside an isolated virtual environment to prevent package version conflicts:

```powershell
# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```
```bash
# macOS / Linux (Bash)
python3 -m venv .venv
source .venv/bin/activate
```

> **Windows PowerShell Execution Policy Note:** If `.Activate.ps1` produces a script execution restriction error, run:
> ```powershell
> Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
> .\.venv\Scripts\Activate.ps1
> ```

---

### Step 3: Install the Package

Choose the installation tier matching your development workflow:

#### Option A: Full Development Environment (Recommended)
Installs core orchestration, FastAPI REST server, pytest test suites, and ruff linter:
```powershell
# Windows & Unix
pip install -e ".[api,dev]"
```

#### Option B: Minimal Core Installation Only
Installs only the lightweight core dependencies (`pydantic` and `pydantic-settings`):
```powershell
pip install -e .
```

#### Option C: All Optional Backing Integrations
Installs LangGraph, OpenAI, Anthropic, ChromaDB, Redis, Celery, Streamlit, and OpenTelemetry:
```powershell
pip install -e ".[all]"
```

---

### Step 4: Configure Local Environment Variables

Copy the provided environment template to `.env`:
```powershell
# Windows (PowerShell)
Copy-Item .env.example .env
```
```bash
# macOS / Linux (Bash)
cp .env.example .env
```

The pre-configured settings in `.env.example` run offline safely:
- `DEFAULT_LLM_PROVIDER=mock`: Deterministic offline execution with zero API keys.
- `CHECKPOINT_ENABLED=true`: Durable SQLite state persistence (`orchestrator_checkpoints.db`).
- `API_HOST=127.0.0.1` and `API_PORT=8000`: Local loopback API server.
- `REVIEW_USERS=dev_admin:dev_token_12345`: Development reviewer credentials.

---

### Step 5: Validate Configuration & Run Diagnostics

Run the diagnostics check to verify that all settings load cleanly:
```powershell
# Using the installed console script:
orchestrator --diagnostics

# Or invoking as a module (useful if user Scripts are not on system PATH):
python -m orchestrator --diagnostics
```

Expected output (emitted to `stderr` to keep `stdout` clean for pipelines):
```
Multi-Agent Orchestrator Configuration Diagnostics:
  - LLM Provider:        mock (fallback: mock)
  - LLM Safety:          timeout=30.0s, retries=2, pricing=default provider rates
  - Concurrency Limit:   4 specialists
  - Persistence:         backend=sqlite, path=orchestrator_checkpoints.db, enabled=True
  - API Server:          http://127.0.0.1:8000
  - API Security:        auth=disabled (loopback dev mode), max_body=1.0MB
  - API Rate Limit:      enabled (60 req / 60s)
```

To emit machine-readable diagnostics on `stdout`:
```powershell
python -m orchestrator --diagnostics --json
```

---

### Step 6: Execute Your First Workflow

#### Human-Readable Terminal Run
```powershell
python -m orchestrator "Research the history of algorithms"
```

#### Structured JSON Output
Logs and telemetry print to `stderr`; clean JSON is output to `stdout` for piping:
```powershell
python -m orchestrator "Explain the Byzantine Generals Problem" --json
```

#### Inspect Detailed Graph Trace Spans
```powershell
python -m orchestrator "Analyze distributed transaction consistency" --trace
```

---

### Step 7: Run the Offline Workflow Evaluation Suite

Run the 9-case deterministic offline evaluation framework to test graph planning, dependency resolution, retry recovery, and HITL escalation:
```powershell
python -m orchestrator --eval
```

To output full evaluation metrics as structured JSON:
```powershell
python -m orchestrator --eval --json
```

---

### Step 8: Start and Test the HTTP API Server

Start the local FastAPI REST service:
```powershell
python -m orchestrator --serve
# Serves on http://127.0.0.1:8000
```

In a separate terminal, test the service health:
```powershell
# Windows (PowerShell)
Invoke-RestMethod -Uri http://127.0.0.1:8000/health
```
```bash
# macOS / Linux (cURL)
curl -s http://127.0.0.1:8000/health
```

Expected JSON response:
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "checkpoint_backend": "sqlite",
  "persistence_enabled": true,
  "active_llm_provider": "mock",
  "active_runs_count": 0,
  "auth_required": false
}
```

Submit a workflow run:
```powershell
# Windows (PowerShell)
$body = @{ task = "Research sorting algorithms" } | ConvertTo-Json
$run = Invoke-RestMethod -Uri http://127.0.0.1:8000/runs -Method Post -Body $body -ContentType "application/json"
$run.run_id
```

---

### Step 9: Launch the Human-in-the-Loop (HITL) Dashboard

To launch the Streamlit human review interface:
```powershell
streamlit run ui/review_app.py
```
Open `http://localhost:8501` in your browser. Log in with the default development reviewer credentials configured in `.env`:
- **Username**: `dev_admin`
- **Token**: `dev_token_12345`

---

## 3. Running Test Suites & Quality Checks

Run the automated test suite and linter:

```powershell
# Run the packaging and setup smoke tests
python -m pytest tests/test_setup_packaging.py

# Run the end-to-end integration suite
python -m pytest tests/test_end_to_end.py

# Run all test suites
python -m pytest tests/

# Run Ruff lint checks
python -m ruff check src/ tests/

# Run Ruff formatting check
python -m ruff format --check src/ tests/
```

---

## 4. Key CLI Commands Reference

| Command | Description |
|---|---|
| `python -m orchestrator "<task>"` | Execute a workflow synchronously |
| `python -m orchestrator "<task>" --json` | Output pure JSON payload to `stdout` |
| `python -m orchestrator "<task>" --trace` | Print OpenTelemetry spans and trace events |
| `python -m orchestrator "<task>" --concurrency 8` | Set maximum parallel specialist threads |
| `python -m orchestrator --status <run_id>` | Query execution status of a saved run |
| `python -m orchestrator --resume <run_id>` | Resume an interrupted workflow from its checkpoint |
| `python -m orchestrator --diagnostics` | Print startup configuration diagnostics |
| `python -m orchestrator --eval` | Execute the offline evaluation benchmark suite |
| `python -m orchestrator --serve` | Start the FastAPI HTTP REST server |

---

## 5. Graceful Shutdown & Interrupted Run Recovery

### 5.1 Graceful Shutdown Lifecycle
When the FastAPI API server or execution manager receives a termination signal (`SIGINT` / `Ctrl+C` or `SIGTERM`):
1. **Admission Rejection**: The server enters shutting-down mode and immediately stops accepting new workflow submissions (`POST /runs`) or resumptions (`POST /runs/{id}/resume`), returning `HTTP 503 Service Unavailable`.
2. **Cooperative Cancellation**: Active in-flight worker threads are signaled cooperatively via their `CancellationToken`. Unstarted subtasks in the dispatch queue are cancelled immediately.
3. **Bounded Grace Period Drain**: The server waits for active specialist workers to finalize their current execution boundary up to `API_SHUTDOWN_GRACE_SECONDS` (default: `5.0` seconds).
4. **Indefinite Hang Prevention**: If a specialist or external call remains blocked after the grace period expires, background worker threads are detached via `executor.shutdown(wait=False, cancel_futures=True)`. Lingering runs are safely transitioned to `interrupted` in SQLite persistence before process exit.

### 5.2 Startup Recovery Semantics
When the application starts up:
1. **Interrupted Run Detection**: The database is scanned for runs that were left in active/running status (`status IN ('running', 'in_progress')`) due to host restart or process crashes.
2. **Safe Transition to 'interrupted'**: Each active run is recovered into an explicit, resumable `interrupted` state. All completed subtask outputs, generated answers, and checkpoint history are preserved.
3. **Terminal Immunity**: Terminal statuses (`completed`, `failed`, `cancelled`, `timed_out`, `approved`, `edited`, `rejected`) are strictly protected and never modified.
4. **No Unintended Double-Execution**: Interrupted runs are not executed automatically on startup. Instead, they remain safely paused in the `interrupted` state until explicitly resumed by a user or operator.
5. **Idempotence**: Startup recovery is strictly idempotent; repeated application restarts or recovery calls will not create duplicate checkpoints or corrupt run states.

### 5.3 Resuming an Interrupted Run
An interrupted run can be resumed via either CLI or API:
```powershell
# Resume via CLI
python -m orchestrator --resume run-1234567890ab

# Resume via HTTP API
curl -X POST http://127.0.0.1:8000/runs/run-1234567890ab/resume
```
Resumption bypasses all completed subtasks (`status == "done"`), resets the run status from `interrupted` to `running`, and executes only the remaining pending subtasks.

### 5.4 Configuration Options
| Setting | Env Variable | Default | Description |
|---|---|---|---|
| `api_shutdown_grace_seconds` | `ORCHESTRATOR_SHUTDOWN_GRACE_SECONDS` | `5.0` | Maximum seconds to wait for active workers during shutdown (0 to 300s). |
| `api_recovery_on_startup` | `ORCHESTRATOR_RECOVERY_ON_STARTUP` | `true` | Whether to automatically scan and recover interrupted runs on server startup. |

### 5.5 Known Limitations
- **Immediate Process Termination (`SIGKILL`)**: A hard kill (`kill -9` or task termination) terminates the process instantly without running shutdown hooks. Such runs will remain marked `running` in SQLite until the next server startup, where startup recovery will safely transition them to `interrupted`.
- **In-Process vs Multi-Host Coordination**: In-process locks guard against concurrency races within a single process. For multi-node distributed deployments, a centralized coordinator (such as Redis leases or database row locks) is recommended.

---

## 6. Production Health Checks & Runtime Diagnostics

The orchestration HTTP API provides specialized operational probes and runtime diagnostics designed for container orchestrators (e.g. Kubernetes, Docker Compose, AWS ECS) and human operators.

### 6.1 Endpoints Overview

| Endpoint | Purpose | HTTP Status Codes | Authentication |
|---|---|---|---|
| `GET /health` | Liveness probe; verifies process responsiveness | `200 OK` | Public / Unauthenticated |
| `GET /ready` | Readiness probe; verifies ability to accept new runs | `200 OK`, `503 Service Unavailable` | Public / Unauthenticated |
| `GET /diagnostics` | Operational diagnostics snapshot & runtime telemetry | `200 OK`, `401 Unauthorized` | Protected (Requires Bearer token when configured) |


### 6.2 Liveness Probe (`GET /health`)
A lightweight, non-blocking check that does not touch disk or persistence locks. Use this for Kubernetes `livenessProbe` to detect hung or deadlocked processes.

**Example Request:**
```bash
curl -s http://127.0.0.1:8000/health
```

**Example Response (`200 OK`):**
```json
{
  "status": "healthy",
  "version": "0.1.0",
  "timestamp": "2026-10-23T12:00:00.000000+00:00",
  "checkpoint_backend": "sqlite",
  "persistence_enabled": true,
  "active_llm_provider": "mock",
  "active_runs_count": 0,
  "auth_required": false
}
```

### 6.3 Readiness Probe (`GET /ready`)
Indicates whether the API server is currently prepared to accept new workflow executions (`POST /runs`) or resumption requests (`POST /runs/{id}/resume`).

Readiness returns `503 Service Unavailable` when:
1. **Application is shutting down**: `manager.is_shutting_down` is `True`.
2. **Persistence dependency is unhealthy**: SQLite file is unreadable, disk full, corrupt, or inaccessible.

**Example Response when Ready (`200 OK`):**
```json
{
  "status": "ready",
  "timestamp": "2026-10-23T12:00:00.000000+00:00",
  "accepting_runs": true,
  "database": "healthy",
  "active_runs": 0,
  "reason": null
}
```

**Example Response when Shutting Down (`503 Service Unavailable`):**
```json
{
  "status": "not_ready",
  "timestamp": "2026-10-23T12:00:00.000000+00:00",
  "accepting_runs": false,
  "database": "healthy",
  "active_runs": 2,
  "reason": "Service is shutting down"
}
```

**Example Response when Persistence Fails (`503 Service Unavailable`):**
```json
{
  "status": "not_ready",
  "timestamp": "2026-10-23T12:00:00.000000+00:00",
  "accepting_runs": false,
  "database": "unhealthy",
  "active_runs": 0,
  "reason": "Database probe failed: Database connection failure"
}
```

### 6.4 Runtime Diagnostics (`GET /diagnostics`)
Returns an authoritative operational snapshot detailing application state, lifecycle status breakdown, persistence connectivity, configured runtime limits, and startup recovery history.

- **Zero Secret Leakage**: API tokens, private keys, passwords, and user host directory paths are never exposed. Database file names are sanitized to safe basenames.
- **Lock-Safe & Non-Blocking**: DB checks and status aggregations use bounded acquisition timeouts to prevent blocking in-flight workflows.

**Example Request:**
```bash
curl -s http://127.0.0.1:8000/diagnostics
```

**Example Response (`200 OK`):**
```json
{
  "timestamp": "2026-10-23T12:00:00.000000+00:00",
  "application": {
    "version": "0.1.0",
    "status": "healthy",
    "is_shutting_down": false,
    "started_at": "2026-10-23T11:50:00.000000+00:00",
    "auth_required": false
  },
  "execution": {
    "active_runs_count": 0,
    "lifecycle_counts": {
      "completed": 12,
      "interrupted": 1,
      "failed": 2,
      "cancelled": 0
    },
    "specialist_concurrency": 4,
    "max_task_chars": 10000,
    "workflow_timeout_seconds": null
  },
  "persistence": {
    "enabled": true,
    "backend": "sqlite",
    "status": "healthy",
    "db_name": "orchestrator_checkpoints.db",
    "detail": null
  },
  "limits": {
    "shutdown_grace_seconds": 5.0,
    "recovery_on_startup": true,
    "rate_limit_enabled": true,
    "rate_limit_requests": 60,
    "rate_limit_window_seconds": 60.0,
    "max_body_bytes": 1048576,
    "max_body_mb": 1.05
  },
  "recovery": {
    "status": "completed",
    "recovered_count": 1,
    "recovered_run_ids": ["run-interrupted-abc"],
    "timestamp": "2026-10-23T11:50:01.000000+00:00"
  }
}
```

### 6.5 Recommended Kubernetes Probe Configuration

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 2
```

### 6.6 Troubleshooting Operational Issues

| Symptom / Reason | Underlying Cause | Recommended Operator Action |
|---|---|---|
| `/ready` returns `503` with `Service is shutting down` | Host or orchestrator initiated pod termination or restart. | Allow active workers to drain; orchestrator will redirect ingress traffic to other ready pods. |
| `/ready` returns `503` with `Database probe failed` | SQLite checkpoint database is locked, unwritable, or disk volume is full. | Verify file permissions on checkpoint path, verify free disk space, and check journal files (`.db-wal`). |
| In-memory active runs high, but workers idle | Workflows waiting for human-in-the-loop review. | Check Streamlit review dashboard (`http://localhost:8501`) or query `/runs/{id}` to review pending approvals. |
| High count of `interrupted` runs in `/diagnostics` | Prior process was forcefully killed (`SIGKILL`) or crashed during run execution. | Query `/runs/{id}` and resume interrupted workflows using `POST /runs/{id}/resume`. |

---

## 7. Operational Endpoint Security & Access-Control Policy

The API enforces a uniform, fail-safe security architecture designed to prevent accidental exposure of workflow execution and sensitive system diagnostics while keeping infrastructure health checks accessible to automated orchestrators.

### 7.1 Comprehensive Endpoint Access Matrix

| Endpoint | Method | Classification | Auth Policy (Token Set) | Dev Mode (No Token) | Rate Limited |
|---|---|---|---|---|---|
| `/health` | `GET` | Infrastructure Liveness Probe | Public (`200 OK`) | Public (`200 OK`) | No |
| `/ready` | `GET` | Infrastructure Readiness Probe | Public (`200 OK` / `503`) | Public (`200 OK` / `503`) | No |
| `/diagnostics` | `GET` | Protected Telemetry & Limits | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/runs` | `POST` | Protected State Mutation (Create)<br>*Optional `Idempotency-Key` header* | `401 Unauthorized` without token<br>`201 Created` with Bearer token<br>`409 Conflict` on payload mismatch | Public (`201 Created` on loopback) | Yes |
| `/runs/{id}` | `GET` | Protected Workflow Inspection | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/runs/{id}/resume` | `POST` | Protected State Mutation (Resume) | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | Yes |
| `/runs/{id}/cancel` | `POST` | Protected State Mutation (Cancel) | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | Yes |
| `/runs/{id}/checkpoints` | `GET` | Protected Checkpoint Inspection | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/runs/{id}/audit` | `GET` | Protected Audit History | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/runs/{id}/audit/export` | `GET` | Protected Audit Export (Run) | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/audit/export` | `GET` | Protected Audit Export (System) | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |
| `/audit/cleanup` | `POST` | Protected Audit Retention Purge | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | Yes |
| `/evaluation` | `GET` | Protected Benchmark Inspection | `401 Unauthorized` without token<br>`200 OK` with Bearer token | Public (`200 OK` on loopback) | No |

### 7.2 Why `/ready` Remains Public While `/diagnostics` Is Protected

- **Readiness (`/ready`)**:
  Container orchestrators (Kubernetes kubelet, AWS ALB, GCP Cloud Run) poll readiness probes to determine when traffic should route to a container. These systems typically cannot supply dynamic Bearer tokens without complex sidecars. `/ready` returns only a coarse binary indicator (`ready` / `not_ready`), database status (`healthy` / `unhealthy` / `disabled`), active workflow count, and a sanitized status string. It exposes no environment variables, file contents, or database paths, making public access safe and standards-compliant.
- **Diagnostics (`/diagnostics`)**:
  Returns comprehensive operational telemetry, including run counts grouped by lifecycle status, specialist concurrency limits, startup recovery summaries (with run IDs), and runtime limits. To protect this operational metadata from unauthenticated network scrapers, `/diagnostics` requires a valid Bearer token whenever authentication is configured.

### 7.3 Authentication Configuration & Mechanics

To enforce authentication, configure `ORCHESTRATOR_API_TOKEN`:
```bash
# Set token via environment variable
export ORCHESTRATOR_API_TOKEN="your-secure-random-token-here"

# Or configure in .env
ORCHESTRATOR_API_TOKEN=your-secure-random-token-here
```

**Security Guarantees:**
1. **Constant-Time Comparison**: Bearer tokens are validated using `hmac.compare_digest` to prevent timing side-channel attacks.
2. **Local Loopback Dev Mode**: When `ORCHESTRATOR_API_TOKEN` is unset or empty, the server operates in unauthenticated development mode on loopback interfaces (`127.0.0.1`, `localhost`, `::1`).
3. **Safe Network Binding Guard**: The API server enforces fail-fast startup validation: attempting to bind to non-loopback addresses (`0.0.0.0`, LAN IPs) without a configured `ORCHESTRATOR_API_TOKEN` raises `ValueError` and terminates startup immediately.

### 7.4 Safe Deployment Recommendations

1. **Always Set a Token in Production**: Generate a high-entropy secret token (e.g. `openssl rand -hex 32`) and supply it via `ORCHESTRATOR_API_TOKEN`.
2. **Restrict CORS Origins**: Set `ORCHESTRATOR_CORS_ORIGINS` to the exact hostnames of authorized dashboards or clients (e.g. `http://dashboard.internal:8501`). Avoid wildcard `*` origins.
3. **Configure Trusted Reverse Proxies**: When running behind Nginx, Envoy, or an AWS Application Load Balancer, specify trusted proxy CIDRs/IPs via `ORCHESTRATOR_TRUSTED_PROXIES` so the rate limiter accurately identifies client IPs from `X-Forwarded-For` rather than the proxy IP.
4. **Enforce Request Size Limits**: The server defaults to a `1MB` payload limit (`ORCHESTRATOR_API_MAX_BODY_BYTES=1048576`), protecting against buffer exhaustion and memory denial-of-service attacks.

### 7.5 Standard Error Responses for Denied Requests

All error responses return clean, sanitized JSON bodies without leaking tokens, secrets, filesystem paths, or Python tracebacks.

#### 1. Missing Bearer Token (`HTTP 401 Unauthorized`)
```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
Content-Type: application/json

{
  "detail": "Missing or invalid authentication credentials."
}
```

#### 2. Invalid Bearer Token (`HTTP 401 Unauthorized`)
```http
HTTP/1.1 401 Unauthorized
WWW-Authenticate: Bearer
Content-Type: application/json

{
  "detail": "Invalid authentication token."
}
```

#### 3. Access Forbidden (`HTTP 403 Forbidden`)
```http
HTTP/1.1 403 Forbidden
Content-Type: application/json

{
  "detail": "Access forbidden. Insufficient permissions."
}
```

#### 4. Rate Limit Exceeded (`HTTP 429 Too Many Requests`)
```http
HTTP/1.1 429 Too Many Requests
Retry-After: 45
Content-Type: application/json

{
  "detail": "Rate limit exceeded: maximum 60 requests per 60.0s window. Try again in 45 seconds."
}
```

#### 5. Oversized Request Body (`HTTP 413 Payload Too Large`)
```http
HTTP/1.1 413 Payload Too Large
Content-Type: application/json

{
  "detail": "Request body size (2000000 bytes) exceeds maximum allowed size of 1048576 bytes."
}
```

---

## 8. Structured Audit Logging & Workflow Event History

The system implements structured, privacy-preserving audit logging to provide security, compliance, and operational observability across workflow state transitions, human reviewer decisions, and security events.

### 8.1 Canonical Event Schema

Every audit record conforms to a strongly-typed schema:

```python
@dataclass
class AuditEvent:
    event_id: str               # Unique identifier (e.g. "aud-a1b2c3d4e5f6")
    timestamp: str              # ISO-8601 UTC timestamp
    event_type: str             # Canonical event type identifier
    outcome: str                # Standardized outcome: success, failure, denied, escalated, interrupted, timeout
    run_id: str | None          # Associated workflow run ID, if applicable
    correlation_id: str | None  # Distributed tracing correlation ID
    actor: str | None           # Identity triggering action (e.g. "system", reviewer username, client IP)
    metadata: dict[str, Any]    # Sanitized event-specific metadata dictionary
```

### 8.2 Supported Audit Event Types

| Category | Event Type (`event_type`) | Common Outcome (`outcome`) | Description |
|---|---|---|---|
| **Workflow Lifecycle** | `workflow.created` | `success` | Workflow run received and initialized by `RunManager`. |
| | `workflow.started` | `success` | Worker thread began execution of task graph. |
| | `workflow.completed` | `success` | Graph reached terminal completion with valid result. |
| | `workflow.failed` | `failure` | Graph execution failed due to unrecoverable specialist or supervisor error. |
| | `workflow.resumed` | `success` | Eligible paused/interrupted workflow resumed from checkpoint. |
| | `workflow.cancelled` | `success` | Cooperative cancellation requested and acknowledged. |
| | `workflow.timed_out` | `timeout` | Execution deadline expired, halting subtask scheduling. |
| | `workflow.interrupted` | `interrupted` | Active run stopped due to service termination exceeding grace period. |
| | `workflow.recovered` | `success` | Orphaned active run transitioned to `interrupted` during startup recovery. |
| **Human-in-the-Loop** | `hitl.escalated` | `escalated` | Reviewer item enqueued due to low confidence or sensitive tool execution. |
| | `hitl.approved` | `success` | Authenticated human approved proposed agent action. |
| | `hitl.rejected` | `success` | Authenticated human rejected proposed action, aborting path. |
| | `hitl.edited` | `success` | Authenticated human substituted revised text or took over task. |
| **Security & Protection** | `security.auth_failure` | `denied` | Request rejected due to missing or invalid Bearer token. |
| | `security.rate_limit_exceeded` | `denied` | Client IP exceeded request throughput limit. |
| **Service Lifecycle** | `service.startup` | `success` | API server lifespan initialized and recovery completed. |
| | `service.shutdown` | `success` | API server initiated graceful shutdown and drained active workers. |

### 8.3 Privacy & Data Sanitization Guarantees

Audit logs are retained longer than ephemeral execution traces and must strictly protect user data and credentials:
1. **Zero Credential Persistence**: Keys matching sensitive patterns (`password`, `secret`, `api_key`, `token`, `bearer`, `auth`) are automatically sanitized to `[REDACTED]`.
2. **Key Masking**: OpenAI-style keys (`sk-...`) and Bearer tokens are regex-replaced with `[REDACTED_KEY]` and `Bearer [REDACTED_TOKEN]`.
3. **Traceback Sanitization**: Python tracebacks in error messages are trimmed to only the final exception statement (`ValueError: Specialist failed`), preventing internal codebase disclosure.
4. **Prompt Summarization**: Full task prompt strings are never stored verbatim in audit events; the system records `task_chars` (character length) and a sanitized 80-character `task_preview`.

### 8.4 Persistence Architecture & Safe Failure Policy

- **Dedicated Storage**: Events are persisted to an `audit_events` table within SQLite, indexed by `run_id`, `timestamp`, and `event_type`.
- **Concurrency Isolation**: Database operations are synchronized via a process-wide reentrant lock (`_get_db_lock`), preventing SQLite database locking collisions during concurrent execution.
- **Safe Failure Policy**: In accordance with enterprise reliability best practices, audit persistence failures log a warning and return cleanly without raising unhandled exceptions or corrupting workflow runs.
- **Idempotent Recovery**: Startup recovery applies deduplication (`dedup=True`) to ensure repeated server restarts do not create redundant `workflow.recovered` events for the same run.

### 8.5 Querying Run Audit History

#### API Endpoint: `GET /runs/{run_id}/audit`
Requires Bearer authentication when configured (`401 Unauthorized` without token).

**Query Parameters:**
- `limit` (*integer*, optional, default `100`, range `1–500`): Maximum events to return.
- `offset` (*integer*, optional, default `0`): Pagination offset.
- `event_type` (*string*, optional): Filter events by specific canonical type.

**Example Response (`HTTP 200 OK`):**
```json
{
  "run_id": "run-a1b2c3d4e5f6",
  "total_events": 4,
  "limit": 100,
  "offset": 0,
  "events": [
    {
      "event_id": "aud-018f3a12b4c5",
      "timestamp": "2026-10-24T10:00:00.123456+00:00",
      "event_type": "workflow.created",
      "outcome": "success",
      "run_id": "run-a1b2c3d4e5f6",
      "correlation_id": null,
      "actor": "system",
      "metadata": {
        "task_chars": 42,
        "task_preview": "Analyze sorting algorithms...",
        "background": false
      }
    },
    {
      "event_id": "aud-018f3a12b4c6",
      "timestamp": "2026-10-24T10:00:00.150200+00:00",
      "event_type": "workflow.started",
      "outcome": "success",
      "run_id": "run-a1b2c3d4e5f6",
      "correlation_id": null,
      "actor": "system",
      "metadata": {}
    },
    {
      "event_id": "aud-018f3a12b4c7",
      "timestamp": "2026-10-24T10:00:01.450120+00:00",
      "event_type": "hitl.escalated",
      "outcome": "escalated",
      "run_id": "run-a1b2c3d4e5f6",
      "correlation_id": null,
      "actor": "system",
      "metadata": {
        "reason": "low_confidence",
        "level": "approve_action"
      }
    },
    {
      "event_id": "aud-018f3a12b4c8",
      "timestamp": "2026-10-24T10:00:02.100500+00:00",
      "event_type": "hitl.approved",
      "outcome": "success",
      "run_id": "run-a1b2c3d4e5f6",
      "correlation_id": null,
      "actor": "security-reviewer",
      "metadata": {
        "decision": "approve"
      }
    }
  ]
}
```

#### Python SDK Client Integration
```python
from orchestrator.api.client import OrchestratorApiClient

client = OrchestratorApiClient(
    base_url="http://127.0.0.1:8000",
    api_token="your-configured-api-token",
)

# Retrieve paginated audit events
audit_resp = client.get_run_audit(
    run_id="run-a1b2c3d4e5f6",
    limit=50,
    offset=0,
)
print(f"Total audit events: {audit_resp.total_events}")
for event in audit_resp.events:
    print(f"[{event.timestamp}] {event.event_type} -> {event.outcome} (by {event.actor})")
```

---

### 8.6 Configurable Audit Retention & Storage Management

To prevent unbounded SQLite storage growth while complying with retention mandates, the audit system provides configurable retention windows, bounded purging, and non-blocking maintenance operations.

#### Retention Configuration Parameters

| Configuration Variable | Default | Allowed Values | Description |
|---|---|---|---|
| `ORCHESTRATOR_AUDIT_RETENTION_DAYS` | `30` | `>= 0` | Retention window in days. Setting `0` disables automatic expiration, keeping audit records indefinitely. |
| `ORCHESTRATOR_AUDIT_CLEANUP_ON_STARTUP` | `false` | `true`, `false` | When `true`, automatically executes a retention purge during server startup lifespan. Defaults to `false` to prevent unexpected deletions. |
| `ORCHESTRATOR_AUDIT_EXPORT_MAX_LIMIT` | `1000` | `1..10000` | Upper limit on records returned in a single export request, preventing memory exhaustion. |

#### Explicit Maintenance Operation: `POST /audit/cleanup`
Enables operators and cron maintenance scripts to trigger bounded retention cleanup.

Requires Bearer authentication when configured (`401 Unauthorized` without token).

**Request Body (`application/json`):**
```json
{
  "max_age_days": 30,
  "before_timestamp": "2026-09-01T00:00:00+00:00",
  "limit": 500
}
```
- `max_age_days` (*integer*, optional): Purge events older than this number of days. If omitted, uses configured `audit_retention_days`.
- `before_timestamp` (*string*, optional, ISO 8601): Purge events strictly older than this UTC timestamp.
- `limit` (*integer*, optional): Maximum number of records to purge in this execution, avoiding long transaction locks.

**Example Response (`HTTP 200 OK`):**
```json
{
  "success": true,
  "deleted_count": 42,
  "cutoff_timestamp": "2026-09-24T12:00:00.000000+00:00",
  "max_age_days": 30,
  "reason": null,
  "error": null
}
```

**Storage Integrity Guarantees:**
- **Zero Impact on Checkpoints**: Deletion queries execute exclusively against `audit_events`. `checkpoints`, `runs`, and HITL review queues are never modified or purged by audit maintenance.
- **Permanent Deletion**: Removed audit events are permanently deleted from SQLite unless an external backup exists.
- **Safe Failure Policy**: Database locking errors or unavailable persistence log warnings and return `success: false` without interrupting active workflows.

---

### 8.7 Safe Multi-Format Audit Export

The audit system supports safe, bounded, streaming-friendly export in **JSON**, **JSON Lines (JSONL)**, and **CSV** formats for compliance review, security information and event management (SIEM), and offline archiving.

#### Export Endpoints
- **Run Audit Export**: `GET /runs/{run_id}/audit/export`
- **System Audit Export**: `GET /audit/export`

Requires Bearer authentication when configured (`401 Unauthorized` without token).

**Query Parameters:**
- `format` (*string*, default `"json"`): Supported values: `json`, `jsonl`, `csv`.
- `limit` (*integer*, default `1000`, range `1..5000`): Maximum events to export per request. Capped by `audit_export_max_limit`.
- `offset` (*integer*, default `0`): Pagination offset.
- `start_time` (*string*, ISO 8601, optional): Filter events on or after this timestamp.
- `end_time` (*string*, ISO 8601, optional): Filter events on or before this timestamp (validated `start_time <= end_time`).
- `event_type` (*string*, optional): Filter by specific canonical event type (e.g. `security.auth_failure`).
- `run_id` (*string*, optional, `/audit/export` only): Filter system events by run ID.

**Export Format Specifications:**
1. **JSON (`application/json`)**: Standard JSON array of sanitized event objects. Content-Disposition: `attachment; filename="audit-export.json"`.
2. **JSON Lines (`application/x-ndjson`)**: One valid JSON object per newline, ideal for streaming ingestion into Splunk, Datadog, or Elasticsearch.
3. **CSV (`text/csv`)**: Standard RFC 4180 CSV with headers:
   `event_id,timestamp,event_type,outcome,run_id,correlation_id,actor,metadata`
   Complex nested dictionaries in `metadata` are safely JSON-encoded strings.

---

### 8.8 Client SDK Integration Examples

The `OrchestratorApiClient` provides high-level convenience methods for audit retention and export operations:

```python
from orchestrator.api.client import OrchestratorApiClient

client = OrchestratorApiClient(
    base_url="http://127.0.0.1:8000",
    api_token="your-configured-api-token",
)

# 1. Export audit records for a workflow run in JSONL format
jsonl_data = client.export_run_audit(
    run_id="run-a1b2c3d4e5f6",
    format="jsonl",
    limit=500,
)
print("Exported JSONL lines:", len(jsonl_data.strip().split("\n")))

# 2. Export system-wide security events in CSV format
csv_data = client.export_audit(
    event_type="security.auth_failure",
    format="csv",
    start_time="2026-10-01T00:00:00Z",
)
print("CSV Header & Rows:\n", csv_data)

# 3. Trigger manual audit retention cleanup (e.g., retain 14 days, max 1000 records)
cleanup_resp = client.cleanup_audit_events(
    max_age_days=14,
    limit=1000,
)
if cleanup_resp.success:
    print(f"Purged {cleanup_resp.deleted_count} events older than {cleanup_resp.cutoff_timestamp}")
```

---

## 9. Frequently Asked Questions & Common Pitfalls

### Q: Why does `orchestrator` show "command not found" in Windows PowerShell?
**A**: When Python packages are installed without administrator privileges, console script executables are placed in `%APPDATA%\Python\Python311\Scripts`, which might not be on your system `$env:PATH`.  
**Fix**: Either add the Scripts directory to your PATH, or simply invoke the module directly using `python -m orchestrator ...`.

### Q: Why does `orchestrator --serve` fail with `ModuleNotFoundError: No module named 'httpx'`?
**A**: The FastAPI server and HTTP client dependencies are optional extras.  
**Fix**: Install the `[api]` extra:
```powershell
pip install -e ".[api]"
```

### Q: Where are workflow checkpoints saved?
**A**: By default, state checkpoints are saved to a local SQLite database named `orchestrator_checkpoints.db` in your current working directory. You can configure a custom location via `CHECKPOINT_DB_PATH=/path/to/checkpoints.db`.

### Q: Can I run without internet access or API keys?
**A**: **Yes, completely.** The default provider is `DEFAULT_LLM_PROVIDER=mock`, which provides deterministic, zero-cost, offline simulation of research, analysis, writing, and code specialists. No paid APIs or external connections are made.

---

## 10. Idempotent Workflow Submission & Safe API Retries

Network drops, gateway timeouts, and client restarts can leave clients uncertain whether a workflow submission succeeded. The Multi-Agent Orchestrator provides first-class support for the `Idempotency-Key` header on `POST /runs`, guaranteeing that repeated requests from the same client replay the original workflow run rather than launching duplicate agent tasks.

### 10.1 Key Mechanics & Lifecycle

1. **Header Name**: `Idempotency-Key: <key>`
2. **Key Validation**:
   - Must be between 1 and 128 characters in length.
   - Must contain only printable ASCII characters (`0x21` to `0x7E`, no whitespace or control characters).
   - Invalid keys return `HTTP 400 Bad Request`.
3. **Optional by Default**:
   - Requests without an `Idempotency-Key` header create distinct workflow runs on every call (standard non-idempotent behavior).
4. **Scoping**:
   - Keys are isolated per authenticated client identity to prevent cross-client collisions.
   - When an API Bearer token is configured, scope is `token:<sha256(token)[:16]>`.
   - In unauthenticated local dev mode, scope defaults to `ip:<client_ip>`.
5. **Fingerprinting & Conflict Detection**:
   - The server computes a SHA-256 hash of the canonical request parameters (`task`, `plan`, `max_concurrency`, `run_in_background`, `timeout_seconds`, `run_id`).
   - If the same key is supplied with an identical payload, the original response is replayed with the response header `Idempotent-Replayed: true`.
   - If the same key is reused with a materially different payload, the API rejects the request with `HTTP 409 Conflict`.
6. **Concurrent Requests**:
   - When concurrent requests arrive with the exact same key, the first request atomically claims `in_progress` status in SQLite.
   - Simultaneous requests wait for the in-progress submission to finalize, then safely replay the completed response without initiating a second workflow.
7. **Failure Retries**:
   - If workflow creation fails (e.g. parameter validation error or abrupt shutdown during initialization), the key transitions to `failed`.
   - The client may immediately retry with the same key to acquire `in_progress` anew.

### 10.2 Process Restart Recovery & Storage Retention

- **Lingering In-Progress Keys**: On service startup, `store.recover_in_progress_idempotency_keys()` reconciles lingering `in_progress` records against the `runs` table. If the corresponding run was persisted, the key transitions to `completed`; otherwise, it transitions to `failed` so the client can safely retry.
- **Configurable Retention**: The retention window is controlled by `ORCHESTRATOR_IDEMPOTENCY_RETENTION_HOURS` (default: `24` hours).
- **Storage Purge**: Operators can purge expired keys using the persistence store API `store.purge_idempotency_keys(max_age_hours=...)`.

### 10.3 Client SDK Example

The `OrchestratorApiClient` provides native support for idempotency keys:

```python
import uuid
from orchestrator.api.client import OrchestratorApiClient

client = OrchestratorApiClient(
    base_url="http://127.0.0.1:8000",
    api_token="your-configured-api-token",
)

# Generate or reuse an idempotency key
idempotency_key = f"submit-{uuid.uuid4()}"

# Initial submission
run_resp = client.create_run(
    task="Analyze distributed caching architectures.",
    idempotency_key=idempotency_key,
)
print("Created run:", run_resp.run_id)

# Safe retry with same key (e.g. after a client-side timeout)
replayed_resp = client.create_run(
    task="Analyze distributed caching architectures.",
    idempotency_key=idempotency_key,
)
print("Replayed run:", replayed_resp.run_id)
assert replayed_resp.run_id == run_resp.run_id
```

---

## 11. Bounded Workflow Admission & Cross-Process Coordination

To guarantee responsiveness and resource stability under high concurrency, the Multi-Agent Orchestrator enforces a strict admission ceiling on the number of concurrently active workflow runs. When active executions reach the configured capacity, incoming requests are predictably rejected with backpressure (`HTTP 429 Too Many Requests`) rather than silently queued or allowed to saturate threads and memory.

When persistent SQLite storage is enabled (`CHECKPOINT_ENABLED=true`), this limit is enforced **globally across all worker processes** sharing the same database file via transactional lease reservations. When operating in in-memory mode, the limit applies locally to the single process.

---

### 11.1 Global Admission Mechanics & Lease Lifecycle

```
Client Request (POST /runs or POST /runs/{id}/resume)
       │
       ▼
┌────────────────────────────────────────────────────────┐
│ FastAPI Route & Security / Idempotency Validation       │
└──────────────────────┬─────────────────────────────────┘
                       │
                       ▼
┌────────────────────────────────────────────────────────┐
│ RunManager Process Admission Reservation               │
│                                                        │
│ 1. Verify shutdown is not in progress (HTTP 503)       │
│ 2. Check local capacity under threading lock           │
│ 3. If SQLite Store: Execute BEGIN IMMEDIATE Tx:        │
│    - Purge stale/expired leases (expires_at < now)    │
│    - Re-check run_id if already admitted (re-entrant)  │
│    - Count active leases: active < max_active_runs     │
│    - INSERT INTO workflow_admissions (run_id, owner_id)│
└──────────────────────┬─────────────────────────────────┘
                       │
       ┌───────────────┴───────────────┐
       │ Capacity Available?           │
      YES                              NO
       │                               │
       ▼                               ▼
┌───────────────────────┐   ┌────────────────────────────┐
│ Dispatch Worker Thread│   │ HTTP 429 Backpressure      │
│ Heartbeat Active Lease│   │ Retry-After: 2             │
│ (Period: 5.0s, TTL: 30s)│ └────────────────────────────┘
└───────────────────────┘
```

1. **Atomic Cross-Process Reservations**:
   - Every process instantiates a unique owner ID (`worker-{pid}-{uuid}`).
   - Admission checks run inside a `BEGIN IMMEDIATE` transaction on SQLite, acquiring a reserved lock that eliminates race conditions across concurrent processes competing for the final capacity slot.
   - The reservation is recorded in the `workflow_admissions` table:
     - `run_id` (`TEXT PRIMARY KEY`): Unique workflow identifier.
     - `owner_id` (`TEXT NOT NULL`): Identifier of the worker process owning the execution.
     - `fence_token` (`INTEGER NOT NULL DEFAULT 1`): Monotonically increasing lease generation token.
     - `status` (`TEXT NOT NULL`): Current lease state (`active`).
     - `acquired_at` (`TEXT NOT NULL`): UTC ISO timestamp of acquisition.
     - `heartbeat_at` (`TEXT NOT NULL`): UTC ISO timestamp of the most recent heartbeat.
     - `expires_at` (`TEXT NOT NULL`): UTC ISO timestamp when the lease expires (`heartbeat_at + lease_ttl`).
2. **Heartbeat Maintenance & Autonomous Renewal**:
   - Each `RunManager` runs a lightweight background daemon thread that periodically refreshes the heartbeat of all its active runs:
     $$\text{heartbeat\_interval} < \text{lease\_ttl}$$
     By default, heartbeats pulse every `5.0` seconds with an expiration TTL of `30.0` seconds.
3. **Crash Recovery & Process Isolation**:
   - If an API worker process crashes ungracefully, its active leases remain untouched until their `expires_at` timestamp elapses.
   - Surviving or newly started worker processes clean expired reservations (`expires_at < now`), preventing dead processes from permanently stranding capacity.
   - **Live Process Shielding**: During startup crash recovery, `find_interrupted_runs()` inspects `workflow_admissions`. Any workflow whose lease has a fresh heartbeat (`expires_at > now`) is actively guarded and will **not** be marked as `interrupted` or touched by other restarting worker processes.
4. **Lifecycle-Safe Slot Release**:
   - Once a workflow completes, fails, is cancelled, times out, or is persisted as `interrupted`, its reservation in `workflow_admissions` is immediately deleted.
   - During graceful shutdown, active workers draining during the shutdown grace period retain their reservations until completion or terminal interruption.
5. **Idempotency & Replay Exemption**:
   - Submitting an identical `Idempotency-Key` for an already finished or in-progress run returns the existing run status without consuming additional admission capacity.
   - If an initial submission is rejected due to capacity exhaustion (`HTTP 429`), its idempotency entry is marked as `failed`, allowing safe retry by the client once capacity opens up.
6. **Resumes Follow Admission**:
   - Resuming an interrupted or HITL-paused run (`POST /runs/{id}/resume`) requires atomic acquisition of an admission slot under the exact same global limits as fresh runs.

---

### 11.2 Configuration & Sizing Guidance

| Configuration Parameter | Environment Variable | Default | Valid Range | Description |
|---|---|---|---|---|
| `max_active_runs` | `ORCHESTRATOR_MAX_ACTIVE_RUNS` | `10` | `1 .. 1024` | Maximum concurrent active workflow runs admitted globally across processes. |
| `admission_lease_ttl_seconds` | `ORCHESTRATOR_ADMISSION_LEASE_TTL_SECONDS` | `30.0` | `1.0 .. 3600.0` | Inactivity duration before an un-heartbeated lease is considered dead. |
| `admission_heartbeat_interval_seconds` | `ORCHESTRATOR_ADMISSION_HEARTBEAT_INTERVAL_SECONDS` | `5.0` | `0.1 .. 300.0` | Heartbeat pulse interval used by worker processes to keep leases alive. |

#### Capacity Sizing Formula
Each admitted workflow run executes in a background thread and can spawn up to `effective_concurrency` parallel specialist agent threads (default: 4):

$$\text{Total Specialist Threads} \approx \text{max\_active\_runs} \times \text{effective\_concurrency}$$

**Recommended Tuning:**
- **Development / Low-Resource Hosts (2–4 CPU cores, 4–8 GB RAM)**:
  `ORCHESTRATOR_MAX_ACTIVE_RUNS=4` (Up to 16 specialist threads).
- **Standard Production Nodes (8–16 CPU cores, 16–32 GB RAM)**:
  `ORCHESTRATOR_MAX_ACTIVE_RUNS=10` to `20` (Up to 40–80 specialist threads).
- **High-Throughput Multi-Worker Setup (Shared SQLite Volume)**:
  For 2–4 Gunicorn/Uvicorn worker processes accessing the same SQLite WAL database, configure `ORCHESTRATOR_MAX_ACTIVE_RUNS=20` to `30`. SQLite `BEGIN IMMEDIATE` transactions cleanly serialize admissions without deadlocks.

---

### 11.3 Scope of Coordination & Deployment Architectures

- **Multi-Process Coordination Scope**: Global coordination applies to all API and worker processes that access the **same underlying SQLite database file** (e.g. multi-process Uvicorn/Gunicorn workers on a single host or processes sharing a shared POSIX volume with POSIX lock support).
- **Multi-Host / Distributed Clusters**: If API worker nodes run on independent hosts with isolated local SQLite databases, global admission is not coordinated across the hosts. In such environments:
  - Configure ingress reverse proxies (Nginx, Envoy, AWS ALB) with `least_conn` routing to distribute load proportionally.
  - Or deploy a centralized database backend / distributed lock manager (e.g., Redis or PostgreSQL) for cross-host clustering.

---

### 11.4 Admission Lease Fencing & Ownership Safety

To guarantee safe execution across concurrent processes, delayed heartbeats, temporary SQLite locks, and process restarts, the orchestrator implements **monotonically increasing fencing tokens** and **ownership-conditional lease renewals**:

```
Worker A (Gen 1)                      SQLite DB                     Worker B (Gen 2)
      │                                   │                                │
      │── Acquire Run (Gen 1) ───────────►│                                │
      │   runs.fence_token=1              │                                │
      │   workflow_admissions(token=1)    │                                │
      │                                   │                                │
   [Delayed / Network Partition]          │                                │
      │                                   │                                │
      │ (Lease expires after 30s)         │                                │
      │                                   │◄── Reclaim & Resume (Gen 2) ───│
      │                                   │    runs.fence_token=2          │
      │                                   │    workflow_admissions(token=2)│
      │                                   │                                │
      │── Checkpoint Write (Gen 1) ──────►│                                │
      │   save_checkpoint(token=1)        │ (REJECT: 1 < 2)                │
      │◄── Raises AdmissionFencingError ──│                                │
      │                                   │                                │
      ▼                                   │                                ▼
Halts Cooperatively                       │                   Continues Safely
(No State Overwrite)                      │                   (Owns Active Lease)
```

1. **Fencing Token Semantics**:
   - Every workflow run tracks a persistent `fence_token` (`INTEGER NOT NULL DEFAULT 0`) in the `runs` table and in active `workflow_admissions` records.
   - Upon initial admission (`start_run`) or subsequent resume (`resume_run`), the transaction increments the token:
     $$\text{new\_token} = \text{runs.fence\_token} + 1$$
     This guarantees strictly monotonic generation numbers across process boundaries, even if `workflow_admissions` rows were deleted or reclaimed.
2. **Ownership-Conditional Lease Renewal & Release**:
   - Lease renewals (`heartbeat_active_runs`) explicitly specify `(owner_id, fence_token)`. If a lease was reclaimed or superseded by a newer worker generation, the heartbeat returns failure for that run ID.
   - Lease release (`release_workflow_admission`) requires matching `owner_id` and `fence_token`. A lagging or recovering worker is barred from clearing a newer worker's active admission slot.
3. **Heartbeat & Transient Failure Tolerance**:
   - Background heartbeat loops tolerate transient SQLite errors (e.g. temporary database locks under heavy write contention) without immediately aborting the run.
   - Workers continue normal execution while the elapsed time since the last successful heartbeat is within `admission_lease_ttl_seconds`.
   - If renewal cannot be confirmed within the full TTL, `lease_lost` is flagged and cooperative cancellation is requested on the workflow's `CancellationToken`.
4. **Guarded State Transitions (`AdmissionFencingError`)**:
   - `RunManager` wraps storage operations inside `FencedCheckpointStore`, binding the execution's `(owner_id, fence_token)`.
   - At every graph state transition, `save_checkpoint` verifies that caller generation matches the active admission and is $\ge$ the latest token in `runs`.
   - If superseded, `save_checkpoint` raises `AdmissionFencingError`, signaling the orchestrator graph to abort immediately.
   - Worker completion cleanup detects `AdmissionFencingError` and `lease_lost = True`, suppressing the writing of `failed_state` to SQLite so that a zombie worker never overwrites state produced by an active or completed successor.
5. **Operational Boundaries & Limitations**:
   - **External Side Effects**: Idempotency and fencing guard persistence integrity within SQLite. External side effects performed before fencing detection (such as outbound webhooks or external API calls) cannot be undone retroactively.
   - **Cooperative Thread Cancellation**: Python threads terminate cooperatively. Workers check cancellation and fencing at graph checkpoint boundaries and specialist node dispatches. Blocking I/O or non-cooperative external code will yield on the next checkpoint boundary.

---

### 11.5 Atomic Fenced State Transitions & Lifecycle Guarantees

To prevent stale or superseded workers from overwriting workflow status, results, errors, or execution metadata, all workflow state modifications are governed by an **atomic, fencing-aware state transition engine**:

```
State Machine:
┌─────────┐       start / init       ┌─────────┐
│ pending │ ───────────────────────► │ running │
└─────────┘                          └────┬────┘
                                          │
            ┌─────────────────────────────┼─────────────────────────────┐
            │                             │                             │
            ▼                             ▼                             ▼
     ┌───────────┐                 ┌─────────────┐               ┌─────────────┐
     │ completed │ (IMMUTABLE)     │  cancelled  │               │  timed_out  │
     └───────────┘                 └──────┬──────┘               └──────┬──────┘
                                          │                             │
            ┌─────────────────────────────┼─────────────────────────────┘
            │                             │
            ▼                             ▼
      ┌───────────┐                ┌─────────────┐
      │  failed   │                │ interrupted │
      └─────┬─────┘                └──────┬──────┘
            │                             │
            └─────────────────────────────┘
                          │
                          ▼ (POST /runs/{id}/resume with Token+1)
                     ┌─────────┐
                     │ running │
                     └─────────┘
```

1. **Terminal State Immutability**:
   - Once a workflow reaches `completed`, `approved`, or `edited`, its status and results become permanently immutable.
   - Any subsequent write attempt—whether from a lagging worker thread, a delayed timeout timer, an unhandled exception handler, or an administrative cancellation request—is rejected with `AdmissionFencingError` or `CheckpointConflictError`.
   - Stale workers can never turn a completed workflow back into `failed`, `cancelled`, or `running`.
2. **Atomic Verification and Mutation (`BEGIN IMMEDIATE`)**:
   - `save_checkpoint` executes inside a single `BEGIN IMMEDIATE` transaction that atomically acquires a reserved database lock prior to reading `runs` or `workflow_admissions`.
   - The fencing generation token, lease ownership, and allowed lifecycle transitions are validated under the exact same lock before inserting into `checkpoints` or updating `runs`.
   - If validation fails, the transaction rolls back cleanly; no checkpoint rows are recorded and no summary fields are touched.
3. **Privileged Administrative Operations**:
   - **Administrative Cancellation (`cancel_run_administratively`)**:
     Executed when a cancellation request arrives for a workflow not actively running in the local process memory. Atomically increments `runs.fence_token = db_fence + 1`, purges any active `workflow_admissions` lease, appends a `cancelled` checkpoint, and updates `runs.status = 'cancelled'`. Any worker currently running that workflow will be fenced out immediately on its next write. If the run is already terminal, cancellation is rejected with `CheckpointConflictError` (`HTTP 409 Conflict`).
   - **Startup & Shutdown Recovery (`recover_run_as_interrupted`)**:
     Atomically inspects runs in active status. If a run's lease has expired or the shutdown grace period elapsed (`force=True`), recovery increments `runs.fence_token = db_fence + 1`, clears the admission slot, appends an `interrupted` checkpoint, and updates `runs.status = 'interrupted'`. Live runs heartbeated by surviving worker processes are shielded from premature interruption.
4. **Resumption Generation Bumping**:
   - Resuming a paused, failed, cancelled, timed-out, or interrupted run requires calling `POST /runs/{id}/resume`.
   - `acquire_workflow_admission` allocates a strictly monotonic generation token:
     $$\text{token}_{\text{resume}} = \text{runs.fence\_token} + 1$$
   - The resumed worker executes with this new token, permanently fencing out the previous worker generation from writing any delayed subtasks.
5. **In-Memory Cache Protection**:
   - When a worker process loses its lease or encounters `AdmissionFencingError`, `RunManager` evicts the run from `_results_cache` (`_results_cache.pop(run_id, None)`) rather than recording a fake terminal failure.
   - Subsequent `GET /runs/{id}` and `cancel_run` queries read the authoritative state directly from SQLite, preventing in-memory split-brain discrepancies.

---

## 12. Safe Tool Execution, Operation Keys & External Side-Effect Idempotency

When autonomous specialists invoke external tools (such as payment gateways, ticket systems, database updates, or remote APIs), execution retries, timeouts, worker crashes, and lease expirations create risks of duplicate side effects. The multi-agent orchestrator provides an end-to-end tool execution safety framework that classifies tool behaviors, derives deterministic operation keys, injects downstream idempotency keys, enforces double-gated admission fencing, and durably records tool lifecycle state.

### 12.1 Tool Execution Safety Taxonomy & Semantics

Every tool registered in `ToolRegistry` is categorized under an `IdempotencyMode`:

| Idempotency Mode | Side Effects? | Downstream Key? | Retries Allowed? | Re-invocation Semantics |
|---|---|---|---|---|
| `READ_ONLY` | No | No | Yes (up to `max_retries`) | Safe for immediate re-execution. Does not write to `tool_executions`. |
| `MUTATING_IDEMPOTENT` | Yes | Yes (`idempotency_key`) | Safe with key | Short-circuits to cached result if already completed; passes stable key downstream. |
| `MUTATING_NON_IDEMPOTENT` | Yes | No | **No** (`effective_retries = 0`) | Blocks concurrent duplicate dispatch (`ToolDuplicateExecutionError`). If outcome is uncertain, raises `ToolUncertainOutcomeError`. |

1. **`IdempotencyMode.READ_ONLY`**:
   - Operations that query, search, read, calculate, or format data without altering external state (e.g., `web_search`, `read_file`, `calculator`).
   - Transient failures (such as network glitches or temporary rate limits) are safely retried up to `max_retries`.
2. **`IdempotencyMode.MUTATING_IDEMPOTENT`**:
   - Operations that modify external resources where the external service natively supports client-provided idempotency keys (e.g., modern REST APIs accepting `Idempotency-Key` headers).
   - Injected with an automated, stable operation key by the orchestrator.
   - If an operation with the same operation key has already recorded status `completed`, the orchestrator short-circuits execution and returns the persisted `result_text` immediately without issuing redundant external requests.
3. **`IdempotencyMode.MUTATING_NON_IDEMPOTENT`**:
   - Operations that mutate external state where downstream systems offer no idempotency protection (e.g., legacy SMS gateways, raw SMTP mail, unkeyed webhook triggers).
   - Automatic retries are strictly disabled (`effective_retries = 0`).
   - If a timeout or socket drop occurs after dispatch, the outcome cannot be verified locally. The orchestrator flags the execution as `uncertain` and raises `ToolUncertainOutcomeError`.
   - Repeated attempts to dispatch an operation currently marked `pending` fail fast with `ToolDuplicateExecutionError`. Re-invoking an `uncertain` operation is rejected unless explicitly allowed via `retry_on_uncertain=True`.

---

### 12.2 Operation Key Derivation & Downstream Idempotency Key Injection

To identify each distinct tool invocation across retries and workflow resumes, the orchestrator derives a deterministic **operation key**:

$$\text{op\_key} = \text{toolop:}\{\text{run\_id}\}:\{\text{subtask\_id}\}:\{\text{tool\_name}\}:\{\text{hash}_{16}\}$$

- **Canonicalization**: The argument dictionary is canonicalized by removing internal workflow parameters (such as `context` and `idempotency_key`), sorting keys alphabetically, and encoding to compact JSON (`separators=(',', ':')`). The first 16 hexadecimal characters of its SHA-256 digest form $\text{hash}_{16}$.
- **Downstream Forwarding**: If the tool function declares an `idempotency_key` parameter or `**kwargs` in its signature, and the tool is configured with `supports_idempotency_key=True`, the orchestrator injects `idempotency_key=op_key` as a keyword argument. Tool adapters should forward this key to external providers (e.g., as an HTTP header `Idempotency-Key: <op_key>`).

---

### 12.3 Double-Gated Admission Fencing & Cooperative Cancellation

To prevent zombie workers, superseded worker processes, or cancelled workflows from triggering external side effects or persisting conflicting tool outcomes, execution is protected by a **double-gate protocol**:

```
[Tool Invocation]
       │
       ▼
[Gate 1: Pre-Dispatch] ───► Is Cancelled? ────────► Raise ToolExecutionError
       │               ───► Fencing Valid? ───────► Raise ToolFencingError
       ▼
[Idempotency Check]   ───► Already Completed? ───► Short-Circuit & Return Result
       │               ───► Already Pending?   ───► Raise ToolDuplicateExecutionError
       │               ───► Uncertain?         ───► Raise ToolUncertainOutcomeError
       ▼
[Record: status='pending']
       │
       ▼
[Execute Tool Callable] (with timeout & retry policy)
       │
       ▼
[Gate 2: Post-Execution] ──► Fencing Valid? ───────► Raise ToolFencingError (Discard Result)
       │
       ▼
[Record: status='completed'] ───► Return Result Text
```

1. **Pre-Dispatch Gate**:
   - Checks `cancellation_token.is_cancelled`. If cancellation has been requested, the dispatch aborts immediately.
   - Checks `store.validate_admission_ownership(run_id, owner_id, fence_token)`. If the worker has lost its admission lease or the run was administratively cancelled, the dispatch aborts with `ToolFencingError`.
2. **Post-Execution Pre-Commit Gate**:
   - Long-running network I/O may outlast a worker's lease renewal heartbeat.
   - Before committing `completed` to the `tool_executions` database table, the orchestrator re-validates `validate_admission_ownership`. If the worker was superseded during the call, the result is rejected and not committed.

---

### 12.4 Lifecycle Status Transitions & Recovery Handling

The orchestrator tracks tool execution states in the `tool_executions` SQLite table:

| Status | Meaning | Recovery Action |
|---|---|---|
| `pending` | Tool call dispatched and currently executing. | Converted to `uncertain` upon worker crash or shutdown recovery. |
| `completed` | Tool call succeeded and result durably recorded. | Cached result reused on identical repeat invocations. |
| `failed` | Tool call failed with a deterministic, verified error. | Re-executable or recorded as permanent task failure. |
| `uncertain` | Tool timed out, connection dropped, or crashed in-flight. | Requires administrative review or `recover_uncertain_tool_executions`. |

- **Crash and Shutdown Recovery**:
  When a node crashes or the orchestrator performs graceful shutdown recovery via `recover_run_as_interrupted` or `cancel_run_administratively`, all in-flight `pending` tool executions for the affected run are atomically transitioned to `uncertain`.
- **Administrative Inspection**:
  Operators can inspect tool execution history for a run using `store.list_tool_executions(run_id)` and mark unresolved entries as recoverable using `store.recover_uncertain_tool_executions(run_id)`.

---

### 12.5 Registering Safe Tools: Code Examples & Best Practices

```python
from orchestrator.tools import @tool, IdempotencyMode

# 1. Read-Only Tool: Safe for automatic retries
@tool(
    name="search_knowledge_base",
    description="Query the vector database for relevant documents.",
    side_effecting=False,
    idempotency_mode=IdempotencyMode.READ_ONLY,
    max_retries=3,
    timeout_seconds=10.0,
)
def search_knowledge_base(query: str) -> str:
    return search_client.search(query)


# 2. Mutating Idempotent Tool: Forwards orchestrator key downstream
@tool(
    name="provision_cloud_vm",
    description="Provision a cloud virtual machine instance.",
    side_effecting=True,
    idempotency_mode=IdempotencyMode.MUTATING_IDEMPOTENT,
    supports_idempotency_key=True,
    timeout_seconds=45.0,
)
def provision_cloud_vm(instance_type: str, region: str, idempotency_key: str = "") -> str:
    # Forward the stable key to the cloud provider API
    vm = cloud_client.create_instance(
        instance_type=instance_type,
        region=region,
        client_token=idempotency_key,
    )
    return vm.instance_id


# 3. Mutating Non-Idempotent Tool: Single-shot, protected against duplicate execution
@tool(
    name="send_legacy_sms_notification",
    description="Dispatch SMS via legacy gateway lacking idempotency keys.",
    side_effecting=True,
    idempotency_mode=IdempotencyMode.MUTATING_NON_IDEMPOTENT,
    retry_on_uncertain=False,
    timeout_seconds=15.0,
)
def send_legacy_sms_notification(phone_number: str, message: str) -> str:
    # Non-retryable; connection drops surface ToolUncertainOutcomeError
    return legacy_sms_gateway.dispatch(phone_number, message)
```

---

### 12.6 Tool Execution Monitoring & Safe Reconciliation Workflow

In high-concurrency and distributed production environments, network timeouts, abrupt worker crashes, or socket disconnects can leave tool executions in an `uncertain` state. Because mutating tools may have triggered side effects on external providers before the connection dropped, the orchestrator provides an authenticated, privacy-preserving monitoring and reconciliation workflow.

#### 1. Inspection Endpoints & Query Filtering

Authorized operators can inspect tool executions for any workflow run via dedicated endpoints:

- **List Tool Executions**:
  `GET /runs/{run_id}/tools` (or `/runs/{run_id}/tool-executions`)
  - Optional Query Parameters:
    - `status`: Filter by execution status (`pending`, `completed`, `failed`, `uncertain`). For example, `GET /runs/{id}/tools?status=uncertain` lists only operations requiring operator intervention.
    - `tool_name`: Filter by specific tool identifier (e.g., `?tool_name=charge_customer`).
- **Inspect Specific Tool Execution**:
  `GET /runs/{run_id}/tools/{operation_key}`
  - Returns complete sanitized execution details including operation key, tool name, subtask ID, fence token, timestamps, and reconciliation history.

#### 2. Argument Redaction & Privacy Shielding

To protect customer privacy and operational credentials, tool execution data returned by the API is automatically scrubbed:
- **Sensitive Key Redaction**: Any dictionary keys matching passwords, secrets, API keys, bearer tokens, auth credentials, or authorization headers are masked to `"[REDACTED]"`.
- **Credential Scrubbing**: String values containing patterns like `sk-...` or Bearer tokens are scrubbed to `"[REDACTED_KEY]"`.
- **Traceback Stripping**: Internal Python stack traces and server file paths in error details are removed, retaining only the final sanitized exception summary.
- Raw unredacted arguments stored during dispatch are never exposed through API responses.

#### 3. Operational Reconciliation Workflow

When a mutating tool execution is in an `uncertain` state, an authorized operator inspects external dashboards (e.g., cloud provider management consoles, payment ledgers, or messaging gateways) to verify whether the side effect took place, and then records the verified outcome:

- **Reconciliation Endpoint**:
  `POST /runs/{run_id}/tools/{operation_key}/reconcile`
- **Request Payload**:
  ```json
  {
    "outcome": "completed",
    "reason": "Verified charge ch_3Mpd9 exists in Stripe dashboard with status succeeded",
    "result_text": "ch_3Mpd9:paid",
    "operator": "ops-engineer@company.com"
  }
  ```

#### 4. Transition Guarantees & Lifecycle Immutability

1. **Reconciliation to `completed`**:
   - The operator confirms the external side effect succeeded out-of-band.
   - The record transitions from `uncertain` to `completed`, storing the verified `result_text` and explanation.
   - When the workflow run is resumed (`POST /runs/{id}/resume`), the specialist node encounters the `completed` status and returns `result_text` immediately without issuing any external requests.
2. **Reconciliation to `failed`**:
   - The operator confirms the external side effect did not occur or failed permanently.
   - The record transitions from `uncertain` to `failed`, storing the verified error details and explanation.
   - When resumed, the specialist node encounters the `failed` status, raises `ToolExecutionError`, and terminates the subtask without redispatching.
3. **Atomic Concurrency Protection (`BEGIN IMMEDIATE`)**:
   - Reconciliation executes under exclusive SQLite write locks. Concurrent conflicting reconciliation attempts (e.g., one operator submitting `completed` while another submits `failed`) are resolved safely: the first transaction commits, and the second is rejected with `HTTP 409 Conflict`.
   - In-flight operations (`status == 'pending'`) cannot be reconciled (`HTTP 409 Conflict`).
4. **Idempotent Repetition**:
   - Submitting duplicate reconciliation requests with the identical outcome returns `HTTP 200 OK` with `"idempotent": true`, preventing state drift or redundant updates.

#### 5. Structured Audit Logging

Every reconciliation action automatically emits a `tool.reconciled` audit event:
```json
{
  "event_id": "aud-7a3b4c5d6e7f",
  "event_type": "tool.reconciled",
  "outcome": "completed",
  "run_id": "run-40291-payment",
  "actor": "ops-engineer@company.com",
  "metadata": {
    "operation_key": "toolop:run-40291-payment:s1:charge:89ab",
    "tool_name": "charge_customer",
    "previous_status": "uncertain",
    "new_status": "completed",
    "reason": "Verified charge ch_3Mpd9 exists in Stripe dashboard",
    "idempotent": false
  }
}
```

#### 6. Operational Boundaries & Recovery Limitations

- **Zero Automatic Redispatch**: The orchestrator strictly avoids automatically redispatching uncertain operations during inspection, reconciliation, startup recovery, or workflow resumption.
- **Human Verification Required**: The system does not claim distributed "exactly-once" external execution across network partitions. Because external providers may have processed a request before a connection failed, human verification via external provider portals is required prior to reconciling an uncertain operation.

### 12.7 Tool Permission Policies & Risk-Based Approval Workflow

To prevent autonomous agents from performing unauthorized actions or triggering irreversible high-risk side effects without human oversight, the orchestrator implements a centralized, enforceable tool permission policy and risk-based approval architecture.

#### 1. Policy Model & Risk Classifications

Every tool carries or inherits an enforceable `ToolPolicy`:
- **Risk Levels (`RiskLevel`)**:
  - `LOW`: Read-only queries, local arithmetic, idempotent lookups (e.g. `calculator`, `web_search`). Executed autonomously without approval.
  - `MEDIUM`: Internal state queries or reversible mutations with minimal blast radius.
  - `HIGH`: Mutating operations with external blast radius (e.g., sending emails, database modifications, provisioning credentials). Requires human approval by default.
  - `CRITICAL`: Irreversible destructive actions, financial transactions, or infrastructure provisioning (e.g., cloud deployments, dropping tables, payment execution). Human approval strictly mandatory.
- **Agent Scoping (`allowed_agents`)**:
  - Defines the whitelist of specialist agent kinds permitted to invoke the tool (e.g., `["code", "analysis"]`).
  - Wildcard `["*"]` grants access to all specialists.
  - Invocations from unauthorized agents or unspecified actors are blocked at the boundary before execution.
- **Approval Flag (`requires_approval`)**:
  - Explicit boolean flag. If `True`, or if `risk_level` is `HIGH` or `CRITICAL`, execution cannot proceed without a verified approval decision.
- **Conservative Default-Deny**:
  - Undeclared or unknown tools default to requiring approval (`registry.requires_approval("unknown") == True`) and raise `ToolNotFoundError` if called directly.

#### 2. Central Boundary Enforcement & Non-Bypassability

Permissions and approval requirements are enforced centrally inside `Tool.__call__`, directly at the tool invocation boundary:
1. **Prompt Immunity**: Agents cannot bypass policy restrictions through deceptive system prompts or synthetic plans; the Python execution gate rejects unauthorized calls before reaching tool code.
2. **Direct Registry Immunity**: `REGISTRY.call(...)` and `REGISTRY.execute(...)` execute through `Tool.__call__`, enforcing authorization and approval checks identically.
3. **Retry & Resume Immunity**: Retrying a subtask or resuming a workflow run re-evaluates the policy at the tool boundary. Unapproved operations remain blocked until approved.

#### 3. Post-Approval Revalidation Gate

Per strict safety requirements, human approval cannot be held indefinitely while operational state shifts. Immediately after approval verification and directly before dispatching the underlying function, the orchestrator revalidates:
1. **Cooperative Cancellation**: Verifies `cancellation_token.is_cancelled`. If the run was cancelled while awaiting approval, execution aborts with `ToolExecutionError`.
2. **Agent Authorization**: Revalidates that the executing specialist is still authorized under `policy.allowed_agents`.
3. **Admission Ownership (Fencing)**: Validates `store.validate_admission_ownership(...)` to guarantee the worker still holds a valid lease and has not been superseded.

#### 4. Durable Approval Persistence (`tool_approvals`)

Approval decisions are persisted in the SQLite store (`tool_approvals` table), indexed by stable `operation_key`:
- **`approved`**: Grants authorization for this exact operation key. Upon workflow resume or subtask retry, the tool execution pipeline retrieves the recorded approval and proceeds.
- **`rejected`**: Permanently blocks execution with `ToolPermissionDeniedError`, recording the rejection justification and approver identity.

#### 5. Authenticated Approval API & Client SDK

Authorized operators review pending executions and record approval decisions:

- **Approve or Reject Execution**:
  `POST /runs/{run_id}/tools/{operation_key}/approve`
  ```json
  {
    "tool_name": "deploy_service",
    "decision": "approved",
    "reason": "Production deploy ticket PROD-1049 approved by SecOps",
    "approver": "secops-lead@company.com"
  }
  ```
- **Inspect Approval Decision**:
  `GET /runs/{run_id}/tools/{operation_key}/approval`

Using the Python SDK:
```python
client = OrchestratorApiClient(base_url="https://orchestrator.internal")
resp = client.approve_tool_execution(
    run_id="run-40291",
    operation_key="toolop:run-40291:s1:deploy_service:89ab",
    tool_name="deploy_service",
    decision="approved",
    reason="Change window open and approved",
    approver="operator-1",
)
```

#### 6. Structured Audit Logging

Policy events are recorded in the central audit system:
- **`tool.policy_denied`**: Recorded when an agent lacks authorization or an operation was rejected.
- **`tool.approval_required`**: Recorded when a high-risk tool blocks awaiting operator decision.
- **`tool.approved`**: Recorded when an authorized operator approves execution.
- Sensitive arguments, credentials, and tokens are scrubbed and never appear in audit metadata.

### 12.8 Approval Integrity, Authorization & Revocation

Task 43 hardens the human-in-the-loop approval lifecycle so approvals are strictly bound to the exact intended operation and arguments, approver identities are verified against explicit policies, decisions cannot conflict or silently overwrite one another, and active approvals can be revoked with audited pre-dispatch revalidation.

#### 1. Exact Operation & Argument Fingerprint Binding

Approvals cannot be applied loosely to arbitrary executions of a tool. Each approval decision is cryptographically bound to the canonical execution parameters:
- **`argument_fingerprint`**: A deterministic SHA-256 hash computed over canonicalized, sorted tool invocation arguments (excluding ephemeral execution context and downstream idempotency tokens).
- **Execution Validation**: When `Tool.__call__` executes an approved action, it re-computes `compute_argument_fingerprint(kwargs)`. If the executing parameters differ in any way from the approved fingerprint, execution is blocked immediately with `ToolApprovalRequiredError`. Approvals cannot be repurposed with altered parameters.

#### 2. Policy & Risk Context Freshness

Approvals capture the policy context present at the time the decision was rendered:
- **`risk_level` & `policy_hash`**: The tool's risk classification and policy definition (including authorized agents and approvers) are persisted alongside the approval.
- **Stale Approval Invalidation**: If an administrator modifies the tool's policy or elevates its risk classification after approval was granted, the tool execution pipeline detects the hash mismatch and requires a fresh approval cycle.

#### 3. Explicit Approver Authorization & Identity Verification

Possession of the general API bearer token does not automatically confer authority to approve high-risk operations:
- **Generic Identity Rejection**: Requests presenting anonymous or generic identities (`"bearer-token"`, `"authenticated-client"`, `"unknown"`, `"anonymous"`) are rejected with `HTTP 403 Forbidden` (`ToolApproverUnauthorizedError`).
- **Tool Policy Whitelisting**: Each tool defines `authorized_approvers` (e.g. `["cfo_alice", "secops_lead"]`). Approvers not matching the tool policy are rejected with `HTTP 403`.
- **Global Approver Controls**: Global configuration via `Settings.tool_approval_allowed_approvers` (env: `ORCHESTRATOR_ALLOWED_APPROVERS`) enforces organization-wide approver restrictions.
- **Audit Logging**: Unauthorized approval attempts are recorded under `tool.approver_denied`.

#### 4. Safe State Transitions & Conflict Protection

Approval records in SQLite (`tool_approvals` table) transition through an audited, deterministic state machine:
- **Idempotency**: Repeated identical requests (`approved` -> `approved`, `rejected` -> `rejected`, `revoked` -> `revoked`) return `HTTP 200` with `idempotent: true` without altering state.
- **Conflict Rejection**: Conflicting transitions (`approved` -> `rejected`, `rejected` -> `approved`, `revoked` -> `approved`) raise `ToolApprovalConflictError` (`HTTP 409 Conflict`). Decisions cannot be silently rewritten.
- **Revocation Transition**: Only `approved` executions can transition to `revoked`.

#### 5. Audited Revocation Mechanism

Active approvals can be explicitly revoked by authorized operators prior to execution:
- **Revocation Endpoint**: `POST /runs/{run_id}/tools/{operation_key}/revoke`
  ```json
  {
    "tool_name": "deploy_service",
    "reason": "Security vulnerability detected in release artifact",
    "revoked_by": "secops_lead"
  }
  ```
- **SDK Method**: `client.revoke_tool_approval(run_id, operation_key, tool_name, reason, revoked_by)`
- **Audit Event**: Records `tool.approval_revoked` with actor, revocation reason, and operation key.

#### 6. Pre-Dispatch Revalidation Gate

Approvals are validated not only during planning or initial invocation, but revalidated immediately before dispatching the underlying Python function:
- **Wait & Retry Protection**: If an approval is revoked in the store while a specialist agent is waiting or preparing retries, the pre-dispatch check detects the revocation and raises `ToolPermissionDeniedError` before the tool executes.
- **Cancellation & Fencing**: Cooperative cancellation tokens and admission fencing tokens are re-checked immediately post-approval before invoking external providers.

### 12.9 Approval Observability & Operational Recovery

Task 44 provides comprehensive operational visibility and safe, privileged recovery for the human-in-the-loop tool approval lifecycle introduced in Tasks 42–43. Operators can discover pending, approved, rejected, and revoked actions, diagnose exact blockers without leaking credentials, and reset stuck approval states back to `pending` without bypassing authorization gates or automatically triggering external side effects.

#### 1. Approval Lifecycle Visibility & Status Filtering

All approval actions and pending requests are queryable via dedicated inspection endpoints:
- **`GET /runs/{run_id}/approvals`**: Returns all recorded approvals for a workflow run.
- **Query Parameter Filtering**:
  - `?status=pending`: Lists operations awaiting operator review.
  - `?status=revoked`: Lists revoked approvals.
  - `?status=rejected`: Lists explicitly rejected actions.
  - `?tool_name=deploy_service`: Filters by tool identifier.
- **Automatic Request Registration**: When a high-risk tool is invoked without prior approval, the execution boundary records a `pending` approval request in `tool_approvals` before raising `ToolApprovalRequiredError`. Operators immediately see new approval requests in the pending queue without manual intervention.

#### 2. Root-Cause Diagnostic Engine (`diagnose_tool_approval`)

When an operation is blocked, stale, or awaiting approval, operators can inspect its state via `GET /runs/{run_id}/tools/{operation_key}/diagnostics` or test candidate invocation arguments via `POST /runs/{run_id}/tools/{operation_key}/diagnostics`.

The engine returns unambiguous machine-readable diagnostic codes:
- **`missing_approval`**: No approval record exists; approval must be submitted.
- **`pending_approval`**: Approval request is registered and awaiting human review.
- **`valid_approved`**: Operation is approved, parameters and policies match, and execution can proceed.
- **`argument_fingerprint_mismatch`**: Invocation arguments have drifted from the parameters bound to the approval.
- **`policy_risk_mismatch`**: Tool risk classification changed after approval was granted.
- **`policy_hash_mismatch`**: Underlying tool policy was modified after approval was recorded.
- **`unauthorized_approver`**: The recorded approver is not permitted by current tool policy.
- **`revoked_approval`**: Approval was explicitly revoked by an operator.
- **`rejected_approval`**: Operation was explicitly rejected by an approver.
- **`workflow_cancelled`**: The workflow run was cancelled, preventing tool dispatch.
- **`admission_fencing_lost`**: Worker lost admission lease ownership to a newer worker generation.

Each diagnostic response includes up to 10 sanitized, relevant audit events explaining the history of the approval decision.

#### 3. Privileged Operational Recovery

If an operation was revoked by mistake, rejected due to stale arguments, or blocked due to policy drift, authorized operators can perform operational recovery:
- **Safe Reset to Pending**: `POST /runs/{run_id}/tools/{operation_key}/recover` resets the approval record back to `pending`.
- **Strict Safety Guarantees**:
  1. **Never Auto-Approves**: The recovery endpoint strictly resets state to `pending`. It never transitions directly to `approved`.
  2. **Never Auto-Dispatches**: Recovery does not trigger execution or redispatch external calls. The tool remains blocked until an authorized human explicitly reviews and approves the pending action via `POST /approve`.
  3. **Mandatory Justification**: Empty or whitespace reasons are rejected (`HTTP 400 Bad Request`).
  4. **Verified Operator Identity**: Anonymous or generic credentials (`"bearer-token"`) are rejected (`HTTP 403 Forbidden`).
  5. **Audited Operation**: Emits a `tool.approval_recovered` audit event detailing the recovering operator, justification, and operation identity.

#### 4. Sensitive Data Sanitization & Privacy

All approval records, diagnostic responses, and audit summaries are scrubbed via `sanitize_tool_approval`:
- Passwords, secret keys, bearer tokens, and API credentials in reasons, revocation notes, or error strings are redacted with `[REDACTED]`.
- Internal tracebacks are scrubbed to prevent internal infrastructure exposure.

#### 5. HTTP API & Python SDK Reference

**API Endpoints**:
- `GET /runs/{run_id}/approvals[?status=...&tool_name=...]` — List approvals with optional filtering.
- `GET /runs/{run_id}/tools/{operation_key}/diagnostics` — Inspect current approval diagnostics.
- `POST /runs/{run_id}/tools/{operation_key}/diagnostics` — Inspect diagnostics with proposed arguments.
- `POST /runs/{run_id}/tools/{operation_key}/recover` — Reset blocked approval back to pending.

**Python SDK (`OrchestratorApiClient`)**:
```python
client = OrchestratorApiClient(base_url="https://orchestrator.internal", api_token="secret")

# 1. List pending approvals
pending = client.list_tool_approvals(run_id="run-101", status="pending")
for app in pending.approvals:
    print(f"Pending: {app.tool_name} (op: {app.operation_key}, risk: {app.risk_level})")

# 2. Diagnose blocked operation
diag = client.get_tool_approval_diagnostics(
    run_id="run-101",
    operation_key="toolop:run-101:s1:deploy_service:89ab",
)
if diag.is_blocked:
    print(f"Blocked code: {diag.diagnostic_code} - {diag.message}")

# 3. Recover revoked approval (resets to pending)
rec = client.recover_tool_approval(
    run_id="run-101",
    operation_key="toolop:run-101:s1:deploy_service:89ab",
    tool_name="deploy_service",
    reason="Artifact vulnerability resolved; ready for re-review",
    operator="secops_lead",
)
assert rec.decision == "pending"  # Requires explicit human approval before execution
```

### 12.10 Approval Expiration, Timeouts & Escalation

To ensure human oversight remains timely and approvals cannot be held open indefinitely or executed
against stale system context, Task 45 introduces time-limited approval lifecycles, proactive timeout
detection, audited escalation, and batch expiration.

#### 1. Configuration & Timeouts

The approval expiration subsystem is governed by validated runtime settings:
- **`tool_approval_timeout_seconds`**: Default duration in seconds (default `3600.0`, bounds `1.0` to
  `604800.0`) before a pending or granted approval expires.
- **`tool_approval_escalation_threshold_seconds`**: Duration in seconds (default `1800.0`, bounds `0.0`
  to `604800.0`) before an unattended pending approval is identified as overdue and escalated.
- **`tool_approval_escalation_enabled`**: Boolean flag (default `True`) controlling automatic escalation
  during scanning.
- **Per-Tool Override (`approval_timeout_seconds`)**: Configurable on `ToolPolicy` to define custom
  expiration durations per tool (e.g. 15 minutes for critical production commands). Changes to per-tool
  timeouts update the policy hash to prevent stale approvals.

#### 2. Durable Expiration Timestamps & Schema Migrations

Approval records in the `tool_approvals` table persist durable timestamps across restarts:
- **`expires_at`**: UTC timestamp when the approval window closes. Computed at creation or recovery time.
- **`expired_at`**: UTC timestamp when the record transitioned to `expired`.
- **`escalated_at`**: UTC timestamp when an escalation alert was registered.
- **`escalation_reason`**: Documented rationale or alert level (e.g., `"Approval request overdue"`).

*Schema Migration*: Existing SQLite databases are migrated non-destructively during store initialization
by inspecting table metadata, safely adding missing columns, and creating index
`idx_tool_approvals_expires_at`.

#### 3. Strict Pre-Dispatch & Retry/Resume Expiry Enforcement

1. **Pre-Execution Check**: In `Tool.__call__`, if `decision == "expired"` or `now >= expires_at`,
   tool invocation is rejected immediately with `ToolApprovalExpiredError`.
2. **Pre-Dispatch Revalidation**: In the downstream execution loop right before invoking external
   functions, the worker queries fresh approval state from the store. If the approval has expired
   while queued or retrying, execution aborts with `ToolApprovalExpiredError`.
3. **Distinct Diagnostics**: The diagnostic engine surfaces `state="expired"`, `status="expired"`,
   `diagnostic_code="expired_approval"`, `is_expired=True`, `is_blocked=True`, `can_execute=False`.
   This remains clearly distinguishable from rejected, revoked, or parameter-mismatched approvals.

#### 4. Safe Timeout Behavior & Operational Recovery

- **Zero Auto-Approval**: Expiry strictly never transitions an operation to approved.
- **Zero Auto-Redispatch**: The orchestrator never redispatches external tools upon expiration or timeout.
- **Operational Recovery Required**: To reconsider an expired operation, an authorized operator must
  call `POST /recover`, which resets the record to `pending` with a newly computed `expires_at`.
  An authorized human must then review and approve the request before execution can occur.

#### 5. Escalation & Batch Scanning

- **Manual Expiration**: `POST /runs/{run_id}/tools/{operation_key}/expire` explicitly marks an approval
  as expired and emits a `tool.approval_expired` audit event.
- **Manual Escalation**: `POST /runs/{run_id}/tools/{operation_key}/escalate` records an alert with
  `tool.approval_escalated`.
- **Batch Scanning**: `POST /runs/{run_id}/approvals/scan-expired` and `POST /approvals/scan-expired`
  scan pending approvals, transition timed-out ones to `expired`, and escalate overdue ones idempotently.

#### 6. Python SDK Client Reference

```python
client = OrchestratorApiClient(base_url="https://orchestrator.internal", api_token="secret")

# 1. Escalate overdue approval
esc = client.escalate_tool_approval(
    run_id="run-101",
    operation_key="toolop:run-101:s1:reboot:abc",
    reason="Pending approval overdue > 30 minutes",
    escalation_level="lead_operator",
)

# 2. Expire approval
exp = client.expire_tool_approval(
    run_id="run-101",
    operation_key="toolop:run-101:s1:reboot:abc",
    reason="Change window closed",
    force=True,
)

# 3. Batch scan expired approvals
scan = client.scan_expired_approvals(run_id="run-101")
print(f"Expired {scan.expired_count} approvals, escalated {scan.escalated_count}")
```

---

### 12.11 Automated Approval Lifecycle Scheduler

Building upon the manual and batch expiration capabilities of Task 45, Task 46 introduces a reliable,
fault-tolerant background scheduler (`ApprovalLifecycleScheduler`) that periodically scans for expired
tool approvals and auto-escalates overdue pending approvals without requiring manual trigger intervention.

#### 1. Architecture & Background Loop

- **`ApprovalLifecycleScheduler`**: Encapsulates background scanning, concurrency synchronization,
  and diagnostic metrics tracking.
- **Process Singleton**: Access via `get_approval_scheduler(store, settings)` and clean test fixture
  isolation via `reset_approval_scheduler()`.
- **Application Lifespan Integration**: Started during FastAPI lifespan initialization in
  `src/orchestrator/api/server.py` and attached to `app.state.approval_scheduler`. Gracefully stopped
  during application shutdown within bounded grace timeouts (`min(2.0, grace)`), preventing orphaned
  daemon threads.
- **Duplicate Prevention**: Repeated calls to `scheduler.start()` are no-ops; only a single active loop
  thread runs per process.

#### 2. Configuration Settings

The scheduler is configured via runtime settings and environment variable aliases:

| Setting Key | Env Aliases | Default | Bounds / Allowed | Description |
|:---|:---|:---:|:---:|:---|
| `tool_approval_scheduler_enabled` | `ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_ENABLED`<br>`TOOL_APPROVAL_SCHEDULER_ENABLED`<br>`APPROVAL_SCHEDULER_ENABLED` | `True` | `bool` | Enables or disables automated background scanning. When disabled, background thread is omitted while manual endpoints remain active. |
| `tool_approval_scan_interval_seconds` | `ORCHESTRATOR_TOOL_APPROVAL_SCAN_INTERVAL_SECONDS`<br>`TOOL_APPROVAL_SCAN_INTERVAL_SECONDS`<br>`APPROVAL_SCAN_INTERVAL_SECONDS`<br>`TOOL_APPROVAL_SCAN_INTERVAL` | `60.0` | `1.0` to `86400.0` (24h) | Interval in seconds between successive background scan cycles. |
| `tool_approval_escalation_enabled` | `TOOL_APPROVAL_ESCALATION_ENABLED` | `True` | `bool` | Whether overdue pending approvals are automatically escalated during scheduled scans. |

#### 3. Expiry and Escalation Automation Safeguards

Each scan cycle executes atomically within `run_scan_cycle()`:
1. **Approval Expiration**: Calls `CheckpointStore.check_and_expire_tool_approvals`, transitioning
   approvals whose persisted `expires_at` has passed (`expires_at <= now`) to `decision = 'expired'`.
2. **Overdue Auto-Escalation**: When `auto_escalate` is enabled, identifies pending approvals approaching
   expiry or waiting longer than `tool_approval_escalation_threshold_seconds` via
   `CheckpointStore.check_overdue_approvals`. Any pending, unescalated approval is escalated via
   `CheckpointStore.escalate_tool_approval`.
3. **Execution Safety Invariant**: The scheduler **never auto-approves**, **never redispatches**, and
   **never executes** tools. Unapproved, expired, or overdue operations strictly require explicit authorized
   human recovery and re-approval before execution.
4. **Idempotence**: Repeated scans on identical state produce zero duplicate state transitions and zero
   duplicate audit events.

#### 4. Concurrency Protection & Fault Tolerance

- **Scan Lock (`_scan_lock`)**: Serializes concurrent background scheduled scans and on-demand manual
  scan requests (`POST /approvals/scan-expired`), preventing database transaction collisions.
- **Safe Error Recovery**: Scan exceptions (e.g., transient SQLite database locks) are caught safely.
  Error details are sanitized using `sanitize_error` (redacting credentials, tokens, and passwords).
  The failure increments `consecutive_failures` and records `last_error` while backing off
  (`min(5.0, scan_interval)`), keeping the background loop alive.
- **Self-Healing**: Subsequent successful scans reset `consecutive_failures` to `0`, clear `last_error`,
  and update `last_successful_scan_at`.

#### 5. Observability & Diagnostics

- **Dedicated Endpoint**: `GET /approvals/scheduler` (Bearer token authenticated) returns
  `ApprovalSchedulerDiagnostics`:
  ```json
  {
    "enabled": true,
    "running": true,
    "scan_interval_seconds": 60.0,
    "last_scan_at": "2026-10-27T12:00:00+00:00",
    "last_successful_scan_at": "2026-10-27T12:00:00+00:00",
    "next_scheduled_scan_at": "2026-10-27T12:01:00+00:00",
    "total_scans": 42,
    "total_expired": 3,
    "total_escalated": 5,
    "consecutive_failures": 0,
    "last_error": null
  }
  ```
- **Unified Diagnostics**: `GET /diagnostics` embeds `approval_scheduler` as a top-level telemetry block.
- **Audit Logging**: Automatic expiration and escalation emit `tool.approval_expired` and
  `tool.approval_escalated` events with sanitized metadata.

#### 6. Python SDK Client Reference

```python
from orchestrator.api.client import OrchestratorApiClient

client = OrchestratorApiClient(base_url="https://orchestrator.internal", api_token="secret")

# Inspect background scheduler status and scan metrics
status = client.get_approval_scheduler_status()
print(
    f"Scheduler running: {status.running}, interval: {status.scan_interval_seconds}s, "
    f"total expired: {status.total_expired}, total escalated: {status.total_escalated}"
)

# Coordinate on-demand manual batch scan
scan = client.scan_expired_approvals()
print(f"Manual scan complete: expired={scan.expired_count}, escalated={scan.escalated_count}")
```

### 12.12 Cross-Process Scheduler Leadership & Lease Safety

When the Orchestrator API runs in a multi-process deployment (such as Uvicorn with multiple `--workers`
or multiple container replicas sharing a single persistent SQLite database), running autonomous background
scanners independently in every process would result in redundant queries and unnecessary database lock
contention. Cross-process scheduler leadership coordinates background scanning so that exactly one worker
process holds the active leadership lease and executes scheduled expiration and escalation scans at a time.

```mermaid
sequenceDiagram
    autonumber
    participant W1 as Worker 1 (Leader)
    participant DB as SQLite DB (scheduler_leases)
    participant W2 as Worker 2 (Standby)

    Note over W1,DB: Startup & Acquisition
    W1->>DB: BEGIN IMMEDIATE; acquire lease (fence_token=1)
    DB-->>W1: Lease granted (expires_at = now + 30s)
    W2->>DB: BEGIN IMMEDIATE; acquire lease
    DB-->>W2: Acquisition rejected (active owner = W1)
    Note over W2: Enters Standby state

    Note over W1,DB: Scheduled Execution & Heartbeat
    W1->>DB: Validate ownership (fence_token=1)
    DB-->>W1: Valid -> Execute scan cycle
    W1->>DB: Renew heartbeat (every 5s)
    DB-->>W1: Expiry refreshed

    Note over W1,W2: Graceful Release or Crash Takeover
    alt Graceful Shutdown
        W1->>DB: Release lease (status='released', expires=now)
        W2->>DB: Immediate acquisition (fence_token=2)
        Note over W2: Transitions to Leader
    else Crashed / Unresponsive Leader
        Note over W1: Crashes or hangs > 30s TTL
        W2->>DB: Attempt takeover after TTL expiry
        DB-->>W2: Takeover granted (fence_token=2)
        Note over W2: Transitions to Leader
        opt Stale W1 Wakes Up
            W1->>DB: Validate ownership (fence_token=1)
            DB-->>W1: Invalid (token 1 < 2) -> Fenced out!
            Note over W1: Transitions to Lost / Standby
        end
    end
```

#### 1. Durable Lease Storage & Schema Migration

Leadership leases are persisted in the `scheduler_leases` SQLite table with atomic `BEGIN IMMEDIATE;`
transactions and busy-retry loops. Existing databases migrate seamlessly without destructive schema alterations.

- **Table Schema**:
  ```sql
  CREATE TABLE IF NOT EXISTS scheduler_leases (
      lease_name TEXT PRIMARY KEY,
      owner_id TEXT NOT NULL,
      status TEXT NOT NULL,
      acquired_at TEXT NOT NULL,
      heartbeat_at TEXT NOT NULL,
      expires_at TEXT NOT NULL,
      fence_token INTEGER NOT NULL DEFAULT 1
  );
  CREATE INDEX IF NOT EXISTS idx_scheduler_leases_expires
      ON scheduler_leases (lease_name, expires_at);
  ```

#### 2. Monotonically Increasing Fencing Tokens

To prevent stale or zombie processes from executing scheduled scans after a network partition or prolonged
thread stall, every lease takeover or initial acquisition generates a strictly monotonic `fence_token`
(`existing_token + 1`).
- The leader must present its assigned `fence_token` when renewing the lease.
- Immediately before starting any scheduled scan cycle, the leader validates that its `fence_token` matches
  the database record.
- If another worker has superseded the lease, validation fails, and the stale worker is immediately fenced
  out, transitioning its state to `"lost"`.

#### 3. Leadership Lifecycle & Takeover Timing

- **Standby Mode**: Non-leader workers remain in standby. Background scan cycles are bypassed with reason
  `"Not scheduler leader"`.
- **Heartbeat & Renewal**: The active leader periodically renews its lease every
  `tool_approval_scheduler_heartbeat_interval_seconds` (default: 5.0 seconds).
- **Graceful Release**: When a leader worker shuts down gracefully, it marks `status = 'released'` and
  sets `expires_at = now()`, allowing standby workers to assume leadership immediately without waiting for
  the lease TTL.
- **Crash Recovery & Takeover**: If a leader process crashes or terminates abruptly (`SIGKILL`), standby
  workers detect that `now() >= expires_at` (default TTL: 30.0 seconds) and atomically take over leadership
  with an incremented `fence_token`.
- **Manual Scan Safety**: Manual scan APIs (`POST /approvals/scan-expired` and SDK `scan_expired_approvals()`)
  remain fully operational from any worker regardless of leadership status, serialized safely via `_scan_lock`.

#### 4. Configuration & Environment Variables

| Setting | Default | Bounds | Environment Variables |
| :--- | :--- | :--- | :--- |
| `tool_approval_scheduler_lease_ttl_seconds` | `30.0` | `1.0` – `3600.0` | `TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`<br>`ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`<br>`APPROVAL_SCHEDULER_LEASE_TTL_SECONDS`<br>`SCHEDULER_LEASE_TTL_SECONDS` |
| `tool_approval_scheduler_heartbeat_interval_seconds` | `5.0` | `0.1` – `300.0` | `TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`<br>`ORCHESTRATOR_TOOL_APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`<br>`APPROVAL_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS`<br>`SCHEDULER_HEARTBEAT_INTERVAL_SECONDS` |

> [!IMPORTANT]
> The cross-field validator requires `tool_approval_scheduler_heartbeat_interval_seconds` to be strictly less
> than `tool_approval_scheduler_lease_ttl_seconds`.

#### 5. Operational Diagnostics & Visibility

The `GET /approvals/scheduler` endpoint returns enriched leadership telemetry:

```json
{
  "enabled": true,
  "running": true,
  "scan_interval_seconds": 60.0,
  "last_scan_at": "2026-10-27T12:00:00+00:00",
  "last_successful_scan_at": "2026-10-27T12:00:00+00:00",
  "next_scheduled_scan_at": "2026-10-27T12:01:00+00:00",
  "total_scans": 42,
  "total_expired": 3,
  "total_escalated": 5,
  "consecutive_failures": 0,
  "last_error": null,
  "is_leader": true,
  "leadership_state": "leader",
  "owner_id": "scheduler-worker-1",
  "lease_owner_id": "scheduler-worker-1",
  "lease_expires_at": "2026-10-27T12:00:30+00:00",
  "lease_renewed_at": "2026-10-27T12:00:05+00:00",
  "fence_token": 1,
  "lease_ttl_seconds": 30.0,
  "heartbeat_interval_seconds": 5.0
}
```

- `leadership_state`: One of `"leader"`, `"standby"`, `"lost"`, `"stopped"`, or `"disabled"`.
- `lease_owner_id`: Identifies which worker holds the active lease, enabling standby workers to report the
  current cluster leader.
- `fence_token`: Monotonically increasing generation number for lease fencing verification.


### 12.13 Scheduler Scan Atomicity & Partial-Failure Recovery

When running automated background scans or manual expiry/escalation requests across batches of approvals,
system disruptions, partial row failures, or unexpected lease loss must never corrupt database state,
duplicate audit events, or orphan approvals. Task 48 establishes robust item-level atomicity, idempotent
recovery, and pre-commit fencing guarantees.

```mermaid
flowchart TD
    Start["Scan Cycle Initiated<br/>(actor: scheduler | operator)"] --> CheckLock["Acquire _scan_lock"]
    CheckLock --> BeginTx["BEGIN IMMEDIATE tx<br/>(busy retry loop, up to 25 attempts)"]
    BeginTx --> UpfrontFence{"actor == 'scheduler'?<br/>Validate lease ownership & fence token"}
    UpfrontFence -- "Superseded" --> RollbackFatal["ROLLBACK transaction<br/>Raise SchedulerFencingError"]
    UpfrontFence -- "Valid / Manual" --> QueryRows["Query pending/active<br/>where expires_at <= cutoff"]
    QueryRows --> LoopRows["Iterate Rows<br/>SAVEPOINT sp_expire_{idx}"]
    LoopRows --> RowExec{"Update row state<br/>decision='expired'"}
    RowExec -- "Success" --> ReleaseSP["RELEASE SAVEPOINT<br/>Record in expired_records"]
    RowExec -- "Error" --> RollbackSP["ROLLBACK TO SAVEPOINT<br/>Record in failed_records"]
    ReleaseSP --> NextRow{"More rows?"}
    RollbackSP --> NextRow
    NextRow -- "Yes" --> LoopRows
    NextRow -- "No" --> PreCommitFence{"actor == 'scheduler'?<br/>Re-validate lease & token"}
    PreCommitFence -- "Lost" --> RollbackAll["ROLLBACK transaction<br/>Raise SchedulerFencingError"]
    PreCommitFence -- "Valid / Manual" --> CommitTx["COMMIT transaction"]
    CommitTx --> AuditEvents["Emit audit events with dedup=True<br/>(check per-operation_key in metadata)"]
    AuditEvents --> ReturnReport["Return scan result<br/>(expired, escalated, failed keys)"]
    RollbackFatal --> HandleLoss["Scheduler state -> 'lost'<br/>is_leader=False, token=None"]
    RollbackAll --> HandleLoss
```

#### 1. Item-Level Atomicity with Nested Savepoints

A batch expiration scan can encounter thousands of records. An isolated error on a single record
(such as SQLite row locking, constraint errors, or corrupted payload data) must not abort or roll back
the entire batch:
- **Outer Transaction**: Scans run inside a serialized `BEGIN IMMEDIATE;` SQLite transaction with bounded
  busy-retry loops (`max_retries = 25`, exponential backoff).
- **Per-Item Savepoint**: Each approval row is isolated within `SAVEPOINT sp_expire_{idx}`.
- **Partial Failure Isolation**: If a write fails for row $k$, the store rolls back specifically to
  `sp_expire_{idx}`, logs a sanitized warning, appends the failure to `failed_records` / `partial_failures`,
  and proceeds with row $k+1$.
- **Durable Progress**: All valid records commit cleanly, while failed records remain in their previous
  valid state (`pending`) for subsequent scan cycles to retry.

#### 2. Stale-Owner Fencing & Pre-Commit Validation

To prevent zombie or partitioned scheduler workers from committing scheduled mutations after another process
has acquired or superseded leadership:
- **Upfront Fencing Check**: When `actor == "scheduler"` and fencing parameters are provided, the lease is
  validated before any rows are updated.
- **Pre-Commit Fencing Revalidation**: Immediately before executing `conn.commit()`, the store re-validates
  `validate_scheduler_lease_ownership()`. If the worker was superseded or lease expired while processing rows,
  `conn.rollback()` is executed immediately, rolling back all batch updates and ensuring zero uncommitted
  mutations enter the database.
- **Scheduler State Transition**: The background scheduler catches `SchedulerFencingError`, marks
  `leadership_state = "lost"`, clears `is_leader = False` and `fence_token = None`, and logs a structured
  warning.

#### 3. Idempotent Retries & Duplicate Event Prevention

Retrying a failed or interrupted scan is guaranteed to be safe and idempotent:
- **State Idempotency**: Pending approvals that were successfully expired on attempt 1 are no longer matched
  by `decision IN ('pending', 'approved')` on attempt 2.
- **Partial Retries**: Approvals that experienced an isolated failure on attempt 1 remain in `pending` and
  are cleanly processed and expired on attempt 2 without touching already-expired records.
- **Audit Event Deduplication**: `record_audit_event(..., dedup=True)` inspects existing audit logs using
  `json_extract(metadata_json, '$.operation_key')` (with fallback to SQLite `LIKE`). Even if a scan or
  manual recovery call is repeated multiple times, exactly one logical `tool.approval_expired` and
  `tool.approval_escalated` audit event is ever recorded per operation key.

#### 4. API & Diagnostics Telemetry

- **`POST /approvals/scan-expired`**:
  Returns partial failure details in `ScanExpiredApprovalsResponse`:
  ```json
  {
    "run_id": "run-xyz",
    "scanned_at": "2026-10-27T12:00:00+00:00",
    "expired_count": 5,
    "escalated_count": 5,
    "expired_operation_keys": ["op-1", "op-2", "op-3", "op-4", "op-5"],
    "escalated_operation_keys": ["op-1", "op-2", "op-3", "op-4", "op-5"],
    "failed_count": 1,
    "failed_operation_keys": ["op-6"]
  }
  ```
- **`GET /approvals/scheduler`**:
  Exposes `last_scan_failed_count` and sanitized error summary in `ApprovalSchedulerDiagnostics`:
  ```json
  {
    "is_leader": true,
    "leadership_state": "leader",
    "total_scans": 120,
    "total_expired": 14,
    "total_escalated": 18,
    "last_scan_failed_count": 0,
    "last_error": null
  }
  ```

#### 5. Recovery Guarantees & Remaining Limitations

| Guarantee | Mechanism | Scope |
| :--- | :--- | :--- |
| **Batch Atomicity** | Nested SQLite savepoints (`sp_expire_{idx}`) | Per-row isolation in batches |
| **No Dirty Stale Commits** | Pre-commit fencing validation & rollback | Stale workers superseded in flight |
| **Idempotent Scan Retries** | Terminal `expired` state transitions | Safe repeated scan execution |
| **Audit Log Integrity** | Operation-key deduplication (`dedup=True`) | Zero duplicate logical audit events |
| **Concurrent Safety** | In-process `_scan_lock` + `BEGIN IMMEDIATE;` | Serialized scan operations |

> [!WARNING]
> **Limitations**:
> 1. SQLite's single-writer architecture can experience busy contention under sustained multi-process write
>    loads; busy retry loops mitigate but do not eliminate disk contention if external processes lock the DB.
> 2. External side effects (e.g. downstream webhooks, push notifications) do not participate in SQLite's
>    transaction boundary; they must rely on downstream idempotency keys for end-to-end deduplication.


### 12.14 Atomic Approval State & Audit Persistence

In high-concurrency environments and failure-recovery scenarios, an approval decision, revocation,
expiration, or escalation must never be committed without its corresponding audit event, nor may an
audit failure leave state transitions half-persisted. Task 49 unifies approval lifecycle transitions and
audit logging into co-located SQLite ACID transactions.

```mermaid
sequenceDiagram
    autonumber
    participant Caller as API / Scheduler Loop
    participant Store as CheckpointStore
    participant Conn as SQLite Connection (in-tx)
    participant DB as SQLite Engine (WAL mode)

    Caller->>Store: expire_tool_approval(run_id, op_key)
    Store->>Conn: UPDATE tool_approvals SET decision='expired'...
    Note over Store,Conn: Same Transaction Connection
    Store->>Conn: record_audit_event(TOOL_APPROVAL_EXPIRED, conn=conn, raise_on_error=True)
    alt Audit Write Fails (Disk, Lock, Constraint)
        Conn-->>Store: Exception (e.g. OperationalError)
        Store->>Conn: conn.rollback()
        Note over DB: Both state change & audit event are rolled back
        Store-->>Caller: Raise error (state remains pending)
    else Audit Write & State Update Succeed
        Store->>Conn: conn.commit()
        Note over DB: State transition & audit event commit atomically
        Store-->>Caller: Success (decision='expired', audit_event_id=aud-xyz)
    end
```

#### 1. Co-Located Transaction Boundaries

Because `tool_approvals` and `audit_events` reside in the same SQLite database file (`db_path`),
all approval lifecycle transitions are performed atomically on a single database connection:
- **Single Expirations (`expire_tool_approval`)**:
  Updates `tool_approvals` and records `tool.approval_expired` audit event on `conn` before `conn.commit()`.
- **Overdue Escalations (`escalate_tool_approval`)**:
  Updates `escalated_at` and records `tool.approval_escalated` audit event on `conn` before `conn.commit()`.
- **Decisions (`record_tool_approval`)**:
  Updates `decision` (`approved` or `rejected`) and records `tool.approved` or `tool.policy_denied`
  on `conn` before `conn.commit()`.
- **Revocations (`revoke_tool_approval`)**:
  Updates `decision = 'revoked'` and records `tool.approval_revoked` on `conn` before `conn.commit()`.
- **Recoveries (`recover_tool_approval`)**:
  Resets `decision = 'pending'` and records `tool.approval_recovered` on `conn` before `conn.commit()`.
- **Batch Scans (`check_and_expire_tool_approvals`)**:
  Each candidate approval row is wrapped in `SAVEPOINT sp_expire_{idx}`. The row update and its audit
  event insert execute within the savepoint. If an audit write fails, only that row rolls back to its
  savepoint, recording the failure in `partial_failures` while other rows proceed cleanly.

#### 2. Audit-Write Failure Rollback Semantics

When `raise_on_error=True` is provided to `record_audit_event()`, audit persistence errors are never
swallowed:
- If the audit event write fails (due to SQLite lock contention, disk corruption, or constraint violations),
  the store invokes `conn.rollback()` (or savepoint rollback).
- The approval record remains in its previous valid state (`pending`, `approved`, etc.).
- Callers receive an explicit error, preventing silent "phantom approvals" or un-audited state mutations.

#### 3. Operation-Key Deduplication Across Retries

Deduplication (`dedup=True`) inspects existing audit logs using `(event_type, outcome, operation_key)`:
- Queries `json_extract(metadata_json, '$.operation_key')` with fallback to `LIKE '%"operation_key": "..."%'`.
- If an audit record already exists for that operation key, insertion is safely skipped.
- Repeated scan cycles, manual recovery retries, or API retries never produce duplicate audit events.

#### 4. Stale-Worker Fencing Rollback

For scheduled executions (`actor == "scheduler"`):
- Pre-commit fencing validation revalidates `validate_scheduler_lease_ownership()`.
- If leadership was superseded while batch rows were in flight, `conn.rollback()` rolls back all updates
  and audit events across the entire transaction.
- Stale workers commit zero approval mutations and zero audit events.

#### 5. Legacy Record Safety & Inspection Policy

- **Non-Invention Rule**: Legacy database records that completed state transitions prior to Task 49
  without corresponding audit events remain valid in SQLite. The orchestrator never retroactively
  manufactures synthetic historical audit events with artificial timestamps.
- **Diagnostic Inspection**: The helper `verify_approval_audit_consistency(operation_key)` non-destructively
  checks whether an approval has a matching audit trail:
  ```python
  diag = store.verify_approval_audit_consistency("op-123")
  # Returns: {
  #   "operation_key": "op-123",
  #   "decision": "expired",
  #   "has_audit_event": True,
  #   "audit_event_count": 1,
  #   "is_legacy_uncommitted_audit": False,
  #   "consistent": True
  # }
  ```

#### 6. Guarantees & Remaining Limitations

| Guarantee | Mechanism | Scope |
| :--- | :--- | :--- |
| **Atomic State & Audit** | Single `conn` + single `conn.commit()` | Approval lifecycle transitions |
| **Audit Failure Rollback** | `raise_on_error=True` + `conn.rollback()` | Zero un-audited state changes |
| **Savepoint Row Isolation** | Nested SQLite savepoints (`sp_expire_{idx}`) | Batch approval scans |
| **Audit Deduplication** | `dedup=True` on `(type, outcome, operation_key)` | Scans, retries, and API calls |
| **Fencing Safety** | Pre-commit lease validation & full rollback | Stale scheduled workers |
| **Legacy Compatibility** | Read-through preservation & no fake backfills | Pre-Task 49 SQLite databases |

> [!WARNING]
> **Limitations**:
> 1. SQLite's single-writer architecture can experience busy lock contention under sustained multi-process
>    write loads; retry loops mitigate but do not completely eliminate lock waits.
> 2. External side effects (e.g. webhooks, SMS alerts) execute outside SQLite's ACID boundary and must
>    rely on downstream idempotency keys for end-to-end deduplication.

### 12.15 Transactional Outbox for Reliable External Notifications

#### 1. Architecture & Durable Outbox Storage
The transactional outbox pattern guarantees that whenever an approval lifecycle mutation occurs
(`tool.approval_requested`, `tool.approval_granted`, `tool.approval_rejected`, `tool.approval_revoked`,
`tool.approval_expired`, `tool.approval_recovered`, `tool.approval_escalated`), an outbox record is
persisted in the exact same SQLite transaction as the approval state change and its audit event.

If the worker process crashes immediately after committing the state change, or if external notification
endpoints are temporarily down, the notification intent is durably preserved in `outbox_events`:

```sql
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT NOT NULL,
    last_error TEXT,
    claimed_by TEXT,
    claimed_at TEXT,
    claim_expires_at TEXT,
    delivered_at TEXT,
    idempotency_key TEXT UNIQUE,
    fence_token INTEGER NOT NULL DEFAULT 1
);
```

#### 2. Delivery Guarantees & Receiver-Side Idempotency
- **At-Least-Once Delivery**: External notification handlers receive notifications with guaranteed
  at-least-once delivery semantics.
- **Idempotency Keys**: Every event includes a stable `idempotency_key` (derived deterministically from
  the event type, operation key, approver/actor, and lifecycle timestamps).
- **Critical Notice on Exactly-Once Delivery**: Exactly-once delivery to external systems (such as Slack,
  email, or webhooks) cannot be mathematically guaranteed without receiver-side deduplication. If a process
  or network connection terminates after sending an external request but before recording the `delivered`
  status in SQLite, the dispatcher will safely re-deliver the event on recovery. Receivers must deduplicate
  incoming notifications using the event's stable `idempotency_key` or `event_id`.

#### 3. Bounded Dispatcher & Multi-Worker Fencing
The background `OutboxDispatcher` coordinates event delivery across multiple processes:
1. **Claiming**: Workers claim eligible events using `claim_outbox_events()`. An event is eligible when:
   - `status = 'pending' AND next_retry_at <= now`, OR
   - `status = 'claimed' AND claim_expires_at <= now` (recovering crashed/stalled workers).
2. **Lease & Fencing Token**: Claiming increments `fence_token` and sets `claim_expires_at` based on
   `outbox_claim_ttl_seconds`.
3. **Stale Owner Rejection**: When reporting delivery results via `update_outbox_event_delivery()`, the
   store validates `claimed_by` and `fence_token`. If another worker took over an expired claim, the stale
   worker's update is rejected with `StaleDispatcherError`, preventing superseded processes from updating
   status.

#### 4. Configurable Retry & Dead-Letter Handling
- When delivery fails with a retryable error, the dispatcher computes exponential backoff:
  `backoff = min(outbox_retry_backoff_max_seconds, outbox_retry_backoff_base_seconds * 2^(attempt - 1))`
- The event's `status` returns to `pending` with `next_retry_at` set to the future timestamp.
- If delivery attempts reach `outbox_max_attempts` (or the handler reports `retryable=False`), the event
  transitions to durable `dead_letter` status with sanitized error metadata in `last_error`.
- Dead-letter events can be inspected via `GET /approvals/outbox?status=dead_letter` and safely re-queued
  via `POST /approvals/outbox/{event_id}/retry`.

#### 5. Notification Handler Interface
```python
from orchestrator.notifications import (
    NotificationDeliveryResult,
    NotificationHandler,
    TestNotificationHandler,
)

class CustomWebhookHandler(NotificationHandler):
    def send_notification(self, event: dict[str, Any]) -> NotificationDeliveryResult:
        idemp_key = event.get("idempotency_key")
        # Transmit with idempotency header to receiver
        return NotificationDeliveryResult(success=True)
```

#### 6. Operational Endpoints & Diagnostics
- `GET /approvals/outbox`: List outbox events with status and event_type filters and pagination.
- `GET /approvals/outbox/diagnostics`: Inspect status counts, oldest pending event, and dispatcher health.
- `POST /approvals/outbox/{event_id}/retry`: Reset failed or dead-letter events for retry.
- `POST /approvals/outbox/dispatch`: Manually trigger a bounded dispatch cycle.

#### 7. Configuration Reference

| Setting | Default | Bounds | Env Variable / Aliases |
| :--- | :--- | :--- | :--- |
| `outbox_enabled` | `true` | `bool` | `OUTBOX_ENABLED` |
| `outbox_dispatcher_enabled` | `true` | `bool` | `OUTBOX_DISPATCHER_ENABLED` |
| `outbox_poll_interval_seconds` | `2.0` | `0.1` - `300.0` | `OUTBOX_POLL_INTERVAL_SECONDS` |
| `outbox_batch_size` | `50` | `1` - `1000` | `OUTBOX_BATCH_SIZE` |
| `outbox_max_attempts` | `5` | `1` - `100` | `OUTBOX_MAX_ATTEMPTS` |
| `outbox_retry_backoff_base_seconds` | `2.0` | `0.1` - `60.0` | `OUTBOX_RETRY_BACKOFF_BASE_SECONDS` |
| `outbox_retry_backoff_max_seconds` | `300.0` | `1.0` - `3600.0` | `OUTBOX_RETRY_BACKOFF_MAX_SECONDS` |
| `outbox_claim_ttl_seconds` | `30.0` | `1.0` - `3600.0` | `OUTBOX_CLAIM_TTL_SECONDS` |


### 12.16 Outbox Delivery Observability & Operational Recovery

#### 1. Observability Architecture & Durable Metrics
Task 51 provides comprehensive operational visibility and safe operator recovery for the transactional outbox.
Metrics are derived directly from durable SQLite tables rather than relying exclusively on volatile in-memory
counters, ensuring telemetry remains accurate across process restarts and multi-worker deployments.

Durable metrics exposed via `GET /approvals/outbox/diagnostics`:
- **Status Counts**: Exact database counts for `pending`, `claimed`, `delivered`, and `dead_letter` states.
- **Oldest Pending Event & Age**: Creation timestamp and elapsed age in seconds of the oldest pending event,
  enabling alerting when notifications are backlogged.
- **Retry Metrics**: Persisted aggregates including `total_attempts`, `total_retries`, `retried_events_count`,
  and `repeated_failures_count`.
- **Delivery Latency**: Sampled latency computed as $(t_{\text{delivered}} - t_{\text{created}})$ across
  the most recent delivered events, reporting `min`, `max`, `avg`, and `p95` latency in seconds.
- **Recent Failures**: Chronological list of recent failed delivery attempts with sanitized error summaries
  (guaranteeing credentials, tokens, and authorization headers are permanently redacted).
- **Expired Claims**: Detection and listing of events currently in `claimed` status whose `claim_expires_at`
  lease has lapsed, facilitating diagnosis of crashed or hung worker processes.
- **Retry Limit Diagnostics**: Granular visibility into events approaching configured retry thresholds
  (`max_configured_attempts - 1`) and events that have exceeded attempt limits.

#### 2. Safe Operator Recovery Safeguards
Operators can remediate stuck, failed, or dead-letter outbox events via individual retry or batch recovery.
The recovery protocol enforces strict safety invariants:
1. **Mandatory Non-Empty Reason**: Every recovery action requires an explicit operator justification recorded
   in the durable audit trail. Empty or whitespace-only reasons are rejected with HTTP 400.
2. **Active Claim Protection**: Events with unexpired claim leases held by an active worker cannot be reset
   silently. Attempts to retry an active event fail with HTTP 409 Conflict unless the operator
   explicitly passes `force=true` (e.g. to recover an ungracefully terminated worker).
3. **Monotonic Fencing Advancement**: Every recovery resets the event to `pending`, clears transient error
   fields, and atomically increments `fence_token`. This invalidates any late-arriving updates from previous
   stale workers via `StaleDispatcherError`.
4. **Permanent Delivery History**: Individual attempts, failures, and recovery operations are appended to
   the append-only `outbox_delivery_history` table. Delivery history is never erased or overwritten.
5. **Structured Audit Trail**: Every recovery operation emits an `outbox.retry_requested` structured audit
   event containing the operator identity, reason, previous status, attempt count, and sanitized error.

#### 3. Operational REST Endpoints & SDK Integration

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/approvals/outbox/diagnostics` | `GET` | Durable metrics, latency, expired claims, and diagnostics. |
| `/approvals/outbox/{event_id}/retry` | `POST` | Safely retry an outbox event with mandatory reason. |
| `/approvals/outbox/recover` | `POST` | Batch-recover eligible dead-letter events with reason. |
| `/approvals/outbox/{event_id}/history` | `GET` | Chronological delivery attempts and recovery audit log. |

Corresponding SDK methods on `OrchestratorApiClient`:
- `client.get_outbox_diagnostics()`
- `client.retry_outbox_event(event_id, reason, operator, reset_attempts, force)`
- `client.recover_outbox_events(reason, event_ids, status_filter, operator, reset_attempts, max_events)`
- `client.get_outbox_event_history(event_id, limit)`

#### 4. Operational Invariants & Limitations
- **No Automatic Tool Approvals**: Outbox recovery strictly affects external notification delivery; it never
  approves tools, bypasses human-in-the-loop review, or re-executes tool operations.
- **At-Least-Once Delivery**: External notifications operate under at-least-once semantics. Receiving webhooks
  must implement deduplication using `idempotency_key` or `event_id`.
- **Database Lock Contention**: While SQLite handles concurrent readers efficiently, batch recovery of
  hundreds of events should be throttled using `max_events` to prevent SQLite lock contention.

---

### 12.17 Production Notification Providers & Webhook Integration

#### 1. Overview & Provider Architecture
Task 52 introduces production-ready external HTTP webhook delivery for the transactional outbox,
governed by an extensible provider registry and factory architecture:
- **`NotificationHandler`**: Base interface contract with
  `send_notification(event) -> NotificationDeliveryResult`.
- **`TestNotificationHandler`**: In-memory test provider for simulation and local offline testing.
- **`HttpWebhookNotificationHandler`**: High-performance HTTP client delivering JSON events with
  SSRF protection, bounded memory buffering, response classification, and credential scrubbing.
- **Provider Registry**: Dynamic lookup via `get_notification_handler(provider, settings, **kwargs)` with
  extensibility through `register_notification_provider(name, factory)`.
- **Dispatcher Integration**: `OutboxDispatcher` automatically uses `get_notification_handler()` when no
  explicit handler instance is injected.

#### 2. HTTP Webhook Delivery Specifications
When delivering events, `HttpWebhookNotificationHandler` sends an HTTP `POST` with:
- **Headers**:
  - `Content-Type: application/json`
  - `User-Agent: Multi-Agent-Orchestrator-Webhook/1.0`
  - `X-Event-ID`: Unique persistent UUID of the outbox record.
  - `X-Event-Type`: State transition name (e.g. `tool.approval_requested`, `tool.approval_granted`).
  - `X-Idempotency-Key`: Stable deduplication key enabling receiver-side idempotent processing.
  - Authorization Header: Formatted as `Authorization: Bearer <token>` or custom header key.
- **JSON Payload**: Contains `event_id`, `event_type`, `payload`, `idempotency_key`, `attempt_count`,
  and `created_at`.
- **Response Classification**:
  - `2xx`: Success (`success=True, retryable=False, delivered_at=ISO8601`).
  - `429`, `5xx`, Timeouts, Connection Failures: Transient failures (`success=False, retryable=True`).
  - `4xx` (except 429), `3xx`: Terminal failures (`success=False, retryable=False`), transitioning immediately
    to `dead_letter` status in SQLite.
  - Redirects (`301`, `302`, `307`, `308`): Strictly rejected (`follow_redirects=False`) to avoid SSRF
    bypass vectors.

#### 3. Security & SSRF Protection
The webhook handler enforces strict security boundaries before dispatching network requests:
- **HTTPS Enforcement**: HTTPS is required by default. Insecure `http://` URLs are rejected unless
  explicitly allowed via `notification_webhook_allow_insecure_http=True` (for testing only).
- **Embedded Credential Rejection**: URLs with embedded credentials (`https://user:pass@host`) are rejected.
- **IP Address Validation**: Rejects loopback (`127.0.0.0/8`, `::1`), RFC 1918 private networks
  (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), link-local / cloud metadata (`169.254.0.0/16`,
  `fe80::/10`), Carrier-Grade NAT (`100.64.0.0/10`), Benchmark (`198.18.0.0/15`), and IPv4-mapped
  IPv6 equivalents (`::ffff:127.0.0.1`).
- **DNS Resolution Check**: Hostnames are resolved via DNS at runtime to verify that target IP addresses do
  not point to restricted infrastructure.
- **Bounded Response Body**: Error response snippets are truncated to
  `notification_webhook_max_response_bytes` (default: 64 KB, capped at 1 MB) to prevent unbounded memory
  usage or disk flooding.
- **Secret Redaction**: Configured webhook authentication tokens and API keys are scrubbed from error
  messages, audit logs, and delivery history entries before database persistence.

#### 4. Configuration Reference
| Configuration Field | Default | Description |
| :--- | :--- | :--- |
| `notification_provider` | `"test"` | Active provider (`"test"`, `"webhook"`). |
| `notification_webhook_url` | `None` | Target webhook HTTPS destination URL. |
| `notification_webhook_auth_token` | `None` | Authentication or Bearer token. |
| `notification_webhook_auth_header` | `"Authorization"` | Authorization header name. |
| `notification_webhook_timeout_seconds` | `10.0` | Timeout in seconds (0.5 to 120.0). |
| `notification_webhook_tls_verify` | `True` | Enforce TLS certificate verification. |
| `notification_webhook_max_response_bytes` | `65536` | Max body bytes in error logs (256-1MB). |
| `notification_webhook_allow_private_ips` | `False` | SSRF bypass for local tests only. |
| `notification_webhook_allow_insecure_http` | `False` | Allow http:// for local tests only. |

#### 5. Operational Invariants & Guarantees
- **At-Least-Once Delivery**: External notifications provide at-least-once delivery guarantees. Webhook
  receivers must enforce idempotency using `X-Idempotency-Key` or `X-Event-ID`.
- **Approval Safety**: Webhook failures never reverse or alter committed tool approval lifecycle decisions.
- **Fenced Database Updates**: Stale dispatcher workers cannot commit delivery status after lease expiration.

---

### 12.18 Webhook Payload Signing, Replay Protection & Secret Rotation

#### 1. Overview & Threat Model
Task 53 hardens external webhook notifications with cryptographic payload verification,
tamper detection, timestamp-based replay protection, and zero-downtime secret rotation:
- **Tamper Resistance**: Guarantees that neither the JSON payload, the event ID, nor the timestamp
  can be modified in transit without invalidating the cryptographic signature.
- **Replay Protection**: Rejects stale or future-dated deliveries outside a configurable time tolerance,
  and guards against duplicate event replay within the tolerance window.
- **Zero-Downtime Secret Rotation**: Enables dual-signature emission during secret rotation transitions,
  allowing receivers to update credentials without dropping delivery verification.

#### 2. HMAC-SHA256 Signing Specifications
When `notification_webhook_signing_secret` is configured, each outbound request attaches a versioned
signature header (default: `X-Webhook-Signature`):
- **Canonical Signed Message**:
  ```python
  signed_bytes = f"{timestamp}.{event_id}.".encode() + payload_bytes
  signature = hmac.new(secret.encode(), signed_bytes, hashlib.sha256).hexdigest()
  ```
- **Header Format**:
  - Single Secret: `X-Webhook-Signature: t=1730376000,v1=9f86d081884c7d65...`
  - Dual Secret (Rotation): `X-Webhook-Signature: t=1730376000,v1=sig_current,v1=sig_previous`
- **Verification Guarantee**: Receiver comparisons MUST use constant-time equality checks
  (`hmac.compare_digest`) to prevent timing side-channel attacks.

#### 3. Timestamp Validation & Replay Defense
- **Drift Tolerance**: Configured via `notification_webhook_signature_tolerance_seconds` (default: 300.0s).
- **Tolerance Enforcement**:
  - If `now - timestamp > tolerance`: Request is rejected as expired / stale.
  - If `now - timestamp < -tolerance`: Request is rejected as excessively future-dated.
- **Replay Protection Cache**:
  The `ReplayProtectionCache` utility tracks seen event IDs within their active tolerance window.
  Re-transmissions of identical event IDs within the window are rejected with a replay error.

#### 4. Safe Secret Rotation Procedure
To rotate a webhook signing secret with zero downtime:
1. **Generate New Secret**: Create a high-entropy secret string `SECRET_NEW`.
2. **Deploy Sender Dual-Signing**:
   - Set `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS = <current_secret>`
   - Set `NOTIFICATION_WEBHOOK_SIGNING_SECRET = <new_secret>`
   - The sender now generates dual `v1` signatures for both secrets in each header.
3. **Update Receivers**: Configure receivers to verify using `SECRET_NEW` (or both secrets).
4. **Retire Old Secret**: Unset `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS` on the sender.

#### 5. Receiver Verification Example
Receiving applications can verify incoming webhook requests using the provided helper:
```python
from orchestrator.notifications.signing import verify_webhook_signature

is_valid = verify_webhook_signature(
    body=request.body(),
    header_value=request.headers.get("X-Webhook-Signature"),
    secrets=["whsec_active_secret", "whsec_previous_secret"],
    event_id=request.headers.get("X-Event-ID"),
    tolerance_seconds=300.0,
    raise_on_error=False,
)
if not is_valid:
    return Response(status_code=401, content="Invalid webhook signature")
```

#### 6. Configuration Reference
| Field Name | Default | Description |
| :--- | :--- | :--- |
| `notification_webhook_signing_secret` | `None` | Active HMAC-SHA256 signing secret. |
| `notification_webhook_signing_secret_previous` | `None` | Previous secret during rotation window. |
| `notification_webhook_signature_header` | `"X-Webhook-Signature"` | HTTP signature header name. |
| `notification_webhook_signature_tolerance_seconds` | `300.0` | Timestamp tolerance window (5-86400s). |

---

### 12.19 Durable Replay Protection & Multi-Process Verification

#### 1. Overview & Threat Model
In Task 53, webhook replay protection relied on an in-memory cache (`ReplayProtectionCache`).
While effective within a single process lifespan, in-memory caches have limitations in production:
- Process restarts wipe out recorded event identities, allowing replayed payloads to be accepted again.
- Multi-process worker deployments (e.g. gunicorn/uvicorn workers or autoscaling pods) do not share memory,
  creating race windows where concurrent requests with identical event IDs can be accepted by different
  workers.

Task 54 resolves these limitations by introducing a durable, multi-process SQLite replay store
(`SqliteReplayStore`) with atomic database-level conflict resolution.

#### 2. Architecture & Design Principles
- **Atomic UPSERT Deduplication**: Uses SQLite `INSERT ... ON CONFLICT(event_id) DO UPDATE ... WHERE
  expires_at < now` to guarantee that concurrent workers attempting to register the same event identity
  resolve atomically at the database layer without race conditions.
- **Strict Verification Ordering**: Cryptographic HMAC signature validation is executed strictly *before*
  replay registration. Invalid or tampered requests are rejected immediately without consuming or recording
  the event identity.
- **Fail-Closed Guarantee**: If the underlying replay storage encounters an operational database error (e.g.,
  disk full, malformed database), signature verification fails closed rather than allowing potentially
  replayed requests.
- **Pruning & Safe Expiration**: Expired replay entries can be pruned via batch cleanup (`cleanup_expired()`)
  without permanently locking out future valid events.

#### 3. Storage Hierarchy
- `BaseReplayStore`: Abstract base class defining `check_and_record()`, `cleanup_expired()`, and `clear()`.
- `SqliteReplayStore`: Durable SQLite implementation storing records in `webhook_replay_records` table
  (`event_id`, `timestamp`, `expires_at`, `recorded_at`).
- `InMemoryReplayCache`: Thread-safe bounded in-memory cache (aliased to `ReplayProtectionCache` for backwards
  compatibility).
- `get_default_replay_store()`: Factory returning the configured replay store based on
  orchestrator `Settings`.

#### 4. Usage Example
```python
from orchestrator.notifications.signing import (
    SqliteReplayStore,
    WebhookSignatureVerifier,
)

store = SqliteReplayStore(db_path="/var/data/orchestrator.db", retention_seconds=86400.0)
verifier = WebhookSignatureVerifier(
    secrets=["whsec_active_secret", "whsec_previous_secret"],
    tolerance_seconds=300.0,
    replay_store=store,
)

is_valid = verifier.verify(
    body=request.body(),
    header_value=request.headers.get("X-Webhook-Signature"),
    event_id=request.headers.get("X-Event-ID"),
    raise_on_error=False,
)
```

#### 5. Configuration Reference
| Field Name | Default | Description |
| :--- | :--- | :--- |
| `notification_webhook_replay_protection_enabled` | `True` | Whether replay protection is enabled. |
| `notification_webhook_replay_storage` | `"sqlite"` | Storage backend: 'sqlite' or 'memory'. |
| `notification_webhook_replay_retention_seconds` | `86400.0` | Replay record retention window in seconds. |
| `notification_webhook_replay_cleanup_batch_size` | `500` | Prune batch size for expired records. |

---

### 12.20 Replay Store Maintenance & Operational Diagnostics

#### 1. Overview & Operational Lifecycle
In Task 54, durable replay protection was introduced via `SqliteReplayStore`. To ensure continuous
operational reliability, storage bounds, and administrative visibility, Task 55 introduces:
- **Automated Lifecycle Maintenance**: Background scheduler (`ReplayCleanupScheduler`) that periodically
  prunes expired replay records in bounded batches using cross-process leadership leases.
- **Durable Diagnostics**: Database-derived aggregate metrics on total, active, and expired records,
  plus historical tracking of the last cleanup execution.
- **Authenticated Operational API**: Secure endpoints (`GET /notifications/replay/diagnostics` and
  `POST /notifications/replay/cleanup`) allowing operators to inspect metrics and trigger bounded prunes.
- **Audit Logging**: Every maintenance operation is recorded to the immutable audit log under
  `notification.replay_cleanup`.

#### 2. Cross-Process Leadership & Concurrency Safety
The automated cleanup worker coordinates across multiple server instances using SQLite-backed scheduler
leases (`acquire_scheduler_lease` on `webhook_replay_cleanup`):
- Only the elected leader executes periodic prunes.
- Standby workers yield to prevent redundant database churn.
- If a leader process halts, the lease expires and standby workers automatically resume maintenance.
- Deletion is strictly bounded by `expires_at < current_time` and batch size limits, guaranteeing that
  active replay identities are never deleted.

#### 3. Administrative REST API
| Endpoint | Method | Auth | Description |
| :--- | :--- | :--- | :--- |
| `/notifications/replay/diagnostics` | GET | Bearer | Inspect aggregate metrics & cleanup status |
| `/notifications/replay/cleanup` | POST | Bearer | Trigger bounded cleanup with operator reason |

Example Cleanup Request:
```bash
curl -X POST https://api.orchestrator.internal/notifications/replay/cleanup \
  -H "Authorization: Bearer <ADMIN_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"limit": 500, "reason": "Routine database pruning"}'
```

#### 4. Configuration Reference
| Field Name | Default | Bounds | Description |
| :--- | :--- | :--- | :--- |
| `notification_webhook_replay_cleanup_enabled` | `True` | bool | Enable background cleanup |
| `notification_webhook_replay_cleanup_interval_seconds` | `300.0` | 1–86400s | Cleanup interval (s) |
| `notification_webhook_replay_cleanup_batch_size` | `500` | 10–10000 | Max deleted per cycle |

---

## 18. Celery Background Workflow Execution Integration (Task 56)

### Overview
Task 56 introduces Celery as an optional distributed execution backend for long-running orchestration
workflows while strictly preserving existing synchronous API semantics, persistence guarantees,
admission limits, fencing tokens, cooperative cancellation, and human-in-the-loop (HITL) approval gates.

```
+-----------------------------------------------------------------------------------------+
|                                    API Gateway                                          |
|  POST /runs (execution_backend="celery" or background=True) -> HTTP 202 Accepted        |
|  - Idempotency-Key validated & recorded                                                 |
|  - Initial run record stored in SQLite (status="queued")                                |
|  - Task enqueued to Celery queue: "orchestrator_workflows"                              |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v
+-----------------------------------------------------------------------------------------+
|                               Celery Task Broker (Redis)                                |
|  Queue: orchestrator_workflows | acks_late=True | task_reject_on_worker_lost=True        |
+-----------------------------------------------------------------------------------------+
                                           |
                                           v
+-----------------------------------------------------------------------------------------+
|                               Distributed Celery Worker                                 |
|  `celery -A orchestrator.celery_app worker -l info -Q orchestrator_workflows`            |
|  - Task: orchestrator.tasks.execute_workflow_run                                        |
|  1. Verify terminal status / duplicate delivery check                                   |
|  2. Check cooperative cancellation token                                                |
|  3. Acquire cross-process admission lease (`acquire_workflow_admission`) & fence token  |
|  4. Execute graph via `RunManager.execute_dispatched_run`                               |
|  5. Checkpoints persisted at every step; outbox events emitted on approval pause        |
|  6. Release admission in `finally` block                                                |
+-----------------------------------------------------------------------------------------+
```

### Key Components

#### 1. Execution Backend Abstraction
The system supports three execution backends configured via `WORKFLOW_EXECUTION_BACKEND` or request payload:
- **`direct`**: In-process synchronous execution blocking until complete (returns HTTP 200/201).
- **`thread`**: In-process background `ThreadPoolExecutor` (returns HTTP 202 Accepted).
- **`celery`**: Distributed asynchronous Celery task queue (returns HTTP 202 Accepted).

#### 2. Worker Lifecycle & Commands
Start a dedicated orchestrator Celery worker:
```bash
celery -A orchestrator.celery_app worker -l info -Q orchestrator_workflows
```

#### 3. Delivery Semantics & Idempotency
- **At-Least-Once Delivery**: Workers use `task_acks_late=True` and `task_reject_on_worker_lost=True`.
- **Deduplication**: Redelivered tasks check the run's terminal status in SQLite. If already completed,
  cancelled, failed, or waiting approval, duplicate deliveries return existing summaries safely without
  re-executing.
- **Run Idempotency**: `Idempotency-Key` headers cache and replay HTTP 202 responses safely.

#### 4. HITL Approval & Safety Preservation
When a tool requires human review, the workflow pauses in `waiting_approval` status, records checkpoints,
and emits outbox notification events. Celery workers complete the task in `waiting_approval` status and
never bypass or auto-approve sensitive actions.

#### 5. Configuration Reference
| Field Name | Default | Bounds / Type | Description |
| :--- | :--- | :--- | :--- |
| `workflow_execution_backend` | `direct` | `direct`, `thread`, `celery` | Active execution engine |
| `celery_enabled` | `False` | bool | Enable Celery task queuing |
| `celery_broker_url` | `redis://localhost:6379/0` | URI | Celery broker URL |
| `celery_result_backend` | `None` | Optional URI | Celery result backend |
| `celery_task_default_queue` | `orchestrator_workflows` | str | Task queue name |
| `celery_worker_concurrency` | `4` | 1–128 | Worker concurrency |
| `celery_task_time_limit` | `1800.0` | > 0 seconds | Hard execution deadline (s) |
| `celery_task_soft_time_limit` | `1500.0` | > 0 seconds | Soft execution deadline (s) |
| `celery_always_eager` | `False` | bool | Local synchronous test mode |

---

## 19. Celery Worker Observability, Monitoring & Operational Recovery (Task 57)

### 1. Overview & Operational Architecture

Task 57 builds production-grade operational observability and recovery capabilities for the Celery
background workflow execution system introduced in Task 56. Distributed worker processes periodically
record heartbeats to a shared SQLite database (`celery_worker_heartbeats`), enabling API nodes and
operators to monitor fleet health, diagnose stuck or interrupted tasks, derive durable queue metrics,
and safely recover workflows across worker crashes or node failures.

```
+-----------------------------------------------------------------------------------------+
|                                    OPERATOR / API                                       |
|  GET /celery/workers  |  GET /celery/queue  |  GET /celery/stuck  |  POST /celery/recover|
+-----------------------------------------------------------------------------------------+
                                             |
                                             v
+-----------------------------------------------------------------------------------------+
|                                DURABLE PERSISTENCE LAYER                                |
|  - celery_worker_heartbeats (worker_id, status, last_heartbeat_at, active_tasks, queues)|
|  - runs (run_id, status, fence_token, latest_step, created_at, updated_at)              |
|  - workflow_admissions (run_id, owner_id, expires_at, heartbeat_at, fence_token)        |
|  - audit_events (event_id, event_type="workflow.recovered", actor, metadata_json)       |
+-----------------------------------------------------------------------------------------+
                                             ^
                                             | Periodic Heartbeats & Lifecycle Signals
+-----------------------------------------------------------------------------------------+
|                                DISTRIBUTED CELERY WORKERS                               |
|  - CeleryWorkerHeartbeatTracker daemon thread (interval: 15s)                           |
|  - Task lifecycle hooks (on_task_start, on_task_success, on_task_failure)               |
|  - Worker signals (@worker_ready: "active", @worker_shutdown: "offline")                |
+-----------------------------------------------------------------------------------------+
```

### 2. Worker Telemetry & Stale Detection

1. **Durable Worker Heartbeats**:
   Each Celery worker node periodically records telemetry via `record_worker_heartbeat()`.
   Records track worker identity (`worker_id`), registration timestamp (`registered_at`),
   last heartbeat timestamp (`last_heartbeat_at`), active task count (`active_tasks`), queues
   list (`queues`), most recent task completion (`last_completed_at`), and most recent sanitized
   failure (`last_failure_at`, `last_failure_reason`).

2. **Heartbeat Tracker Daemon**:
   Workers instantiate `CeleryWorkerHeartbeatTracker`, which starts a daemon thread pulsing
   heartbeats every `celery_worker_heartbeat_interval_seconds` (default: 15.0s).

3. **Stale Worker Identification**:
   A worker is automatically classified as `stale` if its status is `active` but its last recorded
   heartbeat is older than `celery_worker_stale_threshold_seconds` (default: 60.0s).

4. **Lifecycle Hooks**:
   - `on_task_start()`: Increments `active_tasks` and pulses heartbeat.
   - `on_task_success()`: Decrements `active_tasks`, updates `last_completed_at`, and pulses.
   - `on_task_failure()`: Decrements `active_tasks`, updates `last_failure_at` and sanitized error.
   - `@worker_shutdown`: Stops tracker thread and sets status to `offline`.

### 3. Queue & Task Observability

Operational metrics are derived directly from persisted SQLite database state (`runs`,
`workflow_admissions`, and `celery_worker_heartbeats`):
- `queued_count`: Count of workflows waiting in `queued` state.
- `running_count`: Count of workflows currently executing in `running` or `in_progress` status.
- `waiting_approval_count`: Count of workflows paused waiting for human-in-the-loop tool approval.
- `completed_count` & `failed_count`: Total historical completed and failed workflow counts.
- `retry_count`: Total number of recovered workflow runs.
- `oldest_queued_age_seconds`: Age in seconds of the oldest pending queued workflow.
- `active_worker_count`, `stale_worker_count`, `offline_worker_count`: Summary of worker fleet.
- `recent_failures`: Chronological list of recent failed runs with sanitized error descriptions.

All error messages, exception tracebacks, and broker URLs are strictly sanitized to prevent
secret or credential leakage (`_SENSITIVE_KEYS` redaction).

### 4. Stuck & Interrupted Workflow Diagnosis

The system automatically identifies workflows lacking forward progress using bounded criteria:
- **`stuck_queued`**: Workflow in `queued` status exceeding `celery_workflow_stuck_threshold_seconds`.
- **`orphaned_lease`**: Workflow in `running` status without an associated `workflow_admissions` lease.
- **`lease_expired`**: Workflow whose admission lease `expires_at` is in the past.
- **`expired_lease_dead_worker`**: Admission lease expired and owner worker has a stale or missing heartbeat.
- **`deadline_exceeded`**: Workflow running longer than deadline threshold.

**Durable Ownership Safety Invariant**:
A missing worker heartbeat never assumes a worker is dead without verifying the durable
`workflow_admissions` lease. If an active, unexpired lease is held (`expires_at >= now`), the workflow
is marked `recovery_eligible: False`. The system will NEVER overwrite valid active worker ownership.

### 5. Safe Operational Recovery Protocol

Operators can safely recover eligible workflows via `POST /celery/recover`:
1. **Operator Reason Required**: Requires a meaningful operator explanation ($\ge 5$ characters).
2. **Atomic Immediate Transaction**: Executed within SQLite `BEGIN IMMEDIATE` to prevent race conditions.
3. **Terminal Run Protection**: Workflows in `completed` or `cancelled` status cannot be recovered.
4. **HITL Approval Preservation**: Workflows in `waiting_approval` cannot be requeued without explicit
   human review decision. Recovery strictly preserves HITL safety boundaries.
5. **Active Lease Protection**: Raises `RecoveryError` if an active unexpired admission lease is held.
6. **Monotonic Fencing Increment**: Increments fencing token (`new_fence = max(run_fence, adm_fence) + 1`),
   fencing out any delayed execution from previous worker nodes.
7. **Action Application**:
   - `requeue`: Sets run status to `queued`, deletes stale admission, saves checkpoint with transition
     `recover`, and re-dispatches Celery task.
   - `fail`: Marks run status as `failed` with sanitized explanation.
   - `cancel`: Marks run status as `cancelled`.
8. **Audit Trail**: Records structured audit event `AuditEventType.WORKFLOW_RECOVERED` ("workflow.recovered")
   within the same database transaction.

### 6. Configuration Settings

| Setting | Default | Range | Description |
| :--- | :--- | :--- | :--- |
| `celery_worker_heartbeat_interval_seconds` | `15.0` | 1.0–300.0s | Worker heartbeat pulse interval |
| `celery_worker_stale_threshold_seconds` | `60.0` | 5.0–3600.0s | Stale worker threshold |
| `celery_queue_monitoring_interval_seconds` | `30.0` | 1.0–600.0s | Queue monitoring interval |
| `celery_workflow_stuck_threshold_seconds` | `300.0` | 10–86400s | Stuck workflow threshold |
| `celery_recovery_batch_size` | `50` | 1–500 | Max runs per recovery batch |


## 20. Celery Distributed Execution Failure-Injection & Reliability Testing (Task 58)

Task 58 provides integration-level reliability validation for the Celery distributed execution
engine under realistic network, host, process, and broker failure scenarios.

### 1. End-to-End Distributed Lifecycle Tests

Integration tests verify end-to-end correctness across decoupled API and worker process lifecycles:
- **API Submission to Terminal Completion**: Submitting a background Celery run (`POST /runs` with
  `execution_backend="celery"`) returns HTTP 202, enqueues the task, acquires worker admission,
  executes multi-agent steps, releases the lease upon completion, and persists answers.
- **Multiple Workers Competing Under Admission Limits**: When multiple distributed workers consume
  from the same Celery queue, global admission limits (`max_active_runs`) are strictly enforced via
  atomic SQLite admission leases. Excess capacity attempts are rejected with `CapacityExhaustedError`
  until active workers release their leases.
- **Cross-Process State Consistency**: API submission processes, detached Celery workers, and reader
  processes observe monotonically increasing checkpoint steps and fencing tokens across separate
  process lifecycles.
- **Idempotent Submission & Duplicate Task Delivery**: Submissions with `Idempotency-Key` return
  cached 202 responses with `Idempotent-Replayed: true`. Duplicate Celery deliveries of already
  completed or paused tasks are safely detected and skipped without re-running agent steps.
- **HITL Pause and Resumption**: Workflows encountering high-risk tools requiring human review
  cleanly pause in `waiting_approval` (or `pending`), release their admission leases, and resume
  safely via `resume_workflow_run` once an authorized reviewer approves the action.

### 2. Failure-Injection Scenarios & Invariants

- **Worker Crash / Abrupt Termination**:
  Worker terminated while holding lease -> Lease expires after TTL; `GET /celery/stuck` flags
  `expired_lease_dead_worker`.
- **Worker Restart & Task Recovery**:
  Host restart during execution -> Operator recovers via `POST /celery/recover`; task safely requeued.
- **Worker Network Partition**:
  Delayed worker attempts commit with old fence token -> Rejected with `AdmissionFencingError`.
- **Recovery Overwrite Attempt**:
  Operator calls recover while live worker holds lease -> Rejected with `RecoveryError`; active worker
  ownership strictly protected.
- **Broker Unavailability at Submission**:
  Celery broker offline / unreachable -> API returns HTTP 503 and rolls back SQLite queued state.
- **API Process Restart**:
  API process terminates while runs queued/running -> Replaced API server seamlessly resumes from SQLite.
- **HITL Review Bypass Attempt**:
  Operator attempts `requeue` action on unreviewed run -> Strictly rejected with `RecoveryError`.
- **Terminal Workflow Recovery Attempt**:
  Operator attempts recovery on completed or cancelled run -> Rejected with `RecoveryError`.
- **Pre-Dispatch Administrative Cancel**:
  Operator cancels run before worker picks up task -> Worker detects cancellation and aborts.
- **Uncertain External Tool Operation**:
  In-flight / uncertain tool operation exists -> Recovery requeue blocked until reconciled.

### 3. Fencing Tokens & Dual-Worker Protection

When an interrupted workflow is recovered by an operator, the recovery process allocates a new,
strictly monotonic fencing token:
$$\text{fence\_token}_{\text{new}} = \max(\text{run\_fence}, \text{adm\_fence}) + 1$$

Any delayed or partitioned worker that wakes up and attempts to commit checkpoints using an outdated
fencing token is immediately halted with `AdmissionFencingError`. Stale worker writes cannot overwrite
state committed by recovered workers or operator interventions.

### 4. Tool Execution Reconciliation

External side-effecting tool executions recorded in `in_flight` or `uncertain` status represent
indeterminate remote operations (e.g. payment gateway charges or remote deployments). To prevent
blind duplicate external calls:
1. `recover_stuck_workflow` checks active tool executions with status in
   `('running', 'in_flight', 'uncertain')`.
2. Recovery is rejected with `RecoveryError` until the operator inspects external systems and calls
   `store.reconcile_tool_execution(run_id, op_key, outcome="completed", ...)`.
3. Only after reconciliation can the remaining workflow safely be requeued or resumed.

### 5. Outbox & Replay Reliability Under Recovery

Recovery operations record structured audit events (`AuditEventType.WORKFLOW_RECOVERED`) in the same
database transaction as run status updates. Notification outbox dispatching and HMAC webhook signing
remain operational, and downstream webhook recipients verify durable replay protection to prevent
duplicate processing of recovery notices.

### 6. Environmental Limitations & Boundaries

- **SQLite Serialization**: SQLite WAL mode allows concurrent readers but serializes writers with a
  file lock. For high-volume multi-node worker clusters, database contention is bounded by short
  immediate transactions and bounded retry backoffs.
- **At-Least-Once Delivery**: Celery guarantees at-least-once task delivery. Exactly-once execution
  semantics are achieved through application-level idempotency keys and SQLite admission fencing.

---

## 21. Production Deployment Readiness & Containerization (Task 59)

Task 59 introduces production-ready containerization, multi-service separation, container health checks,
graceful shutdown handling, and operational deployment guides for the multi-agent orchestrator.

### 21.1 Architecture & Service Topology

The orchestrator is deployed as decoupled containers coordinating via a message broker and shared persistent
volume:

```
+-----------------------------------------------------------------------------------+
|                            DOCKER COMPOSE TOPOLOGY                                |
|                                                                                   |
|  +------------------------+      +------------------------+      +-------------+  |
|  |     FastAPI Service    |      |  Celery Worker Fleet   |      | Redis 7     |  |
|  |     (service: api)     |      |  (service: worker)     |      | (broker)    |  |
|  |     0.0.0.0:8000       |      |  Scale: 1..N instances |      | Port: 6379  |  |
|  +-----------+------------+      +-----------+------------+      +------+------+  |
|              |                               |                          |         |
|              +---------------+---------------+                          |         |
|                              |                                          |         |
|                              v                                          |         |
|             +----------------------------------+                        |         |
|             | Shared Persistent Storage Volume |                        |         |
|             | (orchestrator_data -> /app/data) |                        |         |
|             | checkpoints.db, WAL & SHM files  |                        |         |
|             +----------------------------------+                        |         |
|                              ^                                          |         |
|                              +------------------------------------------+         |
+-----------------------------------------------------------------------------------+
```

- **`api` Container**: Exposes port 8000 for REST interaction; runs `python -m orchestrator.api.server`.
- **`worker` Container**: Pulls background workflow runs from Celery queue `orchestrator_workflows`.
- **`redis` Container**: Ephemeral task broker using `redis:7-alpine` with append-only persistence.
- **`orchestrator_data` Volume**: Persistent SQLite database and Write-Ahead Log files shared between
  `api` and `worker` containers.

### 21.2 Security & Hardened Container Practices

1. **Unprivileged Non-Root User**: The container builds from `python:3.11-slim` and runs under non-root
   user `orchestrator` (UID 10001, GID 10001).
2. **Network Binding Enforcement**: Binding the API server to non-loopback interfaces (`0.0.0.0`) strictly
   requires configuring `ORCHESTRATOR_API_TOKEN`. Unauthenticated external exposure is rejected at startup.
3. **Build Layer Optimization**: Dependencies (`requirements.txt`, `pyproject.toml`) are cached in a separate
   layer before copying application code.
4. **Clean Build Context**: `.dockerignore` excludes version control, cache directories, and local secrets.

### 21.3 Health Probes & Lifecycle Management

- **API Readiness**: `GET /ready` returns HTTP 200 when ready; returns HTTP 503 during startup recovery or
  graceful shutdown draining.
- **API Liveness**: `GET /health` verifies process execution without touching disk or locks.
- **Worker Probe**: `python -m orchestrator.healthcheck --role worker` checks Celery inspect ping and falls
  back to durable SQLite heartbeat telemetry.
- **Graceful Termination**: Services handle `SIGTERM` with `stop_grace_period: 30s`, completing in-flight
  tasks before closing worker processes.

### 21.4 Quick Commands

```bash
# Build and run containers
docker compose up -d --build

# Scale Celery background workers
docker compose up --scale worker=3 -d

# Check service logs
docker compose logs -f api
docker compose logs -f worker

# Probe container health
docker compose exec api python -m orchestrator.healthcheck --role api --port 8000
docker compose exec worker python -m orchestrator.healthcheck --role worker
docker compose exec api python -m orchestrator.healthcheck --role redis
```

For the comprehensive deployment guide including reverse proxy configuration, SQLite WAL backup/restore,
and Kubernetes/ECS runbooks, see [Production Deployment Guide](DEPLOYMENT_GUIDE.md).











