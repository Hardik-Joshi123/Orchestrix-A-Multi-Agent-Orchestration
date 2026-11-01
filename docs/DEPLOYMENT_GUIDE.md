# Production Deployment & Containerization Guide

This guide details the architecture, configuration, containerization, and operations for deploying the
Multi-Agent Orchestrator in production environments using Docker, Docker Compose, or container
orchestrators such as Kubernetes or Amazon ECS.

---

## 1. System Architecture & Topology

The orchestrator is containerized as a decoupled, multi-tier distributed system composed of three primary
services and a shared persistent volume:

```
                      ┌──────────────────────────────────────┐
                      │  Reverse Proxy / Ingress (TLS Term)  │
                      └──────────────────┬───────────────────┘
                                         │ Bearer Token / HTTP
                                         ▼
                      ┌──────────────────────────────────────┐
                      │      API Server Container (api)      │
                      │       FastAPI (Port 8000, 0.0.0.0)   │
                      └──────┬────────────────────────┬──────┘
                             │                        │
         Enqueue / Inspect   │                        │ Read / Write Checkpoints
                             ▼                        ▼
    ┌──────────────────────────────┐    ┌───────────────────────────────────┐
    │  Message Broker (redis)      │    │ Shared Persistent Volume          │
    │  Redis 7 Alpine              │    │ Mount: /app/data                  │
    └──────────────┬───────────────┘    │ Files: checkpoints.db, WAL, SHM   │
                   │ Task Dispatch      └─────────────────┬─────────────────┘
                   ▼                                      │
    ┌──────────────────────────────┐                      │ Read / Write Checkpoints
    │ Celery Worker Pool (worker)  │                      │ & Worker Heartbeats
    │ Scale: 1..N Worker Replicas  │──────────────────────┘
    └──────────────────────────────┘
```

### Components
1. **API Service (`api`)**:
   - Exposes REST endpoints on port 8000 for workflow submission, status inspection, cancellation, HITL
     approval queue management, and health diagnostics.
   - Enforces Bearer token authentication whenever bound to non-loopback interfaces (`0.0.0.0`).
   - Dispatches background workflows to the Redis task queue or handles synchronous inline execution.
2. **Celery Worker Service (`worker`)**:
   - Consumes workflow execution tasks from the `orchestrator_workflows` queue.
   - Executes multi-agent supervisor/specialist loops, human-in-the-loop gates, and transactional outbox
     notifications.
   - Periodically publishes heartbeats to the shared SQLite database to enable fleet-wide observability.
3. **Message Broker (`redis`)**:
   - High-throughput Redis 7 instance serving as the Celery task broker and result backend.
   - Persists state with append-only file (`appendonly yes`) into the `redis_data` volume.
4. **Shared Persistent Storage (`orchestrator_data`)**:
   - Docker volume mounted at `/app/data` in both `api` and `worker` containers.
   - Stores the primary SQLite database (`orchestrator_checkpoints.db`) alongside active Write-Ahead Log
     (`-wal`) and Shared Memory (`-shm`) index files.

---

## 2. Container Security Model

The production container image adheres to security best practices:
- **Non-Root Execution**: Runs as unprivileged user `orchestrator` (UID 10001, GID 10001).
- **Minimal Base Image**: Built on `python:3.11-slim`, minimizing attack surface and vulnerabilities.
- **Layer Caching**: Dependency manifests (`requirements.txt`, `pyproject.toml`) are copied and installed
  before application code to leverage Docker build cache layers.
- **No Embedded Secrets**: Image contains zero API keys, passwords, or persistent state. All configuration
  is injected at container runtime via environment variables or secret vaults.
- **Context Hygiene**: `.dockerignore` excludes `.git`, `__pycache__`, local `.env` files, virtual
  environments, and temporary SQLite databases from image layers.

---

## 3. Prerequisites

- **Docker Engine**: Version 24.0.0 or later.
- **Docker Compose**: Version 2.20.0 or later (Docker Compose v2).
- **Host Storage**: Local SSD or network volume supporting POSIX file locking (`fcntl`/`flock`).
- **Hardware Sizing**:
  - Minimum: 2 CPU cores, 4 GB RAM (evaluation and staging).
  - Recommended: 4+ CPU cores, 8+ GB RAM (production fleet with multiple workers).

---

## 4. Quickstart Deployment

### Step 1: Clone and Configure Environment
Copy the production environment template and supply required secrets:

```bash
cp .env.docker.example .env
```

Edit `.env` to configure your API authentication token and optional LLM keys:
```env
ORCHESTRATOR_API_TOKEN=your-strong-production-token-32chars
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...
```

### Step 2: Build and Start Services
Start the full stack in detached mode using Docker Compose:

```bash
docker compose up -d --build
```
*(Alternatively, use `make docker-up`)*

### Step 3: Verify Deployment Health
Check container statuses and health probes:

```bash
docker compose ps
```

Inspect logs from all or specific services:
```bash
docker compose logs -f api
docker compose logs -f worker
```

Verify the API readiness probe from host:
```bash
curl -f http://localhost:8000/ready
# Returns: {"status": "ready", ...}
```

Verify authenticated API access:
```bash
curl -H "Authorization: Bearer your-strong-production-token-32chars" \
     http://localhost:8000/diagnostics
```

### Step 4: Teardown Services
To gracefully stop all services while preserving data volumes:

```bash
docker compose down
```
*(To remove persistent data volumes as well, add `-v`: `docker compose down -v`)*

---

## 5. Production Configuration Reference

All settings are configured via environment variables:

| Variable | Default in Compose | Description |
|:---|:---|:---|
| `API_HOST` | `0.0.0.0` | API bind address (`0.0.0.0` requires `ORCHESTRATOR_API_TOKEN`). |
| `API_PORT` | `8000` | Port on which the FastAPI application listens. |
| `ORCHESTRATOR_API_TOKEN` | Required for 0.0.0.0 | Secret Bearer token for authenticating API requests. |
| `CHECKPOINT_DB_PATH` | `/app/data/orchestrator_checkpoints.db` | SQLite DB in shared volume. |
| `CHECKPOINT_ENABLED` | `true` | Enables durable workflow checkpoints, outbox, and audit logs. |
| `ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND` | `celery` | Workflow execution engine (`celery` or `sync`). |
| `CELERY_ENABLED` | `true` | Activates Celery integration. |
| `CELERY_BROKER_URL` | `redis://redis:6379/0` | URL connecting to the Redis message broker. |
| `CELERY_RESULT_BACKEND` | `redis://redis:6379/0` | URL storing background task execution results. |
| `CELERY_TASK_DEFAULT_QUEUE` | `orchestrator_workflows` | Default Celery task queue name. |
| `CELERY_WORKER_CONCURRENCY` | `2` | Number of concurrent execution threads/processes per worker. |
| `DEFAULT_LLM_PROVIDER` | `mock` / `openai` / `anthropic` | Primary LLM inference provider. |
| `API_SHUTDOWN_GRACE_SECONDS` | `30.0` | Timeout for graceful API drain before termination. |

---

## 6. Persistent Storage & SQLite WAL Considerations

The orchestrator relies on SQLite in **Write-Ahead Logging (WAL)** mode for ACID compliance, concurrency,
and zero external database operational overhead.

### WAL Mechanics
When active, SQLite maintains three files in `/app/data/`:
1. `orchestrator_checkpoints.db`: Base database containing schema tables and indexed data.
2. `orchestrator_checkpoints.db-wal`: Write-Ahead Log containing uncommitted and recently committed pages.
3. `orchestrator_checkpoints.db-shm`: Shared-memory index coordinating readers and writers.

### Storage Rules
> [!IMPORTANT]
> - **Shared Volume Requirement**: Both `api` and `worker` containers must mount the **same** underlying
>   filesystem at `/app/data`.
> - **POSIX Lock Support**: The underlying host volume must support POSIX advisory locking (`fcntl`/`flock`).
>   Avoid network filesystems (e.g., standard SMB or NFSv3) that do not reliably support byte-range locks.
> - **Never copy only `.db`**: Backing up only the `.db` file while WAL mode is active will yield a corrupt
>   or outdated snapshot.

### Safe Backup & Restore Procedures
To take an online backup without stopping containers or interrupting in-flight runs, execute the native
backup CLI utility from within the running API container:

```bash
# Perform live, crash-consistent online backup with WAL checkpointing
docker compose exec api python -m orchestrator backup create --tag manual

# List available backups
docker compose exec api python -m orchestrator backup list

# Validate backup archive integrity
docker compose exec api python -m orchestrator backup validate /app/data/backups/backup_manual_*.db
```

For complete disaster recovery runbooks, pre-restore safety backups, and retention policies, see
[Section 13: Disaster Recovery Runbook](#13-database-backup-restore--disaster-recovery-dr-runbook).

### When to Migrate to PostgreSQL
SQLite WAL mode accommodates substantial throughput (hundreds of writes/sec, unbounded readers) when API
and worker instances reside on the same physical host or access a shared POSIX volume.
Consider migrating persistence to client-server RDBMS (e.g. PostgreSQL) when:
- Horizontally scaling API nodes across independent physical hosts or multi-region clusters.
- Operating requirements forbid shared storage volumes between compute instances.
- Enterprise governance mandates centralized database backups and replication topologies.

---

## 7. Celery Worker Fleet Scaling & Concurrency

### Scaling Worker Containers
Because Celery workers pull tasks asynchronously from the shared Redis broker and coordinate through the
persistent store, you can scale workers horizontally with a single command:

```bash
# Scale to 4 worker containers
docker compose up --scale worker=4 -d
```

Each worker automatically registers an identity, heartbeat, and active concurrency count.

### Worker Concurrency Tuning
The default compose configuration sets `--concurrency=2` per container. For CPU-bound or network-bound
specialist agents:
- Set concurrency equal to the allocated CPU cores: `--concurrency=${WORKER_CONCURRENCY:-4}`
- Configure container resource limits in `compose.yaml`:
  ```yaml
  worker:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2048M
  ```

---

## 8. Health Probes & Lifecycle Management

### Health Probes Architecture
The orchestrator distinguishes between **liveness** and **readiness** probes:
- **Readiness Probe (`GET /ready`)**:
  - Validates database connectivity, broker reachability, and server accepting new runs.
  - Returns `200 OK` when ready; returns `503 Service Unavailable` during startup recovery or graceful
    shutdown draining.
- **Liveness Probe (`GET /health`)**:
  - Returns `200 OK` as long as the process is alive. During shutdown, reports `{"status": "shutting_down"}`
    allowing orchestrators to route traffic away without prematurely killing the process.
- **Worker Health Probe (`python -m orchestrator.healthcheck --role worker`)**:
  - Executes Celery inspect ping to verify worker event loop.
  - Falls back to database heartbeat telemetry to confirm recent worker activity.

### Kubernetes Probe Mapping
When deploying on Kubernetes:
```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  initialDelaySeconds: 10
  periodSeconds: 15
readinessProbe:
  httpGet:
    path: /ready
    port: 8000
  initialDelaySeconds: 5
  periodSeconds: 10
```

### Graceful Shutdown Lifecycle
When Docker sends `SIGTERM`:
1. The API server enters draining mode: `/ready` immediately returns `503`, preventing ingress routers from
   sending new workflow submissions.
2. In-flight runs are granted up to `API_SHUTDOWN_GRACE_SECONDS` (30 seconds) to complete subtasks or reach a
   safe checkpoint.
3. The Celery worker receives `SIGTERM` and completes active tasks before closing worker connections.
4. If the grace period expires, remaining processes exit cleanly, and any interrupted runs are automatically
   recovered by the startup recovery manager on subsequent launch.

---

## 9. Reverse Proxy & TLS Termination

Do not expose the API container directly to the public Internet without TLS. Deploy behind a reverse proxy
(Nginx, Caddy, Traefik, or Cloud ALB).

### Example Nginx Configuration
```nginx
server {
    listen 443 ssl http2;
    server_name orchestrator.yourdomain.com;

    ssl_certificate     /etc/ssl/certs/orchestrator.crt;
    ssl_certificate_key /etc/ssl/private/orchestrator.key;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts for long-running workflows
        proxy_read_timeout 120s;
        proxy_connect_timeout 10s;
    }
}
```

---

## 10. Operational Runbooks & Troubleshooting

### Diagnostic Endpoints
- Fleet & Outbox Telemetry: `GET /diagnostics`
- Worker Fleet & Celery Backlog: `GET /celery/workers` and `GET /celery/queues`
- Replay Store Maintenance: `GET /notifications/replay/diagnostics`

### Container CLI Probe Utility
Run one-off diagnostic probes directly inside containers:
```bash
# Probe API server readiness
docker compose exec api python -m orchestrator.healthcheck --role api --port 8000

# Probe Celery worker liveness
docker compose exec worker python -m orchestrator.healthcheck --role worker

# Probe Redis connectivity
docker compose exec api python -m orchestrator.healthcheck --role redis
```

### Environmental Prerequisites & Honesty
When validating deployments in constrained environments where Docker daemon or CLI is absent (e.g. CI runner
without Docker-in-Docker or bare Windows host without Docker Desktop):
- Static specifications (`Dockerfile`, `compose.yaml`, `.dockerignore`, `.env.docker.example`),
  security checks, and unit tests execute deterministically with full test coverage.
- Daemon-dependent integration commands are gracefully skipped via pytest skip marks and reported honestly
  without substituting mocked tests.
- When Docker Engine is provisioned, the test suite automatically executes real image builds, container
  spinups, and live HTTP API verification.

---

## 11. Runtime Deployment Validation & Verification Procedures

This section details the end-to-end verification procedures executed during runtime validation (Task 60).

### 11.1 Container Image Build & Layer Caching Validation
Build the orchestrator image locally and verify the unprivileged non-root execution context:

```bash
# Build production image
docker compose build

# Verify container runs strictly as non-root user (UID 10001)
docker run --rm multi-agent-orchestrator:latest id
# Expected output: uid=10001(orchestrator) gid=10001(orchestrator) groups=10001(orchestrator)

# Verify CLI entrypoint is executable
docker run --rm multi-agent-orchestrator:latest python -m orchestrator --help
```

### 11.2 Discovered Build Caveats & Remedies
During Task 60 pre-deployment inspection and container build simulation, an installation ordering caveat
was identified and fixed:
- **Caveat**: `pyproject.toml` declares dynamic package versioning via setuptools
  (`version = { attr = "orchestrator.__version__" }`) and requires `README.md`.
  If `pip install -e .` is executed before application source code is copied, setuptools
  fails with a `FileNotFoundError` or dynamic metadata resolution error.
- **Remedy**: Pinned dependencies from `requirements.txt` are installed in the initial layer to maximize build
  caching (`RUN pip install -r requirements.txt`). Source code and `README.md` are then copied with `--chown`,
  followed by editable package registration without re-installing dependencies
  (`RUN pip install --no-cache-dir --no-deps -e .`).

### 11.3 Automated Runtime Validation Test Suite
To execute the automated real runtime test suite:

```bash
# Run runtime validation suite
pytest tests/test_docker_runtime_validation.py -v
```
When Docker Engine is active:
1. `test_real_docker_image_build`: Verifies image compilation and non-root execution.
2. `test_real_docker_compose_stack_lifecycle`: Launches Redis, API, and Worker containers,
   verifies healthcheck convergence, submits an asynchronous workflow via HTTP, and asserts
   persisted completion in SQLite.

---

## 12. Production Configuration Validation & Secrets Management

Task 61 introduced strict production configuration validation, multi-layer secret redaction, and
zero-downtime secret rotation capabilities across the orchestrator services.

### 12.1 Startup Configuration Validation

Before services open listening sockets or accept workflow submissions, `validate_configuration()` and
`validate_production_configuration()` audit settings:

1. **Non-Loopback Network Binding**:
   - Binding the API to `0.0.0.0` or any non-loopback interface strictly requires `ORCHESTRATOR_API_TOKEN`.
   - The token must be at least 16 characters long.
   - Values starting with `replace_with_` (from template files) are detected and rejected.
2. **Incompatible Storage and Backend Detection**:
   - In distributed Celery execution mode (`ORCHESTRATOR_WORKFLOW_EXECUTION_BACKEND=celery`), in-memory SQLite
     (`:memory:`) is strictly forbidden because worker processes would operate on disjoint database states.
   - Distributed production requires persistent shared storage (`CHECKPOINT_DB_PATH=/app/data/...`).
3. **Broker and Backend URL Scheme Validation**:
   - `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` must declare recognized URL schemes
     (`redis://`, `rediss://`, `amqp://`, `rpc://`, `sqs://`).
4. **Execution Mode Integrity**:
   - In production environments, `CELERY_ALWAYS_EAGER=true` is rejected to prevent silent fallback to
     synchronous execution in distributed deployments.
5. **Production Webhook Security**:
   - When `NOTIFICATION_PROVIDER=webhook` is enabled in production, `NOTIFICATION_WEBHOOK_SIGNING_SECRET`
     is mandatory for HMAC-SHA256 signature generation.
   - `NOTIFICATION_WEBHOOK_TLS_VERIFY` must remain `true`.
   - `NOTIFICATION_WEBHOOK_ALLOW_INSECURE_HTTP` must remain `false`.

### 12.2 Secrets Management & Redaction

The orchestrator enforces multi-tier redaction across logging, errors, and diagnostics:

- **Diagnostics Endpoints (`GET /diagnostics`)**: Emits operational health, active limits, and metadata
  without returning raw credentials, Bearer tokens, or signing keys.
- **CLI Diagnostics (`python -m orchestrator --diagnostics`)**: Prints non-sensitive configuration states
  with masked secrets (`mask_secret()`).
- **Connection String Masking**: `sanitize_broker_url()` strips both password authority components
  (`redis://:***@host:port`) and query parameter credentials (`?auth=***`, `?token=***`, `?secret=***`).
- **Exception Masking**: `format_config_error()` filters known secret patterns (such as `sk-...` and
  `Bearer ...`) before error messages are emitted to stdout or stderr.

### 12.3 Zero-Downtime Secret Rotation Runbook

The orchestrator supports zero-downtime secret rotation for both API client authentication and outbound
webhook signing.

#### A. API Bearer Token Rotation

The orchestrator accepts both primary and previous tokens simultaneously during rotation windows:
- Primary: `ORCHESTRATOR_API_TOKEN`
- Previous: `ORCHESTRATOR_API_TOKEN_PREVIOUS` (or comma-separated tokens in `ORCHESTRATOR_API_TOKEN`)

**Rotation Procedure**:
1. **Generate New Token**:
   ```bash
   NEW_TOKEN=$(openssl rand -hex 32)
   ```
2. **Configure Dual Acceptance**:
   Update `.env` on API servers:
   ```env
   ORCHESTRATOR_API_TOKEN=<NEW_TOKEN>
   ORCHESTRATOR_API_TOKEN_PREVIOUS=<OLD_TOKEN>
   ```
3. **Deploy / Reload API**:
   Apply rolling reload: `docker compose up -d api`
   Both `<NEW_TOKEN>` and `<OLD_TOKEN>` are now accepted with constant-time verification.
4. **Update API Clients**:
   Configure external clients and upstream services with `<NEW_TOKEN>`.
5. **Retire Old Token**:
   Remove `ORCHESTRATOR_API_TOKEN_PREVIOUS` from `.env` and reload the API container:
   ```env
   ORCHESTRATOR_API_TOKEN=<NEW_TOKEN>
   ORCHESTRATOR_API_TOKEN_PREVIOUS=
   ```

#### B. Webhook Signing Secret Rotation

Outbound webhook payloads are signed using HMAC-SHA256. For safe key rotation without packet drops:
- Current Signing Secret: `NOTIFICATION_WEBHOOK_SIGNING_SECRET`
- Previous Signing Secret: `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS`

**Rotation Procedure**:
1. Update downstream webhook consumers to verify signatures against both new and old signing secrets.
2. Update the orchestrator environment with `NOTIFICATION_WEBHOOK_SIGNING_SECRET=<NEW_SECRET>` and
   `NOTIFICATION_WEBHOOK_SIGNING_SECRET_PREVIOUS=<OLD_SECRET>`.
3. Reload orchestrator services (`docker compose up -d`).
   Outbound webhooks will be signed with the new secret.
4. Once downstream verification is verified, decommission the old secret on receiving endpoints.

---

## 13. Database Backup, Restore & Disaster Recovery (DR) Runbook

Task 62 introduced native online backup, verification, atomic restoration, and retention lifecycle
management for the orchestrator's persistent SQLite database.

### 13.1 Database Inventory & Consistency Scope

The persistent SQLite store (`orchestrator_checkpoints.db`) consolidates 13 core tables:
- **Workflows & Checkpoints**: `runs`, `checkpoints`
- **Security & Idempotency**: `audit_events`, `idempotency_keys`
- **Leases & Fencing**: `workflow_admissions`, `scheduler_leases`
- **HITL Approvals & Tools**: `tool_approvals`, `tool_executions`
- **Outbox & Notifications**: `outbox_events`, `outbox_delivery_history`
- **Replay Protection**: `webhook_replay_records`, `webhook_replay_cleanup_log`
- **Worker Telemetry**: `celery_worker_heartbeats`

### 13.2 Online Backup Procedures

Backups use SQLite's official online backup API, which safely copies committed pages from both the main
database file and active WAL frames without taking read locks or halting server processing.

#### Creating Backups via CLI
```bash
# Create an automated timestamped backup in BACKUP_DIR
docker compose exec api python -m orchestrator backup create

# Create a tagged backup prior to deployment
docker compose exec api python -m orchestrator backup create --tag pre_deploy_v2

# Emit machine-readable JSON metadata
docker compose exec api python -m orchestrator backup create --json
```

#### Listing & Inspecting Available Backups
```bash
docker compose exec api python -m orchestrator backup list
```

#### Validating Backup Integrity
Before using any backup for restoration, validate its structural integrity and table inventory:
```bash
docker compose exec api python -m orchestrator backup validate /app/data/backups/backup_20261101_120000.db
```

### 13.3 Safe Restore & Disaster Recovery Runbook

The restore utility enforces strict safety invariants:
1. Validates backup file headers and executes `PRAGMA integrity_check` before modifying target files.
2. Checks for active running workflows or unexpired admission leases to prevent clobbering live runs.
3. Automatically creates a pre-restore safety snapshot of the active target database.
4. Atomically replaces target database pages and resets WAL journaling.

#### Restore Procedure
```bash
# 1. Stop background workers to pause task processing
docker compose stop worker

# 2. Execute restore command (creates safety backup automatically)
docker compose exec api python -m orchestrator backup restore \
  /app/data/backups/backup_20261101_120000.db

# 3. If target was actively running, pass --force to override
docker compose exec api python -m orchestrator backup restore \
  /app/data/backups/backup_20261101_120000.db --force

# 4. Restart Celery worker fleet
docker compose start worker
```

### 13.4 Retention Management & Lifecycle Policies

Manage storage consumption through automated backup retention:
- `BACKUP_RETENTION_COUNT`: Maximum number of backups to keep (default: `10`).
- `BACKUP_RETENTION_DAYS`: Maximum age in days for retained archives (default: `30`).
- **Protection of Latest Backup**: The single latest valid backup is never deleted, even if it exceeds
  the age threshold.

```bash
# Preview expired backups without deleting
docker compose exec api python -m orchestrator backup clean --dry-run

# Execute active retention cleanup
docker compose exec api python -m orchestrator backup clean
```

### 13.5 Docker Persistent Volume & SQLite Operational Limitations

- **Volume Mounts**: Ensure `/app/data` is mounted to a persistent Docker named volume or host volume
  supporting POSIX advisory file locking (`fcntl`/`flock`).
- **Backup Storage**: Store backups in `/app/data/backups` or an independent volume mount to prevent
  accidental data loss during container recreation.
- **Offsite Archival**: Regularly sync `/app/data/backups` to secure object storage (e.g. AWS S3, GCS)
  using standard cloud CLI tools.
- **SQLite Concurrency Invariants**: SQLite supports unlimited concurrent readers but a single writer.
  The online backup API coordinates via non-blocking page-by-page replication.

### 13.6 Automated Periodic Backup Scheduling

Task 63 introduces automated background backup scheduling with cross-process fencing:

- **Scheduling Coordination**: Leverages the `scheduler_leases` table with monotonic fencing tokens.
  Only the active leader node executes scheduled backups; standby nodes monitor health and take over if
  the leader fails or lease expires.
- **End-to-End Execution**: Each cycle creates an online snapshot, executes deep `PRAGMA integrity_check`
  and schema validation, prunes expired archives per retention policies, and updates audit records.
- **Protection of Latest Backup**: Retention enforcement guarantees the single newest valid backup is
  never deleted under any policy or retention threshold.
- **Resilient Retry Policy**: Transient backup failures trigger bounded exponential backoff
  (`BACKUP_RETRY_BACKOFF_SECONDS` up to `BACKUP_MAX_BACKOFF_SECONDS`) for up to `BACKUP_MAX_RETRIES`.

#### Configuration Parameters

| Environment Variable | Default | Description |
|---|---|---|
| `BACKUP_SCHEDULER_ENABLED` | `false` | Enable automated periodic backup scheduler loop |
| `BACKUP_INTERVAL_SECONDS` | `86400` | Interval between scheduled backup cycles (seconds) |
| `BACKUP_MAX_DURATION_SECONDS` | `300` | Maximum execution duration before timing out |
| `BACKUP_MAX_RETRIES` | `3` | Maximum retry attempts upon transient backup failure |
| `BACKUP_RETRY_BACKOFF_SECONDS` | `5.0` | Initial exponential backoff delay between retries |
| `BACKUP_MAX_BACKOFF_SECONDS` | `60.0` | Maximum exponential backoff cap between retries |
| `BACKUP_SCHEDULER_LEASE_TTL_SECONDS` | `60.0` | Cross-process leadership lease TTL (seconds) |
| `BACKUP_SCHEDULER_HEARTBEAT_INTERVAL_SECONDS` | `10.0` | Lease renewal heartbeat interval (seconds) |

### 13.7 Operational Observability & Control Interfaces

Operational status and diagnostics can be queried via CLI or authenticated REST endpoints:

#### CLI Management Commands

```bash
# Inspect backup scheduler state, leader status, metrics, and storage summary
docker compose exec api python -m orchestrator backup status

# Emit status as JSON for monitoring integrations
docker compose exec api python -m orchestrator backup status --json

# Trigger an immediate on-demand backup execution cycle
docker compose exec api python -m orchestrator backup trigger --reason "Pre-maintenance"
```

#### REST API Endpoints

- `GET /backups/status`: Inspect scheduler leadership, metrics, and latest archive metadata.
- `POST /backups/trigger`: Trigger an authorized immediate online database backup execution cycle.
- `GET /backups/history`: List all existing online database backup archives with metadata.
- `GET /diagnostics`: Includes `backup_scheduler` operational state alongside core subsystem metrics.
### 13.8 Automated Backup Restore Drills & Recovery Verification

To guarantee that generated backups are not merely structurally sound but functionally recoverable,
the orchestrator provides automated and on-demand non-destructive restore drills:

- **Isolated Execution**: Restores a candidate backup archive into a dedicated temporary SQLite database.
  The live production database is strictly protected from modification.
- **SQLite Integrity Verification**: Executes `PRAGMA integrity_check`, `PRAGMA quick_check`, and
  `PRAGMA foreign_key_check` on the isolated database.
- **Recovery Verification**: Samples persisted records across `runs`, `checkpoints`, `audit_events`,
  `outbox_events`, `scheduler_leases`, and `workflow_admissions`, verifying JSON deserialization.
- **Deterministic Cleanup**: All temporary databases, WAL journals, and shared-memory files are safely
  removed even if the verification fails. The tested backup archive is never deleted or altered.
- **Cross-Process Coordination**: Automated periodic drills run on the leader process, with non-overlapping
  execution locks preventing concurrent drills.
- **Persistent History**: Results are stored in the `database_restore_drills` SQLite table with automatic
  retention pruning.

#### Restore Drill CLI Commands

```bash
# Execute an isolated restore drill against the latest backup archive
docker compose exec api python -m orchestrator backup drill run

# Run a restore drill on a specific backup archive with JSON output
docker compose exec api python -m orchestrator backup drill run --backup-path backups/backup.db --json

# Inspect the most recent restore drill status and outcome
docker compose exec api python -m orchestrator backup drill status

# View chronological history of restore drill executions
docker compose exec api python -m orchestrator backup drill history --limit 20
```

#### Restore Drill REST API Endpoints

- `GET /backups/drill/status`: View scheduler drill configuration, latest drill timestamp, and outcome.
- `POST /backups/drill/trigger`: Trigger an authorized immediate restore drill against a backup.
- `GET /backups/drill/history`: Query paginated historical restore drill execution records.

---

## 13.9 Backup Integrity Manifest & Tamper Detection

To ensure backup archives have not suffered bit rot, filesystem corruption, network truncation, or
unauthorized tampering, every database backup generated by the orchestrator is accompanied by an
atomic cryptographic integrity manifest (`<backup_filename>.manifest.json`).

### 13.9.1 Manifest Schema & Deterministic Serialization

The integrity manifest is written using deterministic JSON serialization (UTF-8, 2-space indentation,
sorted keys) via atomic temporary file renaming (`write_atomic_json`), preventing partial writes:

```json
{
  "backup_id": "backup_20261101_120000_a1b2c3d4",
  "created_at": "2026-11-01T12:00:00.123456+00:00",
  "filename": "backup_scheduled_20261101_120000.db",
  "manifest_version": 1,
  "schema_version": 2,
  "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "size_bytes": 204800,
  "tag": "scheduled"
}
```

- **Privacy & Redaction**: The manifest only contains file metadata and checksums. No row contents,
  table data, or secrets are ever included.

### 13.9.2 Verification Status & Tamper Detection

The verification subsystem inspects archive integrity across multiple dimensions:

1. **Path Traversal Protection**: Manifest filenames containing `..`, `/`, or `\` are rejected.
2. **Size Validation**: The actual archive size on disk is checked against `size_bytes`. Truncation or
   payload appending is detected immediately.
3. **Cryptographic Checksum**: The archive is streamed in 64 KiB chunks to compute the SHA-256 digest,
   which is compared in constant time against the manifest's declared checksum.
4. **Three-Valued Outcome**:
   - `verified`: Manifest exists and checksum/size match perfectly (`is_valid = True`).
   - `unverified`: Legacy backup created prior to Task 65 without a companion manifest. Allowed for
     backward compatibility if SQLite checks pass (`is_valid = False`, `manifest_status = "unverified"`).
   - `failed`: Tampering, single-byte corruption, truncation, or malformed
     manifest detected (`is_valid = False`).

### 13.9.3 Operational Interfaces

#### CLI Verification Commands

```bash
# Verify integrity of a backup archive with formatted text summary
python -m orchestrator backup verify backups/backup_scheduled_20261101_120000.db

# Verify integrity and emit machine-readable JSON (exit code 0 on success, 1 on failure)
python -m orchestrator backup verify backups/backup_scheduled_20261101_120000.db --json
```

#### REST API Endpoints

Both endpoints require operator Bearer token authentication (`ORCHESTRATOR_API_TOKEN`):

- `GET /backups/verify?backup_path=backups/backup.db`: Inspects specified archive
  (or latest backup if omitted).
- `POST /backups/verify`: Accepts JSON payload `{"backup_path": "...", "manifest_path": "..."}`.

Example Response:
```json
{
  "status": "verified",
  "is_valid": true,
  "backup_path": "/var/lib/orchestrator/backups/backup_20261101.db",
  "manifest_path": "/var/lib/orchestrator/backups/backup_20261101.db.manifest.json",
  "expected_sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "computed_sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "expected_size_bytes": 204800,
  "actual_size_bytes": 204800,
  "manifest": { ... },
  "error": null,
  "duration_ms": 14.25
}
```

### 13.9.4 Lifecycle Integration & Retention Cleanup

- **Restore Drill Protection**: Before executing an isolated restore drill, `run_restore_drill` validates
  the candidate's manifest. If verification status is `failed`, the drill immediately aborts.
- **Retention Pruning**: When `clean_backups` prunes expired backups, companion `.manifest.json` files
  are deleted alongside their archives.
- **Orphaned Manifest Cleanup**: Any standalone `.manifest.json` whose archive was deleted is cleaned up.
- **Latest Backup Invariant**: The newest backup is unconditionally preserved regardless of manifest status.

### 13.9.5 Security Guarantees & Limitations

> [!IMPORTANT]
> The SHA-256 integrity manifest provides detection of storage bit rot, transmission truncation, and
> untrusted file modifications. However, because manifests are currently unsigned cryptographic checksums,
> an attacker with write access to both the backup directory and manifest files can modify the archive and
> recalculate a matching manifest. For defense-in-depth in untrusted environments, store backups and manifests
> on write-once-read-many (WORM) storage or append-only remote object storage with object locking.

## 13.10 Signed Backup Manifests & Cryptographic Key Rotation

Task 66 elevates database backup integrity to cryptographic non-repudiation by introducing HMAC-SHA256
digital signatures, structured manifest envelopes, and seamless multi-key rotation keyring management.

### 13.10.1 Cryptographic Signature Architecture

Backup integrity manifests use a secure cryptographic envelope format:

```json
{
  "payload": {
    "backup_id": "backup_20261101_120000",
    "created_at": "2026-11-01T12:00:00+00:00",
    "filename": "backup_scheduled_20261101_120000.db",
    "manifest_version": 1,
    "schema_version": 1,
    "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "size_bytes": 204800,
    "tag": "scheduled"
  },
  "signature": {
    "algorithm": "HMAC-SHA256",
    "key_id": "prod-2026-v1",
    "signature_version": 1,
    "value": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
  }
}
```

#### Core Cryptographic Guarantees

1. **Deterministic Canonical Serialization**: The manifest payload is serialized deterministically via
   `canonical_manifest_bytes()` (UTF-8, sorted keys, strict formatting, signature field excluded) prior to
   signing or verification.
2. **Constant-Time Verification**: Verification utilizes `hmac.compare_digest` to prevent timing attacks.
3. **Payload Binding**: The cryptographic signature is irrevocably bound to `backup_id`, `filename`, archive
   `size_bytes`, SQLite `schema_version`, and the archive's SHA-256 checksum. Any modification to payload
   metadata invalidates the signature.
4. **Independent Checksum & Signature Validation**: Verification computes archive SHA-256 and validates the
   cryptographic signature as separate concerns, reporting fine-grained status.

### 13.10.2 Configuration & Environment Variables

- `ORCHESTRATOR_BACKUP_SIGNING_KEY` (Secret, default: `None`): Active symmetric secret for signing new
  manifests.
- `ORCHESTRATOR_BACKUP_SIGNING_KEY_ID` (String, default: `default`): Identifier of active signing key.
- `ORCHESTRATOR_BACKUP_VERIFICATION_KEYS` (JSON / Dict, default: `None`): Keyring mapping `{"id": "secret"}`
  for verifying historical keys.
- `ORCHESTRATOR_BACKUP_REQUIRE_SIGNATURE` (Boolean, default: `false`): Strict mode rejecting unsigned
  manifests or missing verification keys.

### 13.10.3 Key Lifecycle & Safe Zero-Downtime Rotation

Key rotation requires zero downtime and allows verifying archives signed with older keys:

```mermaid
flowchart LR
    A["Step 1: Deploy New Key in Keyring"] --> B["Step 2: Switch Active Signing Key"]
    B --> C["Step 3: Verify New Backups"]
    C --> D["Step 4: Retire Historical Keys"]
```

#### Step-by-Step Rotation Runbook

1. **Provision New Secret**: Generate a cryptographically secure key (minimum 32 bytes / 256 bits):
   ```bash
   python -c "import secrets; print(secrets.token_hex(32))"
   ```
2. **Update Verification Keyring**: Add the new key to `ORCHESTRATOR_BACKUP_VERIFICATION_KEYS` alongside
   existing active and historical keys:
   ```bash
   export ORCHESTRATOR_BACKUP_VERIFICATION_KEYS='{"prod-2025-v1":"old...","prod-2026-v2":"new..."}'
   ```
3. **Switch Active Signing Key**: Update `ORCHESTRATOR_BACKUP_SIGNING_KEY` and
   `ORCHESTRATOR_BACKUP_SIGNING_KEY_ID`:
   ```bash
   export ORCHESTRATOR_BACKUP_SIGNING_KEY="new_secret_bytes_here"
   export ORCHESTRATOR_BACKUP_SIGNING_KEY_ID="prod-2026-v2"
   ```
4. **Verify Active Keyring**:
   ```bash
   python -m orchestrator backup keys
   ```
5. **Retire Decommissioned Keys**: When all backups signed with `prod-2025-v1` pass their retention window,
   remove the key from `ORCHESTRATOR_BACKUP_VERIFICATION_KEYS`. Backups signed with retired keys will report
   `signature_status="missing_key"`.

### 13.10.4 Backward Compatibility & Strict Production Policy

- **Permissive Mode** (`ORCHESTRATOR_BACKUP_REQUIRE_SIGNATURE=false`):
  Unsigned legacy manifests (Task 65) report `signature_status="unsigned"` and are considered valid if
  SHA-256 and archive contents verify.
- **Strict Mode** (`ORCHESTRATOR_BACKUP_REQUIRE_SIGNATURE=true`):
  Enforces zero-trust production posture. Unsigned manifests or manifests signed with unknown/retired keys
  immediately fail verification (`is_valid=false`, `status="failed"`).
- **Emergency Recovery**: If keys are temporarily unavailable, operators can run verification or restore
  drills in non-strict mode (`--strict=false`) while retaining SHA-256 archive validation.

### 13.10.5 Secret Redaction & Diagnostic Safety

To protect key material:
- Key secrets are never logged, formatted into error messages, returned in API payloads, or exposed in
  startup diagnostics.
- The `GET /backups/keys` endpoint and `python -m orchestrator backup keys` command return only sanitized
  metadata: key identifiers, secret lengths, presence of active key, and configured key count.
- `Settings.get_startup_diagnostics()` safely includes keyring counts without revealing key contents.

### 13.10.6 Operational Commands & REST API

#### CLI Commands

```bash
# Verify backup with signature verification (permissive)
python -m orchestrator backup verify backups/backup_20261101.db

# Strictly enforce valid cryptographic signature
python -m orchestrator backup verify backups/backup_20261101.db --strict

# Output machine-readable JSON verification result
python -m orchestrator backup verify backups/backup_20261101.db --strict --json

# Inspect active signing key and verification keyring metadata
python -m orchestrator backup keys --json
```

#### REST API Endpoints

- `GET /backups/verify?backup_path=...&strict=true`: Verifies archive with optional strict signature check.
- `POST /backups/verify`: Body `{"backup_path": "...", "strict": true}`.
- `GET /backups/keys`: Returns safe keyring metadata (active key ID, total keys, key IDs list).

#### Structured Audit Events

- `backup.manifest_signed`: Emitted when a manifest is successfully signed.
- `backup.signature_verified`: Emitted upon successful HMAC signature verification.
- `backup.signature_failed`: Emitted on signature mismatch, unsupported algorithm, or missing key.
