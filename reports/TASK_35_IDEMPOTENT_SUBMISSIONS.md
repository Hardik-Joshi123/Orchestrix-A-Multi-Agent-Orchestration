# Task 35: Idempotent Workflow Submission & Safe API Retries Report

**Repository:** `multi-agent-orchestrator`  
**Date:** 2026-10-27  
**Author:** Antigravity  
**Status:** Completed & Independently Verified  

---

## 1. Executive Summary

Task 35 introduces first-class, durable, concurrent-safe `Idempotency-Key` support to `POST /runs` in the Multi-Agent Orchestration System. In distributed systems, network timeouts, dropped connections, and ambiguous responses often force clients to retry workflow submissions. Without idempotency controls, retries risk launching redundant, long-running agent workflows that waste compute and duplicate execution side-effects.

Task 35 addresses this by providing:
- **`Idempotency-Key` HTTP Header Support**: Optional header on `POST /runs`, adhering to standard IETF idempotency patterns.
- **Strict Format & Character Validation**: Enforces keys between 1 and 128 printable ASCII characters (`0x21` to `0x7E`). Malformed keys are rejected with `HTTP 400 Bad Request`.
- **Client Identity Scoping**: Keys are scoped to the authenticated caller identity (`token:<sha256(token)[:16]>` when Bearer authentication is active; `ip:<client_ip>` in loopback dev mode), eliminating cross-client collisions.
- **Canonical Request Fingerprinting & Conflict Detection**: A deterministic SHA-256 fingerprint is computed from canonical request parameters. Replays with identical parameters replay the original workflow run with `Idempotent-Replayed: true`. Inconsistent parameter reuses are rejected with `HTTP 409 Conflict`.
- **Durable SQLite Persistence**: Table `idempotency_keys` with `PRIMARY KEY (client_scope, key)` guarantees database-level atomicity across processes, restarts, and concurrent workers.
- **In-Flight Concurrency Protection**: Simultaneous requests using the same key synchronize via `threading.Condition` and SQLite status tracking (`in_progress` -> `completed`). Secondary requests wait for the primary thread to complete and replay the response without duplicate execution.
- **Safe Failure & Retry Semantics**: If workflow submission fails before completion, the record transitions to `failed`, freeing clients to retry with the same key.
- **Process Restart Recovery**: Service lifespan startup reconciles lingering `in_progress` records against persisted workflow runs, transitioning valid runs to `completed` and abandoned runs to `failed`.
- **Configurable Retention & Purging**: `ORCHESTRATOR_IDEMPOTENCY_RETENTION_HOURS` (default: 24h, range: 1..87600h) with explicit store purging API (`store.purge_idempotency_keys`).
- **Python Client SDK Support**: `OrchestratorApiClient.create_run(..., idempotency_key=...)` with client-side validation and automatic header propagation.
- **Exhaustive Automated Verification**: 14 dedicated tests in `tests/test_idempotent_submissions.py` covering format validation, exact replaying, 409 conflict detection, concurrency synchronization, restart recovery, auth scoping, retry on failure, and SDK usage. All 14 tests pass, and all 57 regression tests pass with zero errors.

---

## 2. Architecture & Request Lifecycle

### 2.1 Concurrency & State Machine

```
Client Request
      │
      ▼
Has Idempotency-Key? ──No──► Standard Creation (Fast Path) ──► 201 Created (Distinct Run)
      │
     Yes
      ▼
Validate Format (1..128 printable ASCII) ──Invalid──► 400 Bad Request
      │
    Valid
      ▼
Compute Fingerprint (SHA-256 of canonical params) & Scope (token / IP)
      │
      ▼
Acquire Key in SQLite (`PRIMARY KEY (client_scope, key)`)
      ├── Acquired (`in_progress`) ──► Create Workflow Run in RunManager
      │                                       │
      │                                       ├── Success ──► Finalize DB (`completed`, run_id, json)
      │                                       │               Notify waiting threads
      │                                       │               Return 201 Created
      │                                       │
      │                                       └── Failure ──► Mark DB (`failed`)
      │                                                       Notify waiting threads
      │                                                       Return Error (Client may retry)
      │
      ├── Existing (`completed`) ──► Check Fingerprint Match?
      │                                       │
      │                                       ├── Match ──► Replay 201 Created + Header `Idempotent-Replayed: true`
      │                                       └── Mismatch ──► 409 Conflict
      │
      ├── Existing (`in_progress`) ──► Wait via `threading.Condition` / DB polling
      │                                       │
      │                                       └── On completion ──► Replay completed response or handle failure
      │
      └── Existing (`failed`) ──► Transition to `in_progress` (Retry allowed)
```

### 2.2 Client Identity Scoping

To prevent one client from hijacking or conflicting with another client's idempotency keys, keys are scoped:
- **Authenticated Clients**: When `ORCHESTRATOR_API_TOKEN` is configured and supplied via `Authorization: Bearer <token>`, the scope is derived from a cryptographic one-way digest: `token:<sha256(token.encode())[:16]>`. The secret token itself is never stored in plaintext.
- **Development / Loopback Mode**: When running without authentication on loopback interfaces, scope is bound to the client host: `ip:<client_ip>`.

This ensures two different clients using the key `"order-123"` will execute separate workflows without collisions or unauthorized replays.

### 2.3 Canonical Request Fingerprinting

The request fingerprint uniquely captures all parameters that determine workflow behavior:
```python
def compute_request_fingerprint(req: CreateRunRequest) -> str:
    canonical = {
        "task": str(req.task or "").strip(),
        "plan": [
            {
                "id": str(s.id),
                "kind": str(s.kind),
                "instruction": str(s.instruction),
            }
            for s in req.plan
        ]
        if req.plan
        else None,
        "max_concurrency": req.max_concurrency,
        "run_in_background": bool(req.run_in_background),
        "timeout_seconds": req.timeout_seconds,
        "run_id": str(req.run_id or "").strip() if req.run_id else None,
    }
    payload_bytes = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload_bytes).hexdigest()
```

If a client sends an existing key with altered parameters (e.g. changing `max_concurrency` or altering the task description), the server immediately detects the hash divergence and returns:
```http
HTTP/1.1 409 Conflict
Content-Type: application/json

{
  "detail": "Idempotency key 'batch-01' was previously used with a different request payload."
}
```

---

## 3. SQLite Persistence & Concurrency Controls

### 3.1 Database Schema

In `src/orchestrator/persistence/store.py`, `_init_db` initializes the `idempotency_keys` table:

```sql
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT NOT NULL,
    client_scope TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    run_id TEXT,
    status TEXT NOT NULL,          -- 'in_progress', 'completed', 'failed'
    response_code INTEGER,
    response_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (client_scope, key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_created_at
ON idempotency_keys (created_at);
```

### 3.2 In-Flight Synchronization

To guarantee zero race conditions between concurrent requests:
1. `store.acquire_idempotency_key(client_scope, key, request_hash)`:
   - Queries within SQLite transaction lock.
   - If not found: inserts row with `status='in_progress'` and returns `("acquired", None)`.
   - If found with `status='completed'`: returns `("completed", record)`.
   - If found with `status='failed'`: updates to `status='in_progress'`, updates `request_hash`, and returns `("acquired", None)`.
   - If found with `status='in_progress'`: returns `("in_progress", record)`.
2. `store.wait_for_idempotency_completion(client_scope, key, timeout=30.0)`:
   - Uses in-memory `threading.Condition` mapped by `(client_scope, key)` to awaken waiting threads immediately when the winning thread calls `finalize_idempotency_key()` or `fail_idempotency_key()`.
   - Polls database periodically as a robust fallback for multi-worker processes.
3. `store.finalize_idempotency_key(client_scope, key, run_id, response_code, response_json)`:
   - Transitions record to `status='completed'`, saves the serialized `RunResponse` and HTTP code `201`, and notifies all waiting threads.

### 3.3 Startup Recovery & Retention Management

1. **Recovery during Server Startup**:
   In `src/orchestrator/api/server.py` lifespan startup, `store.recover_in_progress_idempotency_keys()` scans for lingering `in_progress` rows:
   - If `run_id` exists in the `runs` table, the key is updated to `status='completed'`.
   - If `run_id` does not exist or is null (the server crashed before persistence completed), the key is transitioned to `status='failed'` with a recovery note, allowing the client to retry.
2. **Configurable Retention**:
   In `src/orchestrator/config.py`:
   - `idempotency_retention_hours`: integer between `1` and `87600` (default: `24` hours).
   - Validated at configuration load and reported via startup diagnostics.
3. **Purge Operation**:
   `store.purge_idempotency_keys(max_age_hours=..., limit=...)` safely deletes expired records older than the cutoff timestamp without affecting checkpoints or runs.

---

## 4. Python Client SDK Integration

`OrchestratorApiClient` in `src/orchestrator/api/client.py` has been updated with full idempotency support:

```python
def create_run(
    self,
    task: str,
    plan: list[dict[str, Any]] | None = None,
    max_concurrency: int | None = None,
    run_in_background: bool = True,
    timeout_seconds: float | None = None,
    run_id: str | None = None,
    idempotency_key: str | None = None,
) -> RunResponse:
    ...
```

- **Validation**: If `idempotency_key` is provided, the client validates that it is a non-empty string `<= 128` characters with only printable ASCII characters. Violations raise `ApiValidationError` immediately before dispatching the HTTP call.
- **Header Delivery**: Transmits `Idempotency-Key: <key>` in request headers.

---

## 5. Verification & Test Results

### 5.1 Dedicated Test Suite (`tests/test_idempotent_submissions.py`)

A comprehensive suite of 14 deterministic tests was executed:

| Test Name | Verified Behavior | Result |
|---|---|---|
| `test_idempotency_config_validation` | Validates `idempotency_retention_hours` bounds (1..87600) and diagnostics. | PASSED |
| `test_idempotency_key_format_validation` | Rejects empty keys, keys > 128 chars, non-printable characters with 400 Bad Request. | PASSED |
| `test_first_submission_creates_run` | Verifies initial submission creates workflow run and records completed key in SQLite. | PASSED |
| `test_repeated_key_returns_identical_run` | Verifies subsequent request returns original `run_id` and `Idempotent-Replayed: true`. | PASSED |
| `test_reusing_key_with_different_payload_returns_conflict` | Verifies altered payload with same key returns 409 Conflict. | PASSED |
| `test_requests_without_key_create_distinct_runs` | Verifies requests omitting header retain default non-idempotent creation. | PASSED |
| `test_concurrent_submissions_with_same_key` | Dispatches 5 concurrent threads with identical key; exactly 1 run created. | PASSED |
| `test_concurrent_submissions_with_distinct_keys` | Dispatches 5 concurrent threads with different keys; all 5 runs created. | PASSED |
| `test_idempotency_persists_across_store_reopen` | Verifies persistence and replaying across SQLite connection reopens. | PASSED |
| `test_startup_recovery_reconciles_in_progress_keys` | Verifies server recovery reconciles persisted runs and marks orphans failed. | PASSED |
| `test_client_scope_isolation_with_auth` | Verifies two distinct Bearer tokens with identical key create distinct runs. | PASSED |
| `test_failed_creation_allows_subsequent_retry` | Verifies failed submission transitions to failed and allows successful retry. | PASSED |
| `test_idempotency_retention_purge` | Verifies `purge_idempotency_keys` removes expired keys and leaves active intact. | PASSED |
| `test_client_sdk_idempotency_support` | Verifies SDK client validation, header transmission, and replaying. | PASSED |

**Result:** `14 passed in 4.55s`

### 5.2 Regression Test Suite

All existing critical suites were executed in parallel:
- `tests/test_api.py`: 16 passed
- `tests/test_concurrency.py`: 14 passed
- `tests/test_shutdown_recovery.py`: 11 passed
- `tests/test_audit_retention_export.py`: 16 passed

**Result:** `57 passed in 49.51s`  
**Linter:** `python -m ruff check src tests` returned `All checks passed!`.

---

## 6. Summary of Changed Files

| File Path | Description of Changes |
|---|---|
| `src/orchestrator/config.py` | Added `idempotency_enabled` and `idempotency_retention_hours` settings, validator, and diagnostics snapshot. |
| `src/orchestrator/persistence/store.py` | Created `idempotency_keys` table and indexes. Added `get_idempotency_record`, `acquire_idempotency_key`, `wait_for_idempotency_completion`, `finalize_idempotency_key`, `fail_idempotency_key`, `recover_in_progress_idempotency_keys`, and `purge_idempotency_keys`. |
| `src/orchestrator/api/routes.py` | Added `validate_idempotency_key`, `get_client_scope`, `compute_request_fingerprint`, and integrated atomic key handling, conflict detection, concurrent waiting, and replay headers into `POST /runs`. |
| `src/orchestrator/api/server.py` | Added `store.recover_in_progress_idempotency_keys()` to lifespan startup recovery. |
| `src/orchestrator/api/client.py` | Added `idempotency_key` argument, client-side format validation (`ApiValidationError`), and `Idempotency-Key` header delivery to `create_run()`. |
| `tests/test_idempotent_submissions.py` | Added 14 unit and integration tests covering the complete idempotency lifecycle. |
| `docs/DEVELOPER_GUIDE.md` | Updated Section 7 API table and created Section 10 detailing Idempotent Workflow Submission, scoping, conflict rules, recovery, and SDK usage. |
| `reports/TASK_35_IDEMPOTENT_SUBMISSIONS.md` | Comprehensive engineering and verification report. |
