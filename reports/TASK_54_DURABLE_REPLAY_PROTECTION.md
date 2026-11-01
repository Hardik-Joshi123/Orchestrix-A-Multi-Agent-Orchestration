# Task 54: Durable Replay Protection & Multi-Process Verification

## Executive Summary
Task 54 hardens webhook signature verification by ensuring replay protection remains durable,
atomic, and race-free across process restarts and concurrent multi-process worker deployments.

In Task 53, webhook replay protection relied on a process-local in-memory cache (`ReplayProtectionCache`).
While sufficient for single-process evaluation, in-memory caches permit replay attacks across worker restarts
or across horizontal multi-process instances (e.g. gunicorn/uvicorn workers or Kubernetes worker pods).

Task 54 introduces a durable SQLite replay backend (`SqliteReplayStore`) backed by atomic database-level
conflict resolution, strict signature-first verification ordering, fail-closed resilience semantics,
and automated expired record lifecycle pruning.

---

## Threat Model & In-Memory Cache Limitations

### 1. The Restart Replay Window
When replay protection state resides exclusively in volatile application memory:
- If an application process crashes or restarts (e.g. during a deploy or scale event), previously seen event
  identities are lost.
- An attacker or network anomaly retransmitting previously delivered requests within the signature tolerance
  window would succeed, causing duplicate downstream execution or side-effects.

### 2. Multi-Process Concurrency Races
In distributed or multi-worker architectures:
- Independent worker processes do not share memory space.
- Concurrent requests presenting identical event IDs can arrive simultaneously at separate workers,
  bypassing process-local deduplication caches and resulting in double execution.

### 3. Identity Consumption by Malicious Payloads
If an unauthenticated request records an event ID before cryptographic verification completes:
- An attacker could emit requests with legitimate event IDs and bogus signatures, permanently exhausting or
  blocking the legitimate sender's forthcoming requests (denial-of-service).

---

## Architectural Implementation

### 1. Durable SQLite Schema & Indexes
Replay records are persisted in a dedicated SQLite table initialized during store bootstrap:
```sql
CREATE TABLE IF NOT EXISTS webhook_replay_records (
    event_id TEXT PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    expires_at REAL NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_webhook_replay_expires
ON webhook_replay_records(expires_at);
```

### 2. Atomic UPSERT with Non-Blocking Expiration
To eliminate concurrency races between worker processes without global transaction locks, recording uses
an atomic SQLite `INSERT ... ON CONFLICT` query:
```sql
INSERT INTO webhook_replay_records (
    event_id, timestamp, expires_at, recorded_at
)
VALUES (?, ?, ?, ?)
ON CONFLICT(event_id) DO UPDATE SET
    timestamp = excluded.timestamp,
    expires_at = excluded.expires_at,
    recorded_at = excluded.recorded_at
WHERE webhook_replay_records.expires_at < ?;
```
- **Fresh Event ID**: Row is inserted (`cur.rowcount == 1`). Accepted.
- **Active Duplicate (Replay)**: Row exists and `expires_at >= now`. The `WHERE` clause evaluates to false,
  no row is updated, and `cur.rowcount == 0`. Rejected as duplicate replay attack.
- **Expired Event ID**: Row exists and `expires_at < now`. The `WHERE` clause evaluates to true, row is
  overwritten, and `cur.rowcount == 1`. Accepted without permanently locking out the event ID.

### 3. Strict Verification Ordering
`WebhookSignatureVerifier.verify()` enforces an immutable order of operations:
1. **Header Parsing**: Extract timestamp `t` and signature hashes `v1`.
2. **Timestamp Drift Tolerance**: Reject requests older than `tolerance_seconds` or too far in the future.
3. **Cryptographic Signature Verification**: Validate HMAC-SHA256 digests against configured candidate
   secrets using constant-time `hmac.compare_digest()`. If no secret matches, reject immediately.
   **Replay identities are NEVER consumed by invalid signatures.**
4. **Replay Store Registration**: Only after cryptographic authenticity is proven, atomically record the
   event ID via `replay_store.check_and_record()`.

### 4. Fail-Closed Resilience
If the replay storage layer encounters an unexpected database error (disk full, corrupted database file,
or connection failure), the verifier logs the operational exception and fails closed:
- When `raise_on_error=True`: Raises `WebhookSignatureError("Replay protection storage error: ...")`.
- When `raise_on_error=False`: Returns `False`.
Under no circumstance will an unverified request be allowed to pass when replay safety cannot be guaranteed.

### 5. Expired Record Lifecycle & Pruning
Expired records are purged safely via batch subquery:
```sql
DELETE FROM webhook_replay_records
WHERE rowid IN (
    SELECT rowid FROM webhook_replay_records
    WHERE expires_at < ?
    LIMIT ?
);
```

---

## Storage Abstraction & Extensibility

### 1. `BaseReplayStore` ABC
```python
class BaseReplayStore(ABC):
    @abstractmethod
    def check_and_record(
        self,
        event_id: str,
        timestamp: int,
        tolerance_seconds: float = 300.0,
        now: float | None = None,
    ) -> bool:
        ...

    @abstractmethod
    def cleanup_expired(
        self,
        now: float | None = None,
        limit: int | None = None,
    ) -> int:
        ...

    @abstractmethod
    def clear(self) -> None:
        ...
```

### 2. Concrete Implementations
- `SqliteReplayStore`: Production-grade durable backend using SQLite UPSERT conflict resolution.
- `InMemoryReplayCache`: Thread-safe in-memory cache with bounded eviction. Aliased to
  `ReplayProtectionCache` for 100% backward compatibility with Task 53.

### 3. Factory Resolution
`get_default_replay_store()` instantiates the configured backend based on `Settings`:
- If `notification_webhook_replay_protection_enabled=False`: returns `None`.
- If `notification_webhook_replay_storage="memory"`: returns `InMemoryReplayCache`.
- If `notification_webhook_replay_storage="sqlite"`: returns `SqliteReplayStore`.

---

## Configuration & Diagnostic Observability

### 1. Settings Reference
| Field Name | Type | Default | Bounds | Description |
| :--- | :--- | :--- | :--- | :--- |
| `notification_webhook_replay_protection_enabled` | bool | `True` | bool | Enable replay protection |
| `notification_webhook_replay_storage` | str | `"sqlite"` | sqlite/memory | Storage backend |
| `notification_webhook_replay_retention_seconds` | float | `86400.0` | 60–2592000s | Retention TTL (s) |
| `notification_webhook_replay_cleanup_batch_size` | int | `500` | 10–10000 | Batch pruning limit |

### 2. Startup Diagnostics
The startup diagnostics dictionary includes full replay protection configuration under
`diagnostics["notifications"]`:
```json
{
  "notifications": {
    "provider": "webhook",
    "signing_enabled": true,
    "signature_tolerance_seconds": 300.0,
    "replay_protection_enabled": true,
    "replay_storage": "sqlite",
    "replay_retention_seconds": 86400.0,
    "replay_cleanup_batch_size": 500
  }
}
```

---

## Verification & Test Results

### 1. Test Suite: `tests/test_durable_replay_protection.py`
| Test Case | Scenario Verified | Result |
| :--- | :--- | :--- |
| `test_durable_replay_basic_duplicate_rejection` | Duplicate requests rejected via SQLite store | PASSED |
| `test_durable_replay_restart_persistence` | Replay protection survives recreation & restart | PASSED |
| `test_durable_replay_concurrent_requests` | 10 concurrent threads race for 1 event; 1 wins | PASSED |
| `test_durable_replay_expiry_and_cleanup` | Expired records pruned; post-expiry events work | PASSED |
| `test_invalid_signature_does_not_consume_replay` | Spoofed signatures fail without consuming ID | PASSED |
| `test_replay_database_failure_fails_closed` | Database operational errors fail closed safely | PASSED |
| `test_secret_rotation_with_durable_replay` | Dual-secret rotation works with durable replay | PASSED |
| `test_replay_disabled_configuration` | Disabling replay allows duplicate deliveries | PASSED |
| `test_factory_get_default_replay_store` | Settings factory selects appropriate backend | PASSED |
| `test_config_validation_replay_settings` | Out-of-bounds config values raise error | PASSED |
| `test_convenience_verify_webhook_signature` | Top-level convenience wrapper verifies with replay | PASSED |

### 2. Full Regression Test Status
- `tests/test_durable_replay_protection.py`: 11 passed.
- `tests/test_webhook_signing.py`: 15 passed.
- `tests/test_notification_webhook.py`: 31 passed.
- `tests/test_transactional_outbox.py`: 12 passed.
- `tests/test_outbox_observability_recovery.py`: 10 passed.
- Approval lifecycle suites (`test_approval_scheduler`, `test_approval_expiration`,
  `test_atomic_approval_audit`, `test_scheduler_scan_recovery`, `test_scheduler_lease`,
  `test_fenced_state_transitions`): 68 passed.
- Total test suite: 147 tests passed (0 failures, 0 regressions).
- Ruff linter: All checks clean.
- Line length validation: All modified files strictly $\le 110$ characters.
