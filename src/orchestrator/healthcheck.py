"""Container healthcheck and probe utility for API, Celery worker, and broker services."""

from __future__ import annotations

import argparse
import sys
import urllib.error
import urllib.request
from typing import Any

from .config import get_settings
from .persistence.store import CheckpointStore


def check_api_health(
    host: str = "127.0.0.1",
    port: int = 8000,
    path: str = "/ready",
    timeout_seconds: float = 5.0,
) -> bool:
    """Check API server readiness via HTTP GET probe."""
    clean_host = (host or "127.0.0.1").strip()
    if clean_host == "0.0.0.0":
        clean_host = "127.0.0.1"
    clean_path = path if path.startswith("/") else f"/{path}"
    url = f"http://{clean_host}:{int(port)}{clean_path}"

    try:
        req = urllib.request.Request(
            url,
            headers={"User-Agent": "Orchestrator-Healthcheck/1.0"},
        )
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return bool(200 <= resp.status < 300)
    except Exception as exc:
        sys.stderr.write(f"API health probe failed for {url}: {exc}\n")
        return False


def check_worker_health(
    store: CheckpointStore | None = None,
    timeout_seconds: float = 5.0,
) -> bool:
    """Check Celery worker liveness and responsiveness."""
    # 1. Attempt Celery control inspect ping if broker and workers are connected
    try:
        from .celery_app import get_celery_app

        celery_app = get_celery_app()
        inspector = celery_app.control.inspect(timeout=timeout_seconds)
        ping_res: dict[str, Any] | None = inspector.ping()
        if ping_res and any("ok" in str(v).lower() for v in ping_res.values()):
            return True
    except Exception as exc:
        sys.stderr.write(f"Celery inspect ping check failed or unavailable: {exc}\n")

    # 2. Fall back to durable worker heartbeat records in SQLite persistence
    try:
        s = store or CheckpointStore()
        if s.enabled:
            settings = get_settings()
            stale_threshold = float(settings.celery_worker_stale_threshold_seconds or 60.0)
            workers = s.list_worker_heartbeats(stale_threshold_seconds=stale_threshold)
            if any(w.get("status") == "active" for w in workers):
                return True
    except Exception as exc:
        sys.stderr.write(f"Worker heartbeat persistence probe failed: {exc}\n")

    return False


def check_redis_health(
    broker_url: str | None = None,
    timeout_seconds: float = 5.0,
) -> bool:
    """Check Redis broker connectivity and ping responsiveness."""
    url = broker_url or get_settings().celery_broker_url or "redis://localhost:6379/0"
    try:
        import redis

        r = redis.from_url(url, socket_timeout=timeout_seconds)
        return bool(r.ping())
    except Exception as exc:
        sys.stderr.write(f"Redis broker probe failed for {url}: {exc}\n")
        return False


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for container health checks."""
    parser = argparse.ArgumentParser(description="Orchestrator container health probe.")
    parser.add_argument(
        "--role",
        choices=["api", "worker", "redis"],
        default="api",
        help="Target container role to inspect.",
    )
    parser.add_argument("--host", default=None, help="API server host.")
    parser.add_argument("--port", type=int, default=None, help="API server port.")
    parser.add_argument("--path", default="/ready", help="HTTP readiness path.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Probe timeout in seconds (default: 5.0).",
    )
    args = parser.parse_args(argv)

    settings = get_settings()
    host = args.host or settings.api_host or "127.0.0.1"
    port = args.port or settings.api_port or 8000

    if args.role == "api":
        healthy = check_api_health(
            host=host,
            port=port,
            path=args.path,
            timeout_seconds=args.timeout,
        )
    elif args.role == "worker":
        healthy = check_worker_health(timeout_seconds=args.timeout)
    else:
        healthy = check_redis_health(
            broker_url=settings.celery_broker_url,
            timeout_seconds=args.timeout,
        )

    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
