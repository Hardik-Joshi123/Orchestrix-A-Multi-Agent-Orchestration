"""Short-term working memory in Redis, scoped to a single task.

Degrades to an in-process dict when Redis is unavailable.
"""

from __future__ import annotations

import json

from ..config import get_settings

_LOCAL: dict[str, dict[str, str]] = {}
_REDIS_CLIENT = None
_CONNECTION_ATTEMPTED = False


def reset_local_store() -> None:
    """Helper for test isolation."""
    global _LOCAL, _REDIS_CLIENT, _CONNECTION_ATTEMPTED
    _LOCAL.clear()
    _REDIS_CLIENT = None
    _CONNECTION_ATTEMPTED = False


class ShortTermMemory:
    def __init__(self, task_id: str, ttl_seconds: int = 3600) -> None:
        self.task_id = task_id
        self.ttl = ttl_seconds
        self._key = f"task:{task_id}:working"
        self._client = self._connect()

    def _connect(self):
        global _REDIS_CLIENT, _CONNECTION_ATTEMPTED
        if _CONNECTION_ATTEMPTED:
            return _REDIS_CLIENT
        _CONNECTION_ATTEMPTED = True
        try:
            import socket
            import urllib.parse

            import redis

            settings = get_settings()
            parsed = urllib.parse.urlparse(settings.redis_url)
            host = parsed.hostname or "localhost"
            port = parsed.port or 6379

            with socket.create_connection((host, port), timeout=0.2):
                pass

            client = redis.from_url(
                settings.redis_url,
                decode_responses=True,
                socket_connect_timeout=0.2,
                socket_timeout=0.2,
            )
            client.ping()
            _REDIS_CLIENT = client
        except Exception:
            _REDIS_CLIENT = None
        return _REDIS_CLIENT

    @property
    def _local(self) -> dict[str, str]:
        return _LOCAL.setdefault(self.task_id, {})

    def set(self, field: str, value) -> None:
        payload = json.dumps(value)
        if self._client:
            try:
                self._client.hset(self._key, field, payload)
                self._client.expire(self._key, self.ttl)
                return
            except Exception:
                pass
        self._local[field] = payload

    def get(self, field: str):
        raw = None
        if self._client:
            try:
                raw = self._client.hget(self._key, field)
            except Exception:
                raw = None
        if raw is None:
            raw = self._local.get(field)
        return json.loads(raw) if raw else None

    def all(self) -> dict:
        data = None
        if self._client:
            try:
                data = self._client.hgetall(self._key)
            except Exception:
                data = None
        if not data:
            data = self._local
        return {k: json.loads(v) for k, v in data.items()}

    def clear(self) -> None:
        if self._client:
            try:
                self._client.delete(self._key)
            except Exception:
                pass
        self._local.clear()
