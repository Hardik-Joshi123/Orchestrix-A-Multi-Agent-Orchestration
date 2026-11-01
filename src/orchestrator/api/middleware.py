"""ASGI request body limit middleware enforcing strict payload size bounds."""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import status
from fastapi.responses import JSONResponse

from ..config import get_settings

logger = logging.getLogger(__name__)


class RequestBodyLimitMiddleware:
    """Enforce request body size limits across both Content-Length and streamed chunked requests."""

    def __init__(self, app: Any, max_bytes: int | None = None) -> None:
        self.app = app
        self._max_bytes_override = max_bytes

    @property
    def max_bytes(self) -> int:
        if self._max_bytes_override is not None:
            return self._max_bytes_override
        env_val = os.environ.get("ORCHESTRATOR_API_MAX_BODY_BYTES")
        if env_val:
            try:
                return int(env_val)
            except ValueError:
                pass
        return get_settings().api_max_body_bytes

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        effective_max = self.max_bytes
        headers = dict(scope.get("headers", []))

        # 1. Fast pre-check: inspect Content-Length header if present
        cl_raw = headers.get(b"content-length")
        if cl_raw is not None:
            try:
                cl_val = int(cl_raw.decode("latin-1"))
                if cl_val < 0:
                    raise ValueError("Negative Content-Length")
                if cl_val > effective_max:
                    res = JSONResponse(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        content={
                            "detail": (
                                f"Request payload exceeds maximum allowed size of {effective_max} bytes."
                            )
                        },
                    )
                    await res(scope, receive, send)
                    return
            except (ValueError, UnicodeDecodeError):
                res = JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"detail": "Malformed Content-Length header."},
                )
                await res(scope, receive, send)
                return

        # 2. Intercept receive() to count incoming chunks for streamed or chunked bodies
        total_bytes = 0
        limit_exceeded = False

        async def limited_receive() -> Any:
            nonlocal total_bytes, limit_exceeded
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body", b"")
                total_bytes += len(chunk)
                if total_bytes > effective_max:
                    limit_exceeded = True
                    raise RuntimeError(f"Payload limit of {effective_max} bytes exceeded.")
            return message

        async def limited_send(message: Any) -> None:
            # If payload limit was exceeded, suppress downstream error responses
            if limit_exceeded:
                return
            await send(message)

        try:
            await self.app(scope, limited_receive, limited_send)
        except Exception:
            if not limit_exceeded:
                raise

        if limit_exceeded:
            res = JSONResponse(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                content={
                    "detail": f"Request payload exceeds maximum allowed size of {effective_max} bytes."
                },
            )
            await res(scope, receive, send)
