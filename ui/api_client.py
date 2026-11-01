"""UI-specific API client interface for the Orchestration Dashboard."""

from __future__ import annotations

import os
from typing import Any

from orchestrator.api.client import (
    ApiAuthenticationError,
    ApiClientError,
    ApiConflictError,
    ApiConnectionError,
    ApiForbiddenError,
    ApiNotFoundError,
    ApiPayloadTooLargeError,
    ApiRateLimitError,
    ApiServerError,
    ApiServiceUnavailableError,
    ApiValidationError,
    OrchestratorApiClient,
)


def get_default_api_base_url() -> str:
    """Determine the default API base URL from environment or fallback to localhost."""
    return os.environ.get("ORCHESTRATOR_API_URL", "http://127.0.0.1:8000").rstrip("/")


def create_ui_client(
    base_url: str | None = None,
    api_token: str | None = None,
    timeout: float = 30.0,
    **kwargs: Any,
) -> OrchestratorApiClient:
    """Create an OrchestratorApiClient instance with defaults for the UI dashboard."""
    url = (base_url or get_default_api_base_url()).rstrip("/")
    token = api_token if api_token is not None else os.environ.get("ORCHESTRATOR_API_TOKEN", "")
    return OrchestratorApiClient(base_url=url, api_token=token, timeout=timeout, **kwargs)


__all__ = [
    "OrchestratorApiClient",
    "ApiClientError",
    "ApiAuthenticationError",
    "ApiConnectionError",
    "ApiValidationError",
    "ApiNotFoundError",
    "ApiConflictError",
    "ApiRateLimitError",
    "ApiPayloadTooLargeError",
    "ApiServerError",
    "ApiForbiddenError",
    "ApiServiceUnavailableError",
    "get_default_api_base_url",
    "create_ui_client",
]
