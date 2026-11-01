"""Notification provider registry and factory for transactional outbox."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from ..config import Settings, get_settings
from .handler import NotificationHandler, TestNotificationHandler
from .webhook import HttpWebhookNotificationHandler

logger = logging.getLogger(__name__)

# Registry mapping provider name to factory callable
_PROVIDER_REGISTRY: dict[str, Callable[..., NotificationHandler]] = {}


def register_notification_provider(
    name: str,
    factory: Callable[..., NotificationHandler],
) -> None:
    """Register a notification provider factory by name.

    Parameters
    ----------
    name:
        Unique provider identifier (e.g. 'test', 'webhook', 'custom').
    factory:
        Callable accepting `settings` and optional keyword arguments, returning
        a `NotificationHandler` instance.
    """
    key = str(name).strip().lower()
    _PROVIDER_REGISTRY[key] = factory
    logger.debug("Registered notification provider '%s'", key)


def _create_test_handler(
    settings: Settings | None = None,
    **kwargs: Any,
) -> NotificationHandler:
    """Factory for in-memory test notification handler."""
    return TestNotificationHandler(
        fail_all=kwargs.get("fail_all", False),
        fail_events=kwargs.get("fail_events"),
        non_retryable_event_ids=kwargs.get("non_retryable_event_ids"),
        record_duplicates=kwargs.get("record_duplicates", True),
    )


def _create_webhook_handler(
    settings: Settings | None = None,
    **kwargs: Any,
) -> NotificationHandler:
    """Factory for HTTP webhook notification handler using settings or kwargs."""
    cfg = settings if settings is not None else get_settings()

    url = kwargs.get("url") or getattr(cfg, "notification_webhook_url", None)
    if not url:
        raise ValueError(
            "Webhook URL must be provided or configured via 'notification_webhook_url'."
        )

    auth_token = kwargs.get(
        "auth_token",
        getattr(cfg, "notification_webhook_auth_token", None),
    )
    auth_header = kwargs.get(
        "auth_header",
        getattr(cfg, "notification_webhook_auth_header", "Authorization"),
    )
    timeout_seconds = kwargs.get(
        "timeout_seconds",
        getattr(cfg, "notification_webhook_timeout_seconds", 10.0),
    )
    tls_verify = kwargs.get(
        "tls_verify",
        getattr(cfg, "notification_webhook_tls_verify", True),
    )
    max_response_bytes = kwargs.get(
        "max_response_bytes",
        getattr(cfg, "notification_webhook_max_response_bytes", 65536),
    )
    allow_private_ips = kwargs.get(
        "allow_private_ips",
        getattr(cfg, "notification_webhook_allow_private_ips", False),
    )
    allow_insecure_http = kwargs.get(
        "allow_insecure_http",
        getattr(cfg, "notification_webhook_allow_insecure_http", False),
    )

    client = kwargs.get("client")
    custom_headers = kwargs.get("custom_headers")
    signing_secret = kwargs.get(
        "signing_secret",
        getattr(cfg, "notification_webhook_signing_secret", None),
    )
    signing_secret_previous = kwargs.get(
        "signing_secret_previous",
        getattr(cfg, "notification_webhook_signing_secret_previous", None),
    )
    signature_header = kwargs.get(
        "signature_header",
        getattr(cfg, "notification_webhook_signature_header", "X-Webhook-Signature"),
    )
    signature_tolerance_seconds = kwargs.get(
        "signature_tolerance_seconds",
        getattr(cfg, "notification_webhook_signature_tolerance_seconds", 300.0),
    )
    clock = kwargs.get("clock")

    return HttpWebhookNotificationHandler(
        url=url,
        auth_token=auth_token,
        auth_header=auth_header,
        timeout_seconds=timeout_seconds,
        tls_verify=tls_verify,
        max_response_bytes=max_response_bytes,
        allow_private_ips=allow_private_ips,
        allow_insecure_http=allow_insecure_http,
        client=client,
        custom_headers=custom_headers,
        signing_secret=signing_secret,
        signing_secret_previous=signing_secret_previous,
        signature_header=signature_header,
        signature_tolerance_seconds=signature_tolerance_seconds,
        clock=clock,
    )


# Register default built-in providers
register_notification_provider("test", _create_test_handler)
register_notification_provider("webhook", _create_webhook_handler)


def get_notification_handler(
    provider: str | None = None,
    settings: Settings | None = None,
    **kwargs: Any,
) -> NotificationHandler:
    """Instantiate a NotificationHandler from the provider registry.

    Parameters
    ----------
    provider:
        Optional provider name ('test', 'webhook', etc.). If None, falls back to
        the configured `settings.notification_provider` (default: 'test').
    settings:
        Optional Settings instance. If None, loaded via `get_settings()`.
    **kwargs:
        Additional configuration passed to the provider factory.

    Returns
    -------
    NotificationHandler:
        Instantiated provider instance.
    """
    cfg = settings if settings is not None else get_settings()
    provider_name = (
        provider
        if provider is not None
        else getattr(cfg, "notification_provider", "test")
    ).strip().lower()

    if provider_name not in _PROVIDER_REGISTRY:
        available = sorted(list(_PROVIDER_REGISTRY.keys()))
        raise ValueError(
            f"Unknown notification provider '{provider_name}'. "
            f"Registered providers: {available}"
        )

    factory = _PROVIDER_REGISTRY[provider_name]
    return factory(settings=cfg, **kwargs)
