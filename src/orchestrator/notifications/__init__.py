"""Transactional outbox notification module."""

from .cleanup import (
    ReplayCleanupScheduler,
    get_replay_cleanup_scheduler,
    reset_replay_cleanup_scheduler,
)
from .dispatcher import OutboxDispatcher, get_outbox_dispatcher
from .handler import NotificationDeliveryResult, NotificationHandler, TestNotificationHandler
from .registry import get_notification_handler, register_notification_provider
from .security import SsrfProtectionError, is_forbidden_ip, validate_webhook_url
from .signing import (
    BaseReplayStore,
    InMemoryReplayCache,
    ReplayProtectionCache,
    SqliteReplayStore,
    WebhookSignatureError,
    WebhookSignatureVerifier,
    format_signature_header,
    generate_webhook_signature,
    get_default_replay_store,
    parse_signature_header,
    verify_webhook_signature,
)
from .webhook import HttpWebhookNotificationHandler

__all__ = [
    "BaseReplayStore",
    "HttpWebhookNotificationHandler",
    "InMemoryReplayCache",
    "NotificationDeliveryResult",
    "NotificationHandler",
    "OutboxDispatcher",
    "ReplayCleanupScheduler",
    "ReplayProtectionCache",
    "SqliteReplayStore",
    "SsrfProtectionError",
    "TestNotificationHandler",
    "WebhookSignatureError",
    "WebhookSignatureVerifier",
    "format_signature_header",
    "generate_webhook_signature",
    "get_default_replay_store",
    "get_notification_handler",
    "get_outbox_dispatcher",
    "get_replay_cleanup_scheduler",
    "is_forbidden_ip",
    "parse_signature_header",
    "register_notification_provider",
    "reset_replay_cleanup_scheduler",
    "validate_webhook_url",
    "verify_webhook_signature",
]
