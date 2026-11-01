"""Thin OpenTelemetry wrapper and workflow trace correlation."""

from __future__ import annotations

import re
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from ..config import get_settings

_TRACER = None
_CURRENT_RUN_ID: ContextVar[str] = ContextVar("current_run_id", default="")

_SENSITIVE_KEY_PATTERNS = re.compile(
    r"(?i)(api[_-]?key|secret|password|token|auth|bearer|credentials)"
)


def reset_tracer() -> None:
    """Helper for test isolation: resets cached tracer state and active run ID."""
    global _TRACER
    _TRACER = None
    _CURRENT_RUN_ID.set("")


def get_current_run_id() -> str:
    """Return the currently scoped workflow run ID, if any."""
    return _CURRENT_RUN_ID.get()


def set_current_run_id(run_id: str) -> None:
    """Manually set the current run ID in context."""
    _CURRENT_RUN_ID.set(run_id or "")


@contextmanager
def trace_run(run_id: str):
    """Context manager scoping a stable run ID to the current execution context."""
    token = _CURRENT_RUN_ID.set(run_id or "")
    try:
        yield run_id
    finally:
        _CURRENT_RUN_ID.reset(token)


def sanitize_trace_attribute(key: str, value: Any) -> str:
    """Sanitize attribute values for safe tracing.

    Strips Python tracebacks, redacts API keys, Bearer tokens, and password/secret
    query strings, and bounds attribute length.
    """
    if value is None:
        return ""
    val_str = str(value).strip()
    if not val_str:
        return ""

    if _SENSITIVE_KEY_PATTERNS.search(key):
        return "[REDACTED]"

    # If it contains a traceback, extract the final exception line
    if "Traceback (most recent call last):" in val_str:
        lines = [line.strip() for line in val_str.splitlines() if line.strip()]
        if lines:
            val_str = lines[-1]

    # Redact sensitive credential patterns
    val_str = re.sub(r"sk-[a-zA-Z0-9_-]{16,}", "[REDACTED_KEY]", val_str)
    val_str = re.sub(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]{16,}", "Bearer [REDACTED_TOKEN]", val_str)
    val_str = re.sub(
        r"(?i)(password|api[_-]?key|secret)\s*[:=]\s*[^\s,;]+", r"\1=[REDACTED]", val_str
    )

    if len(val_str) > 256:
        val_str = val_str[:253] + "..."
    return val_str


def create_trace_event(node: str, event: str, run_id: str = "", **kwargs: Any) -> dict[str, Any]:
    """Create a structured, correlated, sanitized trace event dictionary.

    Guarantees consistent run correlation (run_id / task_id), sanitized error/reason
    diagnostics, and safe metadata attributes without leaking secrets.
    """
    active_run_id = run_id or get_current_run_id()
    evt: dict[str, Any] = {
        "node": node,
        "event": event,
    }
    if active_run_id:
        evt["run_id"] = active_run_id
        evt["task_id"] = active_run_id

    for k, v in kwargs.items():
        if k in ("error", "reason"):
            evt[k] = sanitize_trace_attribute(k, v)
        elif _SENSITIVE_KEY_PATTERNS.search(k):
            evt[k] = "[REDACTED]"
        elif isinstance(v, str):
            evt[k] = sanitize_trace_attribute(k, v)
        else:
            evt[k] = v
    return evt


def setup_tracing():
    """Configure an OTLP exporter if an endpoint is set; safe to call repeatedly."""
    global _TRACER
    if _TRACER is not None:
        return _TRACER
    settings = get_settings()
    try:
        import sys

        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

        class _SafeStderr:
            def write(self, s: str) -> int:
                try:
                    if sys.stderr and not getattr(sys.stderr, "closed", False):
                        return sys.stderr.write(s)
                except Exception:
                    pass
                return 0

            def flush(self) -> None:
                try:
                    if sys.stderr and not getattr(sys.stderr, "closed", False):
                        sys.stderr.flush()
                except Exception:
                    pass

            def isatty(self) -> bool:
                try:
                    return bool(sys.stderr and sys.stderr.isatty())
                except Exception:
                    return False

        provider = TracerProvider(
            resource=Resource.create({"service.name": settings.otel_service_name})
        )
        if settings.otel_exporter_otlp_endpoint:
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

            exporter = OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint)
        else:
            exporter = ConsoleSpanExporter(out=_SafeStderr())
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _TRACER = trace.get_tracer(settings.otel_service_name)
    except Exception:
        _TRACER = False  # sentinel: tried, unavailable
    return _TRACER


@contextmanager
def span(name: str, **attributes):
    """Context manager for OpenTelemetry spans with safe error handling and sanitization."""
    try:
        tracer = setup_tracing()
    except Exception:
        tracer = None

    if not tracer:
        yield None
        return

    try:
        cm = tracer.start_as_current_span(name)
        s = cm.__enter__()
    except Exception:
        yield None
        return

    try:
        active_run_id = (
            attributes.get("run_id")
            or attributes.get("task_id")
            or get_current_run_id()
        )
        if active_run_id:
            s.set_attribute("run_id", str(active_run_id))
            s.set_attribute("task_id", str(active_run_id))

        for k, v in attributes.items():
            sanitized_v = sanitize_trace_attribute(k, v)
            s.set_attribute(k, sanitized_v)
    except Exception:
        pass

    try:
        yield s
    except Exception as exc:
        try:
            cm.__exit__(type(exc), exc, exc.__traceback__)
        except Exception:
            pass
        raise
    else:
        try:
            cm.__exit__(None, None, None)
        except Exception:
            pass
