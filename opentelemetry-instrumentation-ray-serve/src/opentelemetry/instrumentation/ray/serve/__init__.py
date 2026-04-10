from __future__ import annotations

import logging
from typing import Any, Callable, Collection

from wrapt import wrap_function_wrapper

from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.dependencies import get_dependency_conflicts
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap

from opentelemetry.instrumentation.ray.serve.package import _instruments

_logger = logging.getLogger(__name__)

_RAY_HTTP_UTIL_MODULE: str = "ray.serve._private.http_util"
_WRAPPED_CLASS: str = "ASGIAppReplicaWrapper"
_WRAPPED_METHOD: str = "__init__"


class RayServeInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        wrap_function_wrapper(
            _RAY_HTTP_UTIL_MODULE,
            f"{_WRAPPED_CLASS}.{_WRAPPED_METHOD}",
            _wrap_asgi_app_replica_wrapper_init,
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        from ray.serve._private import http_util  # pylint: disable=import-outside-toplevel

        unwrap(http_util.ASGIAppReplicaWrapper, _WRAPPED_METHOD)


def _wrap_asgi_app_replica_wrapper_init(
    wrapped: Callable[..., None],
    instance: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> None:
    # Run original __init__ first so _asgi_app is set.
    wrapped(*args, **kwargs)

    try:
        _instrument_asgi_app(instance)
    except Exception:  # pylint: disable=broad-exception-caught
        _logger.exception("Failed to instrument Ray Serve ASGI app")


def _instrument_asgi_app(instance: Any) -> None:
    app: Any = getattr(instance, "_asgi_app", None)
    if app is None:
        return

    # FastAPI is checked before Starlette because FastAPI subclasses Starlette.
    # If the app *is* a FastAPI/Starlette instance, we either instrument with
    # the dedicated instrumentor or do nothing — we never fall back to
    # OpenTelemetryMiddleware for these, to avoid double-wrapping or breaking
    # framework-specific behavior.
    if _is_fastapi_app(app):
        _try_instrument_fastapi(app)
        return

    if _is_starlette_app(app):
        _try_instrument_starlette(app)
        return

    # Raw ASGI callable: safe to wrap with the generic middleware.
    instance._asgi_app = OpenTelemetryMiddleware(app)


def _is_fastapi_app(app: Any) -> bool:
    try:
        from fastapi import FastAPI  # pylint: disable=import-outside-toplevel
    except ImportError:
        return False
    return isinstance(app, FastAPI)


def _is_starlette_app(app: Any) -> bool:
    try:
        from starlette.applications import (  # pylint: disable=import-outside-toplevel
            Starlette,
        )
    except ImportError:
        return False
    return isinstance(app, Starlette)


def _try_instrument_fastapi(app: Any) -> None:
    from opentelemetry.instrumentation.fastapi import (  # pylint: disable=import-outside-toplevel
        FastAPIInstrumentor,
    )

    conflict = get_dependency_conflicts(
        FastAPIInstrumentor().instrumentation_dependencies()
    )
    if conflict is not None:
        _logger.info("Skipping FastAPI instrumentation: %s", conflict)
        return

    FastAPIInstrumentor.instrument_app(app)


def _try_instrument_starlette(app: Any) -> None:
    from opentelemetry.instrumentation.starlette import (  # pylint: disable=import-outside-toplevel
        StarletteInstrumentor,
    )

    conflict = get_dependency_conflicts(
        StarletteInstrumentor().instrumentation_dependencies()
    )
    if conflict is not None:
        _logger.info("Skipping Starlette instrumentation: %s", conflict)
        return

    StarletteInstrumentor.instrument_app(app)


__all__ = ["RayServeInstrumentor"]
