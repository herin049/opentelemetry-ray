"""
OpenTelemetry Instrumentation for Ray Serve
============================================

This package provides a :class:`RayServeInstrumentor` that automatically
instruments `Ray Serve <https://docs.ray.io/en/latest/serve/index.html>`_
deployments so that every incoming request is traced with
`OpenTelemetry <https://opentelemetry.io/>`_.

Usage
-----

Enable instrumentation before deploying your Ray Serve application:

.. code-block:: python

    from opentelemetry.instrumentation.ray.serve import RayServeInstrumentor

    RayServeInstrumentor().instrument()

The instrumentor can also be loaded automatically via the
``opentelemetry_instrumentor`` entry-point by using the
``opentelemetry-instrument`` CLI command.

How It Works
------------

When a Ray Serve replica is initialized, the instrumentor inspects the
underlying ASGI application and applies the most appropriate tracing
strategy:

1. **FastAPI** applications are instrumented with
   ``FastAPIInstrumentor`` from ``opentelemetry-instrumentation-fastapi``.
2. **Starlette** applications are instrumented with
   ``StarletteInstrumentor`` from ``opentelemetry-instrumentation-starlette``.
3. **Raw ASGI** callables are wrapped with
   ``OpenTelemetryMiddleware`` from ``opentelemetry-instrumentation-asgi``.

FastAPI is checked before Starlette given that ``FastAPI`` is a subclass of
``Starlette``.  If a framework-specific instrumentor is selected, the
generic ASGI middleware is **not** applied, preventing double-wrapping.
"""
# ruff: noqa: PLC0415

from __future__ import annotations

import logging
from typing import Any, Callable, Collection, Final

from wrapt import wrap_function_wrapper  # type: ignore

from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware
from opentelemetry.instrumentation.dependencies import get_dependency_conflicts
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.ray.serve.package import _instruments
from opentelemetry.instrumentation.utils import unwrap

_logger = logging.getLogger(__name__)

_RAY_HTTP_UTIL_MODULE: Final[str] = "ray.serve._private.http_util"
_WRAPPED_CLASS: Final[str] = "ASGIAppReplicaWrapper"
_WRAPPED_METHOD: Final[str] = "__init__"


class RayServeInstrumentor(BaseInstrumentor):
    """Instrumentor for `Ray Serve <https://docs.ray.io/en/latest/serve/index.html>`_.

    Hooks into ``ray.serve._private.http_util.ASGIAppReplicaWrapper`` to
    transparently add OpenTelemetry tracing to every Ray Serve deployment.
    """

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs: Any) -> None:
        wrap_function_wrapper(
            _RAY_HTTP_UTIL_MODULE,
            f"{_WRAPPED_CLASS}.{_WRAPPED_METHOD}",
            _wrap_asgi_app_replica_wrapper_init,
        )

    def _uninstrument(self, **kwargs: Any) -> None:
        # pylint: disable=import-outside-toplevel
        from ray.serve._private import (
            http_util,
        )

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
