from __future__ import annotations

import sys
import types
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from opentelemetry.instrumentation.ray.serve import (
    RayServeInstrumentor,
    _instrument_asgi_app,
    _is_fastapi_app,
    _is_starlette_app,
    _try_instrument_fastapi,
    _try_instrument_starlette,
    _wrap_asgi_app_replica_wrapper_init,
)
from opentelemetry.instrumentation.ray.serve.package import _instruments

_MODULE = "opentelemetry.instrumentation.ray.serve"


def _make_fake_module(name: str, **attrs: Any) -> types.ModuleType:
    """Create a fake module with the given attributes for sys.modules patching."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def _install_fake_fastapi(fastapi_class: type) -> dict[str, types.ModuleType]:
    """Build a sys.modules patch dict for a fake fastapi package."""
    return {"fastapi": _make_fake_module("fastapi", FastAPI=fastapi_class)}


def _install_fake_starlette(
    starlette_class: type,
) -> dict[str, types.ModuleType]:
    """Build a sys.modules patch dict for a fake starlette package."""
    starlette_pkg = _make_fake_module("starlette")
    applications = _make_fake_module(
        "starlette.applications", Starlette=starlette_class,
    )
    return {
        "starlette": starlette_pkg,
        "starlette.applications": applications,
    }


class _FakeFastAPI:
    """Stand-in for ``fastapi.FastAPI`` used for isinstance checks."""


class _FakeStarlette:
    """Stand-in for ``starlette.applications.Starlette`` used for isinstance checks."""


class TestInstrumentationDependencies(unittest.TestCase):
    def test_returns_package_instruments(self) -> None:
        deps = RayServeInstrumentor().instrumentation_dependencies()
        self.assertEqual(deps, _instruments)


class TestInstrument(unittest.TestCase):
    @patch(f"{_MODULE}.wrap_function_wrapper")
    def test_calls_wrap_function_wrapper(
        self, mock_wrap: MagicMock,
    ) -> None:
        RayServeInstrumentor()._instrument()
        mock_wrap.assert_called_once_with(
            "ray.serve._private.http_util",
            "ASGIAppReplicaWrapper.__init__",
            _wrap_asgi_app_replica_wrapper_init,
        )


class TestUninstrument(unittest.TestCase):
    def test_calls_unwrap_with_replica_wrapper(self) -> None:
        fake_class = type("ASGIAppReplicaWrapper", (), {})
        fake_http_util = _make_fake_module(
            "ray.serve._private.http_util",
            ASGIAppReplicaWrapper=fake_class,
        )
        fake_private = _make_fake_module(
            "ray.serve._private", http_util=fake_http_util,
        )
        fake_serve = _make_fake_module("ray.serve", _private=fake_private)
        fake_ray = _make_fake_module("ray", serve=fake_serve)

        modules: dict[str, types.ModuleType] = {
            "ray": fake_ray,
            "ray.serve": fake_serve,
            "ray.serve._private": fake_private,
            "ray.serve._private.http_util": fake_http_util,
        }

        with (
            patch.dict(sys.modules, modules),
            patch(f"{_MODULE}.unwrap") as mock_unwrap,
        ):
            RayServeInstrumentor()._uninstrument()

        mock_unwrap.assert_called_once_with(fake_class, "__init__")


class TestWrapAsgiAppReplicaWrapperInit(unittest.TestCase):
    def test_calls_wrapped_with_args_and_kwargs(self) -> None:
        wrapped = MagicMock(return_value=None)
        instance = MagicMock()
        instance._asgi_app = None  # Skip dispatch path.
        args: tuple[Any, ...] = (1, 2)
        kwargs: dict[str, Any] = {"a": "b"}

        _wrap_asgi_app_replica_wrapper_init(wrapped, instance, args, kwargs)

        wrapped.assert_called_once_with(1, 2, a="b")

    @patch(f"{_MODULE}._instrument_asgi_app")
    def test_calls_instrument_asgi_app_after_wrapped(
        self, mock_instrument: MagicMock,
    ) -> None:
        order: list[str] = []
        wrapped = MagicMock(side_effect=lambda: order.append("wrapped"))
        mock_instrument.side_effect = lambda inst: order.append("instrument")
        instance = MagicMock()

        _wrap_asgi_app_replica_wrapper_init(wrapped, instance, (), {})

        self.assertEqual(order, ["wrapped", "instrument"])
        mock_instrument.assert_called_once_with(instance)

    @patch(f"{_MODULE}._instrument_asgi_app", side_effect=RuntimeError("boom"))
    def test_swallows_instrumentation_exception(
        self, _mock: MagicMock,
    ) -> None:
        wrapped = MagicMock(return_value=None)
        instance = MagicMock()

        with self.assertLogs(_MODULE, level="ERROR") as cm:
            _wrap_asgi_app_replica_wrapper_init(
                wrapped, instance, (), {},
            )

        self.assertTrue(
            any("Failed to instrument" in m for m in cm.output),
        )

    def test_returns_none(self) -> None:
        wrapped = MagicMock(return_value="ignored")
        instance = MagicMock()
        instance._asgi_app = None

        result = _wrap_asgi_app_replica_wrapper_init(
            wrapped, instance, (), {},
        )
        self.assertIsNone(result)

    def test_propagates_wrapped_exception(self) -> None:
        wrapped = MagicMock(side_effect=ValueError("init failed"))
        instance = MagicMock()

        with self.assertRaises(ValueError):
            _wrap_asgi_app_replica_wrapper_init(
                wrapped, instance, (), {},
            )


class TestInstrumentAsgiApp(unittest.TestCase):
    def test_no_op_when_asgi_app_missing(self) -> None:
        instance = types.SimpleNamespace()  # no _asgi_app attribute
        with (
            patch(f"{_MODULE}._is_fastapi_app") as mock_fa,
            patch(f"{_MODULE}._is_starlette_app") as mock_st,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            _instrument_asgi_app(instance)

        mock_fa.assert_not_called()
        mock_st.assert_not_called()
        mock_mw.assert_not_called()

    def test_no_op_when_asgi_app_none(self) -> None:
        instance = types.SimpleNamespace(_asgi_app=None)
        with (
            patch(f"{_MODULE}._is_fastapi_app") as mock_fa,
            patch(f"{_MODULE}._is_starlette_app") as mock_st,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            _instrument_asgi_app(instance)

        mock_fa.assert_not_called()
        mock_st.assert_not_called()
        mock_mw.assert_not_called()

    def test_dispatches_to_fastapi(self) -> None:
        app = object()
        instance = types.SimpleNamespace(_asgi_app=app)

        with (
            patch(f"{_MODULE}._is_fastapi_app", return_value=True),
            patch(f"{_MODULE}._is_starlette_app") as mock_st,
            patch(f"{_MODULE}._try_instrument_fastapi") as mock_fa_try,
            patch(f"{_MODULE}._try_instrument_starlette") as mock_st_try,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            _instrument_asgi_app(instance)

        mock_fa_try.assert_called_once_with(app)
        mock_st.assert_not_called()
        mock_st_try.assert_not_called()
        mock_mw.assert_not_called()
        # _asgi_app must NOT be replaced.
        self.assertIs(instance._asgi_app, app)

    def test_dispatches_to_starlette(self) -> None:
        app = object()
        instance = types.SimpleNamespace(_asgi_app=app)

        with (
            patch(f"{_MODULE}._is_fastapi_app", return_value=False),
            patch(f"{_MODULE}._is_starlette_app", return_value=True),
            patch(f"{_MODULE}._try_instrument_fastapi") as mock_fa_try,
            patch(f"{_MODULE}._try_instrument_starlette") as mock_st_try,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            _instrument_asgi_app(instance)

        mock_st_try.assert_called_once_with(app)
        mock_fa_try.assert_not_called()
        mock_mw.assert_not_called()
        self.assertIs(instance._asgi_app, app)

    def test_wraps_raw_asgi_app_in_middleware(self) -> None:
        app = object()
        wrapped_app = object()
        instance = types.SimpleNamespace(_asgi_app=app)

        with (
            patch(f"{_MODULE}._is_fastapi_app", return_value=False),
            patch(f"{_MODULE}._is_starlette_app", return_value=False),
            patch(
                f"{_MODULE}.OpenTelemetryMiddleware",
                return_value=wrapped_app,
            ) as mock_mw,
            patch(f"{_MODULE}._try_instrument_fastapi") as mock_fa_try,
            patch(f"{_MODULE}._try_instrument_starlette") as mock_st_try,
        ):
            _instrument_asgi_app(instance)

        mock_mw.assert_called_once_with(app)
        mock_fa_try.assert_not_called()
        mock_st_try.assert_not_called()
        self.assertIs(instance._asgi_app, wrapped_app)

    def test_fastapi_failure_does_not_fall_back_to_middleware(self) -> None:
        """FastAPI apps must never be wrapped in OpenTelemetryMiddleware,
        even if the FastAPIInstrumentor is skipped due to a conflict."""
        app = object()
        instance = types.SimpleNamespace(_asgi_app=app)

        with (
            patch(f"{_MODULE}._is_fastapi_app", return_value=True),
            patch(f"{_MODULE}._try_instrument_fastapi") as mock_fa_try,
            patch(f"{_MODULE}._is_starlette_app") as mock_st,
            patch(f"{_MODULE}._try_instrument_starlette") as mock_st_try,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            # _try_instrument_fastapi may silently no-op on conflict; the
            # caller cannot tell. Either way, we must not fall through.
            mock_fa_try.return_value = None
            _instrument_asgi_app(instance)

        mock_fa_try.assert_called_once_with(app)
        mock_st.assert_not_called()
        mock_st_try.assert_not_called()
        mock_mw.assert_not_called()
        self.assertIs(instance._asgi_app, app)

    def test_starlette_failure_does_not_fall_back_to_middleware(self) -> None:
        app = object()
        instance = types.SimpleNamespace(_asgi_app=app)

        with (
            patch(f"{_MODULE}._is_fastapi_app", return_value=False),
            patch(f"{_MODULE}._is_starlette_app", return_value=True),
            patch(f"{_MODULE}._try_instrument_starlette") as mock_st_try,
            patch(f"{_MODULE}.OpenTelemetryMiddleware") as mock_mw,
        ):
            mock_st_try.return_value = None
            _instrument_asgi_app(instance)

        mock_st_try.assert_called_once_with(app)
        mock_mw.assert_not_called()
        self.assertIs(instance._asgi_app, app)


class TestIsFastapiApp(unittest.TestCase):
    def test_returns_true_for_instance(self) -> None:
        with patch.dict(sys.modules, _install_fake_fastapi(_FakeFastAPI)):
            self.assertTrue(_is_fastapi_app(_FakeFastAPI()))

    def test_returns_false_for_other_types(self) -> None:
        non_fastapi: list[object] = [
            None,
            object(),
            "string",
            42,
            _FakeStarlette(),
        ]
        with patch.dict(sys.modules, _install_fake_fastapi(_FakeFastAPI)):
            for value in non_fastapi:
                with self.subTest(value=repr(value)):
                    self.assertFalse(_is_fastapi_app(value))

    def test_returns_false_when_fastapi_not_installed(self) -> None:
        # Force "import fastapi" to raise ImportError.
        with patch.dict(sys.modules, {"fastapi": None}):
            self.assertFalse(_is_fastapi_app(object()))


class TestIsStarletteApp(unittest.TestCase):
    def test_returns_true_for_instance(self) -> None:
        with patch.dict(
            sys.modules, _install_fake_starlette(_FakeStarlette),
        ):
            self.assertTrue(_is_starlette_app(_FakeStarlette()))

    def test_returns_false_for_other_types(self) -> None:
        non_starlette: list[object] = [
            None,
            object(),
            "string",
            42,
            _FakeFastAPI(),
        ]
        with patch.dict(
            sys.modules, _install_fake_starlette(_FakeStarlette),
        ):
            for value in non_starlette:
                with self.subTest(value=repr(value)):
                    self.assertFalse(_is_starlette_app(value))

    def test_returns_false_when_starlette_not_installed(self) -> None:
        with patch.dict(
            sys.modules,
            {"starlette": None, "starlette.applications": None},
        ):
            self.assertFalse(_is_starlette_app(object()))


class TestTryInstrumentFastapi(unittest.TestCase):
    def setUp(self) -> None:
        self.instrumentor_cls = MagicMock()
        self.instrumentor_instance = MagicMock()
        self.instrumentor_cls.return_value = self.instrumentor_instance
        self.instrumentor_instance.instrumentation_dependencies.return_value = (
            "fastapi ~= 0.58",
        )

        self.fake_module = _make_fake_module(
            "opentelemetry.instrumentation.fastapi",
            FastAPIInstrumentor=self.instrumentor_cls,
        )
        self.modules_patch = patch.dict(
            sys.modules,
            {"opentelemetry.instrumentation.fastapi": self.fake_module},
        )
        self.modules_patch.start()
        self.addCleanup(self.modules_patch.stop)

    def test_instruments_when_no_conflict(self) -> None:
        app = object()
        with patch(
            f"{_MODULE}.get_dependency_conflicts", return_value=None,
        ) as mock_conf:
            _try_instrument_fastapi(app)

        mock_conf.assert_called_once_with(("fastapi ~= 0.58",))
        self.instrumentor_cls.instrument_app.assert_called_once_with(app)

    def test_skips_when_conflict(self) -> None:
        app = object()
        conflict = MagicMock()
        conflict.__str__ = lambda self: "conflict-detail"  # type: ignore[assignment]

        with (
            patch(
                f"{_MODULE}.get_dependency_conflicts",
                return_value=conflict,
            ),
            self.assertLogs(_MODULE, level="INFO") as cm,
        ):
            _try_instrument_fastapi(app)

        self.instrumentor_cls.instrument_app.assert_not_called()
        self.assertTrue(
            any("Skipping FastAPI" in m for m in cm.output),
        )


class TestTryInstrumentStarlette(unittest.TestCase):
    def setUp(self) -> None:
        self.instrumentor_cls = MagicMock()
        self.instrumentor_instance = MagicMock()
        self.instrumentor_cls.return_value = self.instrumentor_instance
        self.instrumentor_instance.instrumentation_dependencies.return_value = (
            "starlette ~= 0.13",
        )

        self.fake_module = _make_fake_module(
            "opentelemetry.instrumentation.starlette",
            StarletteInstrumentor=self.instrumentor_cls,
        )
        self.modules_patch = patch.dict(
            sys.modules,
            {"opentelemetry.instrumentation.starlette": self.fake_module},
        )
        self.modules_patch.start()
        self.addCleanup(self.modules_patch.stop)

    def test_instruments_when_no_conflict(self) -> None:
        app = object()
        with patch(
            f"{_MODULE}.get_dependency_conflicts", return_value=None,
        ) as mock_conf:
            _try_instrument_starlette(app)

        mock_conf.assert_called_once_with(("starlette ~= 0.13",))
        self.instrumentor_cls.instrument_app.assert_called_once_with(app)

    def test_skips_when_conflict(self) -> None:
        app = object()
        conflict = MagicMock()
        conflict.__str__ = lambda self: "conflict-detail"  # type: ignore[assignment]

        with (
            patch(
                f"{_MODULE}.get_dependency_conflicts",
                return_value=conflict,
            ),
            self.assertLogs(_MODULE, level="INFO") as cm,
        ):
            _try_instrument_starlette(app)

        self.instrumentor_cls.instrument_app.assert_not_called()
        self.assertTrue(
            any("Skipping Starlette" in m for m in cm.output),
        )


if __name__ == "__main__":
    unittest.main()
