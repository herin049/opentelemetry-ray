from __future__ import annotations

import unittest
from unittest.mock import MagicMock, PropertyMock, patch

from opentelemetry.sdk.resources import Resource

from opentelemetry.resource.detector.ray import (
    RayResourceDetector,
    _WORKER_MODE_NAMES,
    _safe_get,
)
from opentelemetry.resource.detector.ray._attributes import (
    RAY_ACTOR_ID,
    RAY_CLUSTER_GCS_ADDRESS,
    RAY_CLUSTER_SESSION_NAME,
    RAY_JOB_ID,
    RAY_NAMESPACE,
    RAY_NODE_CPU_COUNT,
    RAY_NODE_GPU_COUNT,
    RAY_NODE_ID,
    RAY_VERSION,
    RAY_WORKER_ID,
    RAY_WORKER_PROCESS_TYPE,
)
from opentelemetry.resource.detector.ray.version import __version__


def _make_mock_context(
    *,
    job_id: str = "job_001",
    node_id: str = "node_abc",
    worker_id: str = "worker_xyz",
    namespace: str = "default",
    actor_id: str | None = None,
    session_name: str | None = None,
    gcs_address: str | None = None,
) -> MagicMock:
    """Build a mock ``ray.get_runtime_context()`` return value."""
    ctx = MagicMock()
    ctx.get_job_id.return_value = job_id
    ctx.get_node_id.return_value = node_id
    ctx.get_worker_id.return_value = worker_id
    ctx.namespace = namespace

    if actor_id is None:
        ctx.get_actor_id.side_effect = RuntimeError("no actor")
    else:
        ctx.get_actor_id.return_value = actor_id

    if session_name is None:
        ctx.get_session_name.side_effect = RuntimeError("no session")
    else:
        ctx.get_session_name.return_value = session_name

    if gcs_address is None:
        type(ctx).gcs_address = PropertyMock(
            side_effect=RuntimeError("no gcs"),
        )
    else:
        type(ctx).gcs_address = PropertyMock(return_value=gcs_address)

    return ctx


def _make_mock_ray(
    *,
    version: str = "2.7.0",
    initialized: bool = True,
    context: MagicMock | None = None,
) -> MagicMock:
    """Build a mock ``ray`` module injected via ``sys.modules``."""
    mock_ray = MagicMock()
    mock_ray.__version__ = version
    mock_ray.is_initialized.return_value = initialized
    if context is not None:
        mock_ray.get_runtime_context.return_value = context
    return mock_ray


def _make_mock_ray_private_worker(
    *, mode: int | str | None = 0,
) -> MagicMock:
    """Build a mock ``ray._private.worker`` module."""
    mock_mod = MagicMock()
    mock_mod.global_worker.mode = mode
    return mock_mod


_GET_DEP_CONFLICTS = "opentelemetry.resource.detector.ray.get_dependency_conflicts"


class TestVersion(unittest.TestCase):
    def test_version_string(self) -> None:
        self.assertEqual(__version__, "0.1.0")


class TestSafeGet(unittest.TestCase):
    def test_returns_value_on_success(self) -> None:
        cases: list[tuple[str, object, list[object]]] = [
            ("int", 42, []),
            ("str", "hello", []),
            ("none", None, []),
            ("with_arg", 10, [5]),
        ]
        for label, expected, args in cases:
            with self.subTest(label=label):
                if args:
                    result = _safe_get(lambda x: x * 2, *args)
                else:
                    result = _safe_get(lambda: expected)
                self.assertEqual(result, expected)

    def test_returns_none_on_exception(self) -> None:
        exceptions: list[type[Exception]] = [
            ValueError,
            RuntimeError,
            AttributeError,
            ImportError,
            TypeError,
        ]
        for exc_cls in exceptions:
            with self.subTest(exc=exc_cls.__name__):

                def _raise(e: type[Exception] = exc_cls) -> None:
                    raise e("boom")

                self.assertIsNone(_safe_get(_raise))

    def test_passes_arguments(self) -> None:
        func = MagicMock(return_value="ok")
        result = _safe_get(func, "a", "b")
        func.assert_called_once_with("a", "b")
        self.assertEqual(result, "ok")


class TestWorkerModeNames(unittest.TestCase):
    def test_all_known_modes(self) -> None:
        expected: dict[int, str] = {
            0: "driver",
            1: "worker",
            2: "local",
            3: "spill_worker",
            4: "restore_worker",
        }
        for mode, name in expected.items():
            with self.subTest(mode=mode):
                self.assertEqual(_WORKER_MODE_NAMES[mode], name)

    def test_has_exactly_five_entries(self) -> None:
        self.assertEqual(len(_WORKER_MODE_NAMES), 5)


class TestDetectDependencyConflict(unittest.TestCase):
    @patch(_GET_DEP_CONFLICTS, return_value=MagicMock(__str__=lambda _: "conflict"))
    def test_returns_empty_resource(self, _mock: MagicMock) -> None:
        resource = RayResourceDetector().detect()
        self.assertEqual(dict(resource.attributes), {})

    @patch(_GET_DEP_CONFLICTS, return_value=MagicMock(__str__=lambda _: "conflict"))
    def test_logs_info(self, _mock: MagicMock) -> None:
        with self.assertLogs(
            "opentelemetry.resource.detector.ray", level="INFO",
        ) as cm:
            RayResourceDetector().detect()
        self.assertTrue(
            any("Skipping Ray resource detection" in m for m in cm.output),
        )

    @patch(_GET_DEP_CONFLICTS, return_value=MagicMock(__str__=lambda _: "conflict"))
    def test_raises_when_raise_on_error(self, _mock: MagicMock) -> None:
        detector = RayResourceDetector(raise_on_error=True)
        with self.assertRaises(RuntimeError):
            detector.detect()


class TestDetectRayNotInitialized(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_ray = _make_mock_ray(initialized=False)

    @patch(_GET_DEP_CONFLICTS, return_value=None)
    def test_returns_version_only(self, _mock: MagicMock) -> None:
        with patch.dict(
            "sys.modules", {"ray": self.mock_ray},
        ):
            resource = RayResourceDetector().detect()
        self.assertEqual(
            dict(resource.attributes), {RAY_VERSION: "2.7.0"},
        )

    @patch(_GET_DEP_CONFLICTS, return_value=None)
    def test_logs_warning(self, _mock: MagicMock) -> None:
        with (
            patch.dict("sys.modules", {"ray": self.mock_ray}),
            self.assertLogs(
                "opentelemetry.resource.detector.ray", level="WARNING",
            ) as cm,
        ):
            RayResourceDetector().detect()
        self.assertTrue(
            any("not initialized" in m for m in cm.output),
        )

    @patch(_GET_DEP_CONFLICTS, return_value=None)
    def test_does_not_call_runtime_context(self, _mock: MagicMock) -> None:
        with patch.dict("sys.modules", {"ray": self.mock_ray}):
            RayResourceDetector().detect()
        self.mock_ray.get_runtime_context.assert_not_called()


class TestDetectRayInitialized(unittest.TestCase):
    def _detect(
        self,
        *,
        context: MagicMock | None = None,
        worker_mode: int | str | None = 0,
        version: str = "2.7.0",
    ) -> Resource:
        ctx = context or _make_mock_context()
        mock_ray = _make_mock_ray(
            version=version, initialized=True, context=ctx,
        )

        with (
            patch(_GET_DEP_CONFLICTS, return_value=None),
            patch.dict("sys.modules", {"ray": mock_ray}),
            patch.object(
                RayResourceDetector,
                "_get_worker_mode",
                return_value=(
                    worker_mode if isinstance(worker_mode, int) else None
                ),
            ),
        ):
            return RayResourceDetector().detect()

    def test_mandatory_attributes(self) -> None:
        resource = self._detect()
        attrs = dict(resource.attributes)
        mandatory: dict[str, str] = {
            RAY_VERSION: "2.7.0",
            RAY_JOB_ID: "job_001",
            RAY_NODE_ID: "node_abc",
            RAY_WORKER_ID: "worker_xyz",
            RAY_NAMESPACE: "default",
        }
        for key, expected in mandatory.items():
            with self.subTest(key=key):
                self.assertEqual(attrs[key], expected)

    def test_worker_mode_names(self) -> None:
        for mode, name in _WORKER_MODE_NAMES.items():
            with self.subTest(mode=mode):
                resource = self._detect(worker_mode=mode)
                self.assertEqual(
                    dict(resource.attributes)[RAY_WORKER_PROCESS_TYPE], name,
                )

    def test_worker_mode_unknown_int(self) -> None:
        resource = self._detect(worker_mode=99)
        self.assertEqual(
            dict(resource.attributes)[RAY_WORKER_PROCESS_TYPE], "99",
        )

    def test_worker_mode_none_excluded(self) -> None:
        resource = self._detect(worker_mode=None)
        self.assertNotIn(RAY_WORKER_PROCESS_TYPE, resource.attributes)

    def test_optional_attributes_included(self) -> None:
        cases: list[tuple[str, str, str]] = [
            ("actor_id", RAY_ACTOR_ID, "actor_123"),
            ("session_name", RAY_CLUSTER_SESSION_NAME, "session_xyz"),
            ("gcs_address", RAY_CLUSTER_GCS_ADDRESS, "127.0.0.1:6379"),
        ]
        for label, attr_key, value in cases:
            with self.subTest(attr=label):
                ctx = _make_mock_context(**{label: value})
                resource = self._detect(context=ctx)
                self.assertEqual(
                    dict(resource.attributes)[attr_key], value,
                )

    def test_optional_attributes_excluded_when_none(self) -> None:
        excluded_keys: list[str] = [
            RAY_ACTOR_ID,
            RAY_CLUSTER_SESSION_NAME,
            RAY_CLUSTER_GCS_ADDRESS,
        ]
        resource = self._detect(
            context=_make_mock_context(),
        )
        for key in excluded_keys:
            with self.subTest(key=key):
                self.assertNotIn(key, resource.attributes)

    def test_all_attributes_present(self) -> None:
        ctx = _make_mock_context(
            actor_id="actor_1",
            session_name="sess_1",
            gcs_address="10.0.0.1:6379",
        )
        resource = self._detect(context=ctx, worker_mode=1)
        attrs = dict(resource.attributes)

        expected_keys: list[str] = [
            RAY_VERSION,
            RAY_JOB_ID,
            RAY_NODE_ID,
            RAY_WORKER_ID,
            RAY_NAMESPACE,
            RAY_WORKER_PROCESS_TYPE,
            RAY_ACTOR_ID,
            RAY_CLUSTER_SESSION_NAME,
            RAY_CLUSTER_GCS_ADDRESS,
        ]
        for key in expected_keys:
            with self.subTest(key=key):
                self.assertIn(key, attrs)


class TestGetWorkerMode(unittest.TestCase):
    def _patch_worker_mode(self, mode: object) -> dict[str, MagicMock]:
        mock_worker = _make_mock_ray_private_worker(mode=mode)
        mock_private = MagicMock()
        mock_private.worker = mock_worker
        mock_ray = MagicMock()
        mock_ray._private = mock_private
        return {
            "ray": mock_ray,
            "ray._private": mock_private,
            "ray._private.worker": mock_worker,
        }

    def test_returns_int_mode(self) -> None:
        for mode in (0, 1, 2, 3, 4):
            with self.subTest(mode=mode):
                with patch.dict("sys.modules", self._patch_worker_mode(mode)):
                    result = RayResourceDetector._get_worker_mode()
                self.assertEqual(result, mode)

    def test_returns_none_for_non_int(self) -> None:
        non_ints: list[object] = [None, "driver", 3.14, object()]
        for value in non_ints:
            with self.subTest(value=repr(value)):
                with patch.dict(
                    "sys.modules", self._patch_worker_mode(value),
                ):
                    result = RayResourceDetector._get_worker_mode()
                self.assertIsNone(result)


class TestDetectNodeAttributes(unittest.TestCase):
    def _call(
        self,
        node_id: str,
        nodes_return: list[dict[str, object]] | None,
        *,
        nodes_raise: bool = False,
    ) -> dict[str, str | bool | int]:
        mock_ray = MagicMock()
        if nodes_raise:
            mock_ray.nodes.side_effect = RuntimeError("boom")
        else:
            mock_ray.nodes.return_value = nodes_return

        attrs: dict[str, str | bool | int] = {}
        with patch.dict("sys.modules", {"ray": mock_ray}):
            RayResourceDetector._detect_node_attributes(attrs, node_id)
        return attrs

    def test_adds_cpu_and_gpu(self) -> None:
        nodes = [
            {"NodeID": "n1", "Resources": {"CPU": 8.0, "GPU": 2.0}},
        ]
        attrs = self._call("n1", nodes)
        self.assertEqual(attrs[RAY_NODE_CPU_COUNT], 8)
        self.assertEqual(attrs[RAY_NODE_GPU_COUNT], 2)

    def test_cpu_only(self) -> None:
        nodes = [{"NodeID": "n1", "Resources": {"CPU": 4.0}}]
        attrs = self._call("n1", nodes)
        self.assertEqual(attrs[RAY_NODE_CPU_COUNT], 4)
        self.assertNotIn(RAY_NODE_GPU_COUNT, attrs)

    def test_gpu_only(self) -> None:
        nodes = [{"NodeID": "n1", "Resources": {"GPU": 1.0}}]
        attrs = self._call("n1", nodes)
        self.assertNotIn(RAY_NODE_CPU_COUNT, attrs)
        self.assertEqual(attrs[RAY_NODE_GPU_COUNT], 1)

    def test_no_resources_key(self) -> None:
        nodes = [{"NodeID": "n1"}]
        attrs = self._call("n1", nodes)
        self.assertNotIn(RAY_NODE_CPU_COUNT, attrs)
        self.assertNotIn(RAY_NODE_GPU_COUNT, attrs)

    def test_node_not_found(self) -> None:
        nodes = [{"NodeID": "other", "Resources": {"CPU": 8.0}}]
        attrs = self._call("n1", nodes)
        self.assertEqual(attrs, {})

    def test_empty_nodes_list(self) -> None:
        attrs = self._call("n1", [])
        self.assertEqual(attrs, {})

    def test_nodes_returns_none(self) -> None:
        attrs = self._call("n1", None)
        self.assertEqual(attrs, {})

    def test_nodes_raises(self) -> None:
        attrs = self._call("n1", None, nodes_raise=True)
        self.assertEqual(attrs, {})

    def test_values_cast_to_int(self) -> None:
        cases: list[tuple[str, float | int]] = [
            ("float", 8.0),
            ("int", 8),
        ]
        for label, cpu_val in cases:
            with self.subTest(input_type=label):
                nodes = [{"NodeID": "n1", "Resources": {"CPU": cpu_val}}]
                attrs = self._call("n1", nodes)
                self.assertIsInstance(attrs[RAY_NODE_CPU_COUNT], int)
                self.assertEqual(attrs[RAY_NODE_CPU_COUNT], 8)

    def test_selects_correct_node(self) -> None:
        nodes = [
            {"NodeID": "n1", "Resources": {"CPU": 2.0}},
            {"NodeID": "n2", "Resources": {"CPU": 16.0, "GPU": 4.0}},
            {"NodeID": "n3", "Resources": {"CPU": 8.0}},
        ]
        attrs = self._call("n2", nodes)
        self.assertEqual(attrs[RAY_NODE_CPU_COUNT], 16)
        self.assertEqual(attrs[RAY_NODE_GPU_COUNT], 4)
