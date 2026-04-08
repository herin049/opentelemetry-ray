"""
OpenTelemetry Resource Detector for Ray
========================================

This package provides a :class:`RayResourceDetector` that automatically
discovers `Ray <https://www.ray.io/>`_ runtime attributes and exposes
them as an OpenTelemetry :class:`~opentelemetry.sdk.resources.Resource`.

Usage
-----

Register the detector when configuring the
:class:`~opentelemetry.sdk.trace.TracerProvider`:

.. code-block:: python

    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.resource.detector.ray import RayResourceDetector

    resource = Resource.create().merge(
        RayResourceDetector().detect(),
    )
    provider = TracerProvider(resource=resource)

The detector can also be loaded automatically via the
``opentelemetry_resource_detector`` entry-point by setting the
``OTEL_EXPERIMENTAL_RESOURCE_DETECTORS`` environment variable:

.. code-block:: bash

    export OTEL_EXPERIMENTAL_RESOURCE_DETECTORS="ray"

Detected Attributes
-------------------

The following attributes are always reported when Ray is installed:

- ``ray.version``

When Ray is initialized the detector additionally reports:

- ``ray.job.id``
- ``ray.node.id``
- ``ray.worker.id``
- ``ray.namespace``

The following attributes are reported when available:

- ``ray.worker.process_type``
- ``ray.actor.id``
- ``ray.cluster.session_name``
- ``ray.cluster.gcs_address``
"""

from __future__ import annotations

import logging
from typing import Any, cast

from opentelemetry.instrumentation.dependencies import get_dependency_conflicts
from opentelemetry.sdk.resources import Resource, ResourceDetector

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
from opentelemetry.resource.detector.ray.package import _detects

_logger = logging.getLogger(__name__)

_WORKER_MODE_NAMES: dict[int, str] = {
    0: "driver",
    1: "worker",
    2: "local",
    3: "spill_worker",
    4: "restore_worker",
}


def _safe_get(func: Any, *args: Any) -> Any:
    """Call a function and return its result, or None if it raises."""
    try:
        return func(*args)
    except Exception:  # pylint: disable=broad-exception-caught
        return None


class RayResourceDetector(ResourceDetector):
    def detect(self) -> Resource:
        conflict = get_dependency_conflicts(_detects)
        if conflict is not None:
            _logger.info(
                "Skipping Ray resource detection: %s",
                conflict,
            )
            if self.raise_on_error:
                raise RuntimeError(str(conflict))
            return Resource.get_empty()

        import ray  # pylint: disable=import-outside-toplevel

        attributes: dict[str, str | bool | int] = {
            RAY_VERSION: ray.__version__,
        }

        if not ray.is_initialized():  # pyright: ignore[reportUnknownMemberType]
            _logger.warning(
                "Ray is installed but not initialized. "
                "Only ray.version will be reported.",
            )
            return Resource(attributes)

        ctx = cast(Any, ray.get_runtime_context())  # pyright: ignore[reportUnknownMemberType]

        attributes[RAY_JOB_ID] = ctx.get_job_id()
        attributes[RAY_NODE_ID] = ctx.get_node_id()
        attributes[RAY_WORKER_ID] = ctx.get_worker_id()
        attributes[RAY_NAMESPACE] = ctx.namespace

        mode = _safe_get(self._get_worker_mode)
        if mode is not None:
            attributes[RAY_WORKER_PROCESS_TYPE] = _WORKER_MODE_NAMES.get(
                mode, str(mode)
            )

        actor_id = _safe_get(ctx.get_actor_id)
        if actor_id is not None:
            attributes[RAY_ACTOR_ID] = actor_id

        session_name = _safe_get(ctx.get_session_name)
        if session_name is not None:
            attributes[RAY_CLUSTER_SESSION_NAME] = session_name

        gcs_address = _safe_get(lambda: ctx.gcs_address)
        if gcs_address is not None:
            attributes[RAY_CLUSTER_GCS_ADDRESS] = gcs_address

        return Resource(attributes)

    @staticmethod
    def _get_worker_mode() -> int | None:
        import ray._private.worker as ray_worker  # pylint: disable=import-outside-toplevel

        mode = cast(Any, ray_worker.global_worker.mode)  # pyright: ignore[reportUnknownMemberType]
        if isinstance(mode, int):
            return mode
        return None

    @staticmethod
    def _detect_node_attributes(
        attributes: dict[str, str | bool | int],
        node_id: str,
    ) -> None:
        import ray  # pylint: disable=import-outside-toplevel

        nodes = _safe_get(ray.nodes)  # pyright: ignore[reportUnknownMemberType]
        if not nodes:
            return

        node = next(
            (n for n in nodes if n.get("NodeID") == node_id),
            None,
        )
        if node is None:
            return

        resources = node.get("Resources", {})
        cpu = resources.get("CPU")
        if cpu is not None:
            attributes[RAY_NODE_CPU_COUNT] = int(cpu)
        gpu = resources.get("GPU")
        if gpu is not None:
            attributes[RAY_NODE_GPU_COUNT] = int(gpu)


__all__ = ["RayResourceDetector"]
