from typing import Any, Collection

from opentelemetry.instrumentation.instrumentor import BaseInstrumentor


class RayServeInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]:
        pass

    def _uninstrument(self, **kwargs: Any):
        pass

    def _instrument(self, **kwargs: Any):
        pass
