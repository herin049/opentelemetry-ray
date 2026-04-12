# OpenTelemetry Instrumentation for Ray Serve

[![PyPI](https://img.shields.io/pypi/v/opentelemetry-instrumentation-ray-serve)](https://pypi.org/project/opentelemetry-instrumentation-ray-serve/)
[![Python Version](https://img.shields.io/pypi/pyversions/opentelemetry-instrumentation-ray-serve)](https://pypi.org/project/opentelemetry-instrumentation-ray-serve/)
[![License](https://img.shields.io/pypi/l/opentelemetry-instrumentation-ray-serve)](https://github.com/herin049/opentelemetry-ray/blob/main/LICENSE)
[![GitHub](https://img.shields.io/github/stars/herin049/opentelemetry-ray)](https://github.com/herin049/opentelemetry-ray)

Automatic instrumentation for [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) deployments so that every incoming request is traced with OpenTelemetry.

## Installation

```bash
pip install opentelemetry-instrumentation-ray-serve
```

## Usage

```python
from opentelemetry.instrumentation.ray.serve import RayServeInstrumentor

RayServeInstrumentor().instrument()
```

The instrumentor can also be loaded automatically via the `opentelemetry_instrumentor` entry-point using the `opentelemetry-instrument` CLI command.

## References

- [OpenTelemetry](https://opentelemetry.io/)
- [OpenTelemetry Python](https://opentelemetry-python.readthedocs.io/)
- [Ray Serve](https://docs.ray.io/en/latest/serve/index.html)

## License

Apache-2.0
