# OpenTelemetry Resource Detector for Ray

[![PyPI](https://img.shields.io/pypi/v/opentelemetry-resource-detector-ray)](https://pypi.org/project/opentelemetry-resource-detector-ray/)
[![Python Version](https://img.shields.io/pypi/pyversions/opentelemetry-resource-detector-ray)](https://pypi.org/project/opentelemetry-resource-detector-ray/)
[![License](https://img.shields.io/pypi/l/opentelemetry-resource-detector-ray)](https://github.com/herin049/opentelemetry-ray/blob/main/LICENSE)
[![GitHub](https://img.shields.io/github/stars/herin049/opentelemetry-ray)](https://github.com/herin049/opentelemetry-ray)

A resource detector that captures [Ray](https://www.ray.io/) runtime attributes and exposes them as an OpenTelemetry `Resource`.

## Installation

```bash
pip install opentelemetry-resource-detector-ray
```

## Usage

```python
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.resource.detector.ray import RayResourceDetector

resource = Resource.create().merge(
    RayResourceDetector().detect(),
)
provider = TracerProvider(resource=resource)
```

Or load automatically via the `OTEL_EXPERIMENTAL_RESOURCE_DETECTORS` environment variable:

```bash
export OTEL_EXPERIMENTAL_RESOURCE_DETECTORS="ray"
```

## Detected Attributes

| Attribute | Condition |
|---|---|
| `ray.version` | Always |
| `ray.job.id` | Ray initialized |
| `ray.node.id` | Ray initialized |
| `ray.worker.id` | Ray initialized |
| `ray.namespace` | Ray initialized |
| `ray.worker.process_type` | When available |
| `ray.actor.id` | When available |
| `ray.cluster.session_name` | When available |
| `ray.cluster.gcs_address` | When available |

## References

- [OpenTelemetry](https://opentelemetry.io/)
- [OpenTelemetry Python](https://opentelemetry-python.readthedocs.io/)
- [Ray](https://www.ray.io/)

## License

Apache-2.0
