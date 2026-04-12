# OpenTelemetry Ray

[![License](https://img.shields.io/pypi/l/opentelemetry-resource-detector-ray)](https://github.com/herin049/opentelemetry-ray/blob/main/LICENSE)
[![GitHub](https://img.shields.io/github/stars/herin049/opentelemetry-ray)](https://github.com/herin049/opentelemetry-ray)

OpenTelemetry utilities for [Ray](https://www.ray.io/).

## Packages

| Package | Description | PyPI |
|---|---|---|
| [opentelemetry-resource-detector-ray](./opentelemetry-resource-detector-ray) | Resource detector that captures Ray runtime attributes | [![PyPI](https://img.shields.io/pypi/v/opentelemetry-resource-detector-ray)](https://pypi.org/project/opentelemetry-resource-detector-ray/) |
| [opentelemetry-instrumentation-ray-serve](./opentelemetry-instrumentation-ray-serve) | Instrumentation for Ray Serve deployments | [![PyPI](https://img.shields.io/pypi/v/opentelemetry-instrumentation-ray-serve)](https://pypi.org/project/opentelemetry-instrumentation-ray-serve/) |

## Auto-instrumentation with Ray

Ray supports preloading Python modules into every worker process via the `RAY_preload_python_modules` environment variable. Combined with OpenTelemetry's auto-instrumentation module, this enables tracing across all Ray workers without any code changes:

```bash
export RAY_preload_python_modules="opentelemetry.instrumentation.auto_instrumentation.sitecustomize"
```

This causes each Ray worker to run OpenTelemetry's auto-instrumentation at startup, automatically activating any installed instrumentors (including `opentelemetry-instrumentation-ray-serve`) and resource detectors.

## License

Apache-2.0
