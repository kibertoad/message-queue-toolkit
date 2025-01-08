# Metrics

This packages contains utilities for collecting metrics in `@message-queue-toolkit`

## Prometheus metrics

Metrics that use [Prometheus](https://prometheus.io/) toolkit and [prom-client](https://github.com/siimon/prom-client) library

### MessageProcessingTimePrometheusMetric

Implementation of `MessageMetricsManager` that can be injected into `AbstractQueueService` from `@message-queue-toolkit/core`.

It uses [Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) metric to collect message processing times with `messageType` and `version` labels.

See [MessageProcessingTimePrometheusMetric.ts](lib/prometheus/MessageProcessingTimePrometheusMetric.ts) for available parameters.
