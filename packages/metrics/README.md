# Metrics

This packages contains utilities for collecting metrics in `@message-queue-toolkit`

## Prometheus metrics

Metrics that use [Prometheus](https://prometheus.io/) toolkit and [prom-client](https://github.com/siimon/prom-client) library

### MessageProcessingPrometheusMetric
Abstract class implementing `MessageMetricsManager` interface, that can be injected into `AbstractQueueService` from `@message-queue-toolkit/core`.

It uses [Histogram](https://prometheus.io/docs/concepts/metric_types/#histogram) metric to collect message processing times with labels:
- `messageType` - message type
- `version` - message version
- `queue` - name of the queue or topic
- `result` - processing result

See [MessageProcessingPrometheusMetric.ts](lib/prometheus/MessageProcessingPrometheusMetric.ts) for available parameters.

There are following non-abstract implementations available:
- `MessageProcessingTimeMetric` - registers elapsed time from start to the end of message processing
- `MessageLifetimeMetric` - registers elapsed time from the point where message was initially sent, to the point when it was processed. 
Note: if message is waiting in the queue due to high load or barrier, the waiting time is included in the measurement

### MessageProcessingMultiMetrics
Implementation of `MessageMetricsManager` that allows to use multiple `MessageProcessingPrometheusMetric` instances.

