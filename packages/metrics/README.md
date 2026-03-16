# Metrics

This package contains utilities for collecting metrics in `@message-queue-toolkit`.

## Installation

```sh
npm install @message-queue-toolkit/metrics
```

## Overview

All metrics implement the `MessageMetricsManager` interface from `@message-queue-toolkit/core`, which means they can be passed directly to any `AbstractQueueService` via the `messageMetricsManager` option.

```ts
import { PrometheusMessageProcessingTimeMetric } from '@message-queue-toolkit/metrics'

const metric = new PrometheusMessageProcessingTimeMetric({
  name: 'message_processing_duration_ms',
  helpDescription: 'Time spent processing a message',
  buckets: [10, 50, 100, 500, 1000],
})

// Pass to your queue service
const service = new MyQueueService({ messageMetricsManager: metric })
```

---

## Prometheus metrics

All Prometheus metrics use [prom-client](https://github.com/siimon/prom-client) under the hood.

### Base parameters

All metrics accept `PrometheusMetricParams`:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | yes | Prometheus metric name |
| `helpDescription` | `string` | yes | Prometheus metric description |
| `buckets` | `number[]` | histograms only | Histogram bucket boundaries |
| `messageVersion` | `string \| (metadata) => string \| undefined` | no | Static version string or function to extract version from message metadata |

An optional second argument accepts a custom `prom-client` instance (useful for testing or multi-registry setups).

---

### Histogram metrics (time-based)

Use `Histogram` to measure message timing. Base labels registered on every observation:

| Label | Value |
|---|---|
| `messageType` | Message type identifier |
| `version` | Resolved message version |
| `queue` | Queue or topic name |
| `result` | Processing result status (`consumed`, `published`, `retryLater`, `error`) |

#### Built-in implementations

**`PrometheusMessageProcessingTimeMetric`**
Measures elapsed time from when processing started to when it ended.
```
value = messageProcessingEndTimestamp - messageProcessingStartTimestamp
```

**`PrometheusMessageLifetimeMetric`**
Measures elapsed time from when the message was originally sent to when it was fully processed. Includes any time the message spent waiting in the queue.
```
value = messageProcessingEndTimestamp - messageTimestamp
```
Skips observation if `messageTimestamp` is not available.

**`PrometheusMessageQueueTimeMetric`**
Measures elapsed time from when the message was originally sent to when processing started (i.e., queue wait time only).
```
value = messageProcessingStartTimestamp - messageTimestamp
```
Skips observation if `messageTimestamp` is not available.

#### Custom histogram with extra labels

Extend `PrometheusMessageTimeMetric` to add custom labels. Pass `labelNames` in the params and override `getLabelValuesForProcessedMessage`:

```ts
import { PrometheusMessageTimeMetric } from '@message-queue-toolkit/metrics'
import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { LabelValues } from 'prom-client'

class MyProcessingTimeMetric extends PrometheusMessageTimeMetric<MyMessage, 'env'> {
  protected calculateObservedValue(metadata: ProcessedMessageMetadata<MyMessage>): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }

  protected getLabelValuesForProcessedMessage(): LabelValues<'env'> {
    return { env: process.env.NODE_ENV ?? 'unknown' }
  }
}

const metric = new MyProcessingTimeMetric({
  name: 'message_processing_duration_ms',
  helpDescription: 'Processing time by environment',
  buckets: [10, 50, 100, 500],
  labelNames: ['env'],
})
```

---

### Counter metrics (event-based)

Use `Counter` to count message events. Base labels registered on every increment:

| Label | Value |
|---|---|
| `messageType` | Message type identifier |
| `version` | Resolved message version |
| `queue` | Queue or topic name |
| `result` | Processing result status (`consumed`, `published`, `retryLater`, `error`) |

#### Built-in implementations

**`PrometheusMessageResultCounter`**
Counts all processed messages using only the built-in base labels. No extra configuration needed.

```ts
import { PrometheusMessageResultCounter } from '@message-queue-toolkit/metrics'

const metric = new PrometheusMessageResultCounter({
  name: 'messages_total',
  helpDescription: 'Number of messages processed',
})
```

**`PrometheusMessageErrorCounter`**
Counts only messages that result in an error. Adds an `errorReason` label. Skips all non-error messages.

```ts
import { PrometheusMessageErrorCounter } from '@message-queue-toolkit/metrics'

const metric = new PrometheusMessageErrorCounter({
  name: 'message_errors_total',
  helpDescription: 'Number of messages that failed processing',
  labelNames: ['errorReason'],
})
```

#### Custom counter with extra labels

Extend `PrometheusMessageCounter` and implement `calculateCount`. Override `getLabelValuesForProcessedMessage` when adding custom labels:

```ts
import { PrometheusMessageCounter } from '@message-queue-toolkit/metrics'
import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { LabelValues } from 'prom-client'

class MyRegionCounter extends PrometheusMessageCounter<MyMessage, 'region'> {
  protected calculateCount(): number | null {
    return 1
  }

  protected override getLabelValuesForProcessedMessage(
    metadata: ProcessedMessageMetadata<MyMessage>,
  ): LabelValues<'region'> {
    return { region: metadata.message.region }
  }
}

const metric = new MyRegionCounter({
  name: 'messages_by_region_total',
  helpDescription: 'Number of messages processed, by region',
  labelNames: ['region'],
})
```

When no custom labels are needed, omit `labelNames` and skip overriding `getLabelValuesForProcessedMessage`:

```ts
class MyConsumedCounter extends PrometheusMessageCounter<MyMessage> {
  protected calculateCount(metadata: ProcessedMessageMetadata<MyMessage>): number | null {
    return metadata.processingResult.status === 'consumed' ? 1 : null
  }
}

const metric = new MyConsumedCounter({
  name: 'messages_consumed_total',
  helpDescription: 'Number of successfully consumed messages',
})
```

---

### Using multiple metrics together

`MessageMultiMetricManager` aggregates multiple `MessageMetricsManager` instances and fans out each `registerProcessedMessage` call to all of them.

```ts
import {
  MessageMultiMetricManager,
  PrometheusMessageProcessingTimeMetric,
  PrometheusMessageResultCounter,
  PrometheusMessageErrorCounter,
} from '@message-queue-toolkit/metrics'

const metricsManager = new MessageMultiMetricManager([
  new PrometheusMessageProcessingTimeMetric({
    name: 'message_processing_duration_ms',
    helpDescription: 'Message processing time',
    buckets: [10, 50, 100, 500, 1000],
  }),
  new PrometheusMessageResultCounter({
    name: 'messages_total',
    helpDescription: 'Messages processed',
  }),
  new PrometheusMessageErrorCounter({
    name: 'message_errors_total',
    helpDescription: 'Messages that failed processing',
    labelNames: ['errorReason'],
  }),
])

const service = new MyQueueService({ messageMetricsManager: metricsManager })
```
