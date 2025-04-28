import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type promClient from 'prom-client'
import type { Counter } from 'prom-client'
import { PrometheusMessageMetric } from '../../PrometheusMessageMetric.ts'
import type { PrometheusMetricParams } from '../../types.ts'

export class PrometheusMessageErrorCounter<
  MessagePayload extends object,
> extends PrometheusMessageMetric<
  MessagePayload,
  Counter<'queue' | 'messageType' | 'version' | 'errorReason'>
> {
  protected createMetric(
    client: typeof promClient,
    metricParams: PrometheusMetricParams<MessagePayload>,
  ): Counter {
    return new client.Counter({
      name: metricParams.name,
      help: metricParams.helpDescription,
      labelNames: ['queue', 'messageType', 'version', 'errorReason'],
    })
  }

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayload>): void {
    if (metadata.processingResult.status !== 'error') return

    this.metric.inc(
      {
        queue: metadata.queueName,
        messageType: metadata.messageType,
        errorReason: metadata.processingResult.errorReason,
        version: this.messageVersionGeneratingFunction(metadata),
      },
      1,
    )
  }
}
