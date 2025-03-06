import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { Histogram } from 'prom-client'
import type promClient from 'prom-client'
import { PrometheusMessageMetric } from '../../PrometheusMessageMetric'
import type { PrometheusMetricParams } from '../../types'

export abstract class PrometheusMessageTimeMetric<
  MessagePayload extends object,
> extends PrometheusMessageMetric<
  MessagePayload,
  Histogram<'messageType' | 'version' | 'queue' | 'result'>
> {
  protected createMetric(
    client: typeof promClient,
    metricParams: PrometheusMetricParams<MessagePayload>,
  ): Histogram<'messageType' | 'version' | 'queue' | 'result'> {
    return new client.Histogram({
      name: metricParams.name,
      help: metricParams.helpDescription,
      buckets: metricParams.buckets,
      labelNames: ['messageType', 'version', 'queue', 'result'],
    })
  }
  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayload>): void {
    const observedValue: number | null = this.calculateObservedValue(metadata)

    // Data not available, skipping
    if (observedValue === null) return

    this.metric.observe(
      {
        messageType: metadata.messageType,
        version: this.messageVersionGeneratingFunction(metadata),
        queue: metadata.queueName,
        result: metadata.processingResult.status,
      },
      observedValue,
    )
  }

  protected abstract calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null
}
