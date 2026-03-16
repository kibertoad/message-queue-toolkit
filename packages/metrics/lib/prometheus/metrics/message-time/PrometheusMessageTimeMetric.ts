import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type promClient from 'prom-client'
import type { Histogram, LabelValues } from 'prom-client'
import { PrometheusMessageMetric } from '../../PrometheusMessageMetric.ts'
import type { PrometheusMetricParams } from '../../types.ts'

export type PrometheusMetricTimeParams<
  MessagePayload extends object,
  Labels extends string = never,
> = PrometheusMetricParams<MessagePayload, Labels> & { buckets: number[] }

export abstract class PrometheusMessageTimeMetric<
  MessagePayload extends object,
  Labels extends string = never,
> extends PrometheusMessageMetric<
  MessagePayload,
  Histogram<'messageType' | 'version' | 'queue' | 'result' | Labels>,
  Labels,
  PrometheusMetricTimeParams<MessagePayload, Labels>
> {
  protected createMetric(
    client: typeof promClient,
    metricParams: PrometheusMetricTimeParams<MessagePayload, Labels>,
  ): Histogram<'messageType' | 'version' | 'queue' | 'result' | Labels> {
    return new client.Histogram({
      name: metricParams.name,
      help: metricParams.helpDescription,
      buckets: metricParams.buckets,
      labelNames: [
        'messageType',
        'version',
        'queue',
        'result',
        ...(this.metricParams.labelNames ?? []),
      ],
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
        ...this.getLabelValuesForProcessedMessage(metadata),
      } as LabelValues<'messageType' | 'version' | 'queue' | 'result' | Labels>,
      observedValue,
    )
  }

  protected getLabelValuesForProcessedMessage(
    _metadata: ProcessedMessageMetadata<MessagePayload>,
  ): LabelValues<Labels> {
    return {} as LabelValues<Labels>
  }

  protected abstract calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null
}
