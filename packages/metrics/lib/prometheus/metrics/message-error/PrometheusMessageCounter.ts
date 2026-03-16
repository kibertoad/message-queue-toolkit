import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type promClient from 'prom-client'
import type { Counter, LabelValues } from 'prom-client'
import { PrometheusMessageMetric } from '../../PrometheusMessageMetric.ts'
import type { DefaultLabels, PrometheusMetricParams } from '../../types.ts'

export abstract class PrometheusMessageCounter<
  MessagePayload extends object,
  Labels extends string = never,
> extends PrometheusMessageMetric<MessagePayload, Counter<DefaultLabels | Labels>, Labels> {
  protected createMetric(
    client: typeof promClient,
    metricParams: PrometheusMetricParams<MessagePayload, Labels>,
  ): Counter<DefaultLabels | Labels> {
    return new client.Counter({
      name: metricParams.name,
      help: metricParams.helpDescription,
      labelNames: [
        'queue',
        'messageType',
        'version',
        'result',
        ...(this.metricParams.labelNames ?? []),
      ],
    })
  }

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayload>): void {
    const count = this.calculateCount(metadata)
    if (count === null) return

    this.metric.inc(
      {
        queue: metadata.queueName,
        messageType: metadata.messageType,
        result: metadata.processingResult.status,
        version: this.messageVersionGeneratingFunction(metadata),
        ...this.getLabelValuesForProcessedMessage(metadata),
      } as LabelValues<DefaultLabels | Labels>,
      count,
    )
  }

  protected getLabelValuesForProcessedMessage(
    _metadata: ProcessedMessageMetadata<MessagePayload>,
  ): LabelValues<Labels> {
    return {} as LabelValues<Labels>
  }

  protected abstract calculateCount(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null
}
