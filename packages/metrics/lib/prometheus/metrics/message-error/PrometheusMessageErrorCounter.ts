import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type promClient from 'prom-client'
import type { LabelValues } from 'prom-client'
import type { PrometheusMetricParams } from '../../types.ts'
import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

export class PrometheusMessageErrorCounter<
  MessagePayload extends object,
> extends PrometheusMessageCounter<MessagePayload, 'errorReason'> {
  constructor(
    metricParams: Omit<PrometheusMetricParams<MessagePayload, 'errorReason'>, 'labelNames'>,
    client?: typeof promClient,
  ) {
    super({ ...metricParams, labelNames: ['errorReason'] }, client)
  }

  protected override getLabelValuesForProcessedMessage(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): LabelValues<'errorReason'> {
    return {
      errorReason:
        metadata.processingResult.status === 'error'
          ? metadata.processingResult.errorReason
          : undefined,
    }
  }

  protected calculateCount(metadata: ProcessedMessageMetadata<MessagePayload>): number | null {
    if (metadata.processingResult.status !== 'error') return null

    return 1
  }
}
