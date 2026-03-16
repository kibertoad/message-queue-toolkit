import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { LabelValues } from 'prom-client'
import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

export class PrometheusMessageErrorCounter<
  MessagePayload extends object,
> extends PrometheusMessageCounter<MessagePayload, 'errorReason'> {
  protected getLabelValuesForProcessedMessage(
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
