import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { LabelValues } from 'prom-client'
import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

export class PrometheusMessageByStatusCounter<
  MessagePayload extends object,
> extends PrometheusMessageCounter<MessagePayload, 'resultStatus'> {
  protected getLabelValuesForProcessedMessage(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): LabelValues<'resultStatus'> {
    return {
      resultStatus: metadata.processingResult.status,
    }
  }

  protected calculateCount(): number | null {
    return 1
  }
}
