import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { LabelValues } from 'prom-client'
import { PrometheusMessageCounter } from './PrometheusMessageCounter.ts'

export class PrometheusMessageResultCounter<
  MessagePayload extends object,
> extends PrometheusMessageCounter<MessagePayload, 'result'> {
  protected override getLabelValuesForProcessedMessage(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): LabelValues<'result'> {
    return {
      result: metadata.processingResult.status,
    }
  }

  protected calculateCount(): number | null {
    return 1
  }
}
