import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric.ts'

export class PrometheusMessageProcessingTimeMetric<
  MessagePayload extends object,
> extends PrometheusMessageTimeMetric<MessagePayload> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }
}
