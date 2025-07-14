import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric.ts'

/**
 * This metric measures the time a message spends in the queue before processing starts.
 */
export class PrometheusMessageQueueTimeMetric<
  MessagePayload extends object,
> extends PrometheusMessageTimeMetric<MessagePayload> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null {
    if (!metadata.messageTimestamp) return null
    return metadata.messageProcessingStartTimestamp - metadata.messageTimestamp
  }
}
