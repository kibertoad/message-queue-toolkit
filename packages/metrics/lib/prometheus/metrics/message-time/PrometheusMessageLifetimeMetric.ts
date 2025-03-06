import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric'

export class PrometheusMessageLifetimeMetric<
  MessagePayload extends object,
> extends PrometheusMessageTimeMetric<MessagePayload> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayload>,
  ): number | null {
    if (!metadata.messageTimestamp) {
      return null
    }
    return metadata.messageProcessingEndTimestamp - metadata.messageTimestamp
  }
}
