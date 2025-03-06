import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric'

export class PrometheusMessageLifetimeMetric<
  MessagePayloadSchemas extends object,
> extends PrometheusMessageTimeMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    if (!metadata.messageTimestamp) {
      return null
    }
    return metadata.messageProcessingEndTimestamp - metadata.messageTimestamp
  }
}
