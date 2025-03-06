import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageProcessingMetric } from './PrometheusMessageProcessingMetric'

export class PrometheusMessageLifetimeMetric<
  MessagePayloadSchemas extends object,
> extends PrometheusMessageProcessingMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    if (!metadata.messageTimestamp) {
      return null
    }
    return metadata.messageProcessingEndTimestamp - metadata.messageTimestamp
  }
}
