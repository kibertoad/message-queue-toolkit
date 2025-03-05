import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { MessageProcessingPrometheusMetric } from '../MessageProcessingPrometheusMetric'

export class MessageLifetimeMetric<
  MessagePayloadSchemas extends object,
> extends MessageProcessingPrometheusMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    if (!metadata.messageTimestamp) {
      return null
    }
    return metadata.messageProcessingEndTimestamp - metadata.messageTimestamp
  }
}
