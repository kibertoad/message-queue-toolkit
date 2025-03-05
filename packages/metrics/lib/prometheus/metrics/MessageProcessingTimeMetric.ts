import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { MessageProcessingPrometheusMetric } from '../MessageProcessingPrometheusMetric'

export class MessageProcessingTimeMetric<
  MessagePayloadSchemas extends object,
> extends MessageProcessingPrometheusMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }
}
