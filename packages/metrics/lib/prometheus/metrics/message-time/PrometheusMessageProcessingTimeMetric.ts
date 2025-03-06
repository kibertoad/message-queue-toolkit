import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageTimeMetric } from './PrometheusMessageTimeMetric'

export class PrometheusMessageProcessingTimeMetric<
  MessagePayloadSchemas extends object,
> extends PrometheusMessageTimeMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }
}
