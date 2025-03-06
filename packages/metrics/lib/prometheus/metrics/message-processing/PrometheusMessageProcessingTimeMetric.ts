import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import { PrometheusMessageProcessingMetric } from './PrometheusMessageProcessingMetric'

export class PrometheusMessageProcessingTimeMetric<
  MessagePayloadSchemas extends object,
> extends PrometheusMessageProcessingMetric<MessagePayloadSchemas> {
  protected calculateObservedValue(
    metadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): number | null {
    return metadata.messageProcessingEndTimestamp - metadata.messageProcessingStartTimestamp
  }
}
