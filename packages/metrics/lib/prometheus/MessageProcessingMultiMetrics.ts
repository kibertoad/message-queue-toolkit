import type { MessageMetricsManager, ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import type { MessageProcessingPrometheusMetric } from './MessageProcessingPrometheusMetric'

export class MessageProcessingMultiMetrics<MessagePayloadSchemas extends object>
  implements MessageMetricsManager<MessagePayloadSchemas>
{
  constructor(
    private readonly metrics: MessageProcessingPrometheusMetric<MessagePayloadSchemas>[],
  ) {}

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayloadSchemas>): void {
    for (const metric of this.metrics) {
      metric.registerProcessedMessage(metadata)
    }
  }
}
