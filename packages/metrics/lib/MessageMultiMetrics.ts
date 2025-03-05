import type { MessageMetricsManager, ProcessedMessageMetadata } from '@message-queue-toolkit/core'

export class MessageMultiMetrics<MessagePayloadSchemas extends object>
  implements MessageMetricsManager<MessagePayloadSchemas>
{
  constructor(private readonly metrics: MessageMetricsManager<MessagePayloadSchemas>[]) {}

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayloadSchemas>): void {
    for (const metric of this.metrics) metric.registerProcessedMessage(metadata)
  }
}
