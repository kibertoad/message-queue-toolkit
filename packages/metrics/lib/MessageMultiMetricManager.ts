import type { MessageMetricsManager, ProcessedMessageMetadata } from '@message-queue-toolkit/core'

export class MessageMultiMetricManager<MessagePayloadSchemas extends object>
  implements MessageMetricsManager<MessagePayloadSchemas>
{
  private readonly metrics: MessageMetricsManager<MessagePayloadSchemas>[]

  constructor(metrics: MessageMetricsManager<MessagePayloadSchemas>[]) {
    this.metrics = metrics
  }

  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayloadSchemas>): void {
    for (const metric of this.metrics) metric.registerProcessedMessage(metadata)
  }
}
