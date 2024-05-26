import { AbstractAmqpConsumer } from './AbstractAmqpConsumer'
import type { AMQPTopicCreationConfig, AMQPTopicLocator } from './AbstractAmqpService'
import { deleteAmqpQueue, ensureAmqpTopicSubscription } from './utils/amqpQueueUtils'

export class AbstractAmqpTopicConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractAmqpConsumer<
  MessagePayloadType,
  ExecutionContext,
  PrehandlerOutput,
  AMQPTopicCreationConfig,
  AMQPTopicLocator
> {
  protected override async createMissingEntities(): Promise<void> {
    if (this.deletionConfig && this.creationConfig) {
      await deleteAmqpQueue(this.channel, this.deletionConfig, this.creationConfig)
    }

    await ensureAmqpTopicSubscription(
      this.connection!,
      this.channel,
      this.creationConfig,
      this.locatorConfig,
    )
  }
}
