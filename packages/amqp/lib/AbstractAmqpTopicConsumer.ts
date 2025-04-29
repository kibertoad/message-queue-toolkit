import { AbstractAmqpConsumer } from './AbstractAmqpConsumer.ts'
import type { AMQPTopicCreationConfig, AMQPTopicLocator } from './AbstractAmqpService.ts'
import { deleteAmqpQueue, ensureAmqpTopicSubscription } from './utils/amqpQueueUtils.ts'

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
      // biome-ignore lint/style/noNonNullAssertion: <explanation>
      this.connection!,
      this.channel,
      this.creationConfig,
      this.locatorConfig,
    )
  }
}
