import { AbstractAmqpConsumer } from './AbstractAmqpConsumer'
import type { AMQPTopicCreationConfig, AMQPTopicLocator } from './AbstractAmqpService'
import { ensureAmqpTopicSubscription } from './utils/amqpQueueUtils'

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
  protected override createMissingEntities(): Promise<void> {
    return ensureAmqpTopicSubscription(
      this.connection!,
      this.channel,
      this.creationConfig,
      this.locatorConfig,
    )
  }
}
