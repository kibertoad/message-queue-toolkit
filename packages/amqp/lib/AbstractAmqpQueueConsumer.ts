import { AbstractAmqpConsumer } from './AbstractAmqpConsumer'
import { ensureAmqpQueue } from './utils/amqpQueueUtils'

export class AbstractAmqpQueueConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractAmqpConsumer<MessagePayloadType, ExecutionContext, PrehandlerOutput> {
  protected override createMissingEntities(): Promise<void> {
    return ensureAmqpQueue(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
