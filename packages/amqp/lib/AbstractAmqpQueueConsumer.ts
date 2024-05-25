import { AbstractAmqpConsumer } from './AbstractAmqpConsumer'
import { deleteAmqpQueue, ensureAmqpQueue } from './utils/amqpQueueUtils'

export class AbstractAmqpQueueConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractAmqpConsumer<MessagePayloadType, ExecutionContext, PrehandlerOutput> {
  protected override async createMissingEntities(): Promise<void> {
    if (this.deletionConfig && this.creationConfig) {
      await deleteAmqpQueue(this.channel, this.deletionConfig, this.creationConfig)
    }

    await ensureAmqpQueue(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
