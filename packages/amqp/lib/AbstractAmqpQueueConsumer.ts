import { AbstractAmqpConsumer } from './AbstractAmqpConsumer.ts'
import { deleteAmqpQueue, ensureAmqpQueue } from './utils/amqpQueueUtils.ts'

export class AbstractAmqpQueueConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractAmqpConsumer<MessagePayloadType, ExecutionContext, PrehandlerOutput> {
  protected override async createMissingEntities(): Promise<void> {
    if (this.deletionConfig && this.creationConfig) {
      await deleteAmqpQueue(this.channel, this.deletionConfig, this.creationConfig)
    }

    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    await ensureAmqpQueue(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
