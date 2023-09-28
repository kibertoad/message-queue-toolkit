import type { Either } from '@lokalise/node-core'
import type { QueueConsumer, MultiSchemaConsumerOptions } from '@message-queue-toolkit/core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'

import type { NewAMQPConsumerOptions } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

export abstract class AbstractAmqpConsumerMultiSchema<
    MessagePayloadType extends object,
    ExecutionContext,
  >
  extends AbstractAmqpBaseConsumer<MessagePayloadType>
  implements QueueConsumer
{
  messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext>

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: NewAMQPConsumerOptions &
      MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>,
  ) {
    super(dependencies, options)
    const messageSchemas = options.handlers.map((entry) => entry.schema)

    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
    this.handlerContainer = new HandlerContainer<MessagePayloadType, ExecutionContext>({
      messageTypeField: this.messageTypeField,
      messageHandlers: options.handlers,
    })
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  public override async processMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)

    // @ts-ignore
    return handler.handler(message, this)
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override async preHandlerBarrier(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<boolean> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    // @ts-ignore
    return handler.preHandlerBarrier ? await handler.preHandlerBarrier(message, this) : true
  }
}
