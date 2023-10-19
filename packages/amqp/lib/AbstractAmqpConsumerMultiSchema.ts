import type { Either } from '@lokalise/node-core'
import type {
  QueueConsumer,
  MultiSchemaConsumerOptions,
  BarrierResult,
} from '@message-queue-toolkit/core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'

import type { NewAMQPConsumerOptions } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

export abstract class AbstractAmqpConsumerMultiSchema<
    MessagePayloadType extends object,
    ExecutionContext,
  >
  extends AbstractAmqpBaseConsumer<MessagePayloadType, unknown>
  implements QueueConsumer
{
  messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext>
  protected readonly executionContext: ExecutionContext

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: NewAMQPConsumerOptions &
      MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>,
    executionContext: ExecutionContext,
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
    this.executionContext = executionContext
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  public override async processMessage(
    message: MessagePayloadType,
    messageType: string,
    barrierOutput: unknown,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.handler(message, this.executionContext, barrierOutput)
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override async preHandlerBarrier(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<BarrierResult<unknown>> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.preHandlerBarrier
      ? await handler.preHandlerBarrier(message, this.executionContext)
      : {
          isPassing: true,
          output: undefined,
        }
  }
}
