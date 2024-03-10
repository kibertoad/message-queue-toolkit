import type { Either } from '@lokalise/node-core'
import type {
  QueueConsumer,
  MultiSchemaConsumerOptions,
  BarrierResult,
  Prehandler,
  PrehandlingOutputs,
} from '@message-queue-toolkit/core'
import { HandlerContainer, MessageSchemaContainer } from '@message-queue-toolkit/core'

import type { NewAMQPConsumerOptions } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

export abstract class AbstractAmqpConsumerMultiSchema<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    BarrierOutput = undefined,
  >
  extends AbstractAmqpBaseConsumer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  implements QueueConsumer
{
  messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  handlerContainer: HandlerContainer<MessagePayloadType, ExecutionContext, PrehandlerOutput>
  protected readonly executionContext: ExecutionContext

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: NewAMQPConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput> &
      MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)
    const messageSchemas = options.handlers.map((entry) => entry.schema)

    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
    this.handlerContainer = new HandlerContainer<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput
    >({
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
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, unknown>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.handler(message, this.executionContext, prehandlingOutputs)
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler(messageType)

    return this.processPrehandlersInternal(handlerConfig.prehandlers, message)
  }

  // eslint-disable-next-line max-params
  protected override resolveNextFunction(
    prehandlers: Prehandler<MessagePayloadType, ExecutionContext, unknown>[],
    message: MessagePayloadType,
    index: number,
    prehandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return this.resolveNextPreHandlerFunctionInternal(
      prehandlers,
      this.executionContext,
      message,
      index,
      prehandlerOutput,
      resolve,
      reject,
    )
  }

  protected override async preHandlerBarrier(
    message: MessagePayloadType,
    messageType: string,
    prehandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<BarrierOutput, PrehandlerOutput>(
      messageType,
    )
    // @ts-ignore
    return handler.preHandlerBarrier
      ? await handler.preHandlerBarrier(message, this.executionContext, prehandlerOutput)
      : {
          isPassing: true,
          output: undefined,
        }
  }
}
