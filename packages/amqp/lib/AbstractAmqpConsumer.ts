import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  BarrierResult,
  DeadLetterQueueOptions,
  Prehandler,
  PreHandlingOutputs,
  QueueConsumer,
  QueueConsumerOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import {
  isMessageError,
  parseMessage,
  HandlerContainer,
  MessageSchemaContainer,
} from '@message-queue-toolkit/core'
import type { Connection, Message } from 'amqplib'

import type {
  AMQPConsumerDependencies,
  AMQPLocator,
  AMQPCreationConfig,
} from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { readAmqpMessage } from './amqpMessageReader'

const ABORT_EARLY_EITHER: Either<'abort', never> = { error: 'abort' }

export type AMQPConsumerOptions<
  MessagePayloadType extends object,
  ExecutionContext = undefined,
  PrehandlerOutput = undefined,
> = QueueConsumerOptions<
  AMQPCreationConfig,
  AMQPLocator,
  NonNullable<unknown>, // DeadLetterQueueIntegrationOptions -> empty object for now
  MessagePayloadType,
  ExecutionContext,
  PrehandlerOutput
>

export abstract class AbstractAmqpConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
  >
  extends AbstractAmqpService<
    MessagePayloadType,
    AMQPConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly errorResolver: ErrorResolver
  private readonly executionContext: ExecutionContext
  private readonly deadLetterQueueOptions?: DeadLetterQueueOptions<
    AMQPCreationConfig,
    AMQPLocator,
    NonNullable<unknown>
  >

  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: AMQPConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver
    this.deadLetterQueueOptions = options.deadLetterQueue

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

  async start() {
    await this.init()
    if (!this.channel) throw new Error('Channel is not set')
    await this.consume()
  }

  async init(): Promise<void> {
    if (this.deadLetterQueueOptions) {
      // TODO: https://www.cloudamqp.com/blog/when-and-how-to-use-the-rabbitmq-dead-letter-exchange.html
      throw new Error('deadLetterQueue parameter is not currently supported by the Amqp adapter')
    }

    await super.init()
  }

  async receiveNewConnection(connection: Connection): Promise<void> {
    await super.receiveNewConnection(connection)
    await this.consume()
  }

  private async consume() {
    await this.channel.consume(this.queueName, (message) => {
      if (message === null) {
        return
      }

      const deserializedMessage = this.deserializeMessage(message)
      if (deserializedMessage.error === 'abort') {
        this.channel.nack(message, false, false)
        const messageId = this.tryToExtractId(message)
        this.handleMessageProcessed(null, 'invalid_message', messageId.result)
        return
      }
      // @ts-ignore
      const messageType = deserializedMessage.result[this.messageTypeField]
      const transactionSpanId = `queue_${this.queueName}:${
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        deserializedMessage.result[this.messageTypeField]
      }`

      // @ts-ignore
      const uniqueTransactionKey = deserializedMessage.result[this.messageIdField]
      this.transactionObservabilityManager?.start(transactionSpanId, uniqueTransactionKey)
      if (this.logMessages) {
        const resolvedLogMessage = this.resolveMessageLog(deserializedMessage.result, messageType)
        this.logMessage(resolvedLogMessage)
      }
      this.internalProcessMessage(deserializedMessage.result, messageType)
        .then((result) => {
          if (result.error === 'retryLater') {
            this.channel.nack(message, false, true)
            this.handleMessageProcessed(deserializedMessage.result, 'retryLater')
          }
          if (result.result === 'success') {
            this.channel.ack(message)
            this.handleMessageProcessed(deserializedMessage.result, 'consumed')
          }
        })
        .catch((err) => {
          // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
          // If we fail due to unknown reason, let's retry
          this.channel.nack(message, false, true)
          this.handleMessageProcessed(deserializedMessage.result, 'retryLater')
          this.handleError(err)
        })
        .finally(() => {
          this.transactionObservabilityManager?.stop(uniqueTransactionKey)
        })
    })
  }

  private async internalProcessMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const preHandlerOutput = await this.processPrehandlers(message, messageType)
    const barrierResult = await this.preHandlerBarrier(message, messageType, preHandlerOutput)

    if (barrierResult.isPassing) {
      return this.processMessage(message, messageType, {
        preHandlerOutput,
        barrierOutput: barrierResult.output,
      })
    }
    return { error: 'retryLater' }
  }

  protected override async processMessage(
    message: MessagePayloadType,
    messageType: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)
    return handler.handler(message, this.executionContext, preHandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return this.processPrehandlersInternal(handlerConfig.preHandlers, message)
  }

  protected override async preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput, BarrierOutput>(
      messageType,
    )

    return this.preHandlerBarrierInternal(
      handler.preHandlerBarrier,
      message,
      this.executionContext,
      preHandlerOutput,
    )
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  // eslint-disable-next-line max-params
  protected override resolveNextFunction(
    preHandlers: Prehandler<MessagePayloadType, ExecutionContext, unknown>[],
    message: MessagePayloadType,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return this.resolveNextPreHandlerFunctionInternal(
      preHandlers,
      this.executionContext,
      message,
      index,
      preHandlerOutput,
      resolve,
      reject,
    )
  }

  private deserializeMessage(message: Message): Either<'abort', MessagePayloadType> {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }

    // Empty content for whatever reason
    if (!resolveMessageResult.result) {
      return ABORT_EARLY_EITHER
    }

    const resolveSchemaResult = this.resolveSchema(
      resolveMessageResult.result as MessagePayloadType,
    )
    if (resolveSchemaResult.error) {
      this.handleError(resolveSchemaResult.error)
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = parseMessage(
      resolveMessageResult.result,
      resolveSchemaResult.result,
      this.errorResolver,
    )
    if (isMessageError(deserializationResult.error)) {
      this.handleError(deserializationResult.error)
      return ABORT_EARLY_EITHER
    }

    // Empty content for whatever reason
    if (!deserializationResult.result) {
      return ABORT_EARLY_EITHER
    }

    return {
      result: deserializationResult.result,
    }
  }

  private tryToExtractId(message: Message): Either<'abort', string> {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }
    // Empty content for whatever reason
    if (!resolveMessageResult.result) {
      return ABORT_EARLY_EITHER
    }

    // @ts-ignore
    if (this.messageIdField in resolveMessageResult.result) {
      return {
        // @ts-ignore
        result: resolveMessageResult.result[this.messageIdField],
      }
    }

    return ABORT_EARLY_EITHER
  }

  protected resolveMessage(message: Message) {
    return readAmqpMessage(message, this.errorResolver)
  }
}
