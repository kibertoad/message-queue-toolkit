import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  BarrierResult,
  DeadLetterQueueOptions,
  MessageSchemaContainer,
  ParseMessageResult,
  PreHandlingOutputs,
  Prehandler,
  QueueConsumer,
  QueueConsumerOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import { HandlerContainer, isMessageError, parseMessage } from '@message-queue-toolkit/core'
import type { Connection, Message } from 'amqplib'

import type {
  AMQPConsumerDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
} from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { readAmqpMessage } from './amqpMessageReader'

const ABORT_EARLY_EITHER: Either<'abort', never> = { error: 'abort' }
const DEFAULT_MAX_RETRY_DURATION = 4 * 24 * 60 * 60

export type AMQPConsumerOptions<
  MessagePayloadType extends object,
  ExecutionContext = undefined,
  PrehandlerOutput = undefined,
  CreationConfig extends AMQPQueueCreationConfig = AMQPQueueCreationConfig,
  LocatorConfig extends AMQPQueueLocator = AMQPQueueLocator,
> = QueueConsumerOptions<
  CreationConfig,
  LocatorConfig,
  NonNullable<unknown>, // DeadLetterQueueIntegrationOptions -> empty object for now
  MessagePayloadType,
  ExecutionContext,
  PrehandlerOutput
>

export abstract class AbstractAmqpConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    CreationConfig extends AMQPQueueCreationConfig = AMQPQueueCreationConfig,
    LocatorConfig extends AMQPQueueLocator = AMQPQueueLocator,
  >
  extends AbstractAmqpService<
    MessagePayloadType,
    AMQPConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput,
    CreationConfig,
    LocatorConfig
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly errorResolver: ErrorResolver
  private readonly executionContext: ExecutionContext
  private readonly deadLetterQueueOptions?: DeadLetterQueueOptions<
    AMQPQueueCreationConfig,
    AMQPQueueLocator,
    NonNullable<unknown>
  >
  private readonly maxRetryDuration: number

  public readonly _messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >
  protected readonly queueName: string

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: AMQPConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfig,
      LocatorConfig
    >,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver
    this.deadLetterQueueOptions = options.deadLetterQueue
    this.maxRetryDuration = options.maxRetryDuration ?? DEFAULT_MAX_RETRY_DURATION

    this.queueName = options.locatorConfig
      ? options.locatorConfig.queueName
      : options.creationConfig!.queueName

    this._messageSchemaContainer = this.resolveConsumerMessageSchemaContainer(options)

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

      const messageProcessingStartTimestamp = Date.now()
      const deserializedMessage = this.deserializeMessage(message)
      if (deserializedMessage.error === 'abort') {
        this.channel.nack(message, false, false)
        const messageId = this.tryToExtractId(message)
        this.handleMessageProcessed({
          message: null,
          processingResult: { status: 'error', errorReason: 'invalidMessage' },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
          messageId: messageId.result,
        })
        return
      }
      const { originalMessage, parsedMessage } = deserializedMessage.result

      // @ts-ignore
      const messageType = parsedMessage[this.messageTypeField]
      const transactionSpanId = `queue_${this.queueName}:${
        // @ts-ignore
        parsedMessage[this.messageTypeField]
      }`

      // @ts-ignore
      const uniqueTransactionKey = parsedMessage[this.messageIdField]
      this.transactionObservabilityManager?.start(transactionSpanId, uniqueTransactionKey)
      if (this.logMessages) {
        const resolvedLogMessage = this.resolveMessageLog(parsedMessage, messageType)
        this.logMessage(resolvedLogMessage)
      }
      this.internalProcessMessage(parsedMessage, messageType)
        .then((result) => {
          if (result.result === 'success') {
            this.channel.ack(message)
            this.handleMessageProcessed({
              message: parsedMessage,
              processingResult: { status: 'consumed' },
              messageProcessingStartTimestamp,
              queueName: this.queueName,
            })
            return
          }

          // requeue the message if maxRetryDuration is not exceeded, else ack it to avoid infinite loop
          if (this.shouldBeRetried(originalMessage, this.maxRetryDuration)) {
            // TODO: Add retry delay + republish message updating internal properties
            this.channel.nack(message as Message, false, true)
            this.handleMessageProcessed({
              message: parsedMessage,
              processingResult: { status: 'retryLater' },
              messageProcessingStartTimestamp,
              queueName: this.queueName,
            })
          } else {
            // ToDo move message to DLQ once it is implemented
            this.channel.ack(message)
            this.handleMessageProcessed({
              message: parsedMessage,
              processingResult: { status: 'error', errorReason: 'retryLaterExceeded' },
              messageProcessingStartTimestamp,
              queueName: this.queueName,
            })
          }
        })
        .catch((err) => {
          // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
          // If we fail due to unknown reason, let's retry
          this.channel.nack(message, false, true)
          this.handleMessageProcessed({
            message: parsedMessage,
            processingResult: { status: 'retryLater' },
            messageProcessingStartTimestamp,
            queueName: this.queueName,
          })
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

  protected override processMessage(
    message: MessagePayloadType,
    messageType: string,
    // biome-ignore lint/suspicious/noExplicitAny: We neither know, nor care about the type here
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)
    return handler.handler(message, this.executionContext, preHandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return this.processPrehandlersInternal(handlerConfig.preHandlers, message)
  }

  protected override preHandlerBarrier<BarrierOutput>(
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
    return this._messageSchemaContainer.resolveSchema(message)
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

  private deserializeMessage(
    message: Message,
  ): Either<'abort', ParseMessageResult<MessagePayloadType>> {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }

    // Empty content for whatever reason
    if (!resolveMessageResult.result || !resolveMessageResult.result.body) {
      return ABORT_EARLY_EITHER
    }

    const resolveSchemaResult = this.resolveSchema(
      resolveMessageResult.result.body as MessagePayloadType,
    )
    if (resolveSchemaResult.error) {
      this.handleError(resolveSchemaResult.error)
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = parseMessage(
      resolveMessageResult.result.body,
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
    const resolvedMessage = resolveMessageResult.result

    // Empty content for whatever reason
    if (!resolvedMessage || !resolvedMessage.body) return ABORT_EARLY_EITHER

    // @ts-ignore
    if (this.messageIdField in resolvedMessage.body) {
      return {
        // @ts-ignore
        result: resolvedMessage.body[this.messageIdField],
      }
    }

    return ABORT_EARLY_EITHER
  }

  protected resolveMessage(message: Message) {
    return readAmqpMessage(message, this.errorResolver)
  }
}
