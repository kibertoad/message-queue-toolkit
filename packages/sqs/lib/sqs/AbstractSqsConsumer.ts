import { SendMessageCommand, SetQueueAttributesCommand } from '@aws-sdk/client-sqs'
import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer,
  TransactionObservabilityManager,
  QueueConsumerOptions,
  PreHandlingOutputs,
  Prehandler,
  BarrierResult,
  QueueConsumerDependencies,
  DeadLetterQueueOptions,
  ParseMessageResult,
} from '@message-queue-toolkit/core'
import {
  isMessageError,
  parseMessage,
  HandlerContainer,
  MessageSchemaContainer,
} from '@message-queue-toolkit/core'
import { Consumer } from 'sqs-consumer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import type { SQSMessage } from '../types/MessageTypes'
import { deleteSqs, initSqs } from '../utils/sqsInitter'
import { readSqsMessage } from '../utils/sqsMessageReader'
import { getQueueAttributes } from '../utils/sqsUtils'

import type { SQSCreationConfig, SQSDependencies, SQSQueueLocatorType } from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

type SQSDeadLetterQueueOptions = {
  redrivePolicy: {
    maxReceiveCount: number
  }
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSConsumerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = QueueConsumerOptions<
  CreationConfigType,
  QueueLocatorType,
  SQSDeadLetterQueueOptions,
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  SQSCreationConfig,
  SQSQueueLocatorType
> & {
  /**
   * Omitting properties which will be set internally ins this class
   * `visibilityTimeout` is also omitted to avoid conflicts with queue config
   */
  consumerOverrides?: Omit<
    ConsumerOptions,
    'sqs' | 'queueUrl' | 'handler' | 'handleMessageBatch' | 'visibilityTimeout'
  >
}

export abstract class AbstractSqsConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    ConsumerOptionsType extends SQSConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > = SQSConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    >,
  >
  extends AbstractSqsService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    SQSConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput
  >
  implements QueueConsumer
{
  private consumer?: Consumer
  private readonly transactionObservabilityManager?: TransactionObservabilityManager

  private readonly consumerOptionsOverride: Partial<ConsumerOptions>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >

  protected readonly errorResolver: ErrorResolver
  protected readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  protected readonly executionContext: ExecutionContext

  private readonly deadLetterQueueOptions?: DeadLetterQueueOptions<
    SQSCreationConfig,
    SQSQueueLocatorType,
    SQSDeadLetterQueueOptions
  >

  protected deadLetterQueueUrl?: string

  protected constructor(
    dependencies: SQSConsumerDependencies,
    options: ConsumerOptionsType,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    this.consumerOptionsOverride = options.consumerOverrides ?? {}
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

  override async init(): Promise<void> {
    await super.init()
    await this.initDeadLetterQueue()
  }

  protected async initDeadLetterQueue() {
    if (!this.deadLetterQueueOptions) return

    const { deletionConfig, locatorConfig, creationConfig, redrivePolicy } =
      this.deadLetterQueueOptions

    if (deletionConfig && creationConfig) {
      await deleteSqs(this.sqsClient, deletionConfig, creationConfig)
    }

    const result = await initSqs(this.sqsClient, locatorConfig, creationConfig)
    await this.sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        Attributes: {
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: result.queueArn,
            maxReceiveCount: redrivePolicy.maxReceiveCount,
          }),
        },
      }),
    )

    this.deadLetterQueueUrl = result.queueUrl
  }

  public async start() {
    await this.init()
    if (this.consumer) this.consumer.stop()

    const visibilityTimeout = await this.getQueueVisibilityTimeout()

    this.consumer = Consumer.create({
      sqs: this.sqsClient,
      queueUrl: this.queueUrl,
      visibilityTimeout,
      ...this.consumerOptionsOverride,
      handleMessage: async (message: SQSMessage) => {
        if (message === null) return

        const deserializedMessage = this.deserializeMessage(message)
        if (deserializedMessage.error === 'abort') {
          await this.failProcessing(message)

          const messageId = this.tryToExtractId(message)
          this.handleMessageProcessed(null, 'invalid_message', messageId.result)
          return
        }
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        const messageType = deserializedMessage.result.parsedMessage[this.messageTypeField]
        const transactionSpanId = `queue_${this.queueName}:${messageType}`

        // @ts-ignore
        const uniqueTransactionKey = deserializedMessage.result[this.messageIdField]
        this.transactionObservabilityManager?.start(transactionSpanId, uniqueTransactionKey)
        if (this.logMessages) {
          const resolvedLogMessage = this.resolveMessageLog(
            deserializedMessage.result.parsedMessage,
            messageType,
          )
          this.logMessage(resolvedLogMessage)
        }
        const result: Either<'retryLater' | Error, 'success'> = await this.internalProcessMessage(
          deserializedMessage.result.parsedMessage,
          messageType,
        )
          .catch((err) => {
            this.handleError(err)
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            return { error: err }
          })
          .finally(() => {
            this.transactionObservabilityManager?.stop(uniqueTransactionKey)
          })

        // success
        if (result.result) {
          this.handleMessageProcessed(deserializedMessage.result.originalMessage, 'consumed')
          return message
        }

        // failure
        this.handleMessageProcessed(
          deserializedMessage.result.originalMessage,
          result.error === 'retryLater' ? 'retryLater' : 'error',
        )

        // in case of retryLater, if DLQ is set, we will send the message back to the queue
        if (result.error === 'retryLater' && this.deadLetterQueueUrl) {
          await this.sqsClient.send(
            new SendMessageCommand({
              QueueUrl: this.queueUrl,
              MessageBody: message.Body,
            }),
          )
          return message
        }

        return Promise.reject(result.error)
      },
    })

    this.consumer.on('error', (err) => {
      this.handleError(err, {
        queueName: this.queueName,
      })
    })

    this.consumer.start()
  }

  public override async close(abort?: boolean): Promise<void> {
    await super.close()
    this.consumer?.stop({
      abort: abort ?? false,
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

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override resolveMessage(message: SQSMessage) {
    return readSqsMessage(message, this.errorResolver)
  }

  private tryToExtractId(message: SQSMessage): Either<'abort', string> {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }
    // Empty content for whatever reason
    if (!resolveMessageResult.result) return ABORT_EARLY_EITHER

    // @ts-ignore
    if (this.messageIdField in resolveMessageResult.result) {
      return {
        // @ts-ignore
        result: resolveMessageResult.result[this.messageIdField],
      }
    }

    return ABORT_EARLY_EITHER
  }

  private deserializeMessage(
    message: SQSMessage,
  ): Either<'abort', ParseMessageResult<MessagePayloadType>> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

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

  private async failProcessing(message: SQSMessage) {
    if (!this.deadLetterQueueUrl) return

    const command = new SendMessageCommand({
      QueueUrl: this.deadLetterQueueUrl,
      MessageBody: message.Body,
    })
    await this.sqsClient.send(command)
  }

  private async getQueueVisibilityTimeout(): Promise<number | undefined> {
    let visibilityTimeoutString
    if (this.creationConfig) {
      visibilityTimeoutString = this.creationConfig.queue.Attributes?.VisibilityTimeout
    } else {
      // if user is using locatorConfig, we should look into queue config
      const queueAttributes = await getQueueAttributes(
        this.sqsClient,
        { queueUrl: this.queueUrl },
        ['VisibilityTimeout'],
      )
      visibilityTimeoutString = queueAttributes.result?.attributes?.VisibilityTimeout
    }

    // parseInt is safe because if the value is not a number process should have failed on init
    return visibilityTimeoutString ? parseInt(visibilityTimeoutString) : undefined
  }
}
