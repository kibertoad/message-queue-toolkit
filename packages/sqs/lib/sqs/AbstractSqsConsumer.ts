import {
  ChangeMessageVisibilityCommand,
  SendMessageCommand,
  type SendMessageCommandInput,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs'
import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer as QueueConsumer,
  TransactionObservabilityManager,
  QueueConsumerOptions,
  PrehandlingOutputs,
  Prehandler,
  BarrierResult,
  QueueConsumerDependencies,
  DeletionConfig,
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

import type { SQSCreationConfig, SQSDependencies, SQSQueueLocatorType } from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

// TODO: should we include DLQ types on core?
type DeadLetterQueueOptions<
  CreationConfigType extends SQSCreationConfig,
  QueueLocatorType extends SQSQueueLocatorType,
> = {
  redrivePolicy: { maxReceiveCount: number }
  deletionConfig?: DeletionConfig // TODO: should deletion config be never?
} & (
  | {
      creationConfig: CreationConfigType
      locatorConfig?: never
    }
  | {
      creationConfig?: never
      locatorConfig: QueueLocatorType
    }
)

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
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput
> & {
  consumerOverrides?: Partial<ConsumerOptions>
  deadLetterQueue?: DeadLetterQueueOptions<CreationConfigType, QueueLocatorType>
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
    CreationConfigType,
    QueueLocatorType
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

  private async initDeadLetterQueue() {
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

    if (this.consumer) {
      this.consumer.stop()
    }
    this.consumer = Consumer.create({
      queueUrl: this.queueUrl,
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
        const messageType = deserializedMessage.result[this.messageTypeField]
        const transactionSpanId = `queue_${this.queueName}:${messageType}`

        this.transactionObservabilityManager?.start(transactionSpanId)
        if (this.logMessages) {
          const resolvedLogMessage = this.resolveMessageLog(deserializedMessage.result, messageType)
          this.logMessage(resolvedLogMessage)
        }
        const result: Either<'retryLater' | Error, 'success'> = await this.internalProcessMessage(
          deserializedMessage.result,
          messageType,
        )
          .catch((err) => {
            this.handleError(err)
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            return { error: err }
          })
          .finally(() => {
            this.transactionObservabilityManager?.stop(transactionSpanId)
          })

        // success
        if (result.result) {
          this.handleMessageProcessed(deserializedMessage.result, 'consumed')
          return message
        }

        // failure
        this.handleMessageProcessed(
          deserializedMessage.result,
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
      sqs: this.sqsClient,
      ...this.consumerOptionsOverride,
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
    const prehandlerOutput = await this.processPrehandlers(message, messageType)
    const barrierResult = await this.preHandlerBarrier(message, messageType, prehandlerOutput)

    if (barrierResult.isPassing) {
      return this.processMessage(message, messageType, {
        prehandlerOutput,
        barrierOutput: barrierResult.output,
      })
    }

    return { error: 'retryLater' }
  }

  protected override async processMessage(
    message: MessagePayloadType,
    messageType: string,
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return handler.handler(message, this.executionContext, prehandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return this.processPrehandlersInternal(handlerConfig.prehandlers, message)
  }

  protected override async preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
    prehandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput, BarrierOutput>(
      messageType,
    )

    return this.preHandlerBarrierInternal(
      handler.preHandlerBarrier,
      message,
      this.executionContext,
      prehandlerOutput,
    )
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
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

  private deserializeMessage(message: SQSMessage): Either<'abort', MessagePayloadType> {
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
}
