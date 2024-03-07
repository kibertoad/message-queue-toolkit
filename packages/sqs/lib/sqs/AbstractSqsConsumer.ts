import type { Either, ErrorResolver } from '@lokalise/node-core'
import { isMessageError, parseMessage } from '@message-queue-toolkit/core'
import type {
  QueueConsumer as QueueConsumer,
  NewQueueOptions,
  ExistingQueueOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import { Consumer } from 'sqs-consumer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import type { SQSMessage } from '../types/MessageTypes'
import { readSqsMessage } from '../utils/sqsMessageReader'

import type {
  SQSConsumerDependencies,
  SQSQueueConfig,
  SQSQueueLocatorType,
} from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type ExtraSQSCreationParams = {
  topicArnsWithPublishPermissionsPrefix?: string
  updateAttributesIfExists?: boolean
}

export type SQSCreationConfig = {
  queue: SQSQueueConfig
  updateAttributesIfExists?: boolean
} & ExtraSQSCreationParams

export type NewSQSConsumerOptions<CreationConfigType extends SQSCreationConfig> =
  NewQueueOptions<CreationConfigType> & {
    consumerOverrides?: Partial<ConsumerOptions>
  }

export type ExistingSQSConsumerOptions<
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingQueueOptions<QueueLocatorType> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export abstract class AbstractSqsConsumer<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
      | NewSQSConsumerOptions<CreationConfigType>
      | ExistingSQSConsumerOptions<QueueLocatorType> =
      | NewSQSConsumerOptions<CreationConfigType>
      | ExistingSQSConsumerOptions<QueueLocatorType>,
    ExecutionContext = unknown,
    PrehandlerOutput = unknown,
    BarrierOutput = unknown,
  >
  extends AbstractSqsService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    SQSConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput,
    BarrierOutput
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver
  // @ts-ignore
  protected consumer: Consumer
  private readonly consumerOptionsOverride: Partial<ConsumerOptions>

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    this.consumerOptionsOverride = options.consumerOverrides ?? {}
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

  private tryToExtractId(message: SQSMessage): Either<'abort', string> {
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

  private async failProcessing(_message: SQSMessage) {
    // Not implemented yet - needs dead letter queue
  }

  async start() {
    await this.init()

    if (this.consumer) {
      this.consumer.stop()
    }
    this.consumer = Consumer.create({
      queueUrl: this.queueUrl,
      handleMessage: async (message: SQSMessage) => {
        if (message === null) {
          return
        }

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
            // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
            // If we fail due to unknown reason, let's retry
            this.handleError(err)
            return {
              // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
              error: err,
            }
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

  protected override resolveMessage(message: SQSMessage) {
    return readSqsMessage(message, this.errorResolver)
  }

  public override async close(abort?: boolean): Promise<void> {
    await super.close()
    this.consumer?.stop({
      abort: abort ?? false,
    })
  }
}
