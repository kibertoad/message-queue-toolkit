import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer as QueueConsumer,
  NewQueueOptions,
  ExistingQueueOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import { isMessageError, parseMessage } from '@message-queue-toolkit/core'
import { Consumer } from 'sqs-consumer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import type { SQSMessage } from '../types/MessageTypes'
import { readSqsMessage } from '../utils/sqsMessageReader'

import type {
  SQSConsumerDependencies,
  SQSQueueAWSConfig,
  SQSQueueLocatorType,
} from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type ExtraSQSCreationParams = {
  topicArnsWithPublishPermissionsPrefix?: string
}

export type SQSCreationConfig = {
  queue: SQSQueueAWSConfig
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
  >
  extends AbstractSqsService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    SQSConsumerDependencies
  >
  implements QueueConsumer<MessagePayloadType>
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
    return (await this.preHandlerBarrier(message, messageType))
      ? this.processMessage(message, messageType)
      : { error: 'retryLater' }
  }

  /**
   * Override to implement barrier pattern
   */
  public preHandlerBarrier(_message: MessagePayloadType, _messageType: string): Promise<boolean> {
    return Promise.resolve(true)
  }

  abstract processMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>>

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

        return result.result ? message : Promise.reject(result)
      },
      sqs: this.sqsClient,
      ...this.consumerOptionsOverride,
    })

    this.consumer.on('error', (err) => {
      this.handleError(err)
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
