import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer as QueueConsumer,
  NewQueueOptions,
  ExistingQueueOptions,
  TransactionObservabilityManager,
  Deserializer,
} from '@message-queue-toolkit/core'
import { isMessageError } from '@message-queue-toolkit/core'
import { Consumer } from 'sqs-consumer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import type { SQSMessage } from '../types/MessageTypes'

import type {
  SQSConsumerDependencies,
  SQSQueueAWSConfig,
  SQSQueueLocatorType,
} from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'
import { deserializeSQSMessage } from './sqsMessageDeserializer'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type SQSCreationConfig = {
  queue: SQSQueueAWSConfig
}

export type NewSQSConsumerOptions<
  MessagePayloadType extends object,
  CreationConfigType extends SQSCreationConfig,
> = NewQueueOptions<MessagePayloadType, CreationConfigType> & {
  consumerOverrides?: Partial<ConsumerOptions>
  deserializer?: Deserializer<MessagePayloadType, SQSMessage>
}

export type ExistingSQSConsumerOptions<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingQueueOptions<MessagePayloadType, QueueLocatorType> & {
  consumerOverrides?: Partial<ConsumerOptions>
  deserializer?: Deserializer<MessagePayloadType, SQSMessage>
}

export abstract class AbstractSqsConsumer<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
      | NewSQSConsumerOptions<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptions<MessagePayloadType, QueueLocatorType> =
      | NewSQSConsumerOptions<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptions<MessagePayloadType, QueueLocatorType>,
  >
  extends AbstractSqsService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    SQSConsumerDependencies
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver
  // @ts-ignore
  protected consumer: Consumer
  private readonly consumerOptionsOverride: Partial<ConsumerOptions>
  private readonly deserializer: Deserializer<MessagePayloadType, SQSMessage>

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    this.consumerOptionsOverride = options.consumerOverrides ?? {}
    this.deserializer = options.deserializer ?? deserializeSQSMessage
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: SQSMessage): Either<'abort', MessagePayloadType> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = this.deserializer(message, this.messageSchema, this.errorResolver)

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
        const transactionSpanId = `queue_${this.queueName}:${
          // @ts-ignore
          // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
          deserializedMessage.result[this.messageTypeField]
        }`

        this.transactionObservabilityManager?.start(transactionSpanId)
        const result: Either<'retryLater' | Error, 'success'> = await this.processMessage(
          deserializedMessage.result,
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

        if (result.result) {
          return message
        } else {
          return Promise.reject(result)
        }
      },
      sqs: this.sqsClient,
      ...this.consumerOptionsOverride,
    })

    this.consumer.on('error', (err) => {
      this.handleError(err)
    })

    this.consumer.start()
  }

  public override async close(abort?: boolean): Promise<void> {
    await super.close()
    this.consumer?.stop({
      abort: abort ?? false,
    })
  }
}
