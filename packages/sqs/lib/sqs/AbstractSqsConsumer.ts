import type { Either, ErrorResolver, InternalError } from '@lokalise/node-core'
import type {
  QueueConsumer as QueueConsumer,
  QueueOptions,
  TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import { Consumer } from 'sqs-consumer'
import type { ConsumerOptions } from 'sqs-consumer/src/types'

import { SqsMessageInvalidFormat, SqsValidationError } from '../errors/sqsErrors'

import type { SQSConsumerDependencies, SQSQueueAWSConfig } from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'
import { deserializeSQSMessage } from './sqsMessageDeserializer'
import { ZodType } from 'zod'

export type SQSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
}

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type SQSConsumerOptions<MessagePayloadType extends object> = QueueOptions<
  MessagePayloadType,
  SQSQueueAWSConfig
> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export abstract class AbstractSqsConsumer<MessagePayloadType extends object>
  extends AbstractSqsService<
    MessagePayloadType,
    SQSConsumerOptions<MessagePayloadType>,
    SQSConsumerDependencies
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver
  // @ts-ignore
  protected consumer: Consumer
  private readonly consumerOptionsOverride: Partial<ConsumerOptions>
  private readonly deserializer: <MessagePayloadType>(
    message: any,
    type: ZodType<MessagePayloadType>,
    errorProcessor: ErrorResolver,
  ) => Either<InternalError, MessagePayloadType>

  constructor(
    dependencies: SQSConsumerDependencies,
    options: SQSConsumerOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    this.consumerOptionsOverride = options.consumerOverrides ?? {}
    this.deserializer = options.deserializer || deserializeSQSMessage
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: SQSMessage): Either<'abort', MessagePayloadType> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = this.deserializer(message, this.messageSchema, this.errorResolver)

    if (
      deserializationResult.error instanceof SqsValidationError ||
      deserializationResult.error instanceof SqsMessageInvalidFormat
    ) {
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
