import type { Either } from '@lokalise/node-core'
import type {Consumer as QueueConsumer, QueueOptions, TransactionObservabilityManager} from '@message-queue-toolkit/core'

import {AbstractSqsService, SQSDependencies} from "./AbstractSqsService";
import {SqsMessageInvalidFormat, SqsValidationError} from "../errors/sqsErrors";

import { Consumer } from 'sqs-consumer'
import { deserializeMessage } from './messageDeserializer'
import {ConsumerOptions} from "sqs-consumer/src/types";

export type SQSMessage = {
  MessageId: string
  ReceiptHandle: string
  MD5OfBody: string
  Body: string
}

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type SQSConsumerOptions<MessagePayloadType extends {}> = QueueOptions<MessagePayloadType> & {
  consumerOverrides?: Partial<ConsumerOptions>
}

export abstract class AbstractSqsConsumer<MessagePayloadType extends {}>
  extends AbstractSqsService<MessagePayloadType, SQSConsumerOptions<MessagePayloadType>>
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  // @ts-ignore
  protected consumer: Consumer
  private readonly consumerOptionsOverride: Partial<ConsumerOptions>;

  constructor(dependencies: SQSDependencies, options: SQSConsumerOptions<MessagePayloadType>) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.consumerOptionsOverride = options.consumerOverrides ?? {}
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: SQSMessage): Either<'abort', MessagePayloadType> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = deserializeMessage(
      message,
      this.messageSchema,
      this.errorResolver,
    )

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

  async consume() {
    await this.init()

    this.consumer = Consumer.create({
      queueUrl: this.queueName,
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
        const transactionSpanId = `queue_${this.queueName}:${deserializedMessage.result[this.messageTypeField]}`

        this.transactionObservabilityManager?.start(transactionSpanId)
        const result: Either<'retryLater' | Error, 'success'> = await this.processMessage(deserializedMessage.result)
            .catch((err) => {
              // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
              // If we fail due to unknown reason, let's retry
              this.handleError(err)
              return {
                error: err
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
      ...this.consumerOptionsOverride
    });

    /*
    this.consumer.on('error', (err) => {
      this.handleError(err);
    });

    this.consumer.on('processing_error', (err) => {
      this.handleError(err);
      // @ts-ignore
      if (err?.error === 'retryLater') {
        throw new Error('force retry')
      }
    });

     */

    this.consumer.start()
  }

  override async close(): Promise<void> {
    await super.close()
    this.consumer.stop()
  }

}
