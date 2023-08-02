import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer,
  NewQueueOptions,
  TransactionObservabilityManager,
  ExistingQueueOptions,
  MonoSchemaQueueOptions,
} from '@message-queue-toolkit/core'
import { isMessageError, parseMessage } from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'
import type { ZodSchema } from 'zod'

import type { AMQPConsumerDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { readAmqpMessage } from './amqpMessageReader'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type AMQPLocatorType = { queueName: string }

export type NewAMQPConsumerOptions = NewQueueOptions<CreateAMQPQueueOptions>

export type ExistingAMQPConsumerOptions = ExistingQueueOptions<AMQPLocatorType>

export abstract class AbstractAmqpConsumer<MessagePayloadType extends object>
  extends AbstractAmqpService<MessagePayloadType, AMQPConsumerDependencies>
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>

  constructor(
    dependencies: AMQPConsumerDependencies,
    options:
      | (NewAMQPConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>)
      | (ExistingAMQPConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>),
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    if (!options.locatorConfig?.queueName && !options.creationConfig?.queueName) {
      throw new Error('queueName must be set in either locatorConfig or creationConfig')
    }

    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: Message | null): Either<'abort', MessagePayloadType> {
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

  async start() {
    await this.init()
    if (!this.channel) {
      throw new Error('Channel is not set')
    }

    await this.channel.consume(this.queueName, (message) => {
      if (message === null) {
        return
      }

      const deserializedMessage = this.deserializeMessage(message)
      if (deserializedMessage.error === 'abort') {
        this.channel.nack(message, false, false)
        return
      }
      const transactionSpanId = `queue_${this.queueName}:${
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        deserializedMessage.result[this.messageTypeField]
      }`

      this.transactionObservabilityManager?.start(transactionSpanId)
      this.processMessage(deserializedMessage.result)
        .then((result) => {
          if (result.error === 'retryLater') {
            this.channel.nack(message, false, true)
          }
          if (result.result === 'success') {
            this.channel.ack(message)
          }
        })
        .catch((err) => {
          // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
          // If we fail due to unknown reason, let's retry
          this.channel.nack(message, false, true)
          this.handleError(err)
        })
        .finally(() => {
          this.transactionObservabilityManager?.stop(transactionSpanId)
        })
    })
  }

  protected resolveMessage(message: Message) {
    return readAmqpMessage(message, this.errorResolver)
  }

  protected override resolveSchema(_message: MessagePayloadType) {
    return this.schemaEither
  }
}
