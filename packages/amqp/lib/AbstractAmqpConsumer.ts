import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer,
  QueueOptions,
  TransactionObservabilityManager,
  Deserializer,
} from '@message-queue-toolkit/core'
import { isMessageError } from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'

import type { AMQPConsumerDependencies, AMQPQueueConfig } from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { deserializeAmqpMessage } from './amqpMessageDeserializer'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type AMQPConsumerOptions<MessagePayloadType extends object> = QueueOptions<
  MessagePayloadType,
  AMQPQueueConfig
> & {
  deserializer?: Deserializer<MessagePayloadType, Message>
}

export abstract class AbstractAmqpConsumer<MessagePayloadType extends object>
  extends AbstractAmqpService<MessagePayloadType, AMQPConsumerDependencies>
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver
  private readonly deserializer: Deserializer<MessagePayloadType, Message>

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: AMQPConsumerOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    this.deserializer = options.deserializer ?? deserializeAmqpMessage
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: Message | null): Either<'abort', MessagePayloadType> {
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
}
