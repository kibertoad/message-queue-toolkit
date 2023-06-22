import { types } from 'node:util'

import type { Either } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { Consumer, TransactionObservabilityManager } from '@message-queue-toolkit/core'
import type { Message } from 'amqplib'

import type { AMQPDependencies, QueueParams } from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { AmqpMessageInvalidFormat, AmqpValidationError } from './errors/amqpErrors'
import { deserializeMessage } from './messageDeserializer'
import type { CommonMessage } from './types/MessageTypes'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export abstract class AbstractAmqpConsumer<MessagePayloadType extends CommonMessage>
  extends AbstractAmqpService<MessagePayloadType>
  implements Consumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager

  constructor(params: QueueParams<MessagePayloadType>, dependencies: AMQPDependencies) {
    super(params, dependencies)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
  }

  abstract processMessage(
    messagePayload: MessagePayloadType,
  ): Promise<Either<'retryLater', 'success'>>

  private deserializeMessage(message: Message | null): Either<'abort', MessagePayloadType> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = deserializeMessage(
      message,
      this.messageSchema,
      this.errorResolver,
    )

    if (
      deserializationResult.error instanceof AmqpValidationError ||
      deserializationResult.error instanceof AmqpMessageInvalidFormat
    ) {
      const logObject = resolveGlobalErrorLogObject(deserializationResult.error)
      this.logger.error(logObject)
      this.errorReporter.report({ error: deserializationResult.error })
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

  async consume() {
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
      const transactionSpanId = `queue_${this.queueName}:${deserializedMessage.result.messageType}`

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
          const logObject = resolveGlobalErrorLogObject(err)
          this.logger.error(logObject)
          if (types.isNativeError(err)) {
            this.errorReporter.report({ error: err })
          }
        })
        .finally(() => {
          this.transactionObservabilityManager?.stop(transactionSpanId)
        })
    })
  }
}
