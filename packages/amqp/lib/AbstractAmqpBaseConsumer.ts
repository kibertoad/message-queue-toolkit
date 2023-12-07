import type { Either, ErrorResolver } from '@lokalise/node-core'
import type {
  QueueConsumer,
  NewQueueOptions,
  TransactionObservabilityManager,
  ExistingQueueOptions,
  BarrierResult,
} from '@message-queue-toolkit/core'
import { isMessageError, parseMessage } from '@message-queue-toolkit/core'
import type { Connection, Message } from 'amqplib'

import type { AMQPConsumerDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'
import { readAmqpMessage } from './amqpMessageReader'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}

export type AMQPLocatorType = { queueName: string }

export type NewAMQPConsumerOptions = NewQueueOptions<CreateAMQPQueueOptions>

export type ExistingAMQPConsumerOptions = ExistingQueueOptions<AMQPLocatorType>

export abstract class AbstractAmqpBaseConsumer<
    MessagePayloadType extends object,
    BarrierOutput = undefined,
  >
  extends AbstractAmqpService<MessagePayloadType, AMQPConsumerDependencies>
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  protected readonly errorResolver: ErrorResolver

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: NewAMQPConsumerOptions | ExistingAMQPConsumerOptions,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver

    if (!options.locatorConfig?.queueName && !options.creationConfig?.queueName) {
      throw new Error('queueName must be set in either locatorConfig or creationConfig')
    }
  }

  private async internalProcessMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const barrierResult = await this.preHandlerBarrier(message, messageType)

    if (barrierResult.isPassing) {
      return this.processMessage(message, messageType, barrierResult.output)
    }
    return { error: 'retryLater' }
  }

  protected abstract preHandlerBarrier(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<BarrierResult<BarrierOutput>>

  abstract processMessage(
    message: MessagePayloadType,
    messageType: string,
    barrierOutput: BarrierOutput,
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

  async receiveNewConnection(connection: Connection): Promise<void> {
    await super.receiveNewConnection(connection)
    await this.consume()
  }

  private tryToExtractId(message: Message | null): Either<'abort', string> {
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

  private async consume() {
    await this.channel.consume(this.queueName, (message) => {
      if (message === null) {
        return
      }

      const deserializedMessage = this.deserializeMessage(message)
      if (deserializedMessage.error === 'abort') {
        this.channel.nack(message, false, false)
        const messageId = this.tryToExtractId(message)
        this.handleMessageProcessed(null, 'invalid_message', messageId.result)
        return
      }
      // @ts-ignore
      const messageType = deserializedMessage.result[this.messageTypeField]
      const transactionSpanId = `queue_${this.queueName}:${
        // @ts-ignore
        // eslint-disable-next-line @typescript-eslint/restrict-template-expressions
        deserializedMessage.result[this.messageTypeField]
      }`

      this.transactionObservabilityManager?.start(transactionSpanId)
      if (this.logMessages) {
        const resolvedLogMessage = this.resolveMessageLog(deserializedMessage.result, messageType)
        this.logMessage(resolvedLogMessage)
      }
      this.internalProcessMessage(deserializedMessage.result, messageType)
        .then((result) => {
          if (result.error === 'retryLater') {
            this.channel.nack(message, false, true)
            this.handleMessageProcessed(deserializedMessage.result, 'retryLater')
          }
          if (result.result === 'success') {
            this.channel.ack(message)
            this.handleMessageProcessed(deserializedMessage.result, 'consumed')
          }
        })
        .catch((err) => {
          // ToDo we need sanity check to stop trying at some point, perhaps some kind of Redis counter
          // If we fail due to unknown reason, let's retry
          this.channel.nack(message, false, true)
          this.handleMessageProcessed(deserializedMessage.result, 'retryLater')
          this.handleError(err)
        })
        .finally(() => {
          this.transactionObservabilityManager?.stop(transactionSpanId)
        })
    })
  }

  async start() {
    await this.init()
    if (!this.channel) {
      throw new Error('Channel is not set')
    }

    await this.consume()
  }

  protected resolveMessage(message: Message) {
    return readAmqpMessage(message, this.errorResolver)
  }
}
