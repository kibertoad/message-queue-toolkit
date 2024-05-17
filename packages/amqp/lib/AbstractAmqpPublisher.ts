import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import type {
  BarrierResult,
  MessageInvalidFormatError,
  MessageValidationError,
  QueuePublisherOptions,
  SyncPublisher,
} from '@message-queue-toolkit/core'
import { MessageSchemaContainer, objectToBuffer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { AMQPLocator, AMQPCreationConfig, AMQPDependencies } from './AbstractAmqpService'
import { AbstractAmqpService } from './AbstractAmqpService'

export type AMQPPublisherOptions<MessagePayloadType extends object> = QueuePublisherOptions<
  AMQPCreationConfig,
  AMQPLocator,
  MessagePayloadType
>

export abstract class AbstractAmqpPublisher<MessagePayloadType extends object>
  extends AbstractAmqpService<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  private initPromise?: Promise<void>

  constructor(dependencies: AMQPDependencies, options: AMQPPublisherOptions<MessagePayloadType>) {
    super(dependencies, options)

    const messageSchemas = options.messageSchemas
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
  }

  publish(message: MessagePayloadType): void {
    const resolveSchemaResult = this.resolveSchema(message)
    if (resolveSchemaResult.error) {
      throw resolveSchemaResult.error
    }
    resolveSchemaResult.result.parse(message)

    // If it's not initted yet, do the lazy init
    if (!this.isInitted) {
      // avoid multiple concurrent inits
      if (!this.initPromise) {
        this.initPromise = this.init()
      }

      /**
       * it is intentional that this promise is not awaited, that's how we keep the method invocation synchronous
       * RabbitMQ publish by itself doesn't guarantee that your message is delivered successfully, so this kind of fire-and-forget is not strongly different from how amqp-lib behaves in the first place.
       */
      this.initPromise
        .then(() => {
          this.publish(message)
        })
        .catch((err) => {
          this.handleError(err)
        })
      return
    }

    /**
     * If the message doesn't have a timestamp field -> add it
     * will be used on the consumer to prevent infinite retries on the same message
     */
    if (!this.tryToExtractTimestamp(message)) {
      // @ts-ignore
      message[this.messageTimestampField] = new Date().toISOString()
      this.logger.warn(`${this.messageTimestampField} not defined, adding it automatically`)
    }

    if (this.logMessages) {
      // @ts-ignore
      const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
      this.logMessage(resolvedLogMessage)
    }

    try {
      this.channel.sendToQueue(this.queueName, objectToBuffer(message))
    } catch (err) {
      // Unfortunately, reliable retry mechanism can't be implemented with try-catch block,
      // as not all failures end up here. If connection is closed programmatically, it works fine,
      // but if server closes connection unexpectedly (e. g. RabbitMQ is shut down), then we don't land here
      // @ts-ignore
      if (err.message === 'Channel closed') {
        this.logger.error(`AMQP channel closed`)
        void this.reconnect()
      } else {
        throw new InternalError({
          message: `Error while publishing to AMQP ${(err as Error).message}`,
          errorCode: 'AMQP_PUBLISH_ERROR',
          details: {
            publisher: this.constructor.name,
            queueName: this.queueName,
            // @ts-ignore
            messageType: message[this.messageTypeField] ?? 'unknown',
          },
          cause: err as Error,
        })
      }
    }
  }

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  /* c8 ignore start */
  protected resolveMessage(): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  /* c8 ignore start */
  protected override processPrehandlers(): Promise<unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override preHandlerBarrier<BarrierOutput>(): Promise<BarrierResult<BarrierOutput>> {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  async close(): Promise<void> {
    this.initPromise = undefined
    await super.close()
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */
}
