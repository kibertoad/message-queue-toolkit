import type { Either } from '@lokalise/node-core'
import { copyWithoutUndefined, InternalError } from '@lokalise/node-core'
import type {
  BarrierResult,
  CommonCreationConfigType,
  MessageInvalidFormatError,
  MessageSchemaContainer,
  MessageValidationError,
  QueuePublisherOptions,
  ResolvedMessage,
  SyncPublisher,
} from '@message-queue-toolkit/core'
import { objectToBuffer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v4'

import type { AMQPDependencies } from './AbstractAmqpService.ts'
import { AbstractAmqpService } from './AbstractAmqpService.ts'

export type AMQPPublisherOptions<
  MessagePayloadType extends object,
  CreationConfig extends CommonCreationConfigType,
  LocatorConfig extends object,
> = QueuePublisherOptions<CreationConfig, LocatorConfig, MessagePayloadType> & {
  exchange?: string
  isLazyInitEnabled?: boolean // default is true
}

export abstract class AbstractAmqpPublisher<
    MessagePayloadType extends object,
    MessageOptionsType,
    CreationConfig extends CommonCreationConfigType,
    LocatorConfig extends object,
  >
  extends AbstractAmqpService<
    MessagePayloadType,
    AMQPDependencies,
    unknown,
    unknown,
    CreationConfig,
    LocatorConfig
  >
  implements SyncPublisher<MessagePayloadType, MessageOptionsType>
{
  protected readonly exchange?: string

  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly isLazyInitEnabled: boolean
  private initPromise?: Promise<void>

  constructor(
    dependencies: AMQPDependencies,
    options: AMQPPublisherOptions<MessagePayloadType, CreationConfig, LocatorConfig>,
  ) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
    this.exchange = options.exchange
    this.isLazyInitEnabled = options.isLazyInitEnabled ?? true
  }

  publish(message: MessagePayloadType, options: MessageOptionsType): void {
    const resolveSchemaResult = this.resolveSchema(message)
    if (resolveSchemaResult.error) {
      throw resolveSchemaResult.error
    }
    resolveSchemaResult.result.parse(message)
    const messageProcessingStartTimestamp = Date.now()

    // If it's not initted yet, do the lazy init
    if (!this.isInitted && this.isLazyInitEnabled) {
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
          this.publish(message, options)
        })
        .catch((err) => {
          this.handleError(err)
        })
      return
    }

    if (this.logMessages) {
      // @ts-expect-error
      const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
      this.logMessage(resolvedLogMessage)
    }

    // biome-ignore lint/style/noParameterAssign: it's ok
    message = this.updateInternalProperties(message)

    try {
      this.publishInternal(objectToBuffer(message), options)
      this.handleMessageProcessed({
        message,
        messageProcessingStartTimestamp,
        // @ts-expect-error it will throw an error before if id field is not set
        messageId: message[this.messageIdField],
        processingResult: { status: 'published' },
        queueName: this.resolveTopicOrQueue(),
      })
    } catch (err) {
      // Unfortunately, reliable retry mechanism can't be implemented with try-catch block,
      // as not all failures end up here. If connection is closed programmatically, it works fine,
      // but if server closes connection unexpectedly (e. g. RabbitMQ is shut down), then we don't land here
      // @ts-expect-error
      if (err.message === 'Channel closed') {
        this.logger.error('AMQP channel closed')
        void this.reconnect()
      } else {
        throw new InternalError({
          message: `Error while publishing to AMQP ${(err as Error).message}`,
          errorCode: 'AMQP_PUBLISH_ERROR',
          details: copyWithoutUndefined({
            publisher: this.constructor.name,
            // @ts-expect-error
            queueName: this.queueName,
            exchange: this.exchange,
            // @ts-expect-error
            messageType: message[this.messageTypeField] ?? 'unknown',
          }),
          cause: err as Error,
        })
      }
    }
  }

  protected abstract resolveTopicOrQueue(): string

  protected abstract publishInternal(message: Buffer, options: MessageOptionsType): void

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  /* c8 ignore start */
  protected resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    ResolvedMessage
  > {
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

  override async close(): Promise<void> {
    this.initPromise = undefined
    await super.close()
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */
}
