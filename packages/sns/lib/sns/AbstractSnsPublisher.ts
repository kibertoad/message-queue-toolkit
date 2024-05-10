import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  BarrierResult,
  MessageInvalidFormatError,
  MessageValidationError,
  QueuePublisherOptions,
} from '@message-queue-toolkit/core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'

import type { SNSCreationConfig, SNSDependencies, SNSQueueLocatorType } from './AbstractSnsService'
import { AbstractSnsService } from './AbstractSnsService'

export type SNSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export type SNSPublisherOptions<MessagePayloadType extends object> = QueuePublisherOptions<
  SNSCreationConfig,
  SNSQueueLocatorType,
  MessagePayloadType
>

export abstract class AbstractSnsPublisher<MessagePayloadType extends object>
  extends AbstractSnsService<MessagePayloadType, MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SNSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  private isInitted: boolean
  private initPromise?: Promise<void>

  constructor(dependencies: SNSDependencies, options: SNSPublisherOptions<MessagePayloadType>) {
    super(dependencies, options)

    const messageSchemas = options.messageSchemas
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
    this.isInitted = false
  }

  async publish(message: MessagePayloadType, options: SNSMessageOptions = {}): Promise<void> {
    const messageSchemaResult = this.resolveSchema(message)
    if (messageSchemaResult.error) {
      throw messageSchemaResult.error
    }

    // If it's not initted yet, do the lazy init
    if (!this.isInitted) {
      // avoid multiple concurrent inits
      if (!this.initPromise) {
        this.initPromise = this.init()
      }
      await this.initPromise
    }

    try {
      messageSchemaResult.result.parse(message)

      /**
       * If the message doesn't have a timestamp field -> add it
       * will be used on the consumer to prevent infinite retries on the same message
       */
      // @ts-ignore
      if (!message[this.messageTimestampField]) {
        // @ts-ignore
        message[this.messageTimestampField] = new Date().toISOString()
        this.logger.warn(`${this.messageTimestampField} not defined, adding it automatically`)
      }

      if (this.logMessages) {
        // @ts-ignore
        const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
        this.logMessage(resolvedLogMessage)
      }

      const input = {
        Message: JSON.stringify(message),
        TopicArn: this.topicArn,
        ...options,
      } satisfies PublishCommandInput
      const command = new PublishCommand(input)
      await this.snsClient.send(command)
      this.handleMessageProcessed(message, 'published')
    } catch (error) {
      const err = error as Error
      this.handleError(err)
      throw new InternalError({
        message: `Error while publishing to SNS: ${err.message}`,
        errorCode: 'SNS_PUBLISH_ERROR',
        details: {
          publisher: this.constructor.name,
          topic: this.topicArn,
          // @ts-ignore
          messageType: message[this.messageTypeField] ?? 'unknown',
        },
        cause: err,
      })
    }
  }

  protected override resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    unknown
  > {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  /* c8 ignore start */
  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  protected override processPrehandlers(): Promise<undefined> {
    throw new Error('Not implemented for publisher')
  }

  protected override preHandlerBarrier<BarrierOutput>(): Promise<BarrierResult<BarrierOutput>> {
    throw new Error('Not implemented for publisher')
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */
}
