import type { MessageAttributeValue } from '@aws-sdk/client-sns'
import { PublishCommand } from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  BarrierResult,
  MessageInvalidFormatError,
  MessageValidationError,
  QueuePublisherOptions,
  MessageSchemaContainer,
  OffloadedPayloadPointerPayload,
  ResolvedMessage,
} from '@message-queue-toolkit/core'
import { resolveOutgoingMessageAttributes } from '@message-queue-toolkit/sqs'

import { calculateOutgoingMessageSize } from '../utils/snsUtils'

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

  private initPromise?: Promise<void>

  constructor(dependencies: SNSDependencies, options: SNSPublisherOptions<MessagePayloadType>) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
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
      this.initPromise = undefined
    }

    try {
      const parsedMessage = messageSchemaResult.result.parse(message)

      if (this.logMessages) {
        // @ts-ignore
        const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
        this.logMessage(resolvedLogMessage)
      }

      message = this.updateInternalProperties(message)
      const maybeOffloadedPayloadMessage = await this.offloadMessagePayloadIfNeeded(message, () =>
        calculateOutgoingMessageSize(message),
      )

      await this.sendMessage(maybeOffloadedPayloadMessage, options)
      this.handleMessageProcessed(parsedMessage, 'published')
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
    ResolvedMessage
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

  protected async sendMessage(
    payload: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: SNSMessageOptions,
  ): Promise<void> {
    const attributes = resolveOutgoingMessageAttributes<MessageAttributeValue>(payload)
    const command = new PublishCommand({
      Message: JSON.stringify(payload),
      MessageAttributes: attributes,
      TopicArn: this.topicArn,
      ...options,
    })
    await this.snsClient.send(command)
  }

  /* c8 ignore stop */
}
