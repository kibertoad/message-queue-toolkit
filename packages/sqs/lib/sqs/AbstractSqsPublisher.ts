import type { MessageAttributeValue } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import {
  type AsyncPublisher,
  type BarrierResult,
  DeduplicationRequesterEnum,
  type MessageInvalidFormatError,
  type MessageSchemaContainer,
  type MessageValidationError,
  type OffloadedPayloadPointerPayload,
  type QueuePublisherOptions,
  type ResolvedMessage,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v4'

import type { SQSMessage } from '../types/MessageTypes.ts'
import { resolveOutgoingMessageAttributes } from '../utils/messageUtils.ts'
import { calculateOutgoingMessageSize } from '../utils/sqsUtils.ts'
import type {
  SQSCreationConfig,
  SQSDependencies,
  SQSQueueLocatorType,
} from './AbstractSqsService.ts'
import { AbstractSqsService } from './AbstractSqsService.ts'

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export const PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX = 'payloadOffloading.'
export const OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE = `${PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX}size`

export abstract class AbstractSqsPublisher<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly isDeduplicationEnabled: boolean
  private initPromise?: Promise<void>

  constructor(
    dependencies: SQSDependencies,
    options: QueuePublisherOptions<SQSCreationConfig, SQSQueueLocatorType, MessagePayloadType>,
  ) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
    this.isDeduplicationEnabled = !!options.enablePublisherDeduplication
  }

  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
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
      const messageProcessingStartTimestamp = Date.now()
      const parsedMessage = messageSchemaResult.result.parse(message)

      if (this.logMessages) {
        // @ts-expect-error
        const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
        this.logMessage(resolvedLogMessage)
      }

      // biome-ignore lint/style/noParameterAssign: This is expected
      message = this.updateInternalProperties(message)
      const maybeOffloadedPayloadMessage = await this.offloadMessagePayloadIfNeeded(message, () =>
        calculateOutgoingMessageSize(message),
      )

      if (
        this.isDeduplicationEnabledForMessage(parsedMessage) &&
        (await this.deduplicateMessage(parsedMessage, DeduplicationRequesterEnum.Publisher))
          .isDuplicated
      ) {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'published', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
        })
        return
      }

      await this.sendMessage(maybeOffloadedPayloadMessage, options)
      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp,
        queueName: this.queueName,
      })
    } catch (error) {
      const err = error as Error
      this.handleError(err)
      throw new InternalError({
        message: `Error while publishing to SQS: ${err.message}`,
        errorCode: 'SQS_PUBLISH_ERROR',
        details: {
          publisher: this.constructor.name,
          queueArn: this.queueArn,
          queueName: this.queueName,
          // @ts-expect-error
          messageType: message[this.messageTypeField] ?? 'unknown',
        },
        cause: err,
      })
    }
  }

  /* c8 ignore start */
  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  protected resolveMessage(
    _message: SQSMessage,
  ): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
    throw new Error('Not implemented for publisher')
  }

  protected override processPrehandlers(): Promise<unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override preHandlerBarrier<BarrierOutput>(): Promise<BarrierResult<BarrierOutput>> {
    throw new Error('Not implemented for publisher')
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */

  protected override isDeduplicationEnabledForMessage(message: MessagePayloadType): boolean {
    return this.isDeduplicationEnabled && super.isDeduplicationEnabledForMessage(message)
  }

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  protected async sendMessage(
    payload: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: SQSMessageOptions,
  ): Promise<void> {
    const attributes = resolveOutgoingMessageAttributes<MessageAttributeValue>(payload)
    const command = new SendMessageCommand({
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(payload),
      MessageAttributes: attributes,
      ...options,
    })
    await this.sqsClient.send(command)
  }
}
