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

import type { PubSubMessage } from '../types/MessageTypes.ts'
import { buildOffloadedPayloadAttributes } from '../utils/messageUtils.ts'
import type {
  PubSubCreationConfig,
  PubSubDependencies,
  PubSubQueueLocatorType,
} from './AbstractPubSubService.ts'
import { AbstractPubSubService } from './AbstractPubSubService.ts'

export type PubSubMessageOptions = {
  orderingKey?: string
  attributes?: Record<string, string>
}

export abstract class AbstractPubSubPublisher<MessagePayloadType extends object>
  extends AbstractPubSubService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, PubSubMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly isDeduplicationEnabled: boolean
  private initPromise?: Promise<void>

  constructor(
    dependencies: PubSubDependencies,
    options: QueuePublisherOptions<
      PubSubCreationConfig,
      PubSubQueueLocatorType,
      MessagePayloadType
    >,
  ) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
    this.isDeduplicationEnabled = !!options.enablePublisherDeduplication
  }

  async publish(message: MessagePayloadType, options: PubSubMessageOptions = {}): Promise<void> {
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

      message = this.updateInternalProperties(message)
      const maybeOffloadedPayloadMessage = await this.offloadMessagePayloadIfNeeded(message, () => {
        // Calculate message size for PubSub
        const messageData = Buffer.from(JSON.stringify(message))
        return messageData.length
      })

      if (
        this.isDeduplicationEnabledForMessage(parsedMessage) &&
        (await this.deduplicateMessage(parsedMessage, DeduplicationRequesterEnum.Publisher))
          .isDuplicated
      ) {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'published', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.topicName,
        })
        return
      }

      await this.sendMessage(maybeOffloadedPayloadMessage, options)
      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp,
        queueName: this.topicName,
      })
    } catch (error) {
      const err = error as Error
      this.handleError(err)
      throw new InternalError({
        message: `Error while publishing to PubSub: ${err.message}`,
        errorCode: 'PUBSUB_PUBLISH_ERROR',
        details: {
          publisher: this.constructor.name,
          topicName: this.topicName,
          messageType: this.resolveMessageTypeFromMessage(message) ?? 'unknown',
        },
        cause: err,
      })
    }
  }

  private async sendMessage(
    message: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: PubSubMessageOptions,
  ): Promise<void> {
    const messageData = Buffer.from(JSON.stringify(message))
    const attributes = buildOffloadedPayloadAttributes(message as unknown, options.attributes)

    await this.topic.publishMessage({
      data: messageData,
      attributes,
      orderingKey: options.orderingKey,
    })
  }

  /* c8 ignore start */
  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  protected resolveMessage(
    _message: PubSubMessage,
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

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
