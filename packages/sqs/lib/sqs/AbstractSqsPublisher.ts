import type { MessageAttributeValue } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import {
  type AsyncPublisher,
  type BarrierResult,
  DeduplicationRequesterEnum,
  isOffloadedPayloadPointerPayload,
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
  SQSOptions,
  SQSQueueLocatorType,
} from './AbstractSqsService.ts'
import { AbstractSqsService } from './AbstractSqsService.ts'

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

type BaseSQSPublisherOptions<
  MessagePayloadType extends object,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = QueuePublisherOptions<CreationConfigType, QueueLocatorType, MessagePayloadType> &
  Omit<SQSOptions<CreationConfigType, QueueLocatorType>, 'fifoQueue'>

/**
 * SQS Publisher options with type-safe FIFO queue configuration.
 * When fifoQueue is true, messageGroupIdField and defaultMessageGroupId are available.
 * When fifoQueue is false or omitted, these fields are not allowed.
 */
export type SQSPublisherOptions<
  MessagePayloadType extends object,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = BaseSQSPublisherOptions<MessagePayloadType, CreationConfigType, QueueLocatorType> &
  (
    | {
        /**
         * Set to true for FIFO queues. Enables messageGroupIdField and defaultMessageGroupId options.
         */
        fifoQueue: true
        /**
         * Field name in the message payload to use as MessageGroupId for FIFO queues.
         * If not provided, MessageGroupId must be specified in publish options.
         */
        messageGroupIdField?: string
        /**
         * Default MessageGroupId to use when messageGroupIdField is not present in the message.
         * If not provided and messageGroupIdField is not found, MessageGroupId must be specified in publish options.
         */
        defaultMessageGroupId?: string
      }
    | {
        /**
         * Set to false or omit for standard (non-FIFO) queues.
         */
        fifoQueue?: false
        messageGroupIdField?: never
        defaultMessageGroupId?: never
      }
  )

export const PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX = 'payloadOffloading.'
export const OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE = `${PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX}size`

export abstract class AbstractSqsPublisher<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly isDeduplicationEnabled: boolean
  private readonly messageGroupIdField?: string
  private readonly defaultMessageGroupId?: string
  private initPromise?: Promise<void>

  constructor(dependencies: SQSDependencies, options: SQSPublisherOptions<MessagePayloadType>) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
    this.isDeduplicationEnabled = !!options.enablePublisherDeduplication
    this.messageGroupIdField = options.messageGroupIdField
    this.defaultMessageGroupId = options.defaultMessageGroupId
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

      // Fast read-only pre-check: skip compression/offload for messages already known to
      // be duplicates. This does NOT persist a dedup key, so a transient failure in the
      // expensive work below leaves no key behind and the publish stays safely retriable.
      if (await this.isMessageDuplicated(parsedMessage, DeduplicationRequesterEnum.Publisher)) {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'published', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.queue.name,
        })
        return
      }

      message = this.updateInternalProperties(message)

      // Resolve FIFO options from original message BEFORE offloading
      // (offloaded payload won't have user fields needed for messageGroupIdField)
      const resolvedOptions = this.resolveFifoOptions(message, options)

      const { payload, preBuiltBody } = await this.prepareOutgoingPayload(message)

      // Persist the dedup key only now — immediately before send — so the window in which
      // a stored key plus a failed send could drop the message on retry stays as small as
      // possible and no longer spans compression/offload (a transient failure there leaves
      // no key behind). The pre-check above already skipped the expensive work for the
      // common duplicate case; this check additionally closes the race where a concurrent
      // publish stored the key in the meantime.
      if (
        (await this.deduplicateMessage(parsedMessage, DeduplicationRequesterEnum.Publisher))
          .isDuplicated
      ) {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'published', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.queue.name,
        })
        return
      }

      await this.sendMessage(payload, resolvedOptions, preBuiltBody)
      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: isOffloadedPayloadPointerPayload(payload)
          ? { status: 'published', offloaded: true }
          : { status: 'published' },
        messageProcessingStartTimestamp,
        queueName: this.queue.name,
      })
    } catch (error) {
      const err = error as Error
      this.handleError(err)
      throw new InternalError({
        message: `Error while publishing to SQS: ${err.message}`,
        errorCode: 'SQS_PUBLISH_ERROR',
        details: {
          publisher: this.constructor.name,
          queueArn: this.queue.arn,
          queueName: this.queue.name,
          messageType: this.resolveMessageTypeFromMessage(message) ?? 'unknown',
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

  protected override calculateOutgoingMessageSize(message: MessagePayloadType): number {
    return calculateOutgoingMessageSize(message)
  }

  protected async sendMessage(
    payload: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: SQSMessageOptions,
    preBuiltBody?: string,
  ): Promise<void> {
    const attributes = resolveOutgoingMessageAttributes<MessageAttributeValue>(payload)
    // preBuiltBody is set when codec is active and the payload was not offloaded —
    // it contains the already-compressed codec envelope, so we skip re-serialization.
    const body = preBuiltBody ?? JSON.stringify(payload)
    const command = new SendMessageCommand({
      QueueUrl: this.queue.url,
      MessageBody: body,
      MessageAttributes: attributes,
      ...options,
    })
    await this.sqsClient.send(command)
  }

  /**
   * Resolves FIFO-specific options (MessageGroupId, MessageDeduplicationId)
   */
  private resolveFifoOptions(
    payload: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: SQSMessageOptions,
  ): SQSMessageOptions {
    if (!this.isFifoQueue) {
      return options
    }

    const resolvedOptions = { ...options }

    // Resolve MessageGroupId if not provided
    if (!resolvedOptions.MessageGroupId) {
      if (this.messageGroupIdField) {
        const messageGroupId = (payload as Record<string, unknown>)[this.messageGroupIdField]
        if (typeof messageGroupId === 'string') {
          resolvedOptions.MessageGroupId = messageGroupId
        }
      }

      // Fallback to default if still not set
      if (!resolvedOptions.MessageGroupId && this.defaultMessageGroupId) {
        resolvedOptions.MessageGroupId = this.defaultMessageGroupId
      }
    }

    // Validate that MessageGroupId is present for FIFO queues
    if (!resolvedOptions.MessageGroupId) {
      throw new InternalError({
        message:
          'MessageGroupId is required for FIFO queues. Provide it in publish options, configure messageGroupIdField, or set defaultMessageGroupId.',
        errorCode: 'FIFO_MESSAGE_GROUP_ID_REQUIRED',
        details: {
          queueName: this.queue.name,
          queueUrl: this.queue.url,
          isFifoQueue: this.isFifoQueue,
        },
      })
    }

    return resolvedOptions
  }
}
