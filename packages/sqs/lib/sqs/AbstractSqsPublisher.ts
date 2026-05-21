import type { MessageAttributeValue } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import { buildCodecEnvelope, resolveCodecHandler } from '@message-queue-toolkit/codec'
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

      message = this.updateInternalProperties(message)

      // Resolve FIFO options from original message BEFORE offloading
      // (offloaded payload won't have user fields needed for messageGroupIdField)
      const resolvedOptions = this.resolveFifoOptions(message, options)

      const { payload, preBuiltBody } = await this.prepareOutgoingPayload(message)

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

      await this.sendMessage(payload, resolvedOptions, preBuiltBody)
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

  /**
   * Compresses (when codec is set) or offloads (when store is configured) the message.
   * Returns the payload to send and an optional pre-built body string.
   * When preBuiltBody is set, it is a ready-to-send codec envelope — sendMessage must use it as-is.
   *
   * When both codec and payloadStoreConfig are set, uses a streaming pipeline
   * (JSON → zstd → temp file → store) to avoid materialising the full payload in memory.
   */
  private async prepareOutgoingPayload(message: MessagePayloadType): Promise<{
    payload: MessagePayloadType | OffloadedPayloadPointerPayload
    preBuiltBody?: string
  }> {
    const codec = this.codec

    if (codec) {
      if (this.payloadStoreConfig) {
        // Streaming path: avoids 3× buffer materialisation for large payloads.
        // JSON → zstd → temp file → threshold check → offload or inline envelope.
        const result = await this.compressAndOffloadPayload(message, codec)
        if (result.pointer) {
          return { payload: result.pointer }
        }
        return {
          payload: message,
          preBuiltBody: buildCodecEnvelope(result.compressedBuffer, codec),
        }
      }

      // No offload store — bounded by SQS 256 KB limit, safe to buffer.
      // Serialize once so we can check the raw size before deciding whether to compress.
      const jsonBuffer = Buffer.from(JSON.stringify(message), 'utf8')

      // Skip compression for messages below the configured floor — small payloads
      // often grow when compressed, so we send them as plain JSON instead.
      if (jsonBuffer.byteLength < this.skipCompressionBelow) {
        return { payload: message }
      }

      const compressed = await resolveCodecHandler(codec).compress(jsonBuffer)
      return { payload: message, preBuiltBody: buildCodecEnvelope(compressed, codec) }
    }

    return {
      payload:
        (await this.offloadPayload(message, () => calculateOutgoingMessageSize(message))) ??
        message,
    }
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
      QueueUrl: this.queueUrl,
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
          queueName: this.queueName,
          queueUrl: this.queueUrl,
          isFifoQueue: this.isFifoQueue,
        },
      })
    }

    return resolvedOptions
  }
}
