import type { MessageAttributeValue } from '@aws-sdk/client-sns'
import { PublishCommand } from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'
import { InternalError } from '@lokalise/node-core'
import { buildCodecEnvelope, getCodecName, resolveCodecHandler } from '@message-queue-toolkit/codec'
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
import { resolveOutgoingMessageAttributes } from '@message-queue-toolkit/sqs'

import { calculateOutgoingMessageSize, validateFifoTopicName } from '../utils/snsUtils.ts'

import type {
  SNSCreationConfig,
  SNSDependencies,
  SNSTopicLocatorType,
} from './AbstractSnsService.ts'
import { AbstractSnsService } from './AbstractSnsService.ts'

export type SNSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export type SNSPublisherOptionsBase<MessagePayloadType extends object> = QueuePublisherOptions<
  SNSCreationConfig,
  SNSTopicLocatorType,
  MessagePayloadType
>

/**
 * SNS Publisher options with type-safe FIFO topic configuration.
 * When fifoTopic is true, messageGroupIdField and defaultMessageGroupId are available.
 * When fifoTopic is false or omitted, these fields are not allowed.
 */
export type SNSPublisherOptions<MessagePayloadType extends object> =
  SNSPublisherOptionsBase<MessagePayloadType> &
    (
      | {
          /**
           * Set to true for FIFO topics. Enables messageGroupIdField and defaultMessageGroupId options.
           */
          fifoTopic: true
          /**
           * Field name in the message payload to use as MessageGroupId for FIFO topics.
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
           * Set to false or omit for standard (non-FIFO) topics.
           */
          fifoTopic?: false
          messageGroupIdField?: never
          defaultMessageGroupId?: never
        }
    )

export abstract class AbstractSnsPublisher<MessagePayloadType extends object>
  extends AbstractSnsService<MessagePayloadType, MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SNSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>
  private readonly isDeduplicationEnabled: boolean
  private readonly isFifoTopic: boolean
  private readonly messageGroupIdField?: string
  private readonly defaultMessageGroupId?: string

  private initPromise?: Promise<void>

  constructor(dependencies: SNSDependencies, options: SNSPublisherOptions<MessagePayloadType>) {
    super(dependencies, options)

    this.messageSchemaContainer = this.resolvePublisherMessageSchemaContainer(options)
    this.isDeduplicationEnabled = !!options.enablePublisherDeduplication
    this.isFifoTopic = options.fifoTopic ?? false
    this.messageGroupIdField = options.messageGroupIdField
    this.defaultMessageGroupId = options.defaultMessageGroupId
  }

  override async init(): Promise<void> {
    await super.init()

    // Validate FIFO topic naming conventions
    if (this.isFifoTopic) {
      const topicName = this.locatorConfig?.topicName ?? this.creationConfig?.topic?.Name
      if (topicName) {
        validateFifoTopicName(topicName, this.isFifoTopic)
      }
    }
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
      const messageProcessingStartTimestamp = Date.now()
      const parsedMessage = messageSchemaResult.result.parse(message)
      const topicName =
        this.locatorConfig?.topicName ?? this.creationConfig?.topic?.Name ?? 'unknown'

      // Dedup check before compression/offload: skip expensive work for duplicates.
      if (
        this.isDeduplicationEnabledForMessage(parsedMessage) &&
        (await this.deduplicateMessage(parsedMessage, DeduplicationRequesterEnum.Publisher))
          .isDuplicated
      ) {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'published', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: topicName,
        })
        return
      }

      const updatedMessage = this.updateInternalProperties(message)

      // Resolve FIFO options from original message BEFORE offloading
      // (offloaded payload won't have user fields needed for messageGroupIdField)
      const resolvedOptions = this.resolveFifoOptions(updatedMessage, options)

      const { payload, preBuiltBody } = await this.prepareOutgoingPayload(updatedMessage)

      await this.sendMessage(payload, resolvedOptions, preBuiltBody)

      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp,
        queueName: topicName,
      })
    } catch (error) {
      const err = error as Error
      this.handleError(err)
      throw new InternalError({
        message: `Error while publishing to SNS: ${err.message}`,
        errorCode: 'SNS_PUBLISH_ERROR',
        details: {
          publisher: this.constructor.name,
          topic: this.topicArn,
          messageType: this.resolveMessageTypeFromMessage(message) ?? 'unknown',
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

  protected override isDeduplicationEnabledForMessage(message: MessagePayloadType): boolean {
    return this.isDeduplicationEnabled && super.isDeduplicationEnabledForMessage(message)
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
      const handler = resolveCodecHandler(codec)
      const codecName = getCodecName(codec)

      if (this.payloadStoreConfig) {
        // Streaming path: avoids 3× buffer materialisation for large payloads.
        // JSON → compress → temp file → threshold check → offload or inline envelope.
        const result = await this.compressAndOffloadPayload(message, handler, codecName)
        if (result.pointer) {
          return { payload: result.pointer }
        }
        return {
          payload: message,
          preBuiltBody: buildCodecEnvelope(result.compressedBuffer, codecName),
        }
      }

      // No offload store — bounded by SNS 256 KB limit, safe to buffer.
      // Serialize once so we can check the raw size before deciding whether to compress.
      const jsonBuffer = Buffer.from(JSON.stringify(message), 'utf8')

      // Skip compression for messages below the configured floor — small payloads
      // often grow when compressed, so we send them as plain JSON instead.
      if (jsonBuffer.byteLength < this.skipCompressionBelow) {
        return { payload: message }
      }

      const compressed = await handler.compress(jsonBuffer)
      return { payload: message, preBuiltBody: buildCodecEnvelope(compressed, codecName) }
    }

    return {
      payload:
        (await this.offloadPayload(message, () => calculateOutgoingMessageSize(message))) ??
        message,
    }
  }

  protected async sendMessage(
    payload: MessagePayloadType | OffloadedPayloadPointerPayload,
    options: SNSMessageOptions,
    preBuiltBody?: string,
  ): Promise<void> {
    const attributes = resolveOutgoingMessageAttributes<MessageAttributeValue>(payload)
    // preBuiltBody is set when codec is active and the payload was not offloaded —
    // it contains the already-compressed codec envelope, so we skip re-serialization.
    const body = preBuiltBody ?? JSON.stringify(payload)
    const command = new PublishCommand({
      Message: body,
      MessageAttributes: attributes,
      TopicArn: this.topicArn,
      ...options,
    })
    await this.snsClient.send(command)
  }

  /**
   * Resolves FIFO-specific options (MessageGroupId) from the message payload.
   * This must be called BEFORE payload offloading, as the offloaded payload
   * won't contain the user fields needed for messageGroupIdField resolution.
   *
   * @param payload - The original (non-offloaded) message payload
   * @param options - The SNS message options to augment with FIFO settings
   * @returns The options with resolved MessageGroupId for FIFO topics
   */
  private resolveFifoOptions(
    payload: MessagePayloadType,
    options: SNSMessageOptions,
  ): SNSMessageOptions {
    if (!this.isFifoTopic) {
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

    // Validate MessageGroupId is present for FIFO topics
    if (!resolvedOptions.MessageGroupId) {
      throw new InternalError({
        message:
          'MessageGroupId is required for FIFO topics. Provide it in publish options, configure messageGroupIdField, or set defaultMessageGroupId.',
        errorCode: 'FIFO_MESSAGE_GROUP_ID_REQUIRED',
        details: {
          topicArn: this.topicArn,
        },
      })
    }

    return resolvedOptions
  }
  /* c8 ignore stop */
}
