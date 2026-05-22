import { randomUUID } from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { types } from 'node:util'
import {
  type CommonLogger,
  type Either,
  type ErrorReporter,
  type ErrorResolver,
  resolveGlobalErrorLogObject,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { MakeRequired } from '@lokalise/universal-ts-utils/node'
import {
  MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA,
  type MessageDeduplicationOptions,
} from '@message-queue-toolkit/schemas'
import { getProperty, setProperty } from 'dot-prop'
import type { ZodSchema, ZodType } from 'zod/v4'
import { buildCodecEnvelope, getCodecName, resolveCodecHandler } from '../codec/codecHandler.ts'
import type { MessageCodecHandler, MessageCodecRegistration } from '../codec/messageCodec.ts'
import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors.ts'
import {
  type AcquireLockTimeoutError,
  isAcquireLockTimeoutError,
} from '../message-deduplication/AcquireLockTimeoutError.ts'
import {
  DEFAULT_MESSAGE_DEDUPLICATION_OPTIONS,
  type DeduplicationRequester,
  DeduplicationRequesterEnum,
  type MessageDeduplicationConfig,
  noopReleasableLock,
  type ReleasableLock,
} from '../message-deduplication/messageDeduplicationTypes.ts'
import { jsonStreamStringifySerializer } from '../payload-store/JsonStreamStringifySerializer.ts'
import {
  OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA,
  type OffloadedPayloadPointerPayload,
  type PayloadRef,
} from '../payload-store/offloadedPayloadMessageSchemas.ts'
import type {
  MultiPayloadStoreConfig,
  PayloadStore,
  SinglePayloadStoreConfig,
} from '../payload-store/payloadStoreTypes.ts'
import { isDestroyable, isMultiPayloadStoreConfig } from '../payload-store/payloadStoreTypes.ts'
import type { MessageProcessingResult } from '../types/MessageQueueTypes.ts'
import type {
  DeletionConfig,
  MessageMetricsManager,
  ProcessedMessageMetadata,
  QueueDependencies,
  QueueOptions,
} from '../types/queueOptionsTypes.ts'
import { isRetryDateExceeded } from '../utils/dateUtils.ts'
import { streamWithKnownSizeToBuffer, streamWithKnownSizeToString } from '../utils/streamUtils.ts'
import { toDatePreprocessor } from '../utils/toDateProcessor.ts'
import type {
  BarrierCallback,
  BarrierResult,
  MessageHandlerConfig,
  PreHandlingOutputs,
  Prehandler,
  PrehandlerResult,
} from './HandlerContainer.ts'
import type { HandlerSpy, PublicHandlerSpy } from './HandlerSpy.ts'
import { resolveHandlerSpy, TYPE_NOT_RESOLVED } from './HandlerSpy.ts'
import { MessageSchemaContainer } from './MessageSchemaContainer.ts'
import {
  isMessageTypePathConfig,
  type MessageTypeResolverConfig,
  type MessageTypeResolverContext,
  resolveMessageType,
} from './MessageTypeResolver.ts'

export type Deserializer<MessagePayloadType extends object> = (
  message: unknown,
  type: ZodType<MessagePayloadType>,
  errorProcessor: ErrorResolver,
) => Either<MessageInvalidFormatError | MessageValidationError, MessagePayloadType>

type CommonQueueLocator = {
  queueName: string
}

export type ResolvedMessage = {
  body: unknown
  attributes?: Record<string, unknown>
}

export abstract class AbstractQueueService<
  MessagePayloadSchemas extends object,
  MessageEnvelopeType extends object,
  DependenciesType extends QueueDependencies,
  QueueConfiguration extends object,
  QueueLocatorType extends object = CommonQueueLocator,
  OptionsType extends QueueOptions<QueueConfiguration, QueueLocatorType> = QueueOptions<
    QueueConfiguration,
    QueueLocatorType
  >,
  ExecutionContext = undefined,
  PrehandlerOutput = undefined,
> {
  /**
   * Used to keep track of the number of `retryLater` results received for a message to be able to
   * calculate the delay for the next retry
   */
  private readonly messageRetryLaterCountField = '_internalRetryLaterCount'
  /**
   * Used to know when the message was sent initially so we can have a max retry date and avoid
   * a infinite `retryLater` loop
   */
  protected readonly messageTimestampField: string
  /**
   * Used to know the message deduplication id
   */
  protected readonly messageDeduplicationIdField
  /**
   * Used to know the store-based message deduplication options
   */
  protected readonly messageDeduplicationOptionsField: string
  /**
   * Used to know where metadata is stored - for debug logging purposes only
   */
  protected readonly messageMetadataField: string
  protected readonly errorReporter: ErrorReporter
  public readonly logger: CommonLogger
  protected readonly messageIdField: string
  /**
   * Configuration for resolving message types.
   */
  protected readonly messageTypeResolver?: MessageTypeResolverConfig
  protected readonly logMessages: boolean
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType
  protected readonly deletionConfig?: DeletionConfig
  protected readonly payloadStoreConfig?:
    | MakeRequired<SinglePayloadStoreConfig, 'serializer'>
    | MakeRequired<MultiPayloadStoreConfig, 'serializer'>
  protected readonly messageDeduplicationConfig?: MessageDeduplicationConfig
  protected readonly messageMetricsManager?: MessageMetricsManager<MessagePayloadSchemas>
  protected readonly _handlerSpy?: HandlerSpy<MessagePayloadSchemas>
  protected readonly skipCompressionBelow: number
  protected readonly disableCodecAutoDetection: boolean
  /**
   * Codec handler resolved from the `codec` option, used by `prepareOutgoingPayload`.
   * Undefined when no codec is configured (the common case).
   */
  protected readonly resolvedCodecHandler?: MessageCodecHandler
  /** Codec name matching `resolvedCodecHandler`, written into every codec envelope. */
  protected readonly resolvedCodecName?: string

  protected isInitted: boolean

  get handlerSpy(): PublicHandlerSpy<MessagePayloadSchemas> {
    if (!this._handlerSpy) {
      throw new Error(
        'HandlerSpy was not instantiated, please pass `handlerSpy` parameter during queue service creation.',
      )
    }
    return this._handlerSpy
  }

  constructor(
    { errorReporter, logger, messageMetricsManager }: DependenciesType,
    options: OptionsType,
  ) {
    this.errorReporter = errorReporter
    this.logger = logger
    this.messageMetricsManager = messageMetricsManager

    this.messageIdField = options.messageIdField ?? 'id'
    this.messageTypeResolver = options.messageTypeResolver
    this.messageTimestampField = options.messageTimestampField ?? 'timestamp'
    this.messageDeduplicationIdField = options.messageDeduplicationIdField ?? 'deduplicationId'
    this.messageDeduplicationOptionsField =
      options.messageDeduplicationOptionsField ?? 'deduplicationOptions'
    this.messageMetadataField = options.messageMetadataField ?? 'metadata'
    this.creationConfig = options.creationConfig
    this.locatorConfig = options.locatorConfig
    this.deletionConfig = options.deletionConfig
    this.payloadStoreConfig = options.payloadStoreConfig
      ? {
          serializer: jsonStreamStringifySerializer,
          ...options.payloadStoreConfig,
        }
      : undefined
    this.messageDeduplicationConfig = options.messageDeduplicationConfig

    // Codec options live on role-specific option types (`codec`/`skipCompressionBelow`
    // on publishers, `disableCodecAutoDetection` on consumers), so they are not part of
    // the shared `QueueOptions` constraint. The base class handles both roles, hence the
    // localized widening cast.
    const codecOptions = options as Partial<{
      codec: MessageCodecRegistration
      skipCompressionBelow: number
      disableCodecAutoDetection: boolean
    }>
    this.skipCompressionBelow = codecOptions.skipCompressionBelow ?? 512
    this.disableCodecAutoDetection = codecOptions.disableCodecAutoDetection ?? false
    if (codecOptions.codec) {
      this.resolvedCodecHandler = resolveCodecHandler(codecOptions.codec)
      this.resolvedCodecName = getCodecName(codecOptions.codec)
    }

    this.logMessages = options.logMessages ?? false
    this._handlerSpy = resolveHandlerSpy<MessagePayloadSchemas>(options)
    this.isInitted = false
  }

  protected resolveConsumerMessageSchemaContainer(options: {
    handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
    messageTypeResolver?: MessageTypeResolverConfig
  }) {
    const messageSchemas = options.handlers.map((entry) => ({
      schema: entry.schema,
      messageType: entry.messageType,
    }))
    const messageDefinitions = options.handlers
      .filter((entry) => entry.definition !== undefined)
      .map((entry) => ({
        // biome-ignore lint/style/noNonNullAssertion: filtered above
        definition: entry.definition!,
        messageType: entry.messageType,
      }))

    return new MessageSchemaContainer<MessagePayloadSchemas>({
      messageTypeResolver: options.messageTypeResolver,
      messageSchemas,
      messageDefinitions,
    })
  }

  protected resolvePublisherMessageSchemaContainer(options: {
    messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
    messageTypeResolver?: MessageTypeResolverConfig
  }) {
    const messageSchemas = options.messageSchemas.map((schema) => ({ schema }))

    return new MessageSchemaContainer<MessagePayloadSchemas>({
      messageTypeResolver: options.messageTypeResolver,
      messageSchemas,
      messageDefinitions: [],
    })
  }

  /**
   * Resolves message type from message data and optional attributes using messageTypeResolver.
   *
   * @param messageData - The parsed message data
   * @param messageAttributes - Optional message-level attributes (e.g., PubSub attributes)
   * @returns The resolved message type, or undefined if not configured
   */
  protected resolveMessageTypeFromMessage(
    messageData: unknown,
    messageAttributes?: Record<string, unknown>,
  ): string | undefined {
    if (this.messageTypeResolver) {
      const context: MessageTypeResolverContext = { messageData, messageAttributes }
      return resolveMessageType(this.messageTypeResolver, context)
    }

    return undefined
  }

  protected abstract resolveSchema(
    message: MessagePayloadSchemas,
  ): Either<Error, ZodSchema<MessagePayloadSchemas>>

  protected abstract resolveMessage(
    message: MessageEnvelopeType,
  ): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage>

  /**
   * Format message for logging
   */
  protected resolveMessageLog(
    _processedMessageMetadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ): unknown | null {
    return null
  }

  protected logMessageProcessed(
    processedMessageMetadata: ProcessedMessageMetadata<MessagePayloadSchemas>,
  ) {
    const processedMessageMetadataLog = {
      processingResult: processedMessageMetadata.processingResult,
      messageId: processedMessageMetadata.messageId,
      messageType: processedMessageMetadata.messageType,
      queueName: processedMessageMetadata.queueName,
      messageTimestamp: processedMessageMetadata.messageTimestamp,
      messageDeduplicationId: processedMessageMetadata.messageDeduplicationId,
      messageProcessingStartTimestamp: processedMessageMetadata.messageProcessingStartTimestamp,
      messageProcessingEndTimestamp: processedMessageMetadata.messageProcessingEndTimestamp,
      messageMetadata: stringValueSerializer(processedMessageMetadata.messageMetadata),
    }

    const resolvedMessageLog = this.resolveMessageLog(processedMessageMetadata)

    this.logger.debug(
      {
        processedMessageMetadata: processedMessageMetadataLog,
        ...(resolvedMessageLog ? { message: resolvedMessageLog } : {}),
      },
      `Finished processing message ${processedMessageMetadata.messageId}`,
    )
  }

  protected handleError(err: unknown, context?: Record<string, unknown>) {
    const logObject = resolveGlobalErrorLogObject(err)
    this.logger.error({
      ...logObject,
      ...context,
    })
    if (types.isNativeError(err)) {
      this.errorReporter.report({ error: err, context })
    }
  }

  protected handleMessageProcessed(params: {
    message: MessagePayloadSchemas | null
    processingResult: MessageProcessingResult
    messageProcessingStartTimestamp: number
    queueName: string
    messageId?: string
  }) {
    const { message, processingResult, messageId } = params
    const messageProcessingEndTimestamp = Date.now()

    // Resolve message type once and pass to spy for consistent type resolution
    const messageType = message
      ? (this.resolveMessageTypeFromMessage(message) ?? TYPE_NOT_RESOLVED)
      : TYPE_NOT_RESOLVED
    this._handlerSpy?.addProcessedMessage(
      {
        message,
        processingResult,
      },
      messageId,
      messageType,
    )

    const debugMessageLoggingEnabled = this.logMessages && this.logger.isLevelEnabled('debug')
    if (!debugMessageLoggingEnabled && !this.messageMetricsManager) return

    const processedMessageMetadata = this.resolveProcessedMessageMetadata(
      message,
      processingResult,
      params.messageProcessingStartTimestamp,
      messageProcessingEndTimestamp,
      params.queueName,
      messageId,
    )
    if (debugMessageLoggingEnabled) {
      this.logMessageProcessed(processedMessageMetadata)
    }
    if (this.messageMetricsManager) {
      this.messageMetricsManager.registerProcessedMessage(processedMessageMetadata)
    }
  }

  private resolveProcessedMessageMetadata(
    message: MessagePayloadSchemas | null,
    processingResult: MessageProcessingResult,
    messageProcessingStartTimestamp: number,
    messageProcessingEndTimestamp: number,
    queueName: string,
    messageId?: string,
  ): ProcessedMessageMetadata<MessagePayloadSchemas> {
    // @ts-expect-error
    const resolvedMessageId: string | undefined = message?.[this.messageIdField] ?? messageId

    const messageTimestamp = message ? this.tryToExtractTimestamp(message)?.getTime() : undefined
    const messageType = message ? this.resolveMessageTypeFromMessage(message) : undefined
    const messageDeduplicationId =
      message && this.messageDeduplicationIdField in message
        ? // @ts-expect-error
          message[this.messageDeduplicationIdField]
        : undefined
    const messageMetadata =
      message && this.messageMetadataField in message
        ? // @ts-expect-error
          message[this.messageMetadataField]
        : undefined

    return {
      processingResult,
      messageId: resolvedMessageId ?? '(unknown id)',
      messageType,
      queueName,
      message,
      messageTimestamp,
      messageDeduplicationId,
      messageProcessingStartTimestamp,
      messageProcessingEndTimestamp,
      messageMetadata,
    }
  }

  protected processPrehandlersInternal(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    message: MessagePayloadSchemas,
  ) {
    if (preHandlers.length === 0) {
      return Promise.resolve({} as PrehandlerOutput)
    }

    return new Promise<PrehandlerOutput>((resolve, reject) => {
      try {
        const preHandlerOutput = {} as PrehandlerOutput
        const next = this.resolveNextFunction(
          preHandlers,
          message,
          0,
          preHandlerOutput,
          resolve,
          reject,
        )
        next({ result: 'success' })
      } catch (err) {
        reject(err as Error)
      }
    })
  }

  protected async preHandlerBarrierInternal<BarrierOutput>(
    barrier:
      | BarrierCallback<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput, BarrierOutput>
      | undefined,
    message: MessagePayloadSchemas,
    executionContext: ExecutionContext,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    if (!barrier) {
      // @ts-expect-error
      return {
        isPassing: true,
        output: undefined,
      }
    }

    return await barrier(message, executionContext, preHandlerOutput)
  }

  shouldBeRetried(message: MessagePayloadSchemas, maxRetryDuration: number): boolean {
    const timestamp = this.tryToExtractTimestamp(message) ?? new Date()
    return !isRetryDateExceeded(timestamp, maxRetryDuration)
  }

  protected getMessageRetryDelayInSeconds(message: MessagePayloadSchemas): number {
    // if not defined, this is the first attempt
    const retries = this.tryToExtractNumberOfRetries(message) ?? 0

    // exponential backoff -> (2 ^ (attempts)) * delay
    // delay = 1 second
    return Math.pow(2, retries)
  }

  protected updateInternalProperties(message: MessagePayloadSchemas): MessagePayloadSchemas {
    const messageCopy = { ...message } // clone the message to avoid mutation

    /**
     * If the message doesn't have a timestamp field -> add it
     * will be used to prevent infinite retries on the same message
     */
    if (!this.tryToExtractTimestamp(message)) {
      // @ts-expect-error
      messageCopy[this.messageTimestampField] = new Date().toISOString()
      this.logger.warn(`${this.messageTimestampField} not defined, adding it automatically`)
    }

    /**
     * add/increment the number of retries performed to exponential message delay
     */
    const numberOfRetries = this.tryToExtractNumberOfRetries(message)
    // @ts-expect-error
    messageCopy[this.messageRetryLaterCountField] =
      numberOfRetries !== undefined ? numberOfRetries + 1 : 0

    return messageCopy
  }

  private tryToExtractTimestamp(message: MessagePayloadSchemas): Date | undefined {
    if (this.messageTimestampField in message) {
      // @ts-expect-error
      const res = toDatePreprocessor(message[this.messageTimestampField])
      if (!(res instanceof Date)) {
        throw new Error(`${this.messageTimestampField} invalid type`)
      }

      return res
    }

    return undefined
  }

  private tryToExtractNumberOfRetries(message: MessagePayloadSchemas): number | undefined {
    if (
      this.messageRetryLaterCountField in message &&
      typeof message[this.messageRetryLaterCountField] === 'number'
    ) {
      // @ts-expect-error
      return message[this.messageRetryLaterCountField]
    }

    return undefined
  }

  protected abstract resolveNextFunction(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    message: MessagePayloadSchemas,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ): (preHandlerResult: PrehandlerResult) => void

  protected resolveNextPreHandlerFunctionInternal(
    preHandlers: Prehandler<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[],
    executionContext: ExecutionContext,
    message: MessagePayloadSchemas,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ): (preHandlerResult: PrehandlerResult) => void {
    return (preHandlerResult: PrehandlerResult) => {
      if (preHandlerResult.error) {
        reject(preHandlerResult.error)
      }

      if (preHandlers.length < index + 1) {
        resolve(preHandlerOutput)
      } else {
        // biome-ignore lint/style/noNonNullAssertion: It's ok
        preHandlers[index]!(
          message,
          executionContext,
          preHandlerOutput,
          this.resolveNextPreHandlerFunctionInternal(
            preHandlers,
            executionContext,
            message,
            index + 1,
            preHandlerOutput,
            resolve,
            reject,
          ),
        )
      }
    }
  }

  protected abstract processPrehandlers(
    message: MessagePayloadSchemas,
    messageType: string,
  ): Promise<PrehandlerOutput>

  protected abstract preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadSchemas,
    messageType: string,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>>

  protected abstract processMessage(
    message: MessagePayloadSchemas,
    messageType: string,
    // biome-ignore lint/suspicious/noExplicitAny: This is expected
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>>

  public abstract close(): Promise<unknown>

  /**
   * Resolves the store and store name for outgoing (publishing) messages.
   * For multi-store: uses outgoingStore from config.
   * For single-store: uses the configured store and storeName.
   * @throws Error if payloadStoreConfig is not configured or the named store is not found.
   */
  private resolveOutgoingStore(): { store: PayloadStore; storeName: string } {
    if (!this.payloadStoreConfig) {
      throw new Error('Payload store is not configured')
    }

    if (isMultiPayloadStoreConfig(this.payloadStoreConfig)) {
      const storeName = this.payloadStoreConfig.outgoingStore
      const store = this.payloadStoreConfig.stores[storeName]
      if (!store) {
        throw new Error(
          `Outgoing store "${storeName}" not found in stores configuration. Available stores: ${Object.keys(this.payloadStoreConfig.stores).join(', ')}`,
        )
      }
      return { store, storeName }
    }

    // Single-store configuration
    return { store: this.payloadStoreConfig.store, storeName: this.payloadStoreConfig.storeName }
  }

  /**
   * Resolves store from payloadRef (new format).
   */
  private resolveStoreFromPayloadRef(
    config: SinglePayloadStoreConfig | MultiPayloadStoreConfig,
    payloadRef: PayloadRef,
  ): Either<Error, { store: PayloadStore; payloadId: string }> {
    if (isMultiPayloadStoreConfig(config)) {
      const store = config.stores[payloadRef.store]
      if (!store) {
        return {
          error: new Error(
            `Store "${payloadRef.store}" specified in payloadRef not found in stores configuration. Available stores: ${Object.keys(config.stores).join(', ')}`,
          ),
        }
      }
      return { result: { store, payloadId: payloadRef.id } }
    }

    // Single-store config - validate that payloadRef.store matches configured store name
    if (payloadRef.store !== config.storeName) {
      return {
        error: new Error(
          `Store "${payloadRef.store}" specified in payloadRef does not match configured store name "${config.storeName}". ` +
            'This may indicate a misconfiguration or that the message was published by a different system. ' +
            'If you need to consume messages from multiple stores, consider using MultiPayloadStoreConfig.',
        ),
      }
    }
    return { result: { store: config.store, payloadId: payloadRef.id } }
  }

  /**
   * Resolves store from legacy pointer (old format).
   */
  private resolveStoreFromLegacyPointer(
    config: SinglePayloadStoreConfig | MultiPayloadStoreConfig,
    legacyPointer: string,
  ): Either<Error, { store: PayloadStore; payloadId: string }> {
    if (isMultiPayloadStoreConfig(config)) {
      if (!config.defaultIncomingStore) {
        return {
          error: new Error(
            'Message contains legacy offloadedPayloadPointer format, but no defaultIncomingStore is configured in multi-store setup. Please configure defaultIncomingStore or migrate messages to use payloadRef format.',
          ),
        }
      }
      const store = config.stores[config.defaultIncomingStore]
      if (!store) {
        return {
          error: new Error(
            `Default incoming store "${config.defaultIncomingStore}" not found in stores configuration. Available stores: ${Object.keys(config.stores).join(', ')}`,
          ),
        }
      }
      return { result: { store, payloadId: legacyPointer } }
    }
    return { result: { store: config.store, payloadId: legacyPointer } }
  }

  /**
   * Resolves the store for incoming (consuming) messages based on payload reference.
   * For multi-store with payloadRef: uses the store specified in payloadRef.
   * For multi-store with legacy format: uses defaultIncomingStore.
   * For single-store: always uses the configured store.
   */
  private resolveIncomingStore(
    payloadRef: PayloadRef | undefined,
    legacyPointer: string | undefined,
  ): Either<Error, { store: PayloadStore; payloadId: string }> {
    if (!this.payloadStoreConfig) {
      return { error: new Error('Payload store is not configured') }
    }

    if (payloadRef) {
      return this.resolveStoreFromPayloadRef(this.payloadStoreConfig, payloadRef)
    }

    if (legacyPointer) {
      return this.resolveStoreFromLegacyPointer(this.payloadStoreConfig, legacyPointer)
    }

    return {
      error: new Error(
        'Invalid offloaded payload: neither payloadRef nor offloadedPayloadPointer is present',
      ),
    }
  }

  /**
   * Builds an OffloadedPayloadPointerPayload from the given message and storage metadata.
   * Copies identity fields and preserves the message type field through offloading.
   *
   * We default to the conventional top-level `type` path so that routing/identity fields are
   * handled consistently with `messageIdField`/`messageTimestampField`/etc. Without this
   * fallback, `messageTypeResolver` modes that don't specify a body path silently strip `type`
   * from the offloaded body, breaking downstream SNS subscription FilterPolicy filters.
   */
  private buildPointer(
    message: MessagePayloadSchemas,
    payloadId: string,
    storeName: string,
    size: number,
    codecName?: string,
  ): OffloadedPayloadPointerPayload {
    const result: OffloadedPayloadPointerPayload = {
      payloadRef: {
        id: payloadId,
        store: storeName,
        size,
        ...(codecName ? { codec: codecName } : {}),
      },
      offloadedPayloadPointer: payloadId,
      offloadedPayloadSize: size,
      // @ts-expect-error
      [this.messageIdField]: message[this.messageIdField],
      // @ts-expect-error
      [this.messageTimestampField]: message[this.messageTimestampField],
      // @ts-expect-error
      [this.messageDeduplicationIdField]: message[this.messageDeduplicationIdField],
      // @ts-expect-error
      [this.messageDeduplicationOptionsField]: message[this.messageDeduplicationOptionsField],
    }

    const typePath =
      this.messageTypeResolver && isMessageTypePathConfig(this.messageTypeResolver)
        ? this.messageTypeResolver.messageTypePath
        : 'type'
    const typeValue = getProperty(message, typePath)
    if (typeValue !== undefined) {
      setProperty(result, typePath, typeValue)
    }

    return result
  }

  /**
   * Offloads the message payload to the configured store if it exceeds the size threshold.
   * Returns null if no offloading is needed (store not configured or message fits within threshold).
   *
   * For multi-store configuration, uses the configured outgoingStore.
   * For single-store configuration, uses the single store.
   *
   * The returned pointer includes both the new payloadRef format and legacy fields for backward
   * compatibility. The message type field is always preserved through offloading.
   */
  protected async offloadPayload(
    message: MessagePayloadSchemas,
    messageSizeFn: () => number,
  ): Promise<OffloadedPayloadPointerPayload | null> {
    if (!this.payloadStoreConfig) {
      return null
    }

    if (messageSizeFn() <= this.payloadStoreConfig.messageSizeThreshold) {
      return null
    }

    const { store, storeName } = this.resolveOutgoingStore()
    const serializedPayload = await this.payloadStoreConfig.serializer.serialize(message)

    let payloadId: string
    try {
      payloadId = await store.storePayload(serializedPayload)
    } finally {
      if (isDestroyable(serializedPayload)) {
        await serializedPayload.destroy()
      }
    }

    return this.buildPointer(message, payloadId, storeName, serializedPayload.size)
  }

  /**
   * Compresses (when codec is configured) or offloads (when a payload store is configured)
   * the outgoing message. Shared by all publisher subclasses via the `resolvedCodecHandler`
   * / `resolvedCodecName` fields resolved from the `codec` option in the base constructor.
   *
   * Returns:
   *  - `{ payload, preBuiltBody }` — `preBuiltBody` is a ready-to-send wire body string
   *    (codec envelope, or plain JSON when compression was skipped); `sendMessage` must
   *    use it as-is.
   *  - `{ payload }` — the payload is sent through the normal `JSON.stringify` path
   *    (no codec configured), optionally replaced by an offloaded-payload pointer.
   */
  protected async prepareOutgoingPayload(message: MessagePayloadSchemas): Promise<{
    payload: MessagePayloadSchemas | OffloadedPayloadPointerPayload
    preBuiltBody?: string
  }> {
    const handler = this.resolvedCodecHandler
    const codecName = this.resolvedCodecName

    if (handler && codecName) {
      // Serialize once up-front. The result is reused to apply `skipCompressionBelow`
      // and, on the inline path, as the codec input / plain-JSON wire body — so the
      // message is never stringified twice.
      const json = JSON.stringify(message)

      // Skip compression for messages below the configured floor — small payloads
      // often grow rather than shrink when compressed. Honored whether or not a
      // payload store is configured.
      if (Buffer.byteLength(json, 'utf8') < this.skipCompressionBelow) {
        if (this.payloadStoreConfig) {
          const pointer = await this.offloadPayload(message, () =>
            this.calculateOutgoingMessageSize(message),
          )
          return { payload: pointer ?? message }
        }
        // Reuse the already-serialized JSON as the wire body — no second stringify.
        return { payload: message, preBuiltBody: json }
      }

      if (this.payloadStoreConfig) {
        // Compress once, then offload or inline based on the codec envelope wire size.
        const result = await this.compressAndOffloadPayload(message, handler, codecName)
        if (result.pointer) {
          return { payload: result.pointer }
        }
        return {
          payload: message,
          preBuiltBody: buildCodecEnvelope(result.compressedBuffer, codecName),
        }
      }

      // No offload store — bounded by the transport limit (SQS/SNS 256 KB), safe to buffer.
      const compressed = await handler.compress(Buffer.from(json, 'utf8'))
      return {
        payload: message,
        preBuiltBody: buildCodecEnvelope(compressed, codecName),
      }
    }

    return {
      payload:
        (await this.offloadPayload(message, () => this.calculateOutgoingMessageSize(message))) ??
        message,
    }
  }

  /**
   * Estimates the wire size in bytes of the codec envelope wrapping `compressedSize`
   * compressed bytes. The envelope is `{"__mqtCodec":"<name>","__mqtData":"<base64>"}`:
   * base64 expands the payload to `⌈N/3⌉×4`, and the fixed JSON framing adds 32 chars
   * plus the codec name length.
   */
  protected estimateCodecEnvelopeSize(compressedSize: number, codecName: string): number {
    return Math.ceil(compressedSize / 3) * 4 + 32 + codecName.length
  }

  /**
   * Returns the wire size of the outgoing message in bytes, used by `offloadPayload` to decide
   * whether the payload exceeds `messageSizeThreshold`.
   *
   * Overridden by publisher subclasses (SQS, SNS) to call their transport-specific utility.
   * Not called on the consumer path; consumers do not override this method.
   */
  protected calculateOutgoingMessageSize(_message: MessagePayloadSchemas): number {
    /* c8 ignore next */
    throw new Error('calculateOutgoingMessageSize must be implemented by the publisher subclass')
  }

  /**
   * Compress-and-offload path used when both `codec` and `payloadStoreConfig` are set.
   * The caller (`prepareOutgoingPayload`) has already applied `skipCompressionBelow`, so
   * this method always compresses.
   *
   * **In-memory fast path (string payloads):** when the serializer produces a string and its
   * byte length is below `messageSizeThreshold`, the payload is compressed directly into a
   * Buffer in memory — no temp file is created, no disk I/O occurs.
   *
   * **Streaming path (stream payloads or large strings):** serializes the payload once,
   * pipes it through the codec Transform into a temp file.
   *
   * Either way, the offload decision is made against the **codec envelope wire size**
   * (base64-encoded compressed bytes + JSON framing), not the raw compressed byte count:
   * compression does not always shrink data, so the base64 envelope can exceed
   * `messageSizeThreshold` even when the raw payload did not.
   *
   * @returns
   *  - `{ pointer }` — compressed payload was offloaded; use pointer as the message payload
   *  - `{ compressedBuffer }` — compressed payload fits inline; caller builds the codec envelope
   */
  protected async compressAndOffloadPayload(
    message: MessagePayloadSchemas,
    handler: MessageCodecHandler,
    codecName: string,
  ): Promise<
    | { pointer: OffloadedPayloadPointerPayload; compressedBuffer?: never }
    | { compressedBuffer: Buffer; pointer?: never }
  > {
    if (!this.payloadStoreConfig) {
      throw new Error('Payload store is not configured')
    }
    const threshold = this.payloadStoreConfig.messageSizeThreshold

    const serialized = await this.payloadStoreConfig.serializer.serialize(message)

    try {
      // In-memory fast path: avoid disk I/O entirely for small string payloads.
      if (
        typeof serialized.value === 'string' &&
        Buffer.byteLength(serialized.value, 'utf8') < threshold
      ) {
        const compressed = await handler.compress(Buffer.from(serialized.value, 'utf8'))
        // The wire body is a base64 codec envelope, which can exceed the threshold even
        // when the raw payload did not (compression does not always shrink). Re-check the
        // envelope size and offload the compressed bytes if it no longer fits inline.
        if (this.estimateCodecEnvelopeSize(compressed.length, codecName) > threshold) {
          const { store, storeName } = this.resolveOutgoingStore()
          const payloadId = await store.storePayload({
            value: Readable.from(compressed),
            size: compressed.length,
          })
          return {
            pointer: this.buildPointer(message, payloadId, storeName, compressed.length, codecName),
          }
        }
        return { compressedBuffer: compressed }
      }

      // Streaming pipeline: serializer output → codec transform → temp file.
      // No full-payload buffer is materialised; each codec supplies its own Transform.
      const tmpPath = path.join(os.tmpdir(), randomUUID())
      try {
        await pipeline(
          typeof serialized.value === 'string' ? Readable.from(serialized.value) : serialized.value,
          handler.createCompressStream(),
          fs.createWriteStream(tmpPath),
        )

        const compressedSize = (await fs.promises.stat(tmpPath)).size

        // Compare the envelope wire size (not raw compressed bytes) against the threshold.
        if (this.estimateCodecEnvelopeSize(compressedSize, codecName) > threshold) {
          const { store, storeName } = this.resolveOutgoingStore()
          const payloadId = await store.storePayload({
            value: fs.createReadStream(tmpPath),
            size: compressedSize,
          })
          return {
            pointer: this.buildPointer(message, payloadId, storeName, compressedSize, codecName),
          }
        }

        // Compressed payload fits inline — return the buffer; caller wraps it in a codec envelope.
        return { compressedBuffer: await fs.promises.readFile(tmpPath) }
      } finally {
        try {
          await fs.promises.unlink(tmpPath)
        } catch {
          // ignore cleanup errors
        }
      }
    } finally {
      if (isDestroyable(serialized)) {
        await serialized.destroy()
      }
    }
  }

  /**
   * Retrieve previously offloaded message payload using provided pointer payload.
   * Returns the original payload or an error if the payload was not found or could not be parsed.
   *
   * Supports both new multi-store format (payloadRef) and legacy format (offloadedPayloadPointer).
   *
   * When `resolveDecompressor` is provided and the pointer's `payloadRef.codec` is set, the
   * fetched bytes are treated as raw compressed binary and decompressed before JSON parsing.
   * `resolveDecompressor` is invoked *outside* the catch block: if it throws (e.g. the codec
   * named in the pointer is not registered on this consumer — a deployment misconfiguration,
   * not a bad message), the throw propagates as a retriable error so the message stays on the
   * queue instead of being silently routed to the DLQ.
   */
  protected async retrieveOffloadedMessagePayload(
    maybeOffloadedPayloadPointerPayload: unknown,
    resolveDecompressor?: (codec: string) => (data: Buffer) => Promise<Buffer>,
  ): Promise<Either<Error, unknown>> {
    if (!this.payloadStoreConfig) {
      return {
        error: new Error(
          'Payload store is not configured, cannot retrieve offloaded message payload',
        ),
      }
    }

    const pointerPayloadParseResult = OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA.safeParse(
      maybeOffloadedPayloadPointerPayload,
    )
    if (!pointerPayloadParseResult.success) {
      return {
        error: new Error('Given payload is not a valid offloaded payload pointer payload', {
          cause: pointerPayloadParseResult.error,
        }),
      }
    }

    const parsedPayload = pointerPayloadParseResult.data

    // Resolve which store to use
    const storeResult = this.resolveIncomingStore(
      parsedPayload.payloadRef,
      parsedPayload.offloadedPayloadPointer,
    )
    if (storeResult.error) {
      return storeResult
    }

    const { store, payloadId } = storeResult.result
    const payloadSize = parsedPayload.payloadRef?.size ?? parsedPayload.offloadedPayloadSize
    if (payloadSize === undefined) {
      return {
        error: new Error(
          'Invalid offloaded payload: payload size is missing from both payloadRef and offloadedPayloadSize',
        ),
      }
    }

    // Retrieve the payload from the resolved store
    const serializedOffloadedPayloadReadable = await store.retrievePayload(payloadId)
    if (serializedOffloadedPayloadReadable === null) {
      return {
        error: new Error(`Payload with key ${payloadId} was not found in the store`),
      }
    }

    const codec = parsedPayload.payloadRef?.codec
    if (codec && resolveDecompressor) {
      // Resolve the decompressor OUTSIDE the try/catch. An unknown/unregistered codec is a
      // consumer misconfiguration, not a poison message — letting it throw here makes it a
      // retriable error (the message stays on the queue and becomes visible again after the
      // visibility timeout) instead of being silently routed to the DLQ.
      const decompress = resolveDecompressor(codec)
      // The stream read is likewise kept outside the try/catch so transient retrieval errors
      // (truncated S3 stream, network blip) propagate as thrown exceptions and are retried.
      // Only deterministic failures (corrupt compressed bytes, invalid JSON after
      // decompression) are caught and returned as { error }, which the consumer treats as a
      // poison message and routes to the DLQ. This mirrors the non-codec path below.
      const compressedBuffer = await streamWithKnownSizeToBuffer(
        serializedOffloadedPayloadReadable,
        payloadSize,
      )
      try {
        const decompressed = await decompress(compressedBuffer)
        return { result: JSON.parse(decompressed.toString('utf8')) }
      } catch (e) {
        return {
          error: new Error(`Failed to decompress offloaded payload with codec "${codec}"`, {
            cause: e,
          }),
        }
      }
    }

    const serializedOffloadedPayloadString = await streamWithKnownSizeToString(
      serializedOffloadedPayloadReadable,
      payloadSize,
    )
    try {
      return { result: JSON.parse(serializedOffloadedPayloadString) }
    } catch (e) {
      return { error: new Error('Failed to parse serialized offloaded payload', { cause: e }) }
    }
  }

  /**
   * Checks if the message is duplicated against the deduplication store.
   * Returns true if the message is duplicated.
   * Returns false if message is not duplicated or deduplication config is missing.
   */
  protected async isMessageDuplicated(
    message: MessagePayloadSchemas,
    requester: DeduplicationRequester,
  ): Promise<boolean> {
    if (!this.isDeduplicationEnabledForMessage(message)) {
      return false
    }

    const deduplicationId = this.getMessageDeduplicationId(message) as string
    const deduplicationConfig = this.messageDeduplicationConfig as MessageDeduplicationConfig

    try {
      return await deduplicationConfig.deduplicationStore.keyExists(
        `${requester.toString()}:${deduplicationId}`,
      )
    } catch (err) {
      this.handleError(err)
      // In case of errors, we treat the message as not duplicated to enable further processing
      return false
    }
  }

  /**
   * Checks if the message is duplicated.
   * If it's not, stores the deduplication key in the deduplication store and returns false.
   * If it is, returns true.
   * If deduplication config is not provided, always returns false to allow further processing of the message.
   */
  protected async deduplicateMessage(
    message: MessagePayloadSchemas,
    requester: DeduplicationRequester,
  ): Promise<{ isDuplicated: boolean }> {
    if (!this.isDeduplicationEnabledForMessage(message)) {
      return { isDuplicated: false }
    }

    const deduplicationId = this.getMessageDeduplicationId(message) as string
    const { deduplicationWindowSeconds } = this.getParsedMessageDeduplicationOptions(message)
    const deduplicationConfig = this.messageDeduplicationConfig as MessageDeduplicationConfig

    try {
      const wasDeduplicationKeyStored = await deduplicationConfig.deduplicationStore.setIfNotExists(
        `${requester.toString()}:${deduplicationId}`,
        new Date().toISOString(),
        deduplicationWindowSeconds,
      )

      return { isDuplicated: !wasDeduplicationKeyStored }
    } catch (err) {
      this.handleError(err)
      // In case of errors, we treat the message as not duplicated to enable further processing
      return { isDuplicated: false }
    }
  }

  /**
   * Acquires exclusive lock for the message to prevent concurrent processing.
   * If lock was acquired successfully, returns a lock object that should be released after processing.
   * If lock couldn't be acquired due to timeout (meaning another process acquired it earlier), returns AcquireLockTimeoutError
   * If lock couldn't be acquired for any other reasons or if deduplication config is not provided, always returns a lock object that does nothing, so message processing can continue.
   */
  protected async acquireLockForMessage(
    message: MessagePayloadSchemas,
  ): Promise<Either<AcquireLockTimeoutError, ReleasableLock>> {
    if (!this.isDeduplicationEnabledForMessage(message)) {
      return { result: noopReleasableLock }
    }

    const deduplicationId = this.getMessageDeduplicationId(message) as string
    const deduplicationOptions = this.getParsedMessageDeduplicationOptions(message)
    const deduplicationConfig = this.messageDeduplicationConfig as MessageDeduplicationConfig

    const acquireLockResult = await deduplicationConfig.deduplicationStore.acquireLock(
      `${DeduplicationRequesterEnum.Consumer.toString()}:${deduplicationId}`,
      deduplicationOptions,
    )

    if (acquireLockResult.error && !isAcquireLockTimeoutError(acquireLockResult.error)) {
      this.handleError(acquireLockResult.error)
      return { result: noopReleasableLock }
    }

    return acquireLockResult
  }

  protected isDeduplicationEnabledForMessage(message: MessagePayloadSchemas): boolean {
    return !!this.messageDeduplicationConfig && !!this.getMessageDeduplicationId(message)
  }

  protected getMessageDeduplicationId(message: MessagePayloadSchemas): string | undefined {
    // @ts-expect-error
    return message[this.messageDeduplicationIdField]
  }

  private getParsedMessageDeduplicationOptions(
    message: MessagePayloadSchemas,
  ): Required<MessageDeduplicationOptions> {
    const parsedOptions = MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.safeParse(
      // @ts-expect-error
      message[this.messageDeduplicationOptionsField] ?? {},
    )

    if (parsedOptions.error) {
      this.logger.warn(
        { error: parsedOptions.error.message },
        `${this.messageDeduplicationOptionsField} contains one or more invalid values, falling back to default options`,
      )

      return DEFAULT_MESSAGE_DEDUPLICATION_OPTIONS
    }

    return {
      ...DEFAULT_MESSAGE_DEDUPLICATION_OPTIONS,
      ...Object.fromEntries(
        Object.entries(parsedOptions.data).filter(([_, value]) => value !== undefined),
      ),
    }
  }
}
