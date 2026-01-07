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
  ResourceAvailabilityConfig,
} from '../types/queueOptionsTypes.ts'
import { isRetryDateExceeded } from '../utils/dateUtils.ts'
import { streamWithKnownSizeToString } from '../utils/streamUtils.ts'
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
  protected readonly resourceAvailabilityConfig?: ResourceAvailabilityConfig
  protected readonly payloadStoreConfig?:
    | MakeRequired<SinglePayloadStoreConfig, 'serializer'>
    | MakeRequired<MultiPayloadStoreConfig, 'serializer'>
  protected readonly messageDeduplicationConfig?: MessageDeduplicationConfig
  protected readonly messageMetricsManager?: MessageMetricsManager<MessagePayloadSchemas>
  protected readonly _handlerSpy?: HandlerSpy<MessagePayloadSchemas>

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
    this.creationConfig = options.creationConfig
    this.locatorConfig = options.locatorConfig
    this.deletionConfig = options.deletionConfig
    this.resourceAvailabilityConfig = options.resourceAvailabilityConfig
    this.payloadStoreConfig = options.payloadStoreConfig
      ? {
          serializer: jsonStreamStringifySerializer,
          ...options.payloadStoreConfig,
        }
      : undefined
    this.messageDeduplicationConfig = options.messageDeduplicationConfig

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
  protected resolveMessageLog(message: MessagePayloadSchemas, _messageType: string): unknown {
    return message
  }

  /**
   * Log preformatted and potentially presanitized message payload
   */
  protected logMessage(messageLogEntry: unknown) {
    this.logger.debug(messageLogEntry)
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

    const debugLoggingEnabled = this.logMessages && this.logger.isLevelEnabled('debug')
    if (!debugLoggingEnabled && !this.messageMetricsManager) return

    const processedMessageMetadata = this.resolveProcessedMessageMetadata(
      message,
      processingResult,
      params.messageProcessingStartTimestamp,
      messageProcessingEndTimestamp,
      params.queueName,
      messageId,
    )
    if (debugLoggingEnabled) {
      this.logger.debug(
        { processedMessageMetadata: stringValueSerializer(processedMessageMetadata) },
        `Finished processing message ${processedMessageMetadata.messageId}`,
      )
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
        ? // @ts-ignore
          message[this.messageDeduplicationId]
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
   * Offload message payload to an external store if it exceeds the threshold.
   * Returns a special type that contains a pointer to the offloaded payload or the original payload if it was not offloaded.
   * Requires message size as only the implementation knows how to calculate it.
   *
   * For multi-store configuration, uses the configured outgoingStore.
   * For single-store configuration, uses the single store.
   *
   * The returned payload includes both the new payloadRef format and legacy fields for backward compatibility.
   */
  protected async offloadMessagePayloadIfNeeded(
    message: MessagePayloadSchemas,
    messageSizeFn: () => number,
  ): Promise<MessagePayloadSchemas | OffloadedPayloadPointerPayload> {
    if (
      !this.payloadStoreConfig ||
      messageSizeFn() <= this.payloadStoreConfig.messageSizeThreshold
    ) {
      return message
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

    // Return message with both new and legacy formats for backward compatibility
    const result: OffloadedPayloadPointerPayload = {
      // Extended payload reference format
      payloadRef: {
        id: payloadId,
        store: storeName,
        size: serializedPayload.size,
      },
      // Legacy format for backward compatibility
      offloadedPayloadPointer: payloadId,
      offloadedPayloadSize: serializedPayload.size,
      // @ts-expect-error
      [this.messageIdField]: message[this.messageIdField],
      // @ts-expect-error
      [this.messageTimestampField]: message[this.messageTimestampField],
      // @ts-expect-error
      [this.messageDeduplicationIdField]: message[this.messageDeduplicationIdField],
      // @ts-expect-error
      [this.messageDeduplicationOptionsField]: message[this.messageDeduplicationOptionsField],
    }

    // Preserve message type field if using messageTypePath resolver (supports nested paths)
    if (this.messageTypeResolver && isMessageTypePathConfig(this.messageTypeResolver)) {
      const messageTypePath = this.messageTypeResolver.messageTypePath
      const typeValue = getProperty(message, messageTypePath)
      if (typeValue !== undefined) {
        setProperty(result, messageTypePath, typeValue)
      }
    }

    return result
  }

  /**
   * Retrieve previously offloaded message payload using provided pointer payload.
   * Returns the original payload or an error if the payload was not found or could not be parsed.
   *
   * Supports both new multi-store format (payloadRef) and legacy format (offloadedPayloadPointer).
   */
  protected async retrieveOffloadedMessagePayload(
    maybeOffloadedPayloadPointerPayload: unknown,
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
