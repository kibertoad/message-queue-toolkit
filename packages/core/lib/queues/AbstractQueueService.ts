import { types } from 'node:util'

import type { CommonLogger, Either, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import { resolveGlobalErrorLogObject } from '@lokalise/node-core'
import type { CommonEventDefinition } from '@message-queue-toolkit/schemas'
import type { ZodSchema, ZodType } from 'zod'

import {
  MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA,
  type MessageDeduplicationOptions,
} from '@message-queue-toolkit/schemas'
import type { MessageInvalidFormatError, MessageValidationError } from '../errors/Errors'
import {
  AcquireLockTimeoutError,
  DEFAULT_MESSAGE_DEDUPLICATION_OPTIONS,
  DeduplicationRequester,
  type MessageDeduplicationConfig,
  type ReleasableLock,
  noopReleasableLock,
} from '../message-deduplication/messageDeduplicationTypes'
import { jsonStreamStringifySerializer } from '../payload-store/JsonStreamStringifySerializer'
import {
  OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA,
  type OffloadedPayloadPointerPayload,
} from '../payload-store/offloadedPayloadMessageSchemas'
import type { PayloadStoreConfig } from '../payload-store/payloadStoreTypes'
import { isDestroyable } from '../payload-store/payloadStoreTypes'
import type { MessageProcessingResult } from '../types/MessageQueueTypes'
import type {
  DeletionConfig,
  MessageMetricsManager,
  ProcessedMessageMetadata,
  QueueDependencies,
  QueueOptions,
} from '../types/queueOptionsTypes'
import { isRetryDateExceeded } from '../utils/dateUtils'
import { streamWithKnownSizeToString } from '../utils/streamUtils'
import { toDatePreprocessor } from '../utils/toDateProcessor'
import type {
  BarrierCallback,
  BarrierResult,
  MessageHandlerConfig,
  PreHandlingOutputs,
  Prehandler,
  PrehandlerResult,
} from './HandlerContainer'
import type { HandlerSpy, PublicHandlerSpy } from './HandlerSpy'
import { resolveHandlerSpy } from './HandlerSpy'
import { MessageSchemaContainer } from './MessageSchemaContainer'

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
  protected readonly messageTypeField: string
  protected readonly logMessages: boolean
  protected readonly creationConfig?: QueueConfiguration
  protected readonly locatorConfig?: QueueLocatorType
  protected readonly deletionConfig?: DeletionConfig
  protected readonly payloadStoreConfig?: Omit<PayloadStoreConfig, 'serializer'> &
    Required<Pick<PayloadStoreConfig, 'serializer'>>
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
    this.messageTypeField = options.messageTypeField
    this.messageTimestampField = options.messageTimestampField ?? 'timestamp'
    this.messageDeduplicationIdField = options.messageDeduplicationIdField ?? 'deduplicationId'
    this.messageDeduplicationOptionsField =
      options.messageDeduplicationOptionsField ?? 'deduplicationOptions'
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

    this.logMessages = options.logMessages ?? false
    this._handlerSpy = resolveHandlerSpy<MessagePayloadSchemas>(options)
    this.isInitted = false
  }

  protected resolveConsumerMessageSchemaContainer(options: {
    handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
    messageTypeField: string
  }) {
    const messageSchemas = options.handlers.map((entry) => entry.schema)
    const messageDefinitions: CommonEventDefinition[] = options.handlers
      .map((entry) => entry.definition)
      .filter((entry) => entry !== undefined)

    return new MessageSchemaContainer<MessagePayloadSchemas>({
      messageSchemas,
      messageDefinitions,
      messageTypeField: options.messageTypeField,
    })
  }

  protected resolvePublisherMessageSchemaContainer(options: {
    messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
    messageTypeField: string
  }) {
    const messageSchemas = options.messageSchemas
    const messageDefinitions: readonly CommonEventDefinition[] = []

    return new MessageSchemaContainer<MessagePayloadSchemas>({
      messageSchemas,
      messageDefinitions,
      messageTypeField: options.messageTypeField,
    })
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

    this._handlerSpy?.addProcessedMessage(
      {
        message,
        processingResult,
      },
      messageId,
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
        processedMessageMetadata,
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
    // @ts-ignore
    const resolvedMessageId: string | undefined = message?.[this.messageIdField] ?? messageId

    const messageTimestamp = message ? this.tryToExtractTimestamp(message)?.getTime() : undefined
    const messageType =
      message && this.messageTypeField in message
        ? // @ts-ignore
          message[this.messageTypeField]
        : undefined
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
      // @ts-ignore
      return {
        isPassing: true,
        output: undefined,
      }
    }

    // @ts-ignore
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
      // @ts-ignore
      messageCopy[this.messageTimestampField] = new Date().toISOString()
      this.logger.warn(`${this.messageTimestampField} not defined, adding it automatically`)
    }

    /**
     * add/increment the number of retries performed to exponential message delay
     */
    const numberOfRetries = this.tryToExtractNumberOfRetries(message)
    // @ts-ignore
    messageCopy[this.messageRetryLaterCountField] =
      numberOfRetries !== undefined ? numberOfRetries + 1 : 0

    return messageCopy
  }

  private tryToExtractTimestamp(message: MessagePayloadSchemas): Date | undefined {
    // @ts-ignore
    if (this.messageTimestampField in message) {
      // @ts-ignore
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
      // @ts-ignore
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

  // eslint-disable-next-line max-params
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
        // eslint-disable-next-line @typescript-eslint/no-unsafe-call
        preHandlers[index](
          message,
          executionContext,
          // @ts-ignore
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
   * Offload message payload to an external store if it exceeds the threshold.
   * Returns a special type that contains a pointer to the offloaded payload or the original payload if it was not offloaded.
   * Requires message size as only the implementation knows how to calculate it.
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

    let offloadedPayloadPointer: string
    const serializedPayload = await this.payloadStoreConfig.serializer.serialize(message)
    try {
      offloadedPayloadPointer = await this.payloadStoreConfig.store.storePayload(serializedPayload)
    } finally {
      if (isDestroyable(serializedPayload)) {
        await serializedPayload.destroy()
      }
    }

    return {
      offloadedPayloadPointer,
      offloadedPayloadSize: serializedPayload.size,
      // @ts-ignore
      [this.messageIdField]: message[this.messageIdField],
      // @ts-ignore
      [this.messageTypeField]: message[this.messageTypeField],
      // @ts-ignore
      [this.messageTimestampField]: message[this.messageTimestampField],
      // @ts-ignore
      [this.messageDeduplicationIdField]: message[this.messageDeduplicationIdField],
      // @ts-ignore
      [this.messageDeduplicationOptionsField]: message[this.messageDeduplicationOptionsField],
    }
  }

  /**
   * Retrieve previously offloaded message payload using provided pointer payload.
   * Returns the original payload or an error if the payload was not found or could not be parsed.
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

    const serializedOffloadedPayloadReadable = await this.payloadStoreConfig.store.retrievePayload(
      pointerPayloadParseResult.data.offloadedPayloadPointer,
    )
    if (serializedOffloadedPayloadReadable === null) {
      return {
        error: new Error(
          `Payload with key ${pointerPayloadParseResult.data.offloadedPayloadPointer} was not found in the store`,
        ),
      }
    }

    const serializedOffloadedPayloadString = await streamWithKnownSizeToString(
      serializedOffloadedPayloadReadable,
      pointerPayloadParseResult.data.offloadedPayloadSize,
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
      `${DeduplicationRequester.Consumer.toString()}:${deduplicationId}`,
      deduplicationOptions,
    )

    if (acquireLockResult.error && !(acquireLockResult.error instanceof AcquireLockTimeoutError)) {
      this.handleError(acquireLockResult.error)
      return { result: noopReleasableLock }
    }

    return acquireLockResult
  }

  protected isDeduplicationEnabledForMessage(message: MessagePayloadSchemas): boolean {
    return !!this.messageDeduplicationConfig && !!this.getMessageDeduplicationId(message)
  }

  protected getMessageDeduplicationId(message: MessagePayloadSchemas): string | undefined {
    // @ts-ignore
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
