import { type Either, type ErrorResolver, isError } from '@lokalise/node-core'
import type {
  MessageInvalidFormatError,
  MessageValidationError,
  ResolvedMessage,
} from '@message-queue-toolkit/core'
import {
  type BarrierResult,
  DeduplicationRequesterEnum,
  HandlerContainer,
  type MessageSchemaContainer,
  noopReleasableLock,
  type PreHandlingOutputs,
  type Prehandler,
  parseMessage,
  type QueueConsumer,
  type QueueConsumerDependencies,
  type QueueConsumerOptions,
  type TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import { isRetryableGrpcError } from '../errors/grpcErrors.ts'
import { isSubscriptionDoesNotExistError } from '../errors/SubscriptionDoesNotExistError.ts'
import type { PubSubMessage } from '../types/MessageTypes.ts'
import { hasOffloadedPayload } from '../utils/messageUtils.ts'
import { deletePubSub, initPubSub } from '../utils/pubSubInitter.ts'
import { deserializePubSubMessage } from '../utils/pubSubMessageDeserializer.ts'
import type {
  PubSubCreationConfig,
  PubSubDependencies,
  PubSubQueueLocatorType,
} from './AbstractPubSubService.ts'
import { AbstractPubSubService } from './AbstractPubSubService.ts'

const DEFAULT_MAX_RETRY_DURATION = 4 * 24 * 60 * 60 // 4 days in seconds

/**
 * Default configuration for subscription error retry behavior.
 */
const DEFAULT_SUBSCRIPTION_RETRY_OPTIONS = {
  maxRetries: 5,
  baseRetryDelayMs: 1000,
  maxRetryDelayMs: 30000,
} as const

/**
 * Configuration options for subscription-level error retry behavior.
 * This handles transient errors that occur at the subscription level
 * (e.g., NOT_FOUND, PERMISSION_DENIED after Terraform deployments).
 */
export type SubscriptionRetryOptions = {
  /**
   * Maximum number of retry attempts before giving up.
   * @default 5
   */
  maxRetries?: number
  /**
   * Base delay in milliseconds for exponential backoff.
   * Actual delay = min(baseRetryDelayMs * 2^attempt, maxRetryDelayMs)
   * @default 1000
   */
  baseRetryDelayMs?: number
  /**
   * Maximum delay in milliseconds between retries.
   * @default 30000
   */
  maxRetryDelayMs?: number
}

export type PubSubDeadLetterQueueOptions = {
  deadLetterPolicy: {
    maxDeliveryAttempts: number
  }
  creationConfig?: {
    topic: {
      name: string
    }
  }
  locatorConfig?: {
    topicName: string
  }
}

export type PubSubConsumerDependencies = PubSubDependencies & QueueConsumerDependencies

export type PubSubConsumerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
  QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
> = QueueConsumerOptions<
  CreationConfigType,
  QueueLocatorType,
  PubSubDeadLetterQueueOptions,
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  PubSubCreationConfig,
  PubSubQueueLocatorType
> & {
  consumerOverrides?: {
    flowControl?: {
      maxMessages?: number
      maxBytes?: number
    }
    batching?: {
      maxMessages?: number
      maxMilliseconds?: number
    }
  }
  /**
   * Configuration for subscription-level error retry behavior.
   * Handles transient errors like NOT_FOUND and PERMISSION_DENIED
   * that can occur after Terraform deployments due to GCP's eventual consistency.
   */
  subscriptionRetryOptions?: SubscriptionRetryOptions
}

export abstract class AbstractPubSubConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    CreationConfigType extends PubSubCreationConfig = PubSubCreationConfig,
    QueueLocatorType extends PubSubQueueLocatorType = PubSubQueueLocatorType,
    ConsumerOptionsType extends PubSubConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > = PubSubConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    >,
  >
  extends AbstractPubSubService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    PubSubConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput
  >
  implements QueueConsumer
{
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly consumerOverrides: Partial<ConsumerOptionsType['consumerOverrides']>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >
  private readonly deadLetterQueueOptions?: PubSubDeadLetterQueueOptions
  private readonly isDeduplicationEnabled: boolean
  private readonly subscriptionRetryOptions: Required<SubscriptionRetryOptions>
  private maxRetryDuration: number
  private isConsuming = false
  private isReinitializing = false
  private _fatalError: Error | null = null

  protected readonly errorResolver: ErrorResolver
  protected readonly executionContext: ExecutionContext

  public dlqTopicName?: string
  public readonly _messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  /**
   * Returns the fatal error that caused the consumer to stop, or null if healthy.
   * Use this in health checks to detect permanent subscription failures.
   *
   * @example
   * ```typescript
   * app.get('/health', (req, res) => {
   *   const error = consumer.fatalError
   *   if (error) {
   *     return res.status(503).json({ status: 'unhealthy', error: error.message })
   *   }
   *   return res.status(200).json({ status: 'healthy' })
   * })
   * ```
   */
  public get fatalError(): Error | null {
    return this._fatalError
  }

  protected constructor(
    dependencies: PubSubConsumerDependencies,
    options: ConsumerOptionsType,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver
    this.consumerOverrides = options.consumerOverrides ?? {}
    this.deadLetterQueueOptions = options.deadLetterQueue
    this.maxRetryDuration = options.maxRetryDuration ?? DEFAULT_MAX_RETRY_DURATION
    this.executionContext = executionContext
    this.isDeduplicationEnabled = !!options.enableConsumerDeduplication
    this.subscriptionRetryOptions = {
      maxRetries:
        options.subscriptionRetryOptions?.maxRetries ??
        DEFAULT_SUBSCRIPTION_RETRY_OPTIONS.maxRetries,
      baseRetryDelayMs:
        options.subscriptionRetryOptions?.baseRetryDelayMs ??
        DEFAULT_SUBSCRIPTION_RETRY_OPTIONS.baseRetryDelayMs,
      maxRetryDelayMs:
        options.subscriptionRetryOptions?.maxRetryDelayMs ??
        DEFAULT_SUBSCRIPTION_RETRY_OPTIONS.maxRetryDelayMs,
    }

    this._messageSchemaContainer = this.resolveConsumerMessageSchemaContainer(options)
    this.handlerContainer = new HandlerContainer({
      messageHandlers: options.handlers,
      messageTypeField: options.messageTypeField,
    })
  }

  public override async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig) {
      await deletePubSub(this.pubSubClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initPubSub(
      this.pubSubClient,
      this.locatorConfig,
      this.creationConfig,
      this.deadLetterQueueOptions,
    )

    this.topicName = initResult.topicName
    this.topic = initResult.topic
    this.subscriptionName = initResult.subscriptionName
    this.subscription = initResult.subscription
    this.dlqTopicName = initResult.dlqTopicName

    this.isInitted = true
  }

  public async start(): Promise<void> {
    // Prevent starting multiple times
    if (this.isConsuming) {
      return
    }

    await this.initWithRetry()

    if (!this.subscription) {
      throw new Error('Subscription not initialized after init()')
    }

    // Wait for subscription to exist and be ready
    await this.waitForSubscriptionReady()

    this.isConsuming = true

    this.setupSubscriptionEventHandlers()
  }

  /**
   * Initializes the consumer with retry logic for transient errors.
   *
   * This handles eventual consistency issues that can occur after Terraform deployments
   * where topics/subscriptions may not be immediately visible across all GCP endpoints.
   *
   * @param attempt - Current retry attempt number (1-based)
   */
  private async initWithRetry(attempt = 1): Promise<void> {
    try {
      await this.init()
    } catch (error) {
      // Check if we should retry
      if (attempt >= this.subscriptionRetryOptions.maxRetries) {
        this.logger.error({
          msg: `Failed to initialize subscription after ${attempt} attempts`,
          subscriptionName:
            this.locatorConfig?.subscriptionName ?? this.creationConfig?.subscription?.name,
          topicName: this.locatorConfig?.topicName ?? this.creationConfig?.topic.name,
          error,
        })
        throw error
      }

      // Check if error is retryable using gRPC status codes
      const isRetryableSubscriptionError = isSubscriptionDoesNotExistError(error)
      const isRetryableGrpc = isRetryableGrpcError(error)

      if (!isRetryableSubscriptionError && !isRetryableGrpc) {
        throw error
      }

      // Calculate delay with exponential backoff
      const delay = Math.min(
        this.subscriptionRetryOptions.baseRetryDelayMs * Math.pow(2, attempt - 1),
        this.subscriptionRetryOptions.maxRetryDelayMs,
      )

      this.logger.warn({
        msg: `Retryable error during initialization, attempt ${attempt}/${this.subscriptionRetryOptions.maxRetries}, waiting ${delay}ms`,
        subscriptionName:
          this.locatorConfig?.subscriptionName ?? this.creationConfig?.subscription?.name,
        topicName: this.locatorConfig?.topicName ?? this.creationConfig?.topic.name,
        errorCode: isRetryableGrpc ? error.code : undefined,
        errorMessage:
          isRetryableGrpc || isRetryableSubscriptionError ? error.message : String(error),
        attempt,
        delayMs: delay,
      })

      await new Promise((resolve) => setTimeout(resolve, delay))
      await this.initWithRetry(attempt + 1)
    }
  }

  /**
   * Sets up event handlers for the subscription.
   * Extracted to allow reattachment after reinitialization.
   */
  private setupSubscriptionEventHandlers(): void {
    if (!this.subscription) {
      return
    }

    // Configure message handler
    this.subscription.on('message', async (message: PubSubMessage) => {
      await this.handleMessage(message)
    })

    // Configure error handler with retry logic for transient errors
    this.subscription.on('error', (error: Error & { code?: number }) => {
      this.handleSubscriptionError(error)
    })

    // Configure close handler to detect unexpected disconnections
    this.subscription.on('close', () => {
      this.handleSubscriptionClose()
    })

    // Configure flow control if provided
    // @ts-expect-error - consumerOverrides may have flowControl
    if (this.consumerOverrides?.flowControl) {
      this.subscription.setOptions({
        // @ts-expect-error - flowControl is available
        flowControl: this.consumerOverrides.flowControl,
      })
    }
  }

  /**
   * Handles subscription-level errors.
   *
   * For retryable errors (NOT_FOUND, PERMISSION_DENIED), attempts to reinitialize
   * the subscription with exponential backoff. These errors commonly occur after
   * Terraform deployments due to GCP's eventual consistency.
   *
   * @see https://cloud.google.com/pubsub/docs/reference/error-codes
   */
  private handleSubscriptionError(error: Error & { code?: number }): void {
    // Don't handle errors during shutdown
    if (!this.isConsuming) {
      return
    }

    // Check if this is a retryable subscription error using gRPC status codes
    if (isRetryableGrpcError(error)) {
      this.logger.warn({
        msg: 'Retryable subscription error occurred, attempting to reinitialize',
        subscriptionName: this.subscriptionName,
        topicName: this.topicName,
        errorCode: error.code,
        errorMessage: error.message,
      })

      // Trigger reinitialization with retry
      this.reinitializeWithRetry(1).catch((reinitError) => {
        // Mark consumer as failed - this will be visible via fatalError getter for health checks
        this._fatalError = isError(reinitError) ? reinitError : new Error(String(reinitError))
        this.isConsuming = false
        this.handleError(reinitError)
      })
    } else {
      // Non-retryable error - log and report
      this.handleError(error)
    }
  }

  /**
   * Handles unexpected subscription close events.
   *
   * If the subscription closes while we're still supposed to be consuming,
   * attempts to reinitialize. This can happen due to:
   * - Network issues
   * - GCP service restarts
   * - Subscription deletion/recreation
   */
  private handleSubscriptionClose(): void {
    this.logger.info({
      msg: 'PubSub subscription closed',
      subscriptionName: this.subscriptionName,
      topicName: this.topicName,
      isConsuming: this.isConsuming,
    })

    // If we're still supposed to be consuming, try to reinitialize
    if (this.isConsuming && !this.isReinitializing) {
      this.logger.warn({
        msg: 'Subscription closed unexpectedly while consuming, attempting to reinitialize',
        subscriptionName: this.subscriptionName,
        topicName: this.topicName,
      })

      this.reinitializeWithRetry(1).catch((reinitError) => {
        // Mark consumer as failed - this will be visible via fatalError getter for health checks
        this._fatalError = isError(reinitError) ? reinitError : new Error(String(reinitError))
        this.isConsuming = false
        this.handleError(reinitError)
      })
    }
  }

  /**
   * Reinitializes the subscription with exponential backoff retry.
   *
   * This method:
   * 1. Closes the existing subscription (if any)
   * 2. Waits with exponential backoff
   * 3. Reinitializes the subscription
   * 4. Reattaches event handlers
   *
   * Uses an iterative loop to keep isReinitializing true for the entire
   * retry sequence, preventing concurrent callers from starting their own
   * reinitialization attempts.
   *
   * @param startAttempt - Starting retry attempt number (1-based)
   */
  private async reinitializeWithRetry(startAttempt: number): Promise<void> {
    // Prevent concurrent reinitializations
    if (this.isReinitializing) {
      this.logger.debug({
        msg: 'Reinitialization already in progress, skipping',
        subscriptionName: this.subscriptionName,
      })
      return
    }

    this.isReinitializing = true

    try {
      for (
        let attempt = startAttempt;
        attempt <= this.subscriptionRetryOptions.maxRetries;
        attempt++
      ) {
        // Calculate delay with exponential backoff
        const delay = Math.min(
          this.subscriptionRetryOptions.baseRetryDelayMs * Math.pow(2, attempt - 1),
          this.subscriptionRetryOptions.maxRetryDelayMs,
        )

        this.logger.info({
          msg: `Reinitialization attempt ${attempt}/${this.subscriptionRetryOptions.maxRetries}, waiting ${delay}ms`,
          subscriptionName: this.subscriptionName,
          topicName: this.topicName,
          attempt,
          delayMs: delay,
        })

        // Wait before retry
        await new Promise((resolve) => setTimeout(resolve, delay))

        // Don't continue if we've been stopped during the wait
        if (!this.isConsuming) {
          this.logger.info({
            msg: 'Consumer stopped during reinitialization wait, aborting',
            subscriptionName: this.subscriptionName,
          })
          return
        }

        try {
          // Close existing subscription to remove old event handlers
          if (this.subscription) {
            try {
              this.subscription.removeAllListeners()
              await this.subscription.close()
            } catch {
              // Ignore close errors - subscription may already be closed
            }
          }

          // Reinitialize
          await this.init()

          if (!this.subscription) {
            throw new Error('Subscription not initialized after init()')
          }

          // Wait for subscription to be ready
          await this.waitForSubscriptionReady()

          // Reattach event handlers
          this.setupSubscriptionEventHandlers()

          this.logger.info({
            msg: 'Successfully reinitialized subscription',
            subscriptionName: this.subscriptionName,
            topicName: this.topicName,
            attempt,
          })

          // Success - exit the retry loop
          return
        } catch (error) {
          this.logger.warn({
            msg: `Reinitialization attempt ${attempt} failed, will retry`,
            subscriptionName: this.subscriptionName,
            topicName: this.topicName,
            attempt,
            error,
          })
          // Continue to next iteration
        }
      }

      // All retries exhausted - throw error to be handled by caller's catch block
      throw new Error(
        `Failed to reinitialize subscription ${this.subscriptionName} after ${this.subscriptionRetryOptions.maxRetries} attempts`,
      )
    } finally {
      this.isReinitializing = false
    }
  }

  private async waitForSubscriptionReady(maxAttempts = 100, delayMs = 20): Promise<void> {
    if (!this.subscription) {
      throw new Error('Subscription not initialized')
    }

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const [exists] = await this.subscription.exists()
      if (exists) {
        return
      }
      await new Promise((resolve) => setTimeout(resolve, delayMs))
    }

    throw new Error(
      `Subscription ${this.subscriptionName} did not become ready after ${maxAttempts * delayMs}ms`,
    )
  }

  public override async close(): Promise<void> {
    this.isConsuming = false
    if (this.subscription) {
      // Remove listeners first to prevent close handler from triggering reinitialization
      this.subscription.removeAllListeners()
      await this.subscription.close()
    }
    await super.close()
  }

  // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: message handling requires complex logic
  private async handleMessage(message: PubSubMessage): Promise<void> {
    if (!this.isConsuming) {
      // If we're shutting down, nack the message
      message.nack()
      return
    }

    const messageProcessingStartTimestamp = Date.now()

    try {
      // Parse and validate message (deserializes once via resolveMessage)
      const resolvedMessage = this.resolveMessage(message)
      if (resolvedMessage.error) {
        this.handleMessageProcessed({
          message: resolvedMessage.error.message as unknown as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        this.handleTerminalError(message, 'invalidMessage')
        return
      }

      // Retrieve offloaded payload if needed
      let messagePayload = resolvedMessage.result.body
      if (hasOffloadedPayload(message.attributes)) {
        const retrievalResult = await this.retrieveOffloadedMessagePayload(messagePayload)
        if (retrievalResult.error) {
          this.handleMessageProcessed({
            message: messagePayload as MessagePayloadType,
            processingResult: {
              status: 'error',
              errorReason: 'invalidMessage',
            },
            messageProcessingStartTimestamp,
            queueName: this.subscriptionName ?? this.topicName,
          })
          this.handleTerminalError(message, 'invalidMessage')
          return
        }
        messagePayload = retrievalResult.result
      }

      const resolveSchemaResult = this.resolveSchema(messagePayload as MessagePayloadType)
      if (resolveSchemaResult.error) {
        this.handleMessageProcessed({
          message: messagePayload as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        this.handleError(resolveSchemaResult.error)
        this.handleTerminalError(message, 'invalidMessage')
        return
      }

      const parseResult = parseMessage(
        messagePayload,
        resolveSchemaResult.result,
        this.errorResolver,
      )

      if (parseResult.error) {
        this.handleMessageProcessed({
          message: messagePayload as MessagePayloadType,
          processingResult: {
            status: 'error',
            errorReason: 'invalidMessage',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        this.handleTerminalError(message, 'invalidMessage')
        return
      }

      const validatedMessage = parseResult.result.parsedMessage as MessagePayloadType

      // Acquire lock for message processing
      const acquireLockResult = this.isDeduplicationEnabledForMessage(validatedMessage)
        ? await this.acquireLockForMessage(validatedMessage)
        : { result: noopReleasableLock }

      // Lock cannot be acquired as it is already being processed by another consumer.
      // We don't want to discard message yet as we don't know if the other consumer will be able to process it successfully.
      // We're re-queueing the message, so it can be processed later.
      if (acquireLockResult.error) {
        message.nack()
        return
      }

      // While the consumer was waiting for a lock to be acquired, the message might have been processed
      // by another consumer already, hence we need to check again if the message is not marked as duplicated.
      if (
        this.isDeduplicationEnabledForMessage(validatedMessage) &&
        (await this.isMessageDuplicated(validatedMessage, DeduplicationRequesterEnum.Consumer))
      ) {
        await acquireLockResult.result?.release()
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: { status: 'consumed', skippedAsDuplicate: true },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        return
      }

      const releaseLock = acquireLockResult.result

      // @ts-expect-error
      const messageType = validatedMessage[this.messageTypeField]

      try {
        // Process message
        const processingResult = await this.internalProcessMessage(validatedMessage, messageType)

        if (processingResult.error === 'retryLater') {
          // Check retry duration
          if (this.isRetryDateExceeded(validatedMessage)) {
            this.handleMessageProcessed({
              message: validatedMessage,
              processingResult: {
                status: 'error',
                errorReason: 'retryLaterExceeded',
              },
              messageProcessingStartTimestamp,
              queueName: this.subscriptionName ?? this.topicName,
            })
            this.handleTerminalError(message, 'retryLaterExceeded')
          } else {
            this.handleMessageProcessed({
              message: validatedMessage,
              processingResult: { status: 'retryLater' },
              messageProcessingStartTimestamp,
              queueName: this.subscriptionName ?? this.topicName,
            })
            message.nack() // Retry later
          }
          await releaseLock.release()
          return
        }

        // Success
        await this.deduplicateMessage(validatedMessage, DeduplicationRequesterEnum.Consumer)
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: { status: 'consumed' },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        message.ack()
        await releaseLock.release()
      } catch (error) {
        await releaseLock.release()
        this.handleError(error as Error)
        this.handleMessageProcessed({
          message: validatedMessage,
          processingResult: {
            status: 'error',
            errorReason: 'handlerError',
          },
          messageProcessingStartTimestamp,
          queueName: this.subscriptionName ?? this.topicName,
        })
        this.handleTerminalError(message, 'handlerError')
      }
    } catch (error) {
      this.handleError(error as Error)
      this.handleTerminalError(message, 'invalidMessage')
    }
  }

  private async internalProcessMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const preHandlerOutput = await this.processPrehandlers(message, messageType)
    const barrierResult = await this.preHandlerBarrier(message, messageType, preHandlerOutput)

    if (barrierResult.isPassing) {
      return this.processMessage(message, messageType, {
        preHandlerOutput,
        barrierOutput: barrierResult.output,
      })
    }

    return { error: 'retryLater' }
  }

  protected override resolveMessage(
    message: PubSubMessage,
  ): Either<MessageInvalidFormatError | MessageValidationError, ResolvedMessage> {
    const deserializedPayload = deserializePubSubMessage(message, this.errorResolver)
    if (deserializedPayload.error) {
      return deserializedPayload
    }

    return {
      result: {
        body: deserializedPayload.result,
        attributes: message.attributes,
      },
    }
  }

  protected override resolveSchema(messagePayload: MessagePayloadType) {
    return this._messageSchemaContainer.resolveSchema(messagePayload)
  }

  protected override processMessage(
    message: MessagePayloadType,
    messageType: string,
    // biome-ignore lint/suspicious/noExplicitAny: Expected
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, any>,
  ): Promise<Either<'retryLater', 'success'>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return handler.handler(message, this.executionContext, preHandlingOutputs)
  }

  protected override processPrehandlers(message: MessagePayloadType, messageType: string) {
    const handlerConfig = this.handlerContainer.resolveHandler<PrehandlerOutput>(messageType)

    return this.processPrehandlersInternal(handlerConfig.preHandlers, message)
  }

  protected override preHandlerBarrier<BarrierOutput>(
    message: MessagePayloadType,
    messageType: string,
    preHandlerOutput: PrehandlerOutput,
  ): Promise<BarrierResult<BarrierOutput>> {
    const handler = this.handlerContainer.resolveHandler<PrehandlerOutput, BarrierOutput>(
      messageType,
    )

    return this.preHandlerBarrierInternal(
      handler.preHandlerBarrier,
      message,
      this.executionContext,
      preHandlerOutput,
    )
  }

  protected override resolveNextFunction(
    preHandlers: Prehandler<MessagePayloadType, ExecutionContext, unknown>[],
    message: MessagePayloadType,
    index: number,
    preHandlerOutput: PrehandlerOutput,
    resolve: (value: PrehandlerOutput | PromiseLike<PrehandlerOutput>) => void,
    reject: (err: Error) => void,
  ) {
    return this.resolveNextPreHandlerFunctionInternal(
      preHandlers,
      this.executionContext,
      message,
      index,
      preHandlerOutput,
      resolve,
      reject,
    )
  }

  protected override resolveMessageLog(message: MessagePayloadType, messageType: string): unknown {
    const handler = this.handlerContainer.resolveHandler(messageType)
    return handler.messageLogFormatter(message)
  }

  protected override isDeduplicationEnabledForMessage(message: MessagePayloadType): boolean {
    return this.isDeduplicationEnabled && super.isDeduplicationEnabledForMessage(message)
  }

  private isRetryDateExceeded(message: MessagePayloadType): boolean {
    // @ts-expect-error
    const timestamp = message.timestamp
    if (!timestamp) {
      return false
    }

    const messageTimestamp = new Date(timestamp).getTime()
    const now = Date.now()
    const elapsedSeconds = (now - messageTimestamp) / 1000

    return elapsedSeconds > this.maxRetryDuration
  }

  /**
   * Handles terminal errors by either nacking (if DLQ is configured) or acking (if no DLQ).
   * When no DLQ is configured, acking prevents infinite redelivery of unprocessable messages.
   */
  private handleTerminalError(
    message: PubSubMessage,
    reason: 'invalidMessage' | 'retryLaterExceeded' | 'handlerError',
  ): void {
    if (this.deadLetterQueueOptions) {
      // DLQ configured: nack to trigger DLQ after maxDeliveryAttempts
      message.nack()
    } else {
      // No DLQ: ack to prevent infinite redelivery
      this.logger.warn(
        `Acknowledging message due to ${reason} with no DLQ configured (subscription: ${this.subscriptionName ?? this.topicName})`,
      )
      message.ack()
    }
  }
}
