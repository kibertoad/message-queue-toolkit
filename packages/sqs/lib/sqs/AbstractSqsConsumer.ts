import type { SendMessageCommandInput } from '@aws-sdk/client-sqs'
import {
  ChangeMessageVisibilityCommand,
  SendMessageCommand,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs'
import type { Either, ErrorResolver } from '@lokalise/node-core'
import type { ProcessedMessageMetadata } from '@message-queue-toolkit/core'
import {
  type BarrierResult,
  type DeadLetterQueueOptions,
  DeduplicationRequesterEnum,
  HandlerContainer,
  isMessageError,
  type MessageSchemaContainer,
  noopReleasableLock,
  type ParseMessageResult,
  type PreHandlingOutputs,
  type Prehandler,
  parseMessage,
  type QueueConsumer,
  type QueueConsumerDependencies,
  type QueueConsumerOptions,
  type TransactionObservabilityManager,
} from '@message-queue-toolkit/core'
import type { ConsumerOptions } from 'sqs-consumer'
import { Consumer } from 'sqs-consumer'
import type { ZodSchema } from 'zod/v4'
import type { SQSMessage } from '../types/MessageTypes.ts'
import { hasOffloadedPayload } from '../utils/messageUtils.ts'
import { deleteSqs, initSqs } from '../utils/sqsInitter.ts'
import { readSqsMessage } from '../utils/sqsMessageReader.ts'
import { getQueueAttributes } from '../utils/sqsUtils.ts'
import { PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX } from './AbstractSqsPublisher.ts'
import type {
  SQSCreationConfig,
  SQSDependencies,
  SQSQueueLocatorType,
} from './AbstractSqsService.ts'
import { AbstractSqsService } from './AbstractSqsService.ts'

const ABORT_EARLY_EITHER: Either<'abort', never> = {
  error: 'abort',
}
const DEFAULT_MAX_RETRY_DURATION = 4 * 24 * 60 * 60 // 4 days in seconds
const DEFAULT_BARRIER_SLEEP_CHECK_INTERVAL_IN_MSECS = 5000 // 5 seconds in milliseconds
const DEFAULT_BARRIER_VISIBILITY_EXTENSION_INTERVAL_IN_MSECS = 30000 // 30 seconds in milliseconds
const DEFAULT_BARRIER_VISIBILITY_TIMEOUT_IN_SECONDS = 90 // 90 seconds

type SQSDeadLetterQueueOptions = {
  redrivePolicy: {
    maxReceiveCount: number
  }
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

type SQSConsumerCommonOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends object = SQSQueueLocatorType,
> = QueueConsumerOptions<
  CreationConfigType,
  QueueLocatorType,
  SQSDeadLetterQueueOptions,
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  SQSCreationConfig,
  SQSQueueLocatorType
> & {
  /**
   * Omitting properties which will be set internally ins this class
   * `visibilityTimeout` is also omitted to avoid conflicts with queue config
   */
  consumerOverrides?: Omit<
    ConsumerOptions,
    | 'sqs'
    | 'queueUrl'
    | 'handler'
    | 'handleMessageBatch'
    | 'visibilityTimeout'
    | 'messageAttributeNames'
    | 'messageSystemAttributeNames'
    | 'attributeNames'
  >
  concurrentConsumersAmount?: number
}

type SQSConsumerFifoOptions = {
  fifoQueue: true
  /**
   * Interval in milliseconds between barrier re-checks during sleep period (FIFO queues only).
   * FIFO queues sleep and recheck barriers instead of republishing messages to preserve order.
   * The consumer will sleep and recheck for up to maxRetryDuration before moving to DLQ.
   * Default: 5000 (5 seconds)
   */
  barrierSleepCheckIntervalInMsecs?: number
  /**
   * Interval in milliseconds between visibility timeout extensions during barrier sleep (FIFO queues only).
   * Extensions happen BEFORE each sleep chunk to prevent message from becoming visible during long waits.
   * Should be less than the VisibilityTimeout to prevent message reclaim.
   * Default: 30000 (30 seconds)
   */
  barrierVisibilityExtensionIntervalInMsecs?: number
  /**
   * Duration in seconds to extend visibility timeout to during barrier sleep (FIFO queues only).
   * Each extension sets visibility to this value from NOW (not cumulative).
   * Should be significantly longer than barrierVisibilityExtensionIntervalInMsecs for safety buffer.
   * Default: 90 (90 seconds, providing 60s buffer with 30s check interval)
   */
  barrierVisibilityTimeoutInSeconds?: number
}

type SQSConsumerStandardQueueOptions = {
  fifoQueue?: false
}

export type SQSConsumerOptions<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends object = SQSQueueLocatorType,
> =
  | (SQSConsumerCommonOptions<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > &
      SQSConsumerFifoOptions)
  | (SQSConsumerCommonOptions<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > &
      SQSConsumerStandardQueueOptions)

export abstract class AbstractSqsConsumer<
    MessagePayloadType extends object,
    ExecutionContext,
    PrehandlerOutput = undefined,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    QueueLocatorType extends object = SQSQueueLocatorType,
    ConsumerOptionsType extends SQSConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    > = SQSConsumerOptions<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput,
      CreationConfigType,
      QueueLocatorType
    >,
  >
  extends AbstractSqsService<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType,
    SQSConsumerDependencies,
    ExecutionContext,
    PrehandlerOutput
  >
  implements QueueConsumer
{
  private consumers: Consumer[]
  private readonly concurrentConsumersAmount: number
  private readonly transactionObservabilityManager?: TransactionObservabilityManager
  private readonly consumerOptionsOverride: Partial<ConsumerOptions>
  private readonly handlerContainer: HandlerContainer<
    MessagePayloadType,
    ExecutionContext,
    PrehandlerOutput
  >
  private readonly deadLetterQueueOptions?: DeadLetterQueueOptions<
    SQSCreationConfig,
    SQSQueueLocatorType,
    SQSDeadLetterQueueOptions
  >
  private readonly isDeduplicationEnabled: boolean
  private maxRetryDuration: number
  private cachedContentBasedDeduplication?: boolean
  private readonly barrierSleepCheckIntervalInMsecs: number
  private readonly barrierVisibilityExtensionIntervalInMsecs: number
  private readonly barrierVisibilityTimeoutInSeconds: number

  protected deadLetterQueueUrl?: string
  protected readonly errorResolver: ErrorResolver
  protected readonly executionContext: ExecutionContext

  public readonly _messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  /**
   * Returns true if the consumer has active SQS polling consumers running.
   * Useful for checking if the consumer has started processing messages.
   */
  public get isRunning(): boolean {
    return this.consumers.length > 0
  }

  protected constructor(
    dependencies: SQSConsumerDependencies,
    options: ConsumerOptionsType,
    executionContext: ExecutionContext,
  ) {
    super(dependencies, options)
    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
    this.errorResolver = dependencies.consumerErrorResolver
    this.consumerOptionsOverride = options.consumerOverrides ?? {}
    this.deadLetterQueueOptions = options.deadLetterQueue
    this.maxRetryDuration = options.maxRetryDuration ?? DEFAULT_MAX_RETRY_DURATION
    // Access FIFO-specific options (only available when fifoQueue: true)
    const fifoOptions = options as Partial<SQSConsumerFifoOptions>
    this.barrierSleepCheckIntervalInMsecs =
      fifoOptions.barrierSleepCheckIntervalInMsecs ?? DEFAULT_BARRIER_SLEEP_CHECK_INTERVAL_IN_MSECS
    this.barrierVisibilityExtensionIntervalInMsecs =
      fifoOptions.barrierVisibilityExtensionIntervalInMsecs ??
      DEFAULT_BARRIER_VISIBILITY_EXTENSION_INTERVAL_IN_MSECS
    this.barrierVisibilityTimeoutInSeconds =
      fifoOptions.barrierVisibilityTimeoutInSeconds ?? DEFAULT_BARRIER_VISIBILITY_TIMEOUT_IN_SECONDS
    this.executionContext = executionContext
    this.consumers = []
    this.concurrentConsumersAmount = options.concurrentConsumersAmount ?? 1
    this._messageSchemaContainer = this.resolveConsumerMessageSchemaContainer(options)
    this.handlerContainer = new HandlerContainer<
      MessagePayloadType,
      ExecutionContext,
      PrehandlerOutput
    >({
      messageTypeResolver: this.messageTypeResolver,
      messageHandlers: options.handlers,
    })
    this.isDeduplicationEnabled = !!options.enableConsumerDeduplication
  }

  override async init(): Promise<void> {
    await super.init()
    await this.initDeadLetterQueue()
  }

  protected async initDeadLetterQueue() {
    if (!this.deadLetterQueueOptions) return

    const { deletionConfig, locatorConfig, creationConfig, redrivePolicy } =
      this.deadLetterQueueOptions

    if (deletionConfig && creationConfig) {
      await deleteSqs(this.sqsClient, deletionConfig, creationConfig)
    }

    // DLQ should match the type of the source queue (FIFO DLQ for FIFO source queue)
    const result = await initSqs(this.sqsClient, locatorConfig, creationConfig, this.isFifoQueue)
    await this.sqsClient.send(
      new SetQueueAttributesCommand({
        QueueUrl: this.queueUrl,
        Attributes: {
          RedrivePolicy: JSON.stringify({
            deadLetterTargetArn: result.queueArn,
            maxReceiveCount: redrivePolicy.maxReceiveCount,
          }),
        },
      }),
    )

    this.deadLetterQueueUrl = result.queueUrl
  }

  public async start() {
    await this.init()
    await this.startConsumers()
  }

  /**
   * Creates and starts the SQS consumers.
   * This method is separated from start() to allow subclasses to defer consumer creation
   * until resources are ready (e.g., in non-blocking polling mode).
   */
  protected async startConsumers() {
    await this.stopExistingConsumers()

    const visibilityTimeout = await this.getQueueVisibilityTimeout()

    this.consumers = Array.from({ length: this.concurrentConsumersAmount }, () =>
      this.createConsumer({ visibilityTimeout }),
    )

    for (const consumer of this.consumers) {
      consumer.on('error', (err) => {
        this.handleError(err, { queueName: this.queueName })
      })
      consumer.start()
    }
  }

  public override async close(abort?: boolean): Promise<void> {
    await super.close()
    await this.stopExistingConsumers(abort ?? false)
  }

  private createConsumer(options: { visibilityTimeout: number | undefined }): Consumer {
    return Consumer.create({
      sqs: this.sqsClient,
      queueUrl: this.queueUrl,
      visibilityTimeout: options.visibilityTimeout,
      messageAttributeNames: [`${PAYLOAD_OFFLOADING_ATTRIBUTE_PREFIX}*`],
      // For FIFO queues, request system attributes needed for retry (MessageGroupId and MessageDeduplicationId)
      messageSystemAttributeNames: this.isFifoQueue
        ? ['MessageGroupId', 'MessageDeduplicationId']
        : undefined,
      ...this.consumerOptionsOverride,
      // Suppress FIFO warning (set after overrides to ensure it's not overridden)
      suppressFifoWarning: this.isFifoQueue ? true : undefined,
      // biome-ignore lint/complexity/noExcessiveCognitiveComplexity: fixme
      handleMessage: async (message: SQSMessage) => {
        if (message === null) return message

        const messageProcessingStartTimestamp = Date.now()

        const deserializedMessage = await this.deserializeMessage(message)
        if (deserializedMessage.error === 'abort') {
          await this.failProcessing(message)

          const messageId = this.tryToExtractId(message)
          this.handleMessageProcessed({
            message: null,
            processingResult: { status: 'error', errorReason: 'invalidMessage' },
            messageProcessingStartTimestamp,
            queueName: this.queueName,
            messageId: messageId.result,
          })

          return message
        }
        const { parsedMessage, originalMessage } = deserializedMessage.result

        if (
          this.isDeduplicationEnabledForMessage(parsedMessage) &&
          (await this.isMessageDuplicated(parsedMessage, DeduplicationRequesterEnum.Consumer))
        ) {
          this.handleMessageProcessed({
            message: parsedMessage,
            processingResult: { status: 'consumed', skippedAsDuplicate: true },
            messageProcessingStartTimestamp,
            queueName: this.queueName,
            messageId: this.tryToExtractId(message).result,
          })

          return message
        }

        const acquireLockResult = this.isDeduplicationEnabledForMessage(parsedMessage)
          ? await this.acquireLockForMessage(parsedMessage)
          : { result: noopReleasableLock }

        // Lock cannot be acquired as it is already being processed by another consumer.
        // We don't want to discard message yet as we don't know if the other consumer will be able to process it successfully.
        // We're re-queueing the message, so it can be processed later.
        if (acquireLockResult.error) {
          await this.handleRetryLater(
            message,
            originalMessage,
            parsedMessage,
            messageProcessingStartTimestamp,
            false, // isHandlerRetry = false for lock failures
          )

          return message
        }

        // While the consumer was waiting for a lock to be acquired, the message might have been processed
        // by another consumer already, hence we need to check again if the message is not marked as duplicated.
        if (
          this.isDeduplicationEnabledForMessage(parsedMessage) &&
          (await this.isMessageDuplicated(parsedMessage, DeduplicationRequesterEnum.Consumer))
        ) {
          await acquireLockResult.result?.release()
          this.handleMessageProcessed({
            message: parsedMessage,
            processingResult: { status: 'consumed', skippedAsDuplicate: true },
            messageProcessingStartTimestamp,
            queueName: this.queueName,
            messageId: this.tryToExtractId(message).result,
          })

          return message
        }

        const messageType = this.resolveMessageTypeFromMessage(parsedMessage) ?? 'unknown'
        const transactionSpanId = `queue_${this.queueName}:${messageType}`

        // @ts-expect-error
        const uniqueTransactionKey = parsedMessage[this.messageIdField]
        this.transactionObservabilityManager?.start(transactionSpanId, uniqueTransactionKey)

        const result: Either<'retryLater' | Error, 'success'> = await this.internalProcessMessage(
          parsedMessage,
          messageType,
        )
          .catch((err) => {
            this.handleError(err)
            // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
            return { error: err }
          })
          .finally(() => {
            this.transactionObservabilityManager?.stop(uniqueTransactionKey)
          })

        // success
        if (result.result) {
          await this.deduplicateMessage(parsedMessage, DeduplicationRequesterEnum.Consumer)
          await acquireLockResult.result?.release()
          this.handleMessageProcessed({
            message: originalMessage,
            processingResult: { status: 'consumed' },
            messageProcessingStartTimestamp,
            queueName: this.queueName,
          })

          return message
        }

        if (result.error === 'retryLater') {
          await acquireLockResult.result?.release()
          await this.handleRetryLater(
            message,
            originalMessage,
            parsedMessage,
            messageProcessingStartTimestamp,
            true, // isHandlerRetry = true for handler/barrier retries
          )

          return message
        }

        await acquireLockResult.result?.release()
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'error', errorReason: 'handlerError' },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
        })

        return Promise.reject(result.error)
      },
    })
  }

  private async handleRetryLater(
    message: SQSMessage,
    originalMessage: MessagePayloadType,
    parsedMessage: MessagePayloadType,
    messageProcessingStartTimestamp: number,
    isHandlerRetry: boolean = true,
  ): Promise<void> {
    /**
     * For FIFO queues, we use a special retry approach to preserve message ordering.
     * For handler/barrier retries: sleep and periodically re-check until barrier passes or max sleep exceeded
     * For lock failures: throw error immediately to trigger AWS retry
     */
    if (this.isFifoQueue) {
      // Check if retry is still within the allowed duration
      if (this.shouldBeRetried(originalMessage, this.maxRetryDuration)) {
        // Only use sleep-and-recheck for handler/barrier retries, not lock failures
        if (isHandlerRetry) {
          await this.sleepAndRecheckBarrier(
            message,
            originalMessage,
            parsedMessage,
            messageProcessingStartTimestamp,
          )
          return
        }

        // For lock failures, throw error immediately
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'retryLater' },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
        })
        throw new Error(
          'FIFO queue: Lock acquisition failed. Triggering AWS SQS retry to preserve message order.',
        )
      }
      // Retry duration exceeded for FIFO queue
      await this.failProcessing(message)
      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'error', errorReason: 'retryLaterExceeded' },
        messageProcessingStartTimestamp,
        queueName: this.queueName,
      })
      throw new Error('FIFO queue: Retry duration exceeded. Moving message to DLQ.')
    }

    // Standard queue handling: republish the message with delay
    if (this.shouldBeRetried(originalMessage, this.maxRetryDuration)) {
      const sendMessageParams = await this.buildRetryMessageParamsWithDeduplicationCheck(
        message,
        originalMessage,
      )
      await this.sqsClient.send(new SendMessageCommand(sendMessageParams))

      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'retryLater' },
        messageProcessingStartTimestamp,
        queueName: this.queueName,
      })
    } else {
      await this.failProcessing(message)
      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'error', errorReason: 'retryLaterExceeded' },
        messageProcessingStartTimestamp,
        queueName: this.queueName,
      })
    }
  }

  /**
   * Sleep and periodically recheck barrier for FIFO queues.
   * This method implements the barrier sleep mechanism to avoid unnecessary AWS retries.
   */
  private async sleepAndRecheckBarrier(
    message: SQSMessage,
    originalMessage: MessagePayloadType,
    parsedMessage: MessagePayloadType,
    messageProcessingStartTimestamp: number,
  ): Promise<void> {
    // Extract message type for barrier rechecks
    const messageType = this.resolveMessageTypeFromMessage(parsedMessage) ?? 'unknown'

    // Sleep and periodically recheck barrier until maxRetryDuration is exceeded
    while (this.shouldBeRetried(originalMessage, this.maxRetryDuration)) {
      // Sleep for the check interval
      await this.sleepWithVisibilityTimeoutExtension(message, this.barrierSleepCheckIntervalInMsecs)

      // Re-check barrier
      const result = await this.internalProcessMessage(parsedMessage, messageType).catch((err) => {
        this.handleError(err)
        // eslint-disable-next-line @typescript-eslint/no-unsafe-assignment
        return { error: err }
      })

      // If barrier passed and message processed successfully, we're done
      if ('result' in result && result.result === 'success') {
        this.handleMessageProcessed({
          message: originalMessage,
          processingResult: { status: 'consumed' },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
        })
        return
      }

      // If handler threw an error (not just retryLater), rethrow it
      if (result.error !== 'retryLater') {
        this.handleMessageProcessed({
          message: parsedMessage,
          processingResult: { status: 'error', errorReason: 'handlerError' },
          messageProcessingStartTimestamp,
          queueName: this.queueName,
        })
        throw result.error
      }

      // Barrier still not passing, continue sleeping
    }

    // Max retry duration exceeded, send to DLQ
    await this.failProcessing(message)
    this.handleMessageProcessed({
      message: parsedMessage,
      processingResult: { status: 'error', errorReason: 'retryLaterExceeded' },
      messageProcessingStartTimestamp,
      queueName: this.queueName,
    })
    throw new Error('FIFO queue: Retry duration exceeded. Moving message to DLQ.')
  }

  /**
   * Sleep for the specified duration while optionally extending visibility timeout to prevent message reclaim.
   * AWS SQS will reclaim messages if visibility timeout expires during processing.
   *
   * Visibility extension strategy:
   * - If heartbeatInterval is set and <= extension interval: rely on sqs-consumer's automatic extension
   * - Otherwise: explicitly extend at configured intervals during sleep
   *
   * This prevents redundant API calls when heartbeatInterval already provides sufficient coverage.
   */
  private async sleepWithVisibilityTimeoutExtension(
    message: SQSMessage,
    sleepDurationMs: number,
  ): Promise<void> {
    // Check if heartbeatInterval is set and sufficient for our needs
    // If heartbeatInterval <= extension interval, sqs-consumer will extend frequently enough
    const heartbeatInterval = this.consumerOptionsOverride?.heartbeatInterval
    const extensionIntervalSeconds = this.barrierVisibilityExtensionIntervalInMsecs / 1000
    const shouldExplicitlyExtend =
      !heartbeatInterval || heartbeatInterval > extensionIntervalSeconds

    let remainingSleepMs = sleepDurationMs
    const startTime = Date.now()

    while (remainingSleepMs > 0) {
      // Extend visibility timeout BEFORE sleeping (not after) to prevent message from becoming visible
      // Only extend explicitly if heartbeatInterval is not set or is too long
      if (shouldExplicitlyExtend) {
        try {
          await this.sqsClient.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: this.queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: this.barrierVisibilityTimeoutInSeconds,
            }),
          )
        } catch (err) {
          // Log error but don't fail - the message might have already been processed or deleted
          this.logger.warn({
            message: 'Failed to extend visibility timeout during barrier sleep',
            error: err,
          })
        }
      }

      // Now sleep for the chunk
      const sleepChunkMs = Math.min(
        remainingSleepMs,
        this.barrierVisibilityExtensionIntervalInMsecs,
      )
      await new Promise((resolve) => setTimeout(resolve, sleepChunkMs))

      // Calculate remaining sleep time for next iteration
      remainingSleepMs = sleepDurationMs - (Date.now() - startTime)
    }
  }

  /**
   * Determines whether to use async or sync path for building retry message params.
   * For FIFO queues with locatorConfig, we need to fetch ContentBasedDeduplication from SQS.
   */
  private async buildRetryMessageParamsWithDeduplicationCheck(
    message: SQSMessage,
    originalMessage: MessagePayloadType,
  ): Promise<SendMessageCommandInput> {
    // For FIFO queues, check if ContentBasedDeduplication is explicitly set in creationConfig
    // If not explicitly set (undefined), we must fetch from SQS (async path)
    if (this.isFifoQueue) {
      const contentBasedDedup = this.creationConfig?.queue.Attributes?.ContentBasedDeduplication
      const isContentBasedDedupExplicit =
        contentBasedDedup === 'true' || contentBasedDedup === 'false'

      if (!isContentBasedDedupExplicit) {
        // Need to fetch ContentBasedDeduplication attribute from SQS (locatorConfig or not explicitly set)
        return await this.buildFifoRetryMessageParamsAsync(message, originalMessage)
      }
    }

    // Standard queue or FIFO with explicit ContentBasedDeduplication value - synchronous path
    return this.buildRetryMessageParams(message, originalMessage)
  }

  /**
   * Builds SendMessageCommand parameters for retry, handling FIFO vs standard queues (synchronous)
   */
  private buildRetryMessageParams(
    message: SQSMessage,
    originalMessage: MessagePayloadType,
  ): SendMessageCommandInput {
    const params: SendMessageCommandInput = {
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(this.updateInternalProperties(originalMessage)),
    }

    if (this.isFifoQueue) {
      // FIFO queues: preserve MessageGroupId, no DelaySeconds
      const messageGroupId = message.Attributes?.MessageGroupId
      if (messageGroupId) {
        params.MessageGroupId = messageGroupId
      }

      // Check if ContentBasedDeduplication is enabled via creationConfig (synchronous)
      // If enabled: body changes (retry count increment) generate new deduplication ID automatically
      // If disabled: generate new MessageDeduplicationId for retry (append retry count to avoid duplication)
      const isContentBasedDedup =
        this.creationConfig?.queue.Attributes?.ContentBasedDeduplication === 'true'

      if (!isContentBasedDedup) {
        const deduplicationId = message.Attributes?.MessageDeduplicationId
        if (deduplicationId) {
          // Append retry count to create unique deduplication ID for each retry attempt
          // This prevents SQS from treating retry as duplicate within 5-minute deduplication window
          const retryCountValue = (originalMessage as Record<string, unknown>)
            ._internalRetryLaterCount
          const retryCount = typeof retryCountValue === 'number' ? retryCountValue : 0
          params.MessageDeduplicationId = `${deduplicationId}-retry-${retryCount + 1}`
        }
      }

      // Note: FIFO queues do not support DelaySeconds at the message level.
      // Messages will be retried immediately. Consider using visibility timeout
      // or application-level delay if needed.
    } else {
      // Standard queues: use DelaySeconds for exponential backoff
      params.DelaySeconds = this.getMessageRetryDelayInSeconds(originalMessage)
    }

    return params
  }

  /**
   * Builds FIFO retry message params when ContentBasedDeduplication attribute needs to be fetched from SQS.
   * This is only needed when using locatorConfig (queue not created by this service).
   * Caches the ContentBasedDeduplication value after first fetch to avoid repeated API calls.
   */
  private async buildFifoRetryMessageParamsAsync(
    message: SQSMessage,
    originalMessage: MessagePayloadType,
  ): Promise<SendMessageCommandInput> {
    const params: SendMessageCommandInput = {
      QueueUrl: this.queueUrl,
      MessageBody: JSON.stringify(this.updateInternalProperties(originalMessage)),
    }

    const messageGroupId = message.Attributes?.MessageGroupId
    if (messageGroupId) {
      params.MessageGroupId = messageGroupId
    }

    // Fetch ContentBasedDeduplication attribute from SQS (cached after first fetch)
    let isContentBasedDedup = this.cachedContentBasedDeduplication
    if (isContentBasedDedup === undefined) {
      const queueAttributes = await getQueueAttributes(this.sqsClient, this.queueUrl, [
        'ContentBasedDeduplication',
      ])
      isContentBasedDedup = queueAttributes.result?.attributes?.ContentBasedDeduplication === 'true'
      this.cachedContentBasedDeduplication = isContentBasedDedup
    }

    if (!isContentBasedDedup) {
      const deduplicationId = message.Attributes?.MessageDeduplicationId
      if (deduplicationId) {
        // Append retry count to create unique deduplication ID for each retry attempt
        const retryCountValue = (originalMessage as Record<string, unknown>)
          ._internalRetryLaterCount
        const retryCount = typeof retryCountValue === 'number' ? retryCountValue : 0
        params.MessageDeduplicationId = `${deduplicationId}-retry-${retryCount + 1}`
      }
    }

    return params
  }

  private async stopExistingConsumers(abort?: boolean): Promise<void> {
    await Promise.all(
      this.consumers.map((consumer) =>
        consumer.stop({
          abort,
        }),
      ),
    )
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

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this._messageSchemaContainer.resolveSchema(message as Record<string, unknown>)
  }

  // eslint-disable-next-line max-params
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

  protected override resolveMessageLog(
    processedMessageMetadata: ProcessedMessageMetadata<MessagePayloadType>,
  ): unknown | null {
    if (!processedMessageMetadata.message || !processedMessageMetadata.messageType) {
      return null
    }
    const handler = this.handlerContainer.resolveHandler(processedMessageMetadata.messageType)
    if (!handler.messageLogFormatter) {
      return null
    }
    return handler.messageLogFormatter(processedMessageMetadata.message)
  }

  protected override resolveMessage(message: SQSMessage) {
    return readSqsMessage(message, this.errorResolver)
  }

  protected override isDeduplicationEnabledForMessage(message: MessagePayloadType): boolean {
    return this.isDeduplicationEnabled && super.isDeduplicationEnabledForMessage(message)
  }

  protected async resolveMaybeOffloadedPayloadMessage(message: SQSMessage) {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }

    // Empty content for whatever reason
    if (!resolveMessageResult.result || !resolveMessageResult.result.body) {
      return ABORT_EARLY_EITHER
    }

    if (hasOffloadedPayload(resolveMessageResult.result)) {
      const retrieveOffloadedMessagePayloadResult = await this.retrieveOffloadedMessagePayload(
        resolveMessageResult.result.body,
      )
      if (retrieveOffloadedMessagePayloadResult.error) {
        this.handleError(retrieveOffloadedMessagePayloadResult.error)
        return ABORT_EARLY_EITHER
      }
      resolveMessageResult.result.body = retrieveOffloadedMessagePayloadResult.result
    }

    return resolveMessageResult
  }

  private tryToExtractId(message: SQSMessage): Either<'abort', string> {
    const resolveMessageResult = this.resolveMessage(message)
    if (isMessageError(resolveMessageResult.error)) {
      this.handleError(resolveMessageResult.error)
      return ABORT_EARLY_EITHER
    }
    const resolvedMessage = resolveMessageResult.result

    // Empty content for whatever reason
    if (!resolvedMessage || !resolvedMessage.body) return ABORT_EARLY_EITHER

    // @ts-expect-error
    if (this.messageIdField in resolvedMessage.body) {
      return {
        // @ts-expect-error
        result: resolvedMessage.body[this.messageIdField],
      }
    }

    return ABORT_EARLY_EITHER
  }

  private async deserializeMessage(
    message: SQSMessage,
  ): Promise<Either<'abort', ParseMessageResult<MessagePayloadType>>> {
    if (message === null) {
      return ABORT_EARLY_EITHER
    }

    const resolveMessageResult = await this.resolveMaybeOffloadedPayloadMessage(message)
    if (resolveMessageResult.error) {
      return ABORT_EARLY_EITHER
    }

    const fullMessage = resolveMessageResult.result.body

    const resolveSchemaResult = this.resolveSchema(fullMessage as MessagePayloadType)
    if (resolveSchemaResult.error) {
      this.handleError(resolveSchemaResult.error)
      return ABORT_EARLY_EITHER
    }

    const deserializationResult = parseMessage(
      fullMessage,
      resolveSchemaResult.result,
      this.errorResolver,
    )
    if (isMessageError(deserializationResult.error)) {
      this.handleError(deserializationResult.error)
      return ABORT_EARLY_EITHER
    }
    // Empty content for whatever reason
    if (!deserializationResult.result) {
      return ABORT_EARLY_EITHER
    }

    return {
      result: {
        parsedMessage: deserializationResult.result.parsedMessage,
        originalMessage: deserializationResult.result.originalMessage,
      },
    }
  }

  private async failProcessing(message: SQSMessage) {
    if (!this.deadLetterQueueUrl) return

    const params: SendMessageCommandInput = {
      QueueUrl: this.deadLetterQueueUrl,
      MessageBody: message.Body,
    }

    // For FIFO queues, preserve MessageGroupId when sending to DLQ
    if (this.isFifoQueue) {
      const messageGroupId = message.Attributes?.MessageGroupId
      if (messageGroupId) {
        params.MessageGroupId = messageGroupId
      }
      const deduplicationId = message.Attributes?.MessageDeduplicationId
      if (deduplicationId) {
        params.MessageDeduplicationId = deduplicationId
      }
    }

    const command = new SendMessageCommand(params)
    await this.sqsClient.send(command)
  }

  private async getQueueVisibilityTimeout(): Promise<number | undefined> {
    let visibilityTimeoutString: string | undefined
    if (this.creationConfig) {
      visibilityTimeoutString = this.creationConfig.queue.Attributes?.VisibilityTimeout
    } else {
      // if user is using locatorConfig, we should look into queue config
      const queueAttributes = await getQueueAttributes(this.sqsClient, this.queueUrl, [
        'VisibilityTimeout',
      ])
      visibilityTimeoutString = queueAttributes.result?.attributes?.VisibilityTimeout
    }

    // parseInt is safe because if the value is not a number process should have failed on init
    return visibilityTimeoutString ? Number.parseInt(visibilityTimeoutString, 10) : undefined
  }
}
