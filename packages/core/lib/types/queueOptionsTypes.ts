import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod/v4'
import type { MessageDeduplicationConfig } from '../message-deduplication/messageDeduplicationTypes.ts'
import type { PayloadStoreConfig } from '../payload-store/payloadStoreTypes.ts'
import type { MessageHandlerConfig } from '../queues/HandlerContainer.ts'
import type { HandlerSpy, HandlerSpyParams } from '../queues/HandlerSpy.ts'
import type { MessageTypeResolverConfig } from '../queues/MessageTypeResolver.ts'
import type {
  MessageProcessingResult,
  TransactionObservabilityManager,
} from './MessageQueueTypes.ts'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: CommonLogger
  messageMetricsManager?: MessageMetricsManager
}

export type ProcessedMessageMetadata<MessagePayloadSchemas extends object = object> = {
  /**
   * Id of the message accessed by `messageIdField`
   */
  messageId: string
  /**
   * Message type resolved by `messageTypeResolver`.
   * May be undefined if messageTypeResolver is not configured.
   */
  messageType?: string

  /**
   * Processing result status
   */
  processingResult: MessageProcessingResult

  /**
   * Original message object
   */
  message: MessagePayloadSchemas | null

  /**
   * Name of the queue processing the message
   */
  queueName: string

  /**
   * The timestamp when the message was sent initially, in milliseconds since the epoch
   */
  messageTimestamp: number | undefined

  /**
   * The timestamp when the processing of the message began, in milliseconds since the epoch
   * Note: for publishers the value may be smaller than messageTimestamp
   */
  messageProcessingStartTimestamp: number

  /**
   * The timestamp when the processing of the message ended, in milliseconds since the epoch
   */
  messageProcessingEndTimestamp: number

  /**
   * ID used for the message deduplication, in case it's enabled
   */
  messageDeduplicationId?: string

  /**
   * Message metadata (see ConsumerMessageMetadataType)
   */
  messageMetadata?: Record<string, unknown>
}

export interface MessageMetricsManager<MessagePayloadSchemas extends object = object> {
  /**
   * Executed once message is processed
   * @param metadata - contains basic message processing metadata including processing result and time, as well as the whole message object
   */
  registerProcessedMessage(metadata: ProcessedMessageMetadata<MessagePayloadSchemas>): void
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

/**
 * Common queue options for publishers and consumers.
 *
 * Message type resolution is configured via `messageTypeResolver`.
 * At least one must be provided for routing to work (unless using a single handler).
 */
export type CommonQueueOptions = {
  /**
   * Configuration for resolving message types.
   *
   * Supports three modes:
   * - `{ messageTypePath: string }` - field name at the root of the message
   * - `{ literal: string }` - constant type for all messages
   * - `{ resolver: fn }` - custom function for complex scenarios (e.g., extracting from attributes)
   *
   * @example
   * // Field path - extracts type from message.type
   * { messageTypeResolver: { messageTypePath: 'type' } }
   *
   * @example
   * // Constant type - all messages treated as same type
   * { messageTypeResolver: { literal: 'order.created' } }
   *
   * @example
   * // Custom resolver for Cloud Storage notifications via PubSub
   * {
   *   messageTypeResolver: {
   *     resolver: ({ messageAttributes }) => messageAttributes?.eventType as string
   *   }
   * }
   */
  messageTypeResolver?: MessageTypeResolverConfig
  messageIdField?: string
  messageTimestampField?: string
  messageDeduplicationIdField?: string
  messageDeduplicationOptionsField?: string
  messageMetadataField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
  deletionConfig?: DeletionConfig
  payloadStoreConfig?: PayloadStoreConfig
  messageDeduplicationConfig?: MessageDeduplicationConfig
}

export type CommonCreationConfigType = {
  updateAttributesIfExists?: boolean
}

export type DeletionConfig = {
  deleteIfExists?: boolean
  waitForConfirmation?: boolean
  forceDeleteInProduction?: boolean
}

/**
 * Symbol to indicate no timeout - polling will continue indefinitely.
 * Use this for dev/staging environments where you want to wait indefinitely for resources.
 *
 * @example
 * {
 *   locatorConfig: {
 *     queueUrl: '...',
 *     startupResourcePolling: {
 *       enabled: true,
 *       timeoutMs: NO_TIMEOUT,
 *     }
 *   }
 * }
 */
export const NO_TIMEOUT = Symbol('NO_TIMEOUT')

/**
 * Configuration for startup resource polling mode when resources may not exist at startup.
 *
 * This is useful in scenarios where services have cross-dependencies:
 * - Service A needs to subscribe to Service B's topic
 * - Service B needs to subscribe to Service A's topic
 * - Neither can deploy first without the other's topic existing
 *
 * When `startupResourcePolling` is provided with `enabled: true` inside `locatorConfig`,
 * the consumer will poll for the topic/queue to become available instead of failing immediately.
 *
 * @example
 * // Enable with 5 minute timeout
 * {
 *   locatorConfig: {
 *     queueUrl: '...',
 *     startupResourcePolling: {
 *       enabled: true,
 *       timeoutMs: 5 * 60 * 1000,
 *     }
 *   }
 * }
 *
 * @example
 * // Poll indefinitely (useful for dev/staging environments)
 * {
 *   locatorConfig: {
 *     queueUrl: '...',
 *     startupResourcePolling: {
 *       enabled: true,
 *       timeoutMs: NO_TIMEOUT,
 *     }
 *   }
 * }
 *
 * @example
 * // Custom timeout and interval
 * {
 *   locatorConfig: {
 *     queueUrl: '...',
 *     startupResourcePolling: {
 *       enabled: true,
 *       timeoutMs: 10 * 60 * 1000, // 10 minutes
 *       pollingIntervalMs: 10000,
 *     }
 *   }
 * }
 */
export type StartupResourcePollingConfig = {
  /**
   * Controls whether polling is enabled.
   * Must be set to true to enable polling.
   */
  enabled?: boolean

  /**
   * Maximum time in milliseconds to wait for the resource to become available.
   * Use `NO_TIMEOUT` to disable timeout and poll indefinitely.
   */
  timeoutMs: number | typeof NO_TIMEOUT

  /**
   * Interval in milliseconds between polling attempts.
   * Default: 5000 (5 seconds)
   */
  pollingIntervalMs?: number

  /**
   * Whether to throw an error when timeout is reached.
   * - `true` (default): Throws `StartupResourcePollingTimeoutError` when timeout is reached
   * - `false`: Reports the error via errorReporter, resets the timeout counter, and continues polling
   *
   * Use `false` when you want to be notified about prolonged unavailability but don't want to fail.
   * Default: true
   */
  throwOnTimeout?: boolean

  /**
   * Whether to run polling in non-blocking mode.
   * - `false` (default): init() waits for the resource to become available before resolving
   * - `true`: If resource is not immediately available, init() resolves immediately and
   *   polling continues in the background. When the resource becomes available,
   *   the `onResourceAvailable` callback is invoked.
   *
   * Use `true` when you want the service to start quickly without waiting for dependencies.
   * Default: false
   */
  nonBlocking?: boolean
}

/**
 * Base type for queue locator configurations that includes startup resource polling.
 * Protocol-specific locator types should extend this.
 */
export type BaseQueueLocatorType = {
  /**
   * Configuration for startup resource polling mode.
   * When enabled, the consumer will poll for the resource to become available
   * instead of failing immediately.
   */
  startupResourcePolling?: StartupResourcePollingConfig
}

type NewQueueOptions<CreationConfigType extends CommonCreationConfigType> = {
  creationConfig?: CreationConfigType
}

type ExistingQueueOptions<QueueLocatorType extends object> = {
  locatorConfig?: QueueLocatorType
}

export type QueueOptions<
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
> = CommonQueueOptions &
  (NewQueueOptions<CreationConfigType> & ExistingQueueOptions<QueueLocatorType>)

export type QueuePublisherOptions<
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
  MessagePayloadSchemas extends object,
> = QueueOptions<CreationConfigType, QueueLocatorType> & {
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
  enablePublisherDeduplication?: boolean
}

export type DeadLetterQueueOptions<
  CreationConfigType extends object,
  QueueLocatorType extends object,
  DeadLetterQueueIntegrationOptions extends object,
> = {
  deletionConfig?: DeletionConfig
} & DeadLetterQueueIntegrationOptions &
  NewQueueOptions<CreationConfigType> &
  ExistingQueueOptions<QueueLocatorType>

export type QueueConsumerOptions<
  CreationConfigType extends object,
  QueueLocatorType extends object,
  DeadLetterQueueIntegrationOptions extends object,
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
  DeadLetterQueueCreationConfigType extends object = CreationConfigType,
  DeadLetterQueueQueueLocatorType extends object = QueueLocatorType,
> = QueueOptions<CreationConfigType, QueueLocatorType> & {
  handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
  deadLetterQueue?: DeadLetterQueueOptions<
    DeadLetterQueueCreationConfigType,
    DeadLetterQueueQueueLocatorType,
    DeadLetterQueueIntegrationOptions
  >
  maxRetryDuration?: number
  enableConsumerDeduplication?: boolean
}
