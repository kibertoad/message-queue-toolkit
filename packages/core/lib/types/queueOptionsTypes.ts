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
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
  deletionConfig?: DeletionConfig
  payloadStoreConfig?: PayloadStoreConfig
  messageDeduplicationConfig?: MessageDeduplicationConfig
  /**
   * Configuration for eventual consistency mode.
   * When enabled, the consumer will poll for the topic/queue to become available
   * instead of failing immediately when using locatorConfig.
   * This is useful for handling cross-service dependencies during deployment.
   */
  resourceAvailabilityConfig?: ResourceAvailabilityConfig
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
 * Configuration for eventual consistency mode when resources may not exist at startup.
 *
 * This is useful in scenarios where services have cross-dependencies:
 * - Service A needs to subscribe to Service B's topic
 * - Service B needs to subscribe to Service A's topic
 * - Neither can deploy first without the other's topic existing
 *
 * When `resourceAvailabilityConfig` is provided, the consumer will poll for the
 * topic/queue to become available instead of failing immediately.
 *
 * @example
 * // Development/staging - poll indefinitely
 * {
 *   resourceAvailabilityConfig: {
 *     pollingIntervalMs: 5000,
 *   }
 * }
 *
 * @example
 * // Production - poll with timeout to catch misconfigurations
 * {
 *   resourceAvailabilityConfig: {
 *     timeoutMs: 5 * 60 * 1000, // 5 minutes
 *     pollingIntervalMs: 10000,
 *   }
 * }
 *
 * @example
 * // Temporarily disable without removing config
 * {
 *   resourceAvailabilityConfig: {
 *     enabled: false,
 *     timeoutMs: 5 * 60 * 1000,
 *   }
 * }
 */
export type ResourceAvailabilityConfig = {
  /**
   * Controls whether polling is enabled.
   * Default: true (when resourceAvailabilityConfig is provided)
   * Set to false to temporarily disable polling without removing the config.
   */
  enabled?: boolean

  /**
   * Maximum time in milliseconds to wait for the resource to become available.
   * If not set or set to 0, will poll indefinitely (useful for dev/staging environments).
   * For production, it's recommended to set a reasonable timeout (e.g., 5 minutes).
   * Default: undefined (no timeout - poll indefinitely)
   */
  timeoutMs?: number

  /**
   * Interval in milliseconds between polling attempts.
   * Default: 5000 (5 seconds)
   */
  pollingIntervalMs?: number
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
