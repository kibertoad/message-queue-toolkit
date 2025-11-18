import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod/v4'
import type { MessageDeduplicationConfig } from '../message-deduplication/messageDeduplicationTypes.ts'
import type { PayloadStoreConfig } from '../payload-store/payloadStoreTypes.ts'
import type { MessageHandlerConfig } from '../queues/HandlerContainer.ts'
import type { HandlerSpy, HandlerSpyParams } from '../queues/HandlerSpy.ts'
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
   * Message type accessed by `messageTypeField`
   */
  messageType: string

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

export type CommonQueueOptions = {
  messageTypeField: string
  messageIdField?: string
  messageTimestampField?: string
  messageDeduplicationIdField?: string
  messageDeduplicationOptionsField?: string
  /**
   * Field name containing the business payload within the message envelope.
   *
   * The "payload" is the part of the message relevant for handler processing logic (e.g., user data, order details),
   * as opposed to metadata fields used primarily for observability, routing, error handling, and flow control
   * (e.g., id, timestamp, messageType, deduplicationId).
   *
   * When specified: The handler receives only the extracted payload field, while metadata fields remain accessible
   * from the full message for logging, deduplication, and error reporting.
   *
   * Common patterns:
   * - Envelope structure: { id, type, timestamp, payload: {...} } → handler receives payload content
   * - EventBridge events: { source, detail-type, time, detail: {...} } → use 'detail' as messagePayloadField
   * - Flat structure: Set to undefined to treat the entire message as payload (useful for external systems,
   *   simple use cases, or when you don't control the message format)
   *
   * If the specified field is not found in a message, the system logs a warning and uses the full message as fallback.
   *
   * Default: 'payload'
   */
  messagePayloadField?: string
  /**
   * When true, look up messageTypeField in the full/root message instead of the extracted payload.
   * Only relevant when messagePayloadField is not set to undefined (i.e., when payload extraction is configured).
   *
   * Use case: EventBridge events where:
   * - messagePayloadField: 'detail' (extract nested payload)
   * - messageTypeField: 'detail-type' (type field is in root, not in detail)
   * - messageTypeFromFullMessage: true (look for detail-type in root message)
   *
   * Default: false (look in extracted payload for backward compatibility)
   */
  messageTypeFromFullMessage?: boolean
  /**
   * When messagePayloadField is not set to undefined (i.e., when payload extraction is configured),
   * determines where to look for the timestamp field for metadata extraction.
   *
   * Use case: EventBridge events where:
   * - messagePayloadField: 'detail' (extract nested payload)
   * - messageTimestampField: 'time' (timestamp field is in root, not in detail)
   * - messageTimestampFromFullMessage: true (extract timestamp from root message for metadata/logging)
   *
   * Note: Retry logic always uses the full message for timestamp extraction.
   * This flag only affects metadata extraction in handleMessageProcessed.
   *
   * Default: false (look in extracted payload for backward compatibility)
   */
  messageTimestampFromFullMessage?: boolean
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
