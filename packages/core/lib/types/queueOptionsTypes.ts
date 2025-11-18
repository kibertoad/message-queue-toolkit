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
   * Optional field name to extract the payload from the message.
   * If specified, the handler will receive the value of this field instead of the entire message.
   * This is useful for non-standard message formats like EventBridge events where the actual
   * payload is nested (e.g., in a 'detail' field).
   * If undefined, the entire message is treated as the payload (default behavior).
   */
  messagePayloadField?: string
  /**
   * When true, look up messageTypeField in the full/root message instead of the extracted payload.
   * Only relevant when messagePayloadField is also configured.
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
   * When messagePayloadField is set, determines where to look for the timestamp field for metadata extraction.
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
  /**
   * If true, skip automatic timestamp addition and validation for messages without a timestamp field.
   * This is useful for non-standard message formats that don't include timestamp information in
   * either the envelope or the payload.
   *
   * When enabled:
   * - Messages without timestamp won't have one auto-added
   * - Retry logic will use current time for retry calculations
   *
   * Note: If your message has a timestamp in the envelope but not in the payload (like EventBridge),
   * use messageTimestampFromFullMessage instead of this flag.
   *
   * Default: false (maintains backward compatibility)
   */
  skipMissingTimestampValidation?: boolean
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
