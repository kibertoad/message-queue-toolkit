import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { PayloadStoreConfig } from '../payload-store/payloadStoreTypes'
import type { MessageHandlerConfig } from '../queues/HandlerContainer'
import type { HandlerSpy, HandlerSpyParams } from '../queues/HandlerSpy'

import type { MessageDeduplicationConfig } from '../message-deduplication/messageDeduplicationTypes'
import type { MessageProcessingResult, TransactionObservabilityManager } from './MessageQueueTypes'

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
