import type { CommonLogger, ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { PayloadStoreConfig } from '../payload-store/payloadStoreTypes'
import type { MessageHandlerConfig } from '../queues/HandlerContainer'
import type { HandlerSpy, HandlerSpyParams } from '../queues/HandlerSpy'

import type {
  ConsumerMessageDeduplicationConfig,
  PublisherMessageDeduplicationConfig,
} from '../message-deduplication/messageDeduplicationTypes'
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
   * Processing time in milliseconds
   */
  messageProcessingMilliseconds?: number
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
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
  deletionConfig?: DeletionConfig
  payloadStoreConfig?: PayloadStoreConfig
  publisherMessageDeduplicationConfig?: PublisherMessageDeduplicationConfig
  consumerMessageDeduplicationConfig?: ConsumerMessageDeduplicationConfig
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
}
