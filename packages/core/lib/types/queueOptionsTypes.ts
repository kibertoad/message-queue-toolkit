import type { ErrorReporter, ErrorResolver } from '@lokalise/node-core'
import type { ZodSchema } from 'zod'

import type { MessageHandlerConfig } from '../queues/HandlerContainer'
import type { HandlerSpy, HandlerSpyParams } from '../queues/HandlerSpy'

import type { Logger, TransactionObservabilityManager } from './MessageQueueTypes'

export type QueueDependencies = {
  errorReporter: ErrorReporter
  logger: Logger
}

export type QueueConsumerDependencies = {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

export type CommonQueueOptions = {
  messageTypeField: string
  messageIdField?: string
  handlerSpy?: HandlerSpy<object> | HandlerSpyParams | boolean
  logMessages?: boolean
}

type CommonCreationConfigType = {
  updateAttributesIfExists?: boolean
}

export type DeletionConfig = {
  deleteIfExists?: boolean
  waitForConfirmation?: boolean
  forceDeleteInProduction?: boolean
}

type NewQueueOptions<CreationConfigType extends CommonCreationConfigType> = {
  locatorConfig?: never
  deletionConfig?: DeletionConfig
  creationConfig: CreationConfigType
} & CommonQueueOptions

type ExistingQueueOptions<QueueLocatorType extends object> = {
  locatorConfig: QueueLocatorType
  deletionConfig?: DeletionConfig
  creationConfig?: never
} & CommonQueueOptions

export type QueueOptions<
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
> = CommonQueueOptions &
  (NewQueueOptions<CreationConfigType> | ExistingQueueOptions<QueueLocatorType>)

export type QueuePublisherOptions<
  CreationConfigType extends CommonCreationConfigType,
  QueueLocatorType extends object,
  MessagePayloadSchemas extends object,
> = QueueOptions<CreationConfigType, QueueLocatorType> & {
  messageSchemas: readonly ZodSchema<MessagePayloadSchemas>[]
}

export type QueueConsumerOptions<
  CreationConfigType extends object,
  QueueLocatorType extends object,
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> = QueueOptions<CreationConfigType, QueueLocatorType> & {
  handlers: MessageHandlerConfig<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>[]
}
