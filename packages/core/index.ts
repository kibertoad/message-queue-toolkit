export type {
  QueueConsumer,
  AsyncPublisher,
  SyncPublisher,
  TransactionObservabilityManager,
  Logger,
  SchemaMap,
} from './lib/types/MessageQueueTypes'

export { AbstractQueueService } from './lib/queues/AbstractQueueService'
export type {
  NewQueueOptions,
  ExistingQueueOptions,
  NewQueueOptionsMultiSchema,
  ExistingQueueOptionsMultiSchema,
  QueueDependencies,
  QueueConsumerDependencies,
  Deserializer,
  CommonQueueLocator,
} from './lib/queues/AbstractQueueService'

export {
  isMessageError,
  MessageValidationError,
  MessageInvalidFormatError,
} from './lib/errors/Errors'

export { HandlerContainer, MessageHandlerConfig } from './lib/queues/HandlerContainer'
export type { HandlerContainerOptions, Handler } from './lib/queues/HandlerContainer'

export { MessageSchemaContainer } from './lib/queues/MessageSchemaContainer'
export type { MessageSchemaContainerOptions } from './lib/queues/MessageSchemaContainer'

export { objectToBuffer } from './lib/utils/queueUtils'
export { waitAndRetry } from './lib/utils/waitUtils'
