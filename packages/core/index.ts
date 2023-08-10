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
  MonoSchemaQueueOptions,
  MultiSchemaConsumerOptions,
  QueueDependencies,
  QueueConsumerDependencies,
  Deserializer,
  CommonQueueLocator,
  DeletionConfig,
  MultiSchemaPublisherOptions,
  BarrierCallback,
} from './lib/queues/AbstractQueueService'

export {
  isMessageError,
  MessageValidationError,
  MessageInvalidFormatError,
} from './lib/errors/Errors'

export {
  HandlerContainer,
  MessageHandlerConfig,
  MessageHandlerConfigBuilder,
} from './lib/queues/HandlerContainer'
export type { HandlerContainerOptions, Handler } from './lib/queues/HandlerContainer'

export { MessageSchemaContainer } from './lib/queues/MessageSchemaContainer'
export type { MessageSchemaContainerOptions } from './lib/queues/MessageSchemaContainer'

export { objectToBuffer } from './lib/utils/queueUtils'
export { waitAndRetry } from './lib/utils/waitUtils'
export { parseMessage } from './lib/utils/parseUtils'

export { reloadConfig, isProduction } from './lib/utils/envUtils'
