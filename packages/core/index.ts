export type {
  QueueConsumer,
  AsyncPublisher,
  SyncPublisher,
  TransactionObservabilityManager,
  Logger,
  LogFn,
  SchemaMap,
  ExtraParams,
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
export type {
  BarrierCallbackMultiConsumers,
  BarrierResult,
  BarrierResultPositive,
  BarrierResultNegative,
  HandlerContainerOptions,
  Handler,
  Prehandler,
} from './lib/queues/HandlerContainer'
export { HandlerSpy } from './lib/queues/HandlerSpy'
export type { SpyResult, HandlerSpyParams, PublicHandlerSpy } from './lib/queues/HandlerSpy'

export { MessageSchemaContainer } from './lib/queues/MessageSchemaContainer'
export type { MessageSchemaContainerOptions } from './lib/queues/MessageSchemaContainer'

export { objectToBuffer } from './lib/utils/queueUtils'
export { waitAndRetry } from './lib/utils/waitUtils'
export { parseMessage } from './lib/utils/parseUtils'

export { reloadConfig, isProduction } from './lib/utils/envUtils'
