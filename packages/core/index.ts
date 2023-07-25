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
  QueueDependencies,
  QueueConsumerDependencies,
  Deserializer,
  CommonQueueLocator,
} from './lib/queues/AbstractQueueService'

export type {
  ExistingQueueOptionsMultiSchema,
  NewQueueOptionsMultiSchema,
} from './lib/queues/AbstractQueueServiceMultiSchema'
export { AbstractQueueServiceMultiSchema } from './lib/queues/AbstractQueueServiceMultiSchema'

export {
  isMessageError,
  MessageValidationError,
  MessageInvalidFormatError,
} from './lib/errors/Errors'

export { objectToBuffer } from './lib/utils/queueUtils'
export { waitAndRetry } from './lib/utils/waitUtils'
