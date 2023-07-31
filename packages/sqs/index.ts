export type {
  SQSQueueAWSConfig,
  SQSQueueConfig,
  SQSConsumerDependencies,
  SQSQueueLocatorType,
} from './lib/sqs/AbstractSqsService'

export { AbstractSqsConsumer, SQSCreationConfig } from './lib/sqs/AbstractSqsConsumer'
export type {
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
} from './lib/sqs/AbstractSqsConsumer'
export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export { AbstractSqsPublisher, type SQSMessageOptions } from './lib/sqs/AbstractSqsPublisher'

export { SQSDependencies } from './lib/sqs/AbstractSqsService'

export { assertQueue, deleteQueue, purgeQueue, getQueueAttributes } from './lib/utils/SqsUtils'
export { deserializeSQSMessage } from './lib/sqs/sqsMessageDeserializer'

export type { CommonMessage, SQSMessage } from './lib/types/MessageTypes'
