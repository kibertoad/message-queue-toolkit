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

export { AbstractSqsPublisherMonoSchema } from './lib/sqs/AbstractSqsPublisherMonoSchema'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisherMonoSchema'

export { assertQueue, deleteQueue, purgeQueue, getQueueAttributes } from './lib/utils/SqsUtils'
export { deserializeSQSMessage } from './lib/sqs/sqsMessageDeserializer'

export type { CommonMessage, SQSMessage } from './lib/types/MessageTypes'
