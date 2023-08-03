export type {
  SQSQueueAWSConfig,
  SQSQueueConfig,
  SQSConsumerDependencies,
  SQSQueueLocatorType,
} from './lib/sqs/AbstractSqsService'

export { AbstractSqsConsumer } from './lib/sqs/AbstractSqsConsumer'
export type { SQSCreationConfig } from './lib/sqs/AbstractSqsConsumer'
export { AbstractSqsConsumerMultiSchema } from './lib/sqs/AbstractSqsConsumerMultiSchema'
export { AbstractSqsConsumerMonoSchema } from './lib/sqs/AbstractSqsConsumerMonoSchema'

export type {
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
} from './lib/sqs/AbstractSqsConsumer'
export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export { AbstractSqsPublisherMonoSchema } from './lib/sqs/AbstractSqsPublisherMonoSchema'
export { AbstractSqsPublisherMultiSchema } from './lib/sqs/AbstractSqsPublisherMultiSchema'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisherMonoSchema'

export { assertQueue, deleteQueue, purgeQueueAsync, getQueueAttributes } from './lib/utils/SqsUtils'
export { deserializeSQSMessage } from './lib/sqs/sqsMessageDeserializer'

export type { CommonMessage, SQSMessage } from './lib/types/MessageTypes'
