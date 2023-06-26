export type {
  SQSQueueAWSConfig,
  SQSQueueConfig,
  SQSConsumerDependencies,
} from './lib/sqs/AbstractSqsService'

export { AbstractSqsConsumer } from './lib/sqs/AbstractSqsConsumer'
export type { SQSMessage, SQSConsumerOptions } from './lib/sqs/AbstractSqsConsumer'
export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'
export { SqsValidationError, SqsMessageInvalidFormat } from './lib/errors/sqsErrors'

export { AbstractSqsPublisher } from './lib/sqs/AbstractSqsPublisher'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisher'

export type { CommonMessage } from './lib/types/MessageTypes'
