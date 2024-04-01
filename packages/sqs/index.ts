export type {
  SQSConsumerDependencies,
  SQSQueueLocatorType,
  SQSDependencies,
} from './lib/sqs/AbstractSqsService'

export * from './lib/sqs/AbstractSqsConsumer'
export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export { AbstractSqsPublisher } from './lib/sqs/AbstractSqsPublisher'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisher'

export { assertQueue, deleteQueue, getQueueAttributes, getQueueUrl } from './lib/utils/sqsUtils'
export { deleteSqs, updateQueueAttributes } from './lib/utils/sqsInitter'
export { deserializeSQSMessage } from './lib/utils/sqsMessageDeserializer'
export {
  generateQueuePublishForTopicPolicy,
  generateWildcardSqsArn,
  generateWildcardSnsArn,
} from './lib/utils/sqsAttributeUtils'

export type { CommonMessage, SQSMessage } from './lib/types/MessageTypes'
export { FakeConsumerErrorResolver } from './lib/fakes/FakeConsumerErrorResolver'
