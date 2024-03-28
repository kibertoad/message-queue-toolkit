export type {
  SQSQueueConfig,
  SQSConsumerDependencies,
  SQSQueueLocatorType,
  SQSDependencies,
} from './lib/sqs/AbstractSqsService'

export { AbstractSqsConsumer } from './lib/sqs/AbstractSqsConsumer'
export type { SQSCreationConfig, ExtraSQSCreationParams } from './lib/sqs/AbstractSqsConsumer'
export { AbstractSqsConsumerMultiSchema } from './lib/sqs/AbstractSqsConsumerMultiSchema'

export type {
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
} from './lib/sqs/AbstractSqsConsumer'
export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export { AbstractSqsPublisherMultiSchema } from './lib/sqs/AbstractSqsPublisherMultiSchema'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisherMultiSchema'

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
