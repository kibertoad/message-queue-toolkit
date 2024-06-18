export type {
  ExtraSQSCreationParams,
  SQSDependencies,
  SQSCreationConfig,
  SQSQueueLocatorType,
  SQS_MESSAGE_MAX_SIZE,
} from './lib/sqs/AbstractSqsService'

export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export type { SQSConsumerDependencies, SQSConsumerOptions } from './lib/sqs/AbstractSqsConsumer'
export { AbstractSqsConsumer } from './lib/sqs/AbstractSqsConsumer'

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
