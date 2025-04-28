export type {
  ExtraSQSCreationParams,
  SQSDependencies,
  SQSCreationConfig,
  SQSQueueLocatorType,
} from './sqs/AbstractSqsService'

export { SQS_MESSAGE_MAX_SIZE } from './sqs/AbstractSqsService'

export { SqsConsumerErrorResolver } from './errors/SqsConsumerErrorResolver'

export type { SQSConsumerDependencies, SQSConsumerOptions } from './sqs/AbstractSqsConsumer'
export { AbstractSqsConsumer } from './sqs/AbstractSqsConsumer'

export {
  AbstractSqsPublisher,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from './sqs/AbstractSqsPublisher'
export type { SQSMessageOptions } from './sqs/AbstractSqsPublisher'

export {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  getQueueUrl,
  resolveQueueUrlFromLocatorConfig,
} from './utils/sqsUtils'
export { deleteSqs, updateQueueAttributes } from './utils/sqsInitter'
export { deserializeSQSMessage } from './utils/sqsMessageDeserializer'
export {
  generateQueuePublishForTopicPolicy,
  generateWildcardSqsArn,
  generateWildcardSnsArn,
} from './utils/sqsAttributeUtils'

export type { CommonMessage, SQSMessage } from './types/MessageTypes'
export { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver'

export { calculateOutgoingMessageSize } from './utils/sqsUtils'
export { resolveOutgoingMessageAttributes } from './utils/messageUtils'
