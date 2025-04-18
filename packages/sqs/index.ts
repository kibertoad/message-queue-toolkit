export type {
  ExtraSQSCreationParams,
  SQSDependencies,
  SQSCreationConfig,
  SQSQueueLocatorType,
} from './lib/sqs/AbstractSqsService'

export { SQS_MESSAGE_MAX_SIZE } from './lib/sqs/AbstractSqsService'

export { SqsConsumerErrorResolver } from './lib/errors/SqsConsumerErrorResolver'

export type { SQSConsumerDependencies, SQSConsumerOptions } from './lib/sqs/AbstractSqsConsumer'
export { AbstractSqsConsumer } from './lib/sqs/AbstractSqsConsumer'

export {
  AbstractSqsPublisher,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from './lib/sqs/AbstractSqsPublisher'
export type { SQSMessageOptions } from './lib/sqs/AbstractSqsPublisher'

export {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  getQueueUrl,
  resolveQueueUrlFromLocatorConfig,
} from './lib/utils/sqsUtils'
export { deleteSqs, updateQueueAttributes } from './lib/utils/sqsInitter'
export { deserializeSQSMessage } from './lib/utils/sqsMessageDeserializer'
export {
  generateQueuePublishForTopicPolicy,
  generateWildcardSqsArn,
  generateWildcardSnsArn,
} from './lib/utils/sqsAttributeUtils'

export type { CommonMessage, SQSMessage } from './lib/types/MessageTypes'
export { FakeConsumerErrorResolver } from './lib/fakes/FakeConsumerErrorResolver'

export { calculateOutgoingMessageSize } from './lib/utils/sqsUtils'
export { resolveOutgoingMessageAttributes } from './lib/utils/messageUtils'
