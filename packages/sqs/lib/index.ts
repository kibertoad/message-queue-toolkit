export type {
  ExtraSQSCreationParams,
  SQSDependencies,
  SQSCreationConfig,
  SQSQueueLocatorType,
} from './sqs/AbstractSqsService.ts'

export { SQS_MESSAGE_MAX_SIZE } from './sqs/AbstractSqsService.ts'

export { SqsConsumerErrorResolver } from './errors/SqsConsumerErrorResolver.ts'

export type { SQSConsumerDependencies, SQSConsumerOptions } from './sqs/AbstractSqsConsumer.ts'
export { AbstractSqsConsumer } from './sqs/AbstractSqsConsumer.ts'

export {
  AbstractSqsPublisher,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from './sqs/AbstractSqsPublisher.ts'
export type { SQSMessageOptions } from './sqs/AbstractSqsPublisher.ts'

export {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  getQueueUrl,
  resolveQueueUrlFromLocatorConfig,
} from './utils/sqsUtils.ts'
export { deleteSqs, updateQueueAttributes } from './utils/sqsInitter.ts'
export { deserializeSQSMessage } from './utils/sqsMessageDeserializer.ts'
export {
  generateQueuePublishForTopicPolicy,
  generateWildcardSqsArn,
  generateWildcardSnsArn,
} from './utils/sqsAttributeUtils.ts'

export type { CommonMessage, SQSMessage } from './types/MessageTypes.ts'
export { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver.ts'

export { calculateOutgoingMessageSize } from './utils/sqsUtils.ts'
export { resolveOutgoingMessageAttributes } from './utils/messageUtils.ts'
