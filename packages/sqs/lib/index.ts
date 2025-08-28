export { SqsConsumerErrorResolver } from './errors/SqsConsumerErrorResolver.ts'
export { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver.ts'
export type { SQSConsumerDependencies, SQSConsumerOptions } from './sqs/AbstractSqsConsumer.ts'
export { AbstractSqsConsumer } from './sqs/AbstractSqsConsumer.ts'
export type { SQSMessageOptions } from './sqs/AbstractSqsPublisher.ts'

export {
  AbstractSqsPublisher,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from './sqs/AbstractSqsPublisher.ts'
export type {
  ExtraSQSCreationParams,
  SQSCreationConfig,
  SQSDependencies,
  SQSQueueLocatorType,
} from './sqs/AbstractSqsService.ts'
export { SQS_MESSAGE_MAX_SIZE } from './sqs/AbstractSqsService.ts'
export type { CommonMessage, SQSMessage } from './types/MessageTypes.ts'
export { resolveOutgoingMessageAttributes } from './utils/messageUtils.ts'
export {
  generateQueuePublishForTopicPolicy,
  generateWildcardSnsArn,
  generateWildcardSqsArn,
} from './utils/sqsAttributeUtils.ts'
export { deleteSqs, updateQueueAttributes } from './utils/sqsInitter.ts'
export { deserializeSQSMessage } from './utils/sqsMessageDeserializer.ts'
export {
  assertQueue,
  calculateOutgoingMessageSize,
  deleteQueue,
  getQueueAttributes,
  getQueueUrl,
  resolveQueueUrlFromLocatorConfig,
} from './utils/sqsUtils.ts'
