export { SqsConsumerErrorResolver } from './errors/SqsConsumerErrorResolver.ts'
export { FakeConsumerErrorResolver } from './fakes/FakeConsumerErrorResolver.ts'
export type { SQSConsumerDependencies, SQSConsumerOptions } from './sqs/AbstractSqsConsumer.ts'
export { AbstractSqsConsumer } from './sqs/AbstractSqsConsumer.ts'
export type { SQSMessageOptions, SQSPublisherOptions } from './sqs/AbstractSqsPublisher.ts'

export {
  AbstractSqsPublisher,
  OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE,
} from './sqs/AbstractSqsPublisher.ts'
export {
  type ExtraSQSCreationParams,
  SQS_MESSAGE_MAX_SIZE,
  SQS_RESOURCE_ANY,
  SQS_RESOURCE_CURRENT_QUEUE,
  type SQSCreationConfig,
  type SQSDependencies,
  type SQSOptions,
  type SQSPolicyConfig,
  type SQSQueueConfig,
  type SQSQueueLocatorType,
} from './sqs/AbstractSqsService.ts'
export type { CommonMessage, SQSMessage } from './types/MessageTypes.ts'
export {
  createEventBridgeSchema,
  createEventBridgeSchemas,
  EVENT_BRIDGE_BASE_SCHEMA,
  type EventBridgeBaseSchema,
  type EventBridgeDetail,
} from './utils/eventBridgeSchemaBuilder.ts'
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
  detectFifoQueue,
  getQueueAttributes,
  getQueueUrl,
  isFifoQueueName,
  resolveQueueUrlFromLocatorConfig,
  validateFifoQueueConfiguration,
  validateFifoQueueName,
} from './utils/sqsUtils.ts'
