export { SnsConsumerErrorResolver } from './errors/SnsConsumerErrorResolver.ts'
export type { SNSMessageOptions, SNSPublisherOptions } from './sns/AbstractSnsPublisher.ts'
export { AbstractSnsPublisher } from './sns/AbstractSnsPublisher.ts'
export type {
  SNSCreationConfig,
  SNSDependencies,
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSTopicLocatorType,
} from './sns/AbstractSnsService.ts'
export { AbstractSnsService, SNS_MESSAGE_MAX_SIZE } from './sns/AbstractSnsService.ts'

export type {
  SNSSQSConsumerDependencies,
  SNSSQSConsumerOptions,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType,
} from './sns/AbstractSnsSqsConsumer.ts'
export { AbstractSnsSqsConsumer } from './sns/AbstractSnsSqsConsumer.ts'
export * from './sns/CommonSnsPublisherFactory.ts'
export { FakeConsumer } from './sns/fakes/FakeConsumer.ts'
export * from './sns/SnsPublisherManager.ts'
export type { CommonMessage } from './types/MessageTypes.ts'
export {
  generateFilterAttributes,
  generateTopicSubscriptionPolicy,
} from './utils/snsAttributeUtils.ts'
export { initSns, initSnsSqs } from './utils/snsInitter.ts'
export { deserializeSNSMessage } from './utils/snsMessageDeserializer.ts'
export { readSnsMessage } from './utils/snsMessageReader.ts'
export { subscribeToTopic } from './utils/snsSubscriber.ts'
export {
  assertTopic,
  deleteSubscription,
  deleteTopic,
  findSubscriptionByTopicAndQueue,
  getSubscriptionAttributes,
  getTopicAttributes,
} from './utils/snsUtils.ts'
export { clearCachedCallerIdentity } from './utils/stsUtils.ts'
