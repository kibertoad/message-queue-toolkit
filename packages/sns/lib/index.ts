export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSTopicLocatorType,
  SNSCreationConfig,
  SNSDependencies,
} from './sns/AbstractSnsService.ts'

export { SNS_MESSAGE_MAX_SIZE } from './sns/AbstractSnsService.ts'

export { AbstractSnsService } from './sns/AbstractSnsService.ts'

export { SnsConsumerErrorResolver } from './errors/SnsConsumerErrorResolver.ts'

export type { SNSMessageOptions, SNSPublisherOptions } from './sns/AbstractSnsPublisher.ts'
export { AbstractSnsPublisher } from './sns/AbstractSnsPublisher.ts'

export type {
  SNSSQSConsumerOptions,
  SNSSQSConsumerDependencies,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType,
} from './sns/AbstractSnsSqsConsumer.ts'
export { AbstractSnsSqsConsumer } from './sns/AbstractSnsSqsConsumer.ts'

export type { CommonMessage } from './types/MessageTypes.ts'

export { deserializeSNSMessage } from './utils/snsMessageDeserializer.ts'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
  findSubscriptionByTopicAndQueue,
  getSubscriptionAttributes,
} from './utils/snsUtils.ts'
export { clearCachedCallerIdentity } from './utils/stsUtils.ts'

export { subscribeToTopic } from './utils/snsSubscriber.ts'
export { initSns, initSnsSqs } from './utils/snsInitter.ts'
export { readSnsMessage } from './utils/snsMessageReader.ts'
export {
  generateFilterAttributes,
  generateTopicSubscriptionPolicy,
} from './utils/snsAttributeUtils.ts'

export * from './sns/CommonSnsPublisherFactory.ts'
export * from './sns/SnsPublisherManager.ts'

export { FakeConsumer } from './sns/fakes/FakeConsumer.ts'
