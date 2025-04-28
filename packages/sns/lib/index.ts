export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSTopicLocatorType,
  SNSCreationConfig,
  SNSDependencies,
} from './sns/AbstractSnsService'

export { SNS_MESSAGE_MAX_SIZE } from './sns/AbstractSnsService'

export { AbstractSnsService } from './sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './errors/SnsConsumerErrorResolver'

export type { SNSMessageOptions, SNSPublisherOptions } from './sns/AbstractSnsPublisher'
export { AbstractSnsPublisher } from './sns/AbstractSnsPublisher'

export type {
  SNSSQSConsumerOptions,
  SNSSQSConsumerDependencies,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType,
} from './sns/AbstractSnsSqsConsumer'
export { AbstractSnsSqsConsumer } from './sns/AbstractSnsSqsConsumer'

export type { CommonMessage } from './types/MessageTypes'

export { deserializeSNSMessage } from './utils/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
  findSubscriptionByTopicAndQueue,
  getSubscriptionAttributes,
} from './utils/snsUtils'
export { clearCachedCallerIdentity } from './utils/stsUtils'

export { subscribeToTopic } from './utils/snsSubscriber'
export { initSns, initSnsSqs } from './utils/snsInitter'
export { readSnsMessage } from './utils/snsMessageReader'
export {
  generateFilterAttributes,
  generateTopicSubscriptionPolicy,
} from './utils/snsAttributeUtils'

export * from './sns/CommonSnsPublisherFactory'
export * from './sns/SnsPublisherManager'

export { FakeConsumer } from './sns/fakes/FakeConsumer'
