export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSTopicLocatorType,
  SNSCreationConfig,
  SNSDependencies,
} from './lib/sns/AbstractSnsService'

export { SNS_MESSAGE_MAX_SIZE } from './lib/sns/AbstractSnsService'

export { AbstractSnsService } from './lib/sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './lib/errors/SnsConsumerErrorResolver'

export type { SNSMessageOptions, SNSPublisherOptions } from './lib/sns/AbstractSnsPublisher'
export { AbstractSnsPublisher } from './lib/sns/AbstractSnsPublisher'

export type {
  SNSSQSConsumerOptions,
  SNSSQSConsumerDependencies,
} from './lib/sns/AbstractSnsSqsConsumer'
export { AbstractSnsSqsConsumer } from './lib/sns/AbstractSnsSqsConsumer'

export type { CommonMessage } from './lib/types/MessageTypes'

export { deserializeSNSMessage } from './lib/utils/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
  findSubscriptionByTopicAndQueue,
  getSubscriptionAttributes,
} from './lib/utils/snsUtils'
export { clearCachedCallerIdentity } from './lib/utils/stsUtils'

export { subscribeToTopic } from './lib/utils/snsSubscriber'
export { initSns, initSnsSqs } from './lib/utils/snsInitter'
export { readSnsMessage } from './lib/utils/snsMessageReader'
export {
  generateFilterAttributes,
  generateTopicSubscriptionPolicy,
} from './lib/utils/snsAttributeUtils'

export * from './lib/sns/CommonSnsPublisherFactory'
export * from './lib/sns/SnsPublisherManager'

export { FakeConsumer } from './lib/sns/fakes/FakeConsumer'
