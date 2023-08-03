export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSConsumerDependencies,
  SNSQueueLocatorType,
} from './lib/sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './lib/errors/SnsConsumerErrorResolver'

export { AbstractSnsPublisherMonoSchema } from './lib/sns/AbstractSnsPublisherMonoSchema'
export { AbstractSnsPublisherMultiSchema } from './lib/sns/AbstractSnsPublisherMultiSchema'

export { AbstractSnsSqsConsumerMonoSchema } from './lib/sns/AbstractSnsSqsConsumerMonoSchema'
export { AbstractSnsSqsConsumerMultiSchema } from './lib/sns/AbstractSnsSqsConsumerMultiSchema'
export type {
  ExistingSnsSqsConsumerOptions,
  NewSnsSqsConsumerOptions,
  SNSSQSConsumerDependencies,
  ExistingSnsSqsConsumerOptionsMono,
  NewSnsSqsConsumerOptionsMono,
  SNSSQSQueueLocatorType,
} from './lib/sns/AbstractSnsSqsConsumerMonoSchema'
export type {
  NewSnsSqsConsumerOptionsMulti,
  ExistingSnsSqsConsumerOptionsMulti,
} from './lib/sns/AbstractSnsSqsConsumerMultiSchema'

export type { SNSMessageOptions } from './lib/sns/AbstractSnsPublisherMonoSchema'

export type { CommonMessage } from './lib/types/MessageTypes'

export { deserializeSNSMessage } from './lib/sns/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
} from './lib/utils/snsUtils'

export { subscribeToTopic } from './lib/sns/SnsSubscriber'
export { initSns, initSnsSqs } from './lib/sns/SnsInitter'
export { readSnsMessage } from './lib/sns/snsMessageReader'
