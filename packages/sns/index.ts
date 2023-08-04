export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSConsumerDependencies,
  SNSQueueLocatorType,
  SNSCreationConfig,
  SNSDependencies,
  NewSNSOptions,
  ExistingSNSOptions,
  ExistingSNSOptionsMultiSchema,
  NewSNSOptionsMultiSchema,
} from './lib/sns/AbstractSnsService'

export { AbstractSnsService } from './lib/sns/AbstractSnsService'

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

export { deserializeSNSMessage } from './lib/utils/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
} from './lib/utils/snsUtils'

export { subscribeToTopic } from './lib/utils/snsSubscriber'
export { initSns, initSnsSqs } from './lib/utils/snsInitter'
export { readSnsMessage } from './lib/utils/snsMessageReader'
