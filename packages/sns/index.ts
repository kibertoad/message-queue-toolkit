export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSConsumerDependencies,
  SNSQueueLocatorType,
} from './lib/sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './lib/errors/SnsConsumerErrorResolver'

export { AbstractSnsPublisher } from './lib/sns/AbstractSnsPublisher'
export type { SNSMessageOptions } from './lib/sns/AbstractSnsPublisher'

export type { CommonMessage } from './lib/types/MessageTypes'

export { deserializeSNSMessage } from './lib/sns/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
} from './lib/utils/snsUtils'

export { subscribeToTopic } from './lib/sns/SnsSubscriber'
export { initSns } from './lib/sns/SnsInitter'
