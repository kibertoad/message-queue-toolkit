export type {
  SNSTopicAWSConfig,
  SNSTopicConfig,
  SNSConsumerDependencies,
  SNSQueueLocatorType,
} from './lib/sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './lib/errors/SnsConsumerErrorResolver'

export { AbstractSnsPublisher, type SNSMessageOptions } from './lib/sns/AbstractSnsPublisher'

export { SNSDependencies } from './lib/sns/AbstractSnsService'

export {
  AbstractSnsSqsConsumer,
  type SNSSQSConsumerDependencies,
} from './lib/sns/AbstractSnsSqsConsumer'

export type { CommonMessage } from './lib/types/MessageTypes'

export { deserializeSNSMessage } from './lib/sns/snsMessageDeserializer'

export {
  assertTopic,
  deleteTopic,
  getTopicAttributes,
  deleteSubscription,
} from './lib/utils/snsUtils'
