export type {
  SNSTopicAWSConfig,
  SQSQueueConfig,
  SNSConsumerDependencies,
} from './lib/sns/AbstractSnsService'

export { SnsConsumerErrorResolver } from './lib/errors/SnsConsumerErrorResolver'
export { SnsValidationError, SnsMessageInvalidFormat } from './lib/errors/snsErrors'

export { AbstractSnsPublisher } from './lib/sns/AbstractSnsPublisher'
export type { SNSMessageOptions } from './lib/sns/AbstractSnsPublisher'

export type { CommonMessage } from './lib/types/MessageTypes'

export { deserializeSNSMessage } from './lib/sns/snsMessageDeserializer'
