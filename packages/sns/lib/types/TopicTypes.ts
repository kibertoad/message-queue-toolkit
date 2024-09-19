import type { CreateTopicCommandInput } from '@aws-sdk/client-sns'
import type { SNSTopicLocatorType } from '../sns/AbstractSnsService'

export type TopicResolutionOptions = CreateTopicCommandInput | SNSTopicLocatorType

export function isCreateTopicCommand(value: unknown): value is CreateTopicCommandInput {
  return !!value && !!(value as CreateTopicCommandInput).Name
}

export function isSNSTopicLocatorType(value: unknown): value is SNSTopicLocatorType {
  return (
    !!value &&
    (!!(value as SNSTopicLocatorType).topicArn || !!(value as SNSTopicLocatorType).topicName)
  )
}
