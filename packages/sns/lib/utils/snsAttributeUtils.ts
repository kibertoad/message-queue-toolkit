import type { ZodSchema } from 'zod'

// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
const POLICY_VERSION = '2012-10-17'

export type TopicSubscriptionPolicyParams = {
  topicArn: string
  allowedSqsQueueUrlPrefix?: string
  allowedSourceOwner?: string
}

export function generateTopicSubscriptionPolicy(params: TopicSubscriptionPolicyParams) {
  const sourceOwnerFragment = params.allowedSourceOwner
    ? `"StringEquals":{"AWS:SourceOwner": "${params.allowedSourceOwner}"}`
    : ''
  const supportedSqsQueueUrlPrefixFragment = params.allowedSqsQueueUrlPrefix
    ? `"StringLike":{"sns:Endpoint":"${params.allowedSqsQueueUrlPrefix}"}`
    : ''
  const commaFragment =
    sourceOwnerFragment.length > 0 && supportedSqsQueueUrlPrefixFragment.length > 0 ? ',' : ''

  return `{"Version":"${POLICY_VERSION}","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"${params.topicArn}","Condition":{${sourceOwnerFragment}${commaFragment}${supportedSqsQueueUrlPrefixFragment}}}]}`
}

export function generateFilterAttributes(
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  messageSchemas: ZodSchema<any>[],
  messageTypeField: string,
) {
  const messageTypes = messageSchemas.map((schema) => {
    // @ts-ignore
    // eslint-disable-next-line @typescript-eslint/no-unsafe-member-access
    return schema.shape[messageTypeField].value as string
  })

  return {
    FilterPolicy: JSON.stringify({
      type: messageTypes,
    }),
    FilterPolicyScope: 'MessageBody',
  }
}
