import type { ZodSchema } from 'zod'

// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
const POLICY_VERSION = '2012-10-17'

export function generateTopicSubscriptionPolicy(
  topicArn: string,
  supportedSqsQueueNamePrefix: string,
) {
  return `{"Version":"${POLICY_VERSION}","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSQSSubscription","Effect":"Allow","Principal":{"AWS":"*"},"Action":["sns:Subscribe"],"Resource":"${topicArn}","Condition":{"StringLike":{"sns:Endpoint":"${supportedSqsQueueNamePrefix}"}}}]}`
}

export function generateQueuePublishForTopicPolicy(
  queueUrl: string,
  supportedSnsTopicArnPrefix: string,
) {
  return `{"Version":"${POLICY_VERSION}","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"${queueUrl}","Condition":{"ArnLike":{"aws:SourceArn":"${supportedSnsTopicArnPrefix}"}}}]}`
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
