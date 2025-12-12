import type { ZodSchema } from 'zod/v4'

// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
const POLICY_VERSION = '2012-10-17'

export type TopicSubscriptionPolicyParams = {
  topicArn: string
  allowedSqsQueueUrlPrefix?: string | readonly string[]
  allowedSourceOwner?: string
}

export function generateTopicSubscriptionPolicy(params: TopicSubscriptionPolicyParams) {
  const policyObject = {
    Version: POLICY_VERSION,
    Id: '__default_policy_ID',
    Statement: [
      {
        Sid: 'AllowSQSSubscription',
        Effect: 'Allow',
        Principal: {
          AWS: '*',
        },
        Action: ['sns:Subscribe'],
        Resource: params.topicArn,
        Condition: {},
      },
    ],
  }

  if (params.allowedSourceOwner) {
    // @ts-expect-error
    policyObject.Statement[0].Condition.StringEquals = {
      'AWS:SourceOwner': params.allowedSourceOwner,
    }
  }
  if (params.allowedSqsQueueUrlPrefix?.length && params.allowedSqsQueueUrlPrefix.length > 0) {
    // @ts-expect-error
    policyObject.Statement[0].Condition.StringLike = {
      'sns:Endpoint': params.allowedSqsQueueUrlPrefix,
    }
  }

  return JSON.stringify(policyObject)
}

export function generateFilterAttributes(
  // biome-ignore lint/suspicious/noExplicitAny: Expected
  messageSchemas: ZodSchema<any>[],
  messageTypePath: string,
) {
  const messageTypes = messageSchemas.map((schema) => {
    // @ts-expect-error
    return schema.shape[messageTypePath].value as string
  })

  return {
    FilterPolicy: JSON.stringify({
      type: messageTypes,
    }),
    FilterPolicyScope: 'MessageBody',
  }
}
