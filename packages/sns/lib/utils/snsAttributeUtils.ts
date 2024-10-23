import { GetCallerIdentityCommand, type STSClient } from '@aws-sdk/client-sts'
import type { ZodSchema } from 'zod'

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
    // @ts-ignore
    policyObject.Statement[0].Condition.StringEquals = {
      'AWS:SourceOwner': params.allowedSourceOwner,
    }
  }
  if (params.allowedSqsQueueUrlPrefix?.length && params.allowedSqsQueueUrlPrefix.length > 0) {
    // @ts-ignore
    policyObject.Statement[0].Condition.StringLike = {
      'sns:Endpoint': params.allowedSqsQueueUrlPrefix,
    }
  }

  return JSON.stringify(policyObject)
}

export function generateFilterAttributes(
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
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

/**
 * Manually builds the ARN of a topic based on the current AWS account and the topic name.
 * It follows the following pattern: arn:aws:sns:<region>:<account-id>:<topic-name>
 * Doc -> https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
 *
 * // TODO: add tests
 */
export const buildTopicArn = async (client: STSClient, topicName: string) => {
  const identityResponse = await client.send(new GetCallerIdentityCommand({}))
  const region =
    typeof client.config.region === 'string' ? client.config.region : await client.config.region()

  return `arn:aws:sns:${region}:${identityResponse.Account}:${topicName}`
}
