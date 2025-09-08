// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
import {
  SQS_RESOURCE_ANY,
  SQS_RESOURCE_CURRENT_QUEUE,
  type SQSPolicyConfig,
} from '../sqs/AbstractSqsService.js'

const POLICY_VERSION = '2012-10-17'

export function generateQueuePublishForTopicPolicy(
  queueArn: string,
  supportedSnsTopicArnPrefix: string,
) {
  return `{"Version":"${POLICY_VERSION}","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"${queueArn}","Condition":{"ArnLike":{"aws:SourceArn":"${supportedSnsTopicArnPrefix}"}}}]}`
}

export function generateQueuePolicyFromPolicyConfig(
  queueArn: string,
  policyConfig: SQSPolicyConfig,
): string {
  const statements = (
    Array.isArray(policyConfig.statements) ? policyConfig.statements : [policyConfig.statements]
  ).map((statement) => ({
    Effect: statement?.Effect ?? 'Allow',
    Principal: { AWS: statement?.Principal ?? '*' },
    Action: statement?.Action ?? ['sqs:SendMessage', 'sqs:GetQueueAttributes', 'sqs:GetQueueUrl'],
  }))

  const resource =
    policyConfig.resource === SQS_RESOURCE_CURRENT_QUEUE
      ? queueArn
      : policyConfig.resource === SQS_RESOURCE_ANY
        ? `arn:aws:sqs:*:*:*`
        : policyConfig.resource

  return JSON.stringify({
    Version: POLICY_VERSION,
    Resource: resource,
    Statement: statements,
  })
}

export function generateWildcardSqsArn(sqsQueueArnPrefix: string) {
  return `arn:aws:sqs:*:*:${sqsQueueArnPrefix}`
}

export function generateWildcardSnsArn(snsTopicArnPrefix: string) {
  return `arn:aws:sns:*:*:${snsTopicArnPrefix}`
}
