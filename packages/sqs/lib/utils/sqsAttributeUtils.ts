// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
const POLICY_VERSION = '2012-10-17'

export function generateQueuePublishForTopicPolicy(
  queueArn: string,
  supportedSnsTopicArnPrefix: string,
) {
  return `{"Version":"${POLICY_VERSION}","Id":"__default_policy_ID","Statement":[{"Sid":"AllowSNSPublish","Effect":"Allow","Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"${queueArn}","Condition":{"ArnLike":{"aws:SourceArn":"${supportedSnsTopicArnPrefix}"}}}]}`
}

export function generateWildcardSqsArn(sqsQueueArnPrefix: string) {
  return `arn:aws:sqs:*:*:${sqsQueueArnPrefix}`
}

export function generateWildcardSnsArn(snsTopicArnPrefix: string) {
  return `arn:aws:sns:*:*:${snsTopicArnPrefix}`
}
