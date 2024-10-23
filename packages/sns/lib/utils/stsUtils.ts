import { GetCallerIdentityCommand, type STSClient } from '@aws-sdk/client-sts'

/**
 * Manually builds the ARN of a topic based on the current AWS account and the topic name.
 * It follows the following pattern: arn:aws:sns:<region>:<account-id>:<topic-name>
 * Doc -> https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
 */
export const buildTopicArn = async (client: STSClient, topicName: string) => {
  const identityResponse = await client.send(new GetCallerIdentityCommand({}))
  const region =
    typeof client.config.region === 'string' ? client.config.region : await client.config.region()

  return `arn:aws:sns:${region}:${identityResponse.Account}:${topicName}`
}
