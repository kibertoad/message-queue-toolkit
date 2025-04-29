import {
  GetCallerIdentityCommand,
  type GetCallerIdentityCommandOutput,
  type STSClient,
} from '@aws-sdk/client-sts'

let callerIdentityPromise: Promise<void> | undefined
let callerIdentityCached: GetCallerIdentityCommandOutput | undefined

/**
 * Manually builds the ARN of a topic based on the current AWS account and the topic name.
 * It follows the following pattern: arn:aws:sns:<region>:<account-id>:<topic-name>
 * Doc -> https://docs.aws.amazon.com/IAM/latest/UserGuide/reference-arns.html
 */
export const buildTopicArn = async (client: STSClient, topicName: string) => {
  const identityResponse = await getAndCacheCallerIdentity(client)
  const region =
    typeof client.config.region === 'string' ? client.config.region : await client.config.region()

  return `arn:aws:sns:${region}:${identityResponse.Account}:${topicName}`
}

export const clearCachedCallerIdentity = () => {
  callerIdentityPromise = undefined
  callerIdentityCached = undefined
}

const getAndCacheCallerIdentity = async (
  client: STSClient,
): Promise<GetCallerIdentityCommandOutput> => {
  if (!callerIdentityCached) {
    if (!callerIdentityPromise) {
      callerIdentityPromise = client.send(new GetCallerIdentityCommand({})).then((response) => {
        callerIdentityCached = response
      })
    }

    await callerIdentityPromise
    callerIdentityPromise = undefined
  }

  // biome-ignore lint/style/noNonNullAssertion: <explanation>
  return callerIdentityCached!
}
