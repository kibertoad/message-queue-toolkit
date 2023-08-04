import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import { SubscribeCommand } from '@aws-sdk/client-sns'
import type { SubscribeCommandInput } from '@aws-sdk/client-sns/dist-types/commands/SubscribeCommand'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs'
import { assertQueue } from '@message-queue-toolkit/sqs'

import { assertTopic } from './snsUtils'

export type SNSSubscriptionOptions = Omit<
  SubscribeCommandInput,
  'TopicArn' | 'Endpoint' | 'Protocol' | 'ReturnSubscriptionArn'
>

export async function subscribeToTopic(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  queueConfiguration: CreateQueueCommandInput,
  topicConfiguration: CreateTopicCommandInput,
  subscriptionConfiguration: SNSSubscriptionOptions,
) {
  const topicArn = await assertTopic(snsClient, topicConfiguration)
  const queueUrl = await assertQueue(sqsClient, queueConfiguration)

  const getQueueAttributesCommand = new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: ['QueueArn'],
  })
  const queueAttributesResponse = await sqsClient.send(getQueueAttributesCommand)
  const sqsArn = queueAttributesResponse.Attributes?.QueueArn

  if (!sqsArn) {
    throw new Error(`Queue ${queueUrl} ARN is not defined`)
  }

  const subscribeCommand = new SubscribeCommand({
    TopicArn: topicArn,
    Endpoint: sqsArn,
    Protocol: 'sqs',
    ReturnSubscriptionArn: true,
    ...subscriptionConfiguration,
  })

  const subscriptionResult = await snsClient.send(subscribeCommand)
  return {
    subscriptionArn: subscriptionResult.SubscriptionArn,
    topicArn,
    queueUrl,
  }
}
