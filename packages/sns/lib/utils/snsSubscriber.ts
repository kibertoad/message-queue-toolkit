import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import { SubscribeCommand } from '@aws-sdk/client-sns'
import type { SubscribeCommandInput } from '@aws-sdk/client-sns/dist-types/commands/SubscribeCommand'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { ExtraParams } from '@message-queue-toolkit/core'
import type { ExtraSQSCreationParams } from '@message-queue-toolkit/sqs'
import { assertQueue } from '@message-queue-toolkit/sqs'

import type { ExtraSNSCreationParams } from '../sns/AbstractSnsService'

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
  extraParams?: ExtraSNSCreationParams & ExtraSQSCreationParams & ExtraParams,
) {
  const topicArn = await assertTopic(snsClient, topicConfiguration, {
    queueUrlsWithSubscribePermissionsPrefix: extraParams?.queueUrlsWithSubscribePermissionsPrefix,
  })
  const { queueUrl, queueArn } = await assertQueue(sqsClient, queueConfiguration, {
    topicArnsWithPublishPermissionsPrefix: extraParams?.topicArnsWithPublishPermissionsPrefix,
  })

  const subscribeCommand = new SubscribeCommand({
    TopicArn: topicArn,
    Endpoint: queueArn,
    Protocol: 'sqs',
    ReturnSubscriptionArn: true,
    ...subscriptionConfiguration,
  })

  try {
    const subscriptionResult = await snsClient.send(subscribeCommand)
    return {
      subscriptionArn: subscriptionResult.SubscriptionArn,
      topicArn,
      queueUrl,
      queueArn,
    }
  } catch (err) {
    const logger = extraParams?.logger ?? console
    // @ts-ignore
    logger.error(
      `Error while creating subscription for queue "${queueConfiguration.QueueName}", topic "${
        topicConfiguration.Name
      }": ${(err as Error).message}`,
    )
    throw err
  }
}
