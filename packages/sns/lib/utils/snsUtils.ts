import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import {
  CreateTopicCommand,
  DeleteTopicCommand,
  GetSubscriptionAttributesCommand,
  GetTopicAttributesCommand,
  ListSubscriptionsByTopicCommand,
  SetTopicAttributesCommand,
  UnsubscribeCommand,
} from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'

import type { ExtraSNSCreationParams } from '../sns/AbstractSnsService'

import { generateTopicSubscriptionPolicy } from './snsAttributeUtils'

type AttributesResult = {
  attributes?: Record<string, string>
}

export async function getTopicAttributes(
  snsClient: SNSClient,
  topicArn: string,
): Promise<Either<'not_found', AttributesResult>> {
  const command = new GetTopicAttributesCommand({
    TopicArn: topicArn,
  })

  try {
    const response = await snsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // @ts-ignore
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        // @ts-ignore
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function getSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
): Promise<Either<'not_found', AttributesResult>> {
  const command = new GetSubscriptionAttributesCommand({
    SubscriptionArn: subscriptionArn,
  })

  try {
    const response = await snsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // @ts-ignore
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        // @ts-ignore
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function assertTopic(
  snsClient: SNSClient,
  topicOptions: CreateTopicCommandInput,
  extraParams?: ExtraSNSCreationParams,
) {
  const command = new CreateTopicCommand(topicOptions)
  const response = await snsClient.send(command)

  if (!response.TopicArn) {
    throw new Error('No topic arn in response')
  }
  const topicArn = response.TopicArn

  if (extraParams?.queueUrlsWithSubscribePermissionsPrefix || extraParams?.sourceOwner) {
    const setTopicAttributesCommand = new SetTopicAttributesCommand({
      TopicArn: topicArn,
      AttributeName: 'Policy',
      AttributeValue: generateTopicSubscriptionPolicy({
        topicArn,
        supportedSqsQueueUrlPrefix: extraParams.queueUrlsWithSubscribePermissionsPrefix,
        sourceOwner: extraParams.sourceOwner,
      }),
    })
    await snsClient.send(setTopicAttributesCommand)
  }

  return topicArn
}

export async function deleteTopic(client: SNSClient, topicName: string) {
  try {
    const topicArn = await assertTopic(client, {
      Name: topicName,
    })

    const command = new DeleteTopicCommand({
      TopicArn: topicArn,
    })

    await client.send(command)
  } catch (err) {
    // we don't care it operation has failed
  }
}

export async function deleteSubscription(client: SNSClient, subscriptionArn: string) {
  const command = new UnsubscribeCommand({
    SubscriptionArn: subscriptionArn,
  })
  try {
    await client.send(command)
  } catch (err) {
    // we don't care it operation has failed
  }
}

export async function findSubscriptionByTopicAndQueue(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
) {
  const listSubscriptionsCommand = new ListSubscriptionsByTopicCommand({
    TopicArn: topicArn,
  })

  const listSubscriptionResult = await snsClient.send(listSubscriptionsCommand)
  return listSubscriptionResult.Subscriptions?.find((entry) => {
    return entry.Endpoint === queueArn
  })
}
