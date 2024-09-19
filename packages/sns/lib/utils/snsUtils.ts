import {
  type CreateTopicCommandInput,
  type SNSClient,
  paginateListTopics,
} from '@aws-sdk/client-sns'
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
import { calculateOutgoingMessageSize as sqsCalculateOutgoingMessageSize } from '@message-queue-toolkit/sqs'

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

  if (extraParams?.queueUrlsWithSubscribePermissionsPrefix || extraParams?.allowedSourceOwner) {
    const setTopicAttributesCommand = new SetTopicAttributesCommand({
      TopicArn: topicArn,
      AttributeName: 'Policy',
      AttributeValue: generateTopicSubscriptionPolicy({
        topicArn,
        allowedSqsQueueUrlPrefix: extraParams.queueUrlsWithSubscribePermissionsPrefix,
        allowedSourceOwner: extraParams.allowedSourceOwner,
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
  } catch (_) {
    // we don't care it operation has failed
  }
}

export async function deleteSubscription(client: SNSClient, subscriptionArn: string) {
  const command = new UnsubscribeCommand({
    SubscriptionArn: subscriptionArn,
  })
  try {
    await client.send(command)
  } catch (_) {
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

export async function getTopicArnByName(snsClient: SNSClient, topicName?: string): Promise<string> {
  if (!topicName) {
    throw new Error('topicName is not provided')
  }

  // Use paginator to automatically handle NextToken
  const paginator = paginateListTopics({ client: snsClient }, {})
  for await (const page of paginator) {
    for (const topic of page.Topics || []) {
      if (topic.TopicArn?.includes(topicName)) {
        return topic.TopicArn
      }
    }
  }
  throw new Error(`Failed to resolve topic by name ${topicName}`)
}

/**
 * Calculates the size of an outgoing SNS message.
 *
 * SNS imposes a 256 KB limit on the total size of a message, which includes both the message body and any metadata (attributes).
 * This function currently computes the size based solely on the message body, as no attributes are included at this time.
 * For future updates, if message attributes are added, their sizes should also be considered.
 *
 * Reference: https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
 *
 * A wrapper around the equivalent function in the SQS package.
 */
export const calculateOutgoingMessageSize = (message: unknown) =>
  sqsCalculateOutgoingMessageSize(message)
