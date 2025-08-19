import type { SNSClient, SubscribeCommandInput } from '@aws-sdk/client-sns'
import { SetSubscriptionAttributesCommand, SubscribeCommand } from '@aws-sdk/client-sns'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { ExtraParams } from '@message-queue-toolkit/core'
import type { ExtraSQSCreationParams } from '@message-queue-toolkit/sqs'
import { assertQueue } from '@message-queue-toolkit/sqs'
import type { ExtraSNSCreationParams } from '../sns/AbstractSnsService.ts'
import {
  isCreateTopicCommand,
  isSNSTopicLocatorType,
  type TopicResolutionOptions,
} from '../types/TopicTypes.ts'
import { assertTopic, findSubscriptionByTopicAndQueue, getTopicArnByName } from './snsUtils.ts'

export type SNSSubscriptionOptions = Omit<
  SubscribeCommandInput,
  'TopicArn' | 'Endpoint' | 'Protocol' | 'ReturnSubscriptionArn'
> & { updateAttributesIfExists: boolean }

async function resolveTopicArnToSubscribeTo(
  snsClient: SNSClient,
  stsClient: STSClient,
  topicConfiguration: TopicResolutionOptions,
  extraParams: (ExtraSNSCreationParams & ExtraSQSCreationParams & ExtraParams) | undefined,
) {
  //If topicArn is present, let's use it and return early.
  if (isSNSTopicLocatorType(topicConfiguration) && topicConfiguration.topicArn) {
    return topicConfiguration.topicArn
  }

  //If input configuration is capable of creating a topic, let's create it and return its ARN.
  if (isCreateTopicCommand(topicConfiguration)) {
    return await assertTopic(snsClient, stsClient, topicConfiguration, {
      queueUrlsWithSubscribePermissionsPrefix: extraParams?.queueUrlsWithSubscribePermissionsPrefix,
      allowedSourceOwner: extraParams?.allowedSourceOwner,
      forceTagUpdate: extraParams?.forceTagUpdate,
    })
  }

  //Last option: let's not create a topic but resolve a ARN based on the desired topic name.
  return await getTopicArnByName(snsClient, topicConfiguration.topicName)
}

export async function subscribeToTopic(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  queueConfiguration: CreateQueueCommandInput,
  topicConfiguration: TopicResolutionOptions,
  subscriptionConfiguration: SNSSubscriptionOptions,
  extraParams?: ExtraSNSCreationParams & ExtraSQSCreationParams & ExtraParams,
) {
  const topicArn = await resolveTopicArnToSubscribeTo(
    snsClient,
    stsClient,
    topicConfiguration,
    extraParams,
  )

  const { queueUrl, queueArn } = await assertQueue(sqsClient, queueConfiguration, {
    topicArnsWithPublishPermissionsPrefix: extraParams?.topicArnsWithPublishPermissionsPrefix,
    updateAttributesIfExists: extraParams?.updateAttributesIfExists,
    forceTagUpdate: extraParams?.forceTagUpdate,
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
    logger.error(
      `Error while creating subscription for queue "${queueConfiguration.QueueName}", topic "${
        isCreateTopicCommand(topicConfiguration)
          ? topicConfiguration.Name
          : topicConfiguration.topicName
      }": ${(err as Error).message}`,
    )

    if (
      subscriptionConfiguration.updateAttributesIfExists &&
      (err as Error).message.indexOf('Subscription already exists with different attributes') !== -1
    ) {
      const result = await tryToUpdateSubscription(
        snsClient,
        topicArn,
        queueArn,
        subscriptionConfiguration,
      )
      if (!result) {
        logger.error('Failed to update subscription')
        throw err
      }
      return {
        subscriptionArn: result.SubscriptionArn,
        topicArn,
        queueUrl,
        queueArn,
      }
    }

    throw err
  }
}

async function tryToUpdateSubscription(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
  subscriptionConfiguration: SNSSubscriptionOptions,
) {
  const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
  if (!subscription || !subscriptionConfiguration.Attributes) {
    return undefined
  }

  const setSubscriptionAttributesCommands = Object.entries(
    subscriptionConfiguration.Attributes,
  ).map(([key, value]) => {
    return new SetSubscriptionAttributesCommand({
      SubscriptionArn: subscription.SubscriptionArn,
      AttributeName: key,
      AttributeValue: value,
    })
  })

  for (const command of setSubscriptionAttributesCommands) {
    await snsClient.send(command)
  }

  return subscription
}
