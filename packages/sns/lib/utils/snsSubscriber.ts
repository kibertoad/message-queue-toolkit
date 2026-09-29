import type { SNSClient, SubscribeCommandInput } from '@aws-sdk/client-sns'
import { SetSubscriptionAttributesCommand, SubscribeCommand } from '@aws-sdk/client-sns'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import {
  type CommonLogger,
  InternalError,
  isError,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { ExtraParams } from '@message-queue-toolkit/core'
import type { ExtraSQSCreationParams } from '@message-queue-toolkit/sqs'
import { assertQueue } from '@message-queue-toolkit/sqs'
import type { ExtraSNSCreationParams } from '../sns/AbstractSnsService.ts'
import {
  isCreateTopicCommand,
  isSNSTopicLocatorType,
  type TopicResolutionOptions,
} from '../types/TopicTypes.ts'
import { assertTopic, findSubscriptionByTopicAndQueue } from './snsUtils.ts'
import { buildTopicArn } from './stsUtils.ts'

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
  if (isSNSTopicLocatorType(topicConfiguration)) {
    if (topicConfiguration.topicArn) return topicConfiguration.topicArn
    if (topicConfiguration.topicName) return buildTopicArn(stsClient, topicConfiguration.topicName)
  }

  if (isCreateTopicCommand(topicConfiguration)) {
    return await assertTopic(snsClient, stsClient, topicConfiguration, {
      queueUrlsWithSubscribePermissionsPrefix: extraParams?.queueUrlsWithSubscribePermissionsPrefix,
      allowedSourceOwner: extraParams?.allowedSourceOwner,
      forceTagUpdate: extraParams?.forceTagUpdate,
    })
  }

  throw new InternalError({
    errorCode: 'invalid_topic_configuration',
    message: 'Invalid topic configuration provided, cannot resolve topic ARN',
    details: { topicConfiguration: stringValueSerializer(topicConfiguration) },
  })
}

/**
 * Asserts the SNS topic (or resolves the ARN of a located one), asserts the SQS queue and then
 * asserts the subscription of the queue to the topic.
 */
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

  const subscriptionArn = await assertSubscription(
    snsClient,
    topicArn,
    queueArn,
    subscriptionConfiguration,
    {
      queueName: queueConfiguration.QueueName,
      topicName: isCreateTopicCommand(topicConfiguration)
        ? topicConfiguration.Name
        : topicConfiguration.topicName,
    },
    extraParams?.logger,
  )

  return { subscriptionArn, topicArn, queueUrl, queueArn }
}

/**
 * Subscribes an existing SQS queue to an existing SNS topic. Subscribing is idempotent, so an
 * existing subscription is returned as is, or has its attributes updated when they differ and
 * `updateAttributesIfExists` is enabled.
 */
export async function assertSubscription(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
  subscriptionConfiguration: SNSSubscriptionOptions,
  errorContext: { queueName?: string; topicName?: string },
  logger?: CommonLogger,
): Promise<string | undefined> {
  const subscribeCommand = new SubscribeCommand({
    TopicArn: topicArn,
    Endpoint: queueArn,
    Protocol: 'sqs',
    ReturnSubscriptionArn: true,
    ...subscriptionConfiguration,
  })

  try {
    const subscriptionResult = await snsClient.send(subscribeCommand)
    return subscriptionResult.SubscriptionArn
  } catch (err) {
    if (!isError(err)) throw err

    const resolvedLogger = logger ?? console
    const errMessage = `Error while creating subscription for queue "${errorContext.queueName}", topic "${errorContext.topicName}": ${err.message}`

    if (
      subscriptionConfiguration.updateAttributesIfExists &&
      err.message.includes('Subscription already exists with different attributes')
    ) {
      resolvedLogger.warn(`${errMessage}. Trying to update subscription`)

      const result = await tryToUpdateSubscription(
        snsClient,
        topicArn,
        queueArn,
        subscriptionConfiguration,
      )
      if (result) return result.SubscriptionArn

      resolvedLogger.error('Failed to update subscription')
    } else {
      resolvedLogger.error(errMessage)
    }

    throw new InternalError({
      errorCode: 'sns_subscription_creation_failed',
      message: errMessage,
      details: { queueName: errorContext.queueName, topicArn, originalError: err.message },
      cause: err,
    })
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
