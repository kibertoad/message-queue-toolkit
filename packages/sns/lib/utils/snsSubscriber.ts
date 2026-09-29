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

/**
 * Options for the subscription of a queue to a topic.
 *
 * There are two modes for this config:
 * 1. Creation (default): the subscription is created, or updated if it already exists.
 * 2. Locate only: for subscriptions managed externally. The subscription is only located, never created,
 *  and the given attributes (e.g. `FilterPolicy`) are applied to it.
 */
export type SNSSubscriptionOptions =
  /** Creation */
  | (Omit<SubscribeCommandInput, 'TopicArn' | 'Endpoint' | 'Protocol' | 'ReturnSubscriptionArn'> & {
      updateAttributesIfExists: boolean
      /** Should not be present when the subscription is created */
      locateOnly?: never
    })
  /** Locate only */
  | {
      /** Marks the subscription as located only */
      locateOnly: true
      /** Attributes applied to the located subscription */
      Attributes?: SubscribeCommandInput['Attributes']
    }

/** Subscription options that create the subscription, required when subscribing */
export type SNSSubscriptionCreationOptions = Exclude<SNSSubscriptionOptions, { locateOnly: true }>

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
  subscriptionConfiguration: SNSSubscriptionCreationOptions,
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
  subscriptionConfiguration: SNSSubscriptionCreationOptions,
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
  subscriptionConfiguration: SNSSubscriptionCreationOptions,
) {
  const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
  if (!subscription?.SubscriptionArn || !subscriptionConfiguration.Attributes) {
    return undefined
  }

  await setSubscriptionAttributes(
    snsClient,
    subscription.SubscriptionArn,
    subscriptionConfiguration.Attributes,
  )

  return subscription
}

/**
 * Applies the given attributes to an existing subscription, one by one as SNS only allows setting a single
 * attribute per call.
 */
export async function setSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: NonNullable<SubscribeCommandInput['Attributes']>,
): Promise<void> {
  for (const [key, value] of Object.entries(attributes)) {
    await snsClient.send(
      new SetSubscriptionAttributesCommand({
        SubscriptionArn: subscriptionArn,
        AttributeName: key,
        AttributeValue: value,
      }),
    )
  }
}
