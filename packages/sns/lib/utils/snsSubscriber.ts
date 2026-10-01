import { isDeepStrictEqual } from 'node:util'
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
import { assertTopic, findConfirmedSubscriptionArn, getSubscriptionAttributes } from './snsUtils.ts'
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
      /**
       * When enabled, only `FilterPolicy` and `FilterPolicyScope` are managed. Other subscription
       * attributes (e.g. `RawMessageDelivery`, `RedrivePolicy`) are neither checked nor written,
       * and are expected to be set by external tooling such as Terraform.
       */
      manageOnlyFilterPolicy?: boolean
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
 * Subscribes an existing SQS queue to an existing SNS topic. An existing subscription is checked
 * with read-only calls first and is only written to when its attributes differ from the
 * configured ones and `updateAttributesIfExists` is enabled. Only the differing attributes are
 * written.
 */
export async function assertSubscription(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
  subscriptionConfiguration: SNSSubscriptionCreationOptions,
  errorContext: { queueName?: string; topicName?: string },
  logger?: CommonLogger,
): Promise<string | undefined> {
  const { updateAttributesIfExists, manageOnlyFilterPolicy, locateOnly, ...subscribeInput } =
    subscriptionConfiguration
  const attributes = resolveManagedAttributes(subscribeInput.Attributes, manageOnlyFilterPolicy)
  const resolvedLogger = logger ?? console
  const errMessagePrefix = `Error while creating subscription for queue "${errorContext.queueName}", topic "${errorContext.topicName}"`

  const reconcileExistingSubscription = async (subscriptionArn: string) => {
    const changedAttributes = await findChangedSubscriptionAttributes(
      snsClient,
      subscriptionArn,
      attributes,
    )
    if (Object.keys(changedAttributes).length === 0) return subscriptionArn

    const errMessage = `${errMessagePrefix}: Subscription already exists with different attributes: ${Object.keys(changedAttributes).join(', ')}`
    if (!updateAttributesIfExists) {
      resolvedLogger.error(errMessage)
      throw new InternalError({
        errorCode: 'sns_subscription_creation_failed',
        message: errMessage,
        details: { queueName: errorContext.queueName, topicArn },
      })
    }

    resolvedLogger.warn(`${errMessage}. Updating subscription`)
    await writeSubscriptionAttributes(snsClient, subscriptionArn, changedAttributes)
    return subscriptionArn
  }

  const existingSubscriptionArn = await findConfirmedSubscriptionArn(snsClient, topicArn, queueArn)
  if (existingSubscriptionArn) return reconcileExistingSubscription(existingSubscriptionArn)

  try {
    const subscriptionResult = await snsClient.send(
      new SubscribeCommand({
        ...subscribeInput,
        Attributes: attributes,
        TopicArn: topicArn,
        Endpoint: queueArn,
        Protocol: 'sqs',
        ReturnSubscriptionArn: true,
      }),
    )
    return subscriptionResult.SubscriptionArn
  } catch (err) {
    if (!isError(err)) throw err

    // Another instance may have created the subscription after the lookup above
    if (err.message.includes('Subscription already exists with different attributes')) {
      const concurrentSubscriptionArn = await findConfirmedSubscriptionArn(
        snsClient,
        topicArn,
        queueArn,
      )
      if (concurrentSubscriptionArn) return reconcileExistingSubscription(concurrentSubscriptionArn)
    }

    const errMessage = `${errMessagePrefix}: ${err.message}`
    resolvedLogger.error(errMessage)
    throw new InternalError({
      errorCode: 'sns_subscription_creation_failed',
      message: errMessage,
      details: { queueName: errorContext.queueName, topicArn, originalError: err.message },
      cause: err,
    })
  }
}

/**
 * Applies the given attributes to an existing subscription. Current attributes are read first, and
 * only the ones that differ are written.
 */
export async function setSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: NonNullable<SubscribeCommandInput['Attributes']>,
): Promise<void> {
  const changedAttributes = await findChangedSubscriptionAttributes(
    snsClient,
    subscriptionArn,
    attributes,
  )
  await writeSubscriptionAttributes(snsClient, subscriptionArn, changedAttributes)
}

// SNS only allows setting a single attribute per call
async function writeSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: Record<string, string>,
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

const FILTER_POLICY_ATTRIBUTES = ['FilterPolicy', 'FilterPolicyScope']

// Values SNS reports for attributes that were never set explicitly
const SUBSCRIPTION_ATTRIBUTE_DEFAULTS: Record<string, string> = {
  FilterPolicyScope: 'MessageAttributes',
  RawMessageDelivery: 'false',
}

function resolveManagedAttributes(
  attributes: Record<string, string> | undefined,
  manageOnlyFilterPolicy: boolean | undefined,
): Record<string, string> | undefined {
  if (!attributes || !manageOnlyFilterPolicy) return attributes

  return Object.fromEntries(
    Object.entries(attributes).filter(([name]) => FILTER_POLICY_ATTRIBUTES.includes(name)),
  )
}

async function findChangedSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: Record<string, string> | undefined,
): Promise<Record<string, string>> {
  if (!attributes || Object.keys(attributes).length === 0) return {}

  const currentAttributesResult = await getSubscriptionAttributes(snsClient, subscriptionArn)
  const currentAttributes = currentAttributesResult.result?.attributes ?? {}

  return Object.fromEntries(
    Object.entries(attributes).filter(
      ([name, value]) =>
        !isSameAttributeValue(
          name,
          value,
          currentAttributes[name] ?? SUBSCRIPTION_ATTRIBUTE_DEFAULTS[name],
        ),
    ),
  )
}

// JSON attributes are compared structurally, as SNS does not preserve their formatting
function isSameAttributeValue(name: string, expected: string, current: string | undefined) {
  if (expected === current) return true
  if (current === undefined || !name.endsWith('Policy')) return false

  try {
    return isDeepStrictEqual(JSON.parse(expected), JSON.parse(current))
  } catch {
    return false
  }
}
