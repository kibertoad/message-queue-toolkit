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

/** Subscription attributes that can be managed for an SQS subscription */
export const SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES = [
  'FilterPolicy',
  'FilterPolicyScope',
  'RawMessageDelivery',
  'RedrivePolicy',
] as const

export type SubscriptionManagedAttributeName = (typeof SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES)[number]

type SubscriptionAttributeManagementOptions = {
  /**
   * Attributes whose value is owned by this config. Defaults to all of
   * {@link SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES}.
   *
   * A managed attribute that is missing from `Attributes` is reset on the existing subscription
   * (`FilterPolicy` and `RedrivePolicy` are removed, the others go back to their defaults).
   * Attributes that are not managed are neither checked nor written, so external tooling such as
   * Terraform can own them, and must not be present in `Attributes`.
   */
  managedAttributes?: readonly SubscriptionManagedAttributeName[]
}

/**
 * Options for the subscription of a queue to a topic.
 *
 * There are two modes for this config:
 * 1. Creation (default): the subscription is created, or updated if it already exists.
 * 2. Locate only: for subscriptions managed externally. The subscription is only located, never created,
 *  and its managed attributes (e.g. `FilterPolicy`) are applied to it.
 */
export type SNSSubscriptionOptions =
  /** Creation */
  | (Omit<SubscribeCommandInput, 'TopicArn' | 'Endpoint' | 'Protocol' | 'ReturnSubscriptionArn'> &
      SubscriptionAttributeManagementOptions & {
        updateAttributesIfExists: boolean
        /** Should not be present when the subscription is created */
        locateOnly?: never
      })
  /** Locate only */
  | (SubscriptionAttributeManagementOptions & {
      /** Marks the subscription as located only */
      locateOnly: true
      /** Attributes applied to the located subscription */
      Attributes?: SubscribeCommandInput['Attributes']
    })

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
 * with read-only calls first and is only written to when its managed attributes differ from the
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
  const { updateAttributesIfExists, managedAttributes, locateOnly, ...subscribeInput } =
    subscriptionConfiguration
  const resolvedManagedAttributes = resolveManagedAttributes(
    subscribeInput.Attributes,
    managedAttributes,
  )
  const resolvedLogger = logger ?? console
  const errMessagePrefix = `Error while creating subscription for queue "${errorContext.queueName}", topic "${errorContext.topicName}"`

  const reconcileExistingSubscription = async (subscriptionArn: string) => {
    const changedAttributes = await findChangedSubscriptionAttributes(
      snsClient,
      subscriptionArn,
      subscribeInput.Attributes,
      resolvedManagedAttributes,
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
 * Applies the given attributes to an existing subscription, resetting managed attributes that are
 * missing from them. Current attributes are read first, and only the ones that differ are written.
 */
export async function setSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: SubscribeCommandInput['Attributes'],
  managedAttributes?: readonly SubscriptionManagedAttributeName[],
): Promise<void> {
  const changedAttributes = await findChangedSubscriptionAttributes(
    snsClient,
    subscriptionArn,
    attributes,
    resolveManagedAttributes(attributes, managedAttributes),
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
        // AWS rejects an empty RedrivePolicy, omitting the value is what removes it
        AttributeValue: key === 'RedrivePolicy' && value === '' ? undefined : value,
      }),
    )
  }
}

// Values that reset an attribute to the state of a subscription where it was never set
const SUBSCRIPTION_ATTRIBUTE_UNSET_VALUES: Record<SubscriptionManagedAttributeName, string> = {
  FilterPolicy: '{}',
  FilterPolicyScope: 'MessageAttributes',
  RawMessageDelivery: 'false',
  RedrivePolicy: '',
}

function isManagedAttributeName(name: string): name is SubscriptionManagedAttributeName {
  return (SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES as readonly string[]).includes(name)
}

function resolveManagedAttributes(
  attributes: Record<string, string> | undefined,
  managedAttributes: readonly SubscriptionManagedAttributeName[] | undefined,
): readonly SubscriptionManagedAttributeName[] {
  const resolvedManagedAttributes = managedAttributes ?? SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES
  const unmanagedAttributes = Object.keys(attributes ?? {}).filter(
    (name) => isManagedAttributeName(name) && !resolvedManagedAttributes.includes(name),
  )
  if (unmanagedAttributes.length > 0) {
    throw new InternalError({
      errorCode: 'invalid_subscription_configuration',
      message: `Subscription attributes ${unmanagedAttributes.join(', ')} are set, but not listed in managedAttributes`,
    })
  }

  return resolvedManagedAttributes
}

/**
 * Compares the current attributes of the subscription with the expected ones. Every managed
 * attribute is compared, falling back to its unset value when it is not configured. Configured
 * attributes outside {@link SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES} are compared, but never reset.
 */
async function findChangedSubscriptionAttributes(
  snsClient: SNSClient,
  subscriptionArn: string,
  attributes: Record<string, string> | undefined,
  managedAttributes: readonly SubscriptionManagedAttributeName[],
): Promise<Record<string, string>> {
  const expectedAttributes: Record<string, string> = {}
  // Iterating in the declared order writes FilterPolicy before FilterPolicyScope
  for (const name of SUBSCRIPTION_MANAGED_ATTRIBUTE_NAMES) {
    if (managedAttributes.includes(name)) {
      expectedAttributes[name] = attributes?.[name] ?? SUBSCRIPTION_ATTRIBUTE_UNSET_VALUES[name]
    }
  }
  for (const [name, value] of Object.entries(attributes ?? {})) {
    if (!isManagedAttributeName(name)) expectedAttributes[name] = value
  }
  if (Object.keys(expectedAttributes).length === 0) return {}

  const currentAttributesResult = await getSubscriptionAttributes(snsClient, subscriptionArn)
  const currentAttributes = currentAttributesResult.result?.attributes ?? {}

  const changedAttributes = Object.fromEntries(
    Object.entries(expectedAttributes).filter(
      ([name, value]) => !isSameAttributeValue(name, value, currentAttributes[name]),
    ),
  )

  // Filter policy scope means nothing without a filter policy, so it is not reset on its own
  const resultingFilterPolicy = expectedAttributes.FilterPolicy ?? currentAttributes.FilterPolicy
  if (
    isUnsetAttributeValue('FilterPolicy', resultingFilterPolicy) &&
    isUnsetAttributeValue('FilterPolicyScope', changedAttributes.FilterPolicyScope)
  ) {
    delete changedAttributes.FilterPolicyScope
  }

  return changedAttributes
}

function isUnsetAttributeValue(name: string, value: string | undefined): boolean {
  if (value === undefined || value === '') return true
  if (name === 'FilterPolicy' && value === '{}') return true

  return isManagedAttributeName(name) && SUBSCRIPTION_ATTRIBUTE_UNSET_VALUES[name] === value
}

// JSON attributes are compared structurally, as SNS does not preserve their formatting
function isSameAttributeValue(name: string, expected: string, current: string | undefined) {
  if (expected === current) return true
  if (isUnsetAttributeValue(name, expected) && isUnsetAttributeValue(name, current)) return true
  if (current === undefined || !name.endsWith('Policy')) return false

  try {
    return isDeepStrictEqual(JSON.parse(expected), JSON.parse(current))
  } catch {
    return false
  }
}
