import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import { InternalError, isError, isInternalError } from '@lokalise/node-core'
import type {
  DeletionConfig,
  ExtraParams,
  PollingErrorCallback,
  StartupResourcePollingCheckResult,
} from '@message-queue-toolkit/core'
import {
  isProduction,
  isStartupResourcePollingEnabled,
  waitForResource,
} from '@message-queue-toolkit/core'
import {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  getQueueUrl,
  type SQSCreationConfig,
} from '@message-queue-toolkit/sqs'
import type { SNSCreationConfig, SNSTopicLocatorType } from '../sns/AbstractSnsService.ts'
import type { SNSSQSQueueLocatorType } from '../sns/AbstractSnsSqsConsumer.ts'
import { isCreateTopicCommand } from '../types/TopicTypes.ts'
import type { SNSSubscriptionOptions } from './snsSubscriber.ts'
import { assertSubscription, subscribeToTopic } from './snsSubscriber.ts'
import {
  assertTopic,
  deleteSubscription,
  deleteTopic,
  findSubscriptionByTopicAndQueue,
  getTopicAttributes,
} from './snsUtils.ts'
import { buildTopicArn } from './stsUtils.ts'

/**
 * Result of {@link initSnsSqs} when all resources are confirmed available.
 * The function returns `undefined` instead when running in non-blocking
 * startup-polling mode and resources aren't yet ready — the
 * `onResourcesReady` callback in `InitSnsSqsExtraParams` fires later with
 * the same fields once they're resolved.
 */
export type InitSnsSqsResult = {
  subscriptionArn: string
  topicArn: string
  queueUrl: string
  queueArn: string
  queueName: string
}

export type InitSnsSqsExtraParams = ExtraParams & {
  /**
   * Callback invoked when resources become available in non-blocking mode.
   * Only called when startupResourcePolling.nonBlocking is true and resources were not immediately available.
   */
  onResourcesReady?: (result: {
    topicArn: string
    queueUrl: string
    queueArn: string
    subscriptionArn: string
    queueName: string
  }) => void
  /**
   * Callback invoked when background resource polling or subscription creation fails in non-blocking mode.
   * This can happen due to polling timeout or subscription creation failure.
   */
  onResourcesError?: PollingErrorCallback
}

export type InitSnsExtraParams = ExtraParams & {
  /**
   * Callback invoked when topic becomes available in non-blocking mode.
   * Only called when startupResourcePolling.nonBlocking is true and topic was not immediately available.
   */
  onTopicReady?: (result: { topicArn: string }) => void
}

type StartupResourcePolling = NonNullable<SNSSQSQueueLocatorType['startupResourcePolling']>

const RESOURCE_NOT_FOUND_ERROR_CODE = 'sns_sqs_resource_not_found'

type ResourceResolutionOptions = {
  /**
   * When set, located resources are waited for (blocking). Otherwise they are checked once, and
   * an error with RESOURCE_NOT_FOUND_ERROR_CODE is thrown if they don't exist.
   */
  polling?: StartupResourcePolling
  /**
   * Whether a located topic is checked to exist when it is not being waited for. Skipped when the
   * subscription is created, as subscribing already fails for a missing topic.
   */
  checkLocatedTopic: boolean
  extraParams?: ExtraParams
}

async function waitForOrCheckResource<T>(
  resourceName: string,
  notFoundMessage: string,
  checkFn: () => Promise<StartupResourcePollingCheckResult<T>>,
  options: ResourceResolutionOptions,
): Promise<T> {
  if (options.polling) {
    const result = await waitForResource({
      config: { ...options.polling, nonBlocking: false },
      resourceName,
      logger: options.extraParams?.logger,
      errorReporter: options.extraParams?.errorReporter,
      checkFn,
    })
    if (result === undefined) throw new Error(`Could not resolve ${resourceName}`)

    return result
  }

  const checkResult = await checkFn()
  if (!checkResult.isAvailable) {
    throw new InternalError({ errorCode: RESOURCE_NOT_FOUND_ERROR_CODE, message: notFoundMessage })
  }

  return checkResult.result
}

/**
 * Topic is located when the locator references it, otherwise it is created.
 * Locator takes precedence.
 */
async function resolveTopicArn(
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig: SNSSQSQueueLocatorType | undefined,
  creationConfig: (SNSCreationConfig & SQSCreationConfig) | undefined,
  options: ResourceResolutionOptions,
): Promise<string> {
  const locatedTopicArn =
    locatorConfig?.topicArn ??
    (locatorConfig?.topicName ? await buildTopicArn(stsClient, locatorConfig.topicName) : undefined)

  if (locatedTopicArn) {
    const topicArn = locatedTopicArn
    // A topic that also has creation config is never waited for nor checked
    if (creationConfig?.topic || (!options.polling && !options.checkLocatedTopic)) return topicArn

    return await waitForOrCheckResource<string>(
      `SNS topic ${topicArn}`,
      `Topic with topicArn ${topicArn} does not exist.`,
      async () => {
        const result = await getTopicAttributes(snsClient, topicArn)
        if (result.error === 'not_found') return { isAvailable: false }
        return { isAvailable: true, result: topicArn }
      },
      options,
    )
  }

  if (!creationConfig?.topic) {
    throw new Error(
      'Either creationConfig.topic, locatorConfig.topicArn or locatorConfig.topicName must be provided',
    )
  }

  return await assertTopic(snsClient, stsClient, creationConfig.topic, {
    queueUrlsWithSubscribePermissionsPrefix:
      creationConfig?.queueUrlsWithSubscribePermissionsPrefix,
    allowedSourceOwner: creationConfig?.allowedSourceOwner,
    forceTagUpdate: creationConfig?.forceTagUpdate,
  })
}

type ResolvedQueue = { queueUrl: string; queueArn: string; queueName: string }

/**
 * Queue is created when creation config is given, otherwise it is located.
 * Creation config takes precedence.
 */
async function resolveQueue(
  sqsClient: SQSClient,
  locatorConfig: SNSSQSQueueLocatorType | undefined,
  creationConfig: (SNSCreationConfig & SQSCreationConfig) | undefined,
  options: ResourceResolutionOptions,
): Promise<ResolvedQueue> {
  if (creationConfig?.queue) {
    return assertQueue(sqsClient, creationConfig.queue, {
      topicArnsWithPublishPermissionsPrefix: creationConfig.topicArnsWithPublishPermissionsPrefix,
      updateAttributesIfExists: creationConfig.updateAttributesIfExists,
      forceTagUpdate: creationConfig.forceTagUpdate,
    })
  }

  const queueReference = locatorConfig?.queueUrl
    ? `queueUrl ${locatorConfig.queueUrl}`
    : `queueName ${locatorConfig?.queueName}`

  return await waitForOrCheckResource<ResolvedQueue>(
    `SQS queue with ${queueReference}`,
    `Queue with ${queueReference} does not exist.`,
    async () => {
      // Queue URL is resolved on every check, so a queue located by name can be waited for as well
      let queueUrl = locatorConfig?.queueUrl
      if (!queueUrl) {
        if (!locatorConfig?.queueName) {
          throw new Error(
            'Either locatorConfig.queueUrl or locatorConfig.queueName must be provided',
          )
        }
        const queueUrlResult = await getQueueUrl(sqsClient, locatorConfig.queueName)
        if (queueUrlResult.error === 'not_found') return { isAvailable: false }
        queueUrl = queueUrlResult.result
      }

      const queueAttributesResult = await getQueueAttributes(sqsClient, queueUrl, ['QueueArn'])
      if (queueAttributesResult.error === 'not_found') return { isAvailable: false }

      const queueArn = queueAttributesResult.result?.attributes?.QueueArn
      if (!queueArn) throw new Error(`Could not resolve QueueArn for queue at ${queueUrl}`)

      const queueName = queueUrl.split('/').pop()
      if (!queueName) throw new Error(`Could not resolve queue name from queueUrl ${queueUrl}`)

      return { isAvailable: true, result: { queueUrl, queueArn, queueName } }
    },

    options,
  )
}

async function findConfirmedSubscriptionArn(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
): Promise<string | undefined> {
  const subscription = await findSubscriptionByTopicAndQueue(snsClient, topicArn, queueArn)
  const subscriptionArn = subscription?.SubscriptionArn

  // Unconfirmed subscriptions are listed with a status placeholder instead of an ARN
  return subscriptionArn?.startsWith('arn:') ? subscriptionArn : undefined
}

/**
 * Subscription is used as is when its ARN is given, created (or updated) when subscriptionConfig is
 * given, and otherwise located by looking up the queue's subscription on the topic.
 */
async function resolveSubscriptionArn(
  snsClient: SNSClient,
  topicArn: string,
  queue: ResolvedQueue,
  locatorConfig: SNSSQSQueueLocatorType | undefined,
  subscriptionConfig: SNSSubscriptionOptions | undefined,
  options: ResourceResolutionOptions,
): Promise<string> {
  if (locatorConfig?.subscriptionArn) return locatorConfig.subscriptionArn

  if (subscriptionConfig) {
    const subscriptionArn = await assertSubscription(
      snsClient,
      topicArn,
      queue.queueArn,
      subscriptionConfig,
      { queueName: queue.queueName, topicName: topicArn.split(':').pop() },
      options.extraParams?.logger,
    )
    if (!subscriptionArn) throw new Error('Failed to subscribe')

    return subscriptionArn
  }

  return await waitForOrCheckResource<string>(
    `SNS subscription of SQS queue ${queue.queueArn} to topic ${topicArn}`,
    `Subscription of queue ${queue.queueArn} to topic ${topicArn} does not exist.`,
    async () => {
      const subscriptionArn = await findConfirmedSubscriptionArn(
        snsClient,
        topicArn,
        queue.queueArn,
      )
      if (!subscriptionArn) return { isAvailable: false }
      return { isAvailable: true, result: subscriptionArn }
    },
    options,
  )
}

async function resolveSnsSqsResources(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig: SNSSQSQueueLocatorType | undefined,
  creationConfig: (SNSCreationConfig & SQSCreationConfig) | undefined,
  subscriptionConfig: SNSSubscriptionOptions | undefined,
  options: ResourceResolutionOptions,
): Promise<InitSnsSqsResult> {
  const topicArn = await resolveTopicArn(
    snsClient,
    stsClient,
    locatorConfig,
    creationConfig,
    options,
  )

  const queue = await resolveQueue(sqsClient, locatorConfig, creationConfig, options)
  const subscriptionArn = await resolveSubscriptionArn(
    snsClient,
    topicArn,
    queue,
    locatorConfig,
    subscriptionConfig,
    options,
  )

  return { topicArn, subscriptionArn, ...queue }
}

function validateInitSnsSqsConfig(
  locatorConfig: SNSSQSQueueLocatorType | undefined,
  creationConfig: (SNSCreationConfig & SQSCreationConfig) | undefined,
  subscriptionConfig: SNSSubscriptionOptions | undefined,
): void {
  if (!creationConfig?.topic && !locatorConfig?.topicArn && !locatorConfig?.topicName) {
    throw new Error(
      'If locatorConfig.subscriptionArn is not specified, creationConfig.topic is mandatory in order to attempt to create missing topic and subscribe to it OR locatorConfig.name or locatorConfig.topicArn parameter is mandatory, to create subscription for existing topic.',
    )
  }
  if (!creationConfig?.queue && !locatorConfig?.queueUrl && !locatorConfig?.queueName) {
    throw new Error(
      'Either creationConfig.queue is mandatory in order to create the queue, or locatorConfig.queueUrl or locatorConfig.queueName is mandatory in order to locate the existing queue',
    )
  }
  if (creationConfig?.queue && !creationConfig.queue.QueueName) {
    throw new Error(
      'If locatorConfig.subscriptionArn is not specified, creationConfig.queue.QueueName parameter is mandatory, as there will be an attempt to create the missing queue',
    )
  }
  if (!subscriptionConfig && creationConfig?.queue) {
    throw new Error(
      'If creationConfig.queue is specified, subscriptionConfig is mandatory, as the subscription of a queue being created cannot be located',
    )
  }
}

/**
 * Initializes an SNS topic, an SQS queue and the subscription of the queue to the topic. Each
 * resource is resolved independently:
 * - Topic: located when `locatorConfig.topicArn` or `locatorConfig.topicName` is given, otherwise
 *   created from `creationConfig.topic`.
 * - Queue: created when `creationConfig.queue` is given, otherwise located from
 *   `locatorConfig.queueUrl` or `locatorConfig.queueName`.
 * - Subscription: created (or updated) when `subscriptionConfig` is given, otherwise located by
 *   looking up the queue's subscription on the topic.
 *
 * When `locatorConfig.subscriptionArn` is given, every resource is located and `creationConfig` and
 * `subscriptionConfig` are ignored.
 *
 * Located resources are waited for when `locatorConfig.startupResourcePolling` is enabled. In
 * non-blocking mode, `undefined` is returned if they are not immediately available, and
 * `onResourcesReady` is invoked once they are.
 */
export async function initSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig?: SNSSQSQueueLocatorType,
  creationConfig?: SNSCreationConfig & SQSCreationConfig,
  subscriptionConfig?: SNSSubscriptionOptions,
  extraParams?: InitSnsSqsExtraParams,
): Promise<InitSnsSqsResult | undefined> {
  const isFullyLocated = !!locatorConfig?.subscriptionArn
  const resolvedCreationConfig = isFullyLocated ? undefined : creationConfig
  const resolvedSubscriptionConfig = isFullyLocated ? undefined : subscriptionConfig

  validateInitSnsSqsConfig(locatorConfig, resolvedCreationConfig, resolvedSubscriptionConfig)

  const resolve = (options: Omit<ResourceResolutionOptions, 'extraParams'>) =>
    resolveSnsSqsResources(
      sqsClient,
      snsClient,
      stsClient,
      locatorConfig,
      resolvedCreationConfig,
      resolvedSubscriptionConfig,
      { ...options, extraParams },
    )

  const startupResourcePolling = locatorConfig?.startupResourcePolling
  if (!isStartupResourcePollingEnabled(startupResourcePolling)) {
    return await resolve({ checkLocatedTopic: !resolvedSubscriptionConfig })
  }
  if (startupResourcePolling.nonBlocking !== true) {
    return await resolve({ polling: startupResourcePolling, checkLocatedTopic: true })
  }

  // Non-blocking: resources are returned directly if they are all available on the first check,
  // otherwise they keep being resolved in background and onResourcesReady is invoked once ready
  try {
    return await resolve({ checkLocatedTopic: true })
  } catch (err) {
    if (!isInternalError(err) || err.errorCode !== RESOURCE_NOT_FOUND_ERROR_CODE) throw err

    extraParams?.logger?.info({
      message: 'SNS/SQS resources not immediately available, resolving them in background',
      reason: err.message,
    })
  }

  resolve({ polling: startupResourcePolling, checkLocatedTopic: true })
    .then((result) => extraParams?.onResourcesReady?.(result))
    .catch((err) => {
      const error = isError(err) ? err : new Error(String(err))
      extraParams?.logger?.error({
        message: 'Background SNS/SQS resources resolution failed',
        error,
      })
      // Blocking polling only throws when it gives up, so the error is final
      extraParams?.onResourcesError?.(error, { isFinal: true })
    })

  return undefined
}

export async function deleteSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  deletionConfig: DeletionConfig,
  queueConfiguration: CreateQueueCommandInput,
  topicConfiguration: CreateTopicCommandInput | undefined,
  subscriptionConfiguration: SNSSubscriptionOptions,
  extraParams?: ExtraParams,
  topicLocator?: SNSTopicLocatorType,
) {
  if (!deletionConfig.deleteIfExists) {
    return
  }

  if (isProduction() && !deletionConfig.forceDeleteInProduction) {
    throw new Error(
      'You are running autodeletion in production. This can and probably will cause a loss of data. If you are absolutely sure you want to do this, please set deletionConfig.forceDeleteInProduction to true',
    )
  }

  const { subscriptionArn } = await subscribeToTopic(
    sqsClient,
    snsClient,
    stsClient,
    queueConfiguration,
    // biome-ignore lint/style/noNonNullAssertion: Checked by type
    topicConfiguration ?? topicLocator!,
    subscriptionConfiguration,
    extraParams,
  )

  if (!subscriptionArn) {
    throw new Error('subscriptionArn must be set for automatic deletion')
  }

  await deleteQueue(
    sqsClient,
    // biome-ignore lint/style/noNonNullAssertion: It's ok
    queueConfiguration.QueueName!,
    deletionConfig.waitForConfirmation !== false,
  )

  if (topicConfiguration) {
    const topicName = isCreateTopicCommand(topicConfiguration)
      ? topicConfiguration.Name
      : 'undefined'
    if (!topicName) {
      throw new Error('Failed to resolve topic name')
    }
    await deleteTopic(snsClient, stsClient, topicName)
  }
  await deleteSubscription(snsClient, subscriptionArn)
}

export async function deleteSns(
  snsClient: SNSClient,
  stsClient: STSClient,
  deletionConfig: DeletionConfig,
  creationConfig: SNSCreationConfig,
) {
  if (!deletionConfig.deleteIfExists) {
    return
  }

  if (isProduction() && !deletionConfig.forceDeleteInProduction) {
    throw new Error(
      'You are running autodeletion in production. This can and probably will cause a loss of data. If you are absolutely sure you want to do this, please set deletionConfig.forceDeleteInProduction to true',
    )
  }

  if (!creationConfig.topic?.Name) {
    throw new Error('topic.Name must be set for automatic deletion')
  }

  await deleteTopic(snsClient, stsClient, creationConfig.topic.Name)
}

async function initSnsWithLocator(
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig: SNSTopicLocatorType,
  extraParams?: InitSnsExtraParams,
) {
  if (!locatorConfig.topicArn && !locatorConfig.topicName) {
    throw new Error(
      'When locatorConfig for the topic is specified, either topicArn or topicName must be specified',
    )
  }

  const topicArn =
    locatorConfig.topicArn ?? (await buildTopicArn(stsClient, locatorConfig.topicName ?? ''))

  const startupResourcePolling = locatorConfig.startupResourcePolling

  // If startup resource polling is enabled, poll for topic to become available
  if (isStartupResourcePollingEnabled(startupResourcePolling)) {
    const nonBlocking = startupResourcePolling.nonBlocking === true

    const topicResult = await waitForResource({
      config: startupResourcePolling,
      resourceName: `SNS topic ${topicArn}`,
      logger: extraParams?.logger,
      errorReporter: extraParams?.errorReporter,
      onResourceAvailable: () => {
        if (nonBlocking) {
          extraParams?.onTopicReady?.({ topicArn })
        }
      },
      checkFn: async () => {
        const result = await getTopicAttributes(snsClient, topicArn)
        if (result.error === 'not_found') {
          return { isAvailable: false }
        }
        return { isAvailable: true, result: result.result }
      },
    })

    // In non-blocking mode, return early if topic wasn't immediately available
    if (nonBlocking && topicResult === undefined) {
      return { topicArn, resourcesReady: false }
    }
  } else {
    // Original behavior: check once and fail immediately if not found
    const checkResult = await getTopicAttributes(snsClient, topicArn)
    if (checkResult.error === 'not_found') {
      throw new Error(`Topic with topicArn ${topicArn} does not exist.`)
    }
  }

  return { topicArn, resourcesReady: true }
}

export async function initSns(
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig?: SNSTopicLocatorType,
  creationConfig?: SNSCreationConfig,
  extraParams?: InitSnsExtraParams,
) {
  if (locatorConfig) {
    return initSnsWithLocator(snsClient, stsClient, locatorConfig, extraParams)
  }

  // create new topic if it does not exist
  if (!creationConfig) {
    throw new Error(
      'When locatorConfig for the topic is not specified, creationConfig of the topic is mandatory',
    )
  }
  // biome-ignore lint/style/noNonNullAssertion: it's ok
  const topicArn = await assertTopic(snsClient, stsClient, creationConfig.topic!, {
    queueUrlsWithSubscribePermissionsPrefix: creationConfig.queueUrlsWithSubscribePermissionsPrefix,
    allowedSourceOwner: creationConfig.allowedSourceOwner,
    forceTagUpdate: creationConfig.forceTagUpdate,
  })

  return { topicArn, resourcesReady: true }
}
