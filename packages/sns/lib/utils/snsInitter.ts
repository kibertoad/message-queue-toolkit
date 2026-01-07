import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { STSClient } from '@aws-sdk/client-sts'
import type { Either } from '@lokalise/node-core'
import type { DeletionConfig, ExtraParams } from '@message-queue-toolkit/core'
import {
  isProduction,
  isStartupResourcePollingEnabled,
  waitForResource,
} from '@message-queue-toolkit/core'
import {
  deleteQueue,
  getQueueAttributes,
  resolveQueueUrlFromLocatorConfig,
  type SQSCreationConfig,
} from '@message-queue-toolkit/sqs'
import type { SNSCreationConfig, SNSTopicLocatorType } from '../sns/AbstractSnsService.ts'
import type { SNSSQSQueueLocatorType } from '../sns/AbstractSnsSqsConsumer.ts'
import { isCreateTopicCommand, type TopicResolutionOptions } from '../types/TopicTypes.ts'
import type { SNSSubscriptionOptions } from './snsSubscriber.ts'
import { subscribeToTopic } from './snsSubscriber.ts'
import { assertTopic, deleteSubscription, deleteTopic, getTopicAttributes } from './snsUtils.ts'
import { buildTopicArn } from './stsUtils.ts'

export type InitSnsSqsExtraParams = ExtraParams & {
  /**
   * Callback invoked when resources become available in non-blocking mode.
   * Only called when startupResourcePolling.nonBlocking is true and resources were not immediately available.
   */
  onResourcesReady?: (result: { topicArn: string; queueUrl: string }) => void
}

export type InitSnsExtraParams = ExtraParams & {
  /**
   * Callback invoked when topic becomes available in non-blocking mode.
   * Only called when startupResourcePolling.nonBlocking is true and topic was not immediately available.
   */
  onTopicReady?: (result: { topicArn: string }) => void
}

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: fixme
export async function initSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig?: SNSSQSQueueLocatorType,
  creationConfig?: SNSCreationConfig & SQSCreationConfig,
  subscriptionConfig?: SNSSubscriptionOptions,
  extraParams?: InitSnsSqsExtraParams,
): Promise<{
  subscriptionArn: string
  topicArn: string
  queueUrl: string
  queueName: string
  resourcesReady: boolean
}> {
  if (!locatorConfig?.subscriptionArn) {
    if (!creationConfig?.topic && !locatorConfig?.topicArn && !locatorConfig?.topicName) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, creationConfig.topic is mandatory in order to attempt to create missing topic and subscribe to it OR locatorConfig.name or locatorConfig.topicArn parameter is mandatory, to create subscription for existing topic.',
      )
    }
    if (!creationConfig?.queue) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, creationConfig.queue parameter is mandatory, as there will be an attempt to create the missing queue',
      )
    }
    if (!creationConfig.queue.QueueName) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, creationConfig.queue.QueueName parameter is mandatory, as there will be an attempt to create the missing queue',
      )
    }
    if (!subscriptionConfig) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, subscriptionConfig parameter is mandatory, as there will be an attempt to create the missing subscription',
      )
    }

    const topicResolutionOptions: TopicResolutionOptions = {
      ...(locatorConfig as SNSSQSQueueLocatorType),
      ...creationConfig.topic,
    }

    const { subscriptionArn, topicArn, queueUrl } = await subscribeToTopic(
      sqsClient,
      snsClient,
      stsClient,
      creationConfig.queue,
      topicResolutionOptions,
      subscriptionConfig,
      {
        updateAttributesIfExists: creationConfig.updateAttributesIfExists,
        queueUrlsWithSubscribePermissionsPrefix:
          creationConfig.queueUrlsWithSubscribePermissionsPrefix,
        allowedSourceOwner: creationConfig.allowedSourceOwner,
        topicArnsWithPublishPermissionsPrefix: creationConfig.topicArnsWithPublishPermissionsPrefix,
        logger: extraParams?.logger,
        forceTagUpdate: creationConfig.forceTagUpdate,
      },
    )
    if (!subscriptionArn) {
      throw new Error('Failed to subscribe')
    }
    return {
      subscriptionArn,
      topicArn,
      queueName: creationConfig.queue.QueueName,
      queueUrl,
      resourcesReady: true,
    }
  }

  const queueUrl = await resolveQueueUrlFromLocatorConfig(sqsClient, locatorConfig)

  // Check for existing resources, using the locators
  const subscriptionTopicArn =
    locatorConfig.topicArn ?? (await buildTopicArn(stsClient, locatorConfig.topicName ?? ''))

  const startupResourcePolling = locatorConfig.startupResourcePolling

  // If startup resource polling is enabled, poll for resources to become available
  if (isStartupResourcePollingEnabled(startupResourcePolling)) {
    const nonBlocking = startupResourcePolling.nonBlocking === true

    // Track availability for non-blocking mode coordination
    let topicAvailable = false
    let queueAvailable = false

    const notifyIfBothReady = () => {
      if (nonBlocking && topicAvailable && queueAvailable) {
        extraParams?.onResourcesReady?.({ topicArn: subscriptionTopicArn, queueUrl })
      }
    }

    // Wait for topic to become available
    const topicResult = await waitForResource({
      config: startupResourcePolling,
      resourceName: `SNS topic ${subscriptionTopicArn}`,
      logger: extraParams?.logger,
      errorReporter: extraParams?.errorReporter,
      onResourceAvailable: () => {
        topicAvailable = true
        notifyIfBothReady()
      },
      checkFn: async () => {
        const result = await getTopicAttributes(snsClient, subscriptionTopicArn)
        if (result.error === 'not_found') {
          return { isAvailable: false }
        }
        return { isAvailable: true, result: result.result }
      },
    })

    // If topic was immediately available, mark it
    if (topicResult !== undefined) {
      topicAvailable = true
    }

    // If non-blocking and topic wasn't immediately available, return early
    // Background polling will continue and call notifyIfBothReady when topic is available
    if (nonBlocking && topicResult === undefined) {
      const splitUrl = queueUrl.split('/')
      // biome-ignore lint/style/noNonNullAssertion: It's ok
      const queueName = splitUrl[splitUrl.length - 1]!

      // Also start polling for queue in background so we can notify when both are ready
      waitForResource({
        config: startupResourcePolling,
        resourceName: `SQS queue ${queueUrl}`,
        logger: extraParams?.logger,
        errorReporter: extraParams?.errorReporter,
        onResourceAvailable: () => {
          queueAvailable = true
          notifyIfBothReady()
        },
        checkFn: async () => {
          const result = await getQueueAttributes(sqsClient, queueUrl)
          if (result.error === 'not_found') {
            return { isAvailable: false }
          }
          return { isAvailable: true, result: result.result }
        },
      })
        .then((result) => {
          // If queue was immediately available, waitForResource returns the result
          // but doesn't call onResourceAvailable, so we handle it here
          if (result !== undefined) {
            queueAvailable = true
            notifyIfBothReady()
          }
        })
        .catch((error) => {
          extraParams?.logger?.error({
            message: 'Background polling for SQS queue failed',
            queueUrl,
            error,
          })
        })

      return {
        subscriptionArn: locatorConfig.subscriptionArn,
        topicArn: subscriptionTopicArn,
        queueUrl,
        queueName,
        resourcesReady: false,
      }
    }

    // Wait for queue to become available
    const queueResult = await waitForResource({
      config: startupResourcePolling,
      resourceName: `SQS queue ${queueUrl}`,
      logger: extraParams?.logger,
      errorReporter: extraParams?.errorReporter,
      onResourceAvailable: () => {
        queueAvailable = true
        notifyIfBothReady()
      },
      checkFn: async () => {
        const result = await getQueueAttributes(sqsClient, queueUrl)
        if (result.error === 'not_found') {
          return { isAvailable: false }
        }
        return { isAvailable: true, result: result.result }
      },
    })

    // If queue was immediately available, mark it
    if (queueResult !== undefined) {
      queueAvailable = true
    }

    // If non-blocking and queue wasn't immediately available, return early
    if (nonBlocking && queueResult === undefined) {
      const splitUrl = queueUrl.split('/')
      // biome-ignore lint/style/noNonNullAssertion: It's ok
      const queueName = splitUrl[splitUrl.length - 1]!

      return {
        subscriptionArn: locatorConfig.subscriptionArn,
        topicArn: subscriptionTopicArn,
        queueUrl,
        queueName,
        resourcesReady: false,
      }
    }
  } else {
    // Original behavior: check resources once and fail immediately if not found
    const checkPromises: Promise<Either<'not_found', unknown>>[] = []
    const topicPromise = getTopicAttributes(snsClient, subscriptionTopicArn)
    checkPromises.push(topicPromise)

    const queuePromise = getQueueAttributes(sqsClient, queueUrl)
    checkPromises.push(queuePromise)

    const [topicCheckResult, queueCheckResult] = await Promise.all(checkPromises)

    if (queueCheckResult?.error === 'not_found') {
      throw new Error(`Queue with queueUrl ${queueUrl} does not exist.`)
    }
    if (topicCheckResult?.error === 'not_found') {
      throw new Error(`Topic with topicArn ${subscriptionTopicArn} does not exist.`)
    }
  }

  let queueName: string
  if (queueUrl) {
    const splitUrl = queueUrl.split('/')
    // biome-ignore lint/style/noNonNullAssertion: It's ok
    queueName = splitUrl[splitUrl.length - 1]!
  } else {
    // biome-ignore lint/style/noNonNullAssertion: It's ok
    queueName = creationConfig!.queue.QueueName!
  }

  return {
    subscriptionArn: locatorConfig.subscriptionArn,
    topicArn: subscriptionTopicArn,
    queueUrl,
    queueName,
    resourcesReady: true,
  }
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
