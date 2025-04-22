import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import type { DeletionConfig, ExtraParams } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import {
  type SQSCreationConfig,
  resolveQueueUrlFromLocatorConfig,
} from '@message-queue-toolkit/sqs'
import { deleteQueue, getQueueAttributes } from '@message-queue-toolkit/sqs'

import type { SNSCreationConfig, SNSTopicLocatorType } from '../sns/AbstractSnsService'
import type { SNSSQSQueueLocatorType } from '../sns/AbstractSnsSqsConsumer'

import type { STSClient } from '@aws-sdk/client-sts'
import type { Either } from '@lokalise/node-core'
import { type TopicResolutionOptions, isCreateTopicCommand } from '../types/TopicTypes'
import type { SNSSubscriptionOptions } from './snsSubscriber'
import { subscribeToTopic } from './snsSubscriber'
import {
  assertTopic,
  deleteSubscription,
  deleteTopic,
  getTopicArnByName,
  getTopicAttributes,
} from './snsUtils'

// biome-ignore lint/complexity/noExcessiveCognitiveComplexity: <explanation>
export async function initSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig?: SNSSQSQueueLocatorType,
  creationConfig?: SNSCreationConfig & SQSCreationConfig,
  subscriptionConfig?: SNSSubscriptionOptions,
  extraParams?: ExtraParams,
): Promise<{ subscriptionArn: string; topicArn: string; queueUrl: string; queueName: string }> {
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
    }
  }

  const queueUrl = await resolveQueueUrlFromLocatorConfig(sqsClient, locatorConfig)

  const checkPromises: Promise<Either<'not_found', unknown>>[] = []
  // Check for existing resources, using the locators
  const subscriptionTopicArn =
    locatorConfig.topicArn ?? (await getTopicArnByName(snsClient, locatorConfig.topicName))
  const topicPromise = getTopicAttributes(snsClient, subscriptionTopicArn)
  checkPromises.push(topicPromise)

  const queuePromise = getQueueAttributes(sqsClient, queueUrl)
  checkPromises.push(queuePromise)

  const [topicCheckResult, queueCheckResult] = await Promise.all(checkPromises)

  if (queueCheckResult?.error === 'not_found') {
    throw new Error(`Queue with queueUrl ${queueUrl} does not exist.`)
  }
  if (topicCheckResult.error === 'not_found') {
    throw new Error(`Topic with topicArn ${locatorConfig.topicArn} does not exist.`)
  }

  let queueName: string
  if (queueUrl) {
    const splitUrl = queueUrl.split('/')
    queueName = splitUrl[splitUrl.length - 1]
  } else {
    queueName = creationConfig!.queue.QueueName!
  }

  return {
    subscriptionArn: locatorConfig.subscriptionArn,
    topicArn: subscriptionTopicArn,
    queueUrl,
    queueName,
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
    topicConfiguration ?? topicLocator!,
    subscriptionConfiguration,
    extraParams,
  )

  if (!subscriptionArn) {
    throw new Error('subscriptionArn must be set for automatic deletion')
  }

  await deleteQueue(
    sqsClient,
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
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

export async function initSns(
  snsClient: SNSClient,
  stsClient: STSClient,
  locatorConfig?: SNSTopicLocatorType,
  creationConfig?: SNSCreationConfig,
) {
  if (locatorConfig) {
    const topicArn =
      locatorConfig.topicArn ?? (await getTopicArnByName(snsClient, locatorConfig.topicName))
    const checkResult = await getTopicAttributes(snsClient, topicArn)
    if (checkResult.error === 'not_found') {
      throw new Error(`Topic with topicArn ${locatorConfig.topicArn} does not exist.`)
    }

    return {
      topicArn,
    }
  }

  // create new topic if it does not exist
  if (!creationConfig) {
    throw new Error(
      'When locatorConfig for the topic is not specified, creationConfig of the topic is mandatory',
    )
  }
  const topicArn = await assertTopic(snsClient, stsClient, creationConfig.topic!, {
    queueUrlsWithSubscribePermissionsPrefix: creationConfig.queueUrlsWithSubscribePermissionsPrefix,
    allowedSourceOwner: creationConfig.allowedSourceOwner,
    forceTagUpdate: creationConfig.forceTagUpdate,
  })
  return {
    topicArn,
  }
}
