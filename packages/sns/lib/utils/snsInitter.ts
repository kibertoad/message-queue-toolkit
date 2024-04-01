import type { SNSClient, CreateTopicCommandInput } from '@aws-sdk/client-sns'
import type { SQSClient, CreateQueueCommandInput } from '@aws-sdk/client-sqs'
import type { DeletionConfig, ExtraParams } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import type { SQSCreationConfig } from '@message-queue-toolkit/sqs'
import { deleteQueue, getQueueAttributes } from '@message-queue-toolkit/sqs'

import type { SNSCreationConfig, SNSQueueLocatorType } from '../sns/AbstractSnsService'
import type { SNSSQSQueueLocatorType } from '../sns/AbstractSnsSqsConsumer'

import type { SNSSubscriptionOptions } from './snsSubscriber'
import { subscribeToTopic } from './snsSubscriber'
import { assertTopic, deleteSubscription, deleteTopic, getTopicAttributes } from './snsUtils'

export async function initSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  locatorConfig?: SNSSQSQueueLocatorType,
  creationConfig?: SNSCreationConfig & SQSCreationConfig,
  subscriptionConfig?: SNSSubscriptionOptions,
  extraParams?: ExtraParams,
) {
  if (!locatorConfig?.subscriptionArn) {
    if (!creationConfig?.topic) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, creationConfig.topic parameter is mandatory, as there will be an attempt to create the missing topic',
      )
    }
    if (!creationConfig?.queue) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, creationConfig.queue parameter is mandatory, as there will be an attempt to create the missing queue',
      )
    }
    if (!subscriptionConfig) {
      throw new Error(
        'If locatorConfig.subscriptionArn is not specified, subscriptionConfig parameter is mandatory, as there will be an attempt to create the missing subscription',
      )
    }

    const { subscriptionArn, topicArn, queueUrl } = await subscribeToTopic(
      sqsClient,
      snsClient,
      creationConfig.queue,
      creationConfig.topic,
      subscriptionConfig,
      {
        updateAttributesIfExists: creationConfig.updateAttributesIfExists,
        queueUrlsWithSubscribePermissionsPrefix:
          creationConfig.queueUrlsWithSubscribePermissionsPrefix,
        allowedSourceOwner: creationConfig.allowedSourceOwner,
        topicArnsWithPublishPermissionsPrefix: creationConfig.topicArnsWithPublishPermissionsPrefix,
        logger: extraParams?.logger,
      },
    )
    if (!subscriptionArn) {
      throw new Error('Failed to subscribe')
    }
    return {
      subscriptionArn,
      topicArn,
      queueUrl,
    }
  }

  // Check for existing resources, using the locators

  const queuePromise = getQueueAttributes(sqsClient, locatorConfig)
  const topicPromise = getTopicAttributes(snsClient, locatorConfig.topicArn)

  const [queueCheckResult, topicCheckResult] = await Promise.all([queuePromise, topicPromise])

  if (queueCheckResult.error === 'not_found') {
    throw new Error(`Queue with queueUrl ${locatorConfig.queueUrl} does not exist.`)
  }
  if (topicCheckResult.error === 'not_found') {
    throw new Error(`Topic with topicArn ${locatorConfig.topicArn} does not exist.`)
  }

  return {
    subscriptionArn: locatorConfig.subscriptionArn,
    topicArn: locatorConfig.topicArn,
    queueUrl: locatorConfig.queueUrl,
  }
}

export async function deleteSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  deletionConfig: DeletionConfig,
  queueConfiguration: CreateQueueCommandInput,
  topicConfiguration: CreateTopicCommandInput,
  subscriptionConfiguration: SNSSubscriptionOptions,
  extraParams?: ExtraParams,
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
    queueConfiguration,
    topicConfiguration,
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
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  await deleteTopic(snsClient, topicConfiguration.Name!)
  await deleteSubscription(snsClient, subscriptionArn)
}

export async function deleteSns(
  snsClient: SNSClient,
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

  if (!creationConfig.topic.Name) {
    throw new Error('topic.Name must be set for automatic deletion')
  }

  await deleteTopic(snsClient, creationConfig.topic.Name)
}

export async function initSns(
  snsClient: SNSClient,
  locatorConfig?: SNSQueueLocatorType,
  creationConfig?: SNSCreationConfig,
) {
  if (locatorConfig) {
    const checkResult = await getTopicAttributes(snsClient, locatorConfig.topicArn)
    if (checkResult.error === 'not_found') {
      throw new Error(`Topic with topicArn ${locatorConfig.topicArn} does not exist.`)
    }

    return {
      topicArn: locatorConfig.topicArn,
    }
  }

  // create new topic if it does not exist
  if (!creationConfig) {
    throw new Error(
      'When locatorConfig for the topic is not specified, creationConfig of the topic is mandatory',
    )
  }
  const topicArn = await assertTopic(snsClient, creationConfig.topic, {
    queueUrlsWithSubscribePermissionsPrefix: creationConfig.queueUrlsWithSubscribePermissionsPrefix,
    allowedSourceOwner: creationConfig.allowedSourceOwner,
  })
  return {
    topicArn,
  }
}
