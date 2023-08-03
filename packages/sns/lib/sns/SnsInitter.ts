import type { SNSClient, CreateTopicCommandInput } from '@aws-sdk/client-sns'
import type { SQSClient, CreateQueueCommandInput } from '@aws-sdk/client-sqs'
import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'
import type { SQSCreationConfig } from '@message-queue-toolkit/sqs'

import { assertTopic, deleteSubscription, deleteTopic, getTopicAttributes } from '../utils/snsUtils'

import type { SNSCreationConfig, SNSQueueLocatorType } from './AbstractSnsService'
import type { SNSSQSQueueLocatorType } from './AbstractSnsSqsConsumerMonoSchema'
import type { SNSSubscriptionOptions } from './SnsSubscriber'
import { subscribeToTopic } from './SnsSubscriber'

export async function initSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  locatorConfig?: SNSSQSQueueLocatorType,
  creationConfig?: SNSCreationConfig & SQSCreationConfig,
  subscriptionConfig?: SNSSubscriptionOptions,
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

    const { subscriptionArn } = await subscribeToTopic(
      sqsClient,
      snsClient,
      creationConfig.queue,
      creationConfig.topic,
      subscriptionConfig,
    )
    if (!subscriptionArn) {
      throw new Error('Failed to subscribe')
    }
    return {
      subscriptionArn,
    }
  }
  return {
    subscriptionArn: locatorConfig.subscriptionArn,
  }
}

export async function deleteSnsSqs(
  sqsClient: SQSClient,
  snsClient: SNSClient,
  deletionConfig: DeletionConfig,
  queueConfiguration: CreateQueueCommandInput,
  topicConfiguration: CreateTopicCommandInput,
  subscriptionConfiguration: SNSSubscriptionOptions,
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
  )

  if (!subscriptionArn) {
    throw new Error('subscriptionArn must be set for automatic deletion')
  }

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
  const topicArn = await assertTopic(snsClient, creationConfig.topic)
  return {
    topicArn,
  }
}
