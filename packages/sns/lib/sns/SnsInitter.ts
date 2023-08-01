import type { SNSClient } from '@aws-sdk/client-sns'
import type { SQSClient } from '@aws-sdk/client-sqs'
import type { SQSCreationConfig } from '@message-queue-toolkit/sqs'

import { assertTopic, getTopicAttributes } from '../utils/snsUtils'

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
