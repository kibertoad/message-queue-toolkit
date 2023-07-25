import type { SNSClient } from '@aws-sdk/client-sns'

import { assertTopic, getTopicAttributes } from '../utils/snsUtils'

import type { SNSCreationConfig, SNSQueueLocatorType } from './AbstractSnsService'

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
