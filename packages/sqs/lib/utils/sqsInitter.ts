import type { SQSClient } from '@aws-sdk/client-sqs'
import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'

import type { SQSCreationConfig } from '../sqs/AbstractSqsConsumer'
import type { SQSQueueLocatorType } from '../sqs/AbstractSqsService'

import { assertQueue, deleteQueue, getQueueAttributes } from './sqsUtils'

export async function deleteSqs(
  sqsClient: SQSClient,
  deletionConfig: DeletionConfig,
  creationConfig: SQSCreationConfig,
) {
  if (!deletionConfig.deleteIfExists) {
    return
  }

  if (isProduction() && !deletionConfig.forceDeleteInProduction) {
    throw new Error(
      'You are running autodeletion in production. This can and probably will cause a loss of data. If you are absolutely sure you want to do this, please set deletionConfig.forceDeleteInProduction to true',
    )
  }

  if (!creationConfig.queue.QueueName) {
    throw new Error('QueueName must be set for automatic deletion')
  }

  await deleteQueue(sqsClient, creationConfig.queue.QueueName)
}

export async function initSqs(
  sqsClient: SQSClient,
  locatorConfig?: SQSQueueLocatorType,
  creationConfig?: SQSCreationConfig,
) {
  // reuse existing queue only
  if (locatorConfig) {
    const checkResult = await getQueueAttributes(sqsClient, locatorConfig, ['QueueArn'])
    if (checkResult.error === 'not_found') {
      throw new Error(`Queue with queueUrl ${locatorConfig.queueUrl} does not exist.`)
    }

    const queueArn = checkResult.result?.attributes?.QueueArn
    if (!queueArn) {
      throw new Error('Queue ARN was not set')
    }

    const queueUrl = locatorConfig.queueUrl

    const splitUrl = queueUrl.split('/')
    const queueName = splitUrl[splitUrl.length - 1]
    return {
      queueArn,
      queueUrl,
      queueName,
    }
  }

  // create new queue if does not exist
  if (!creationConfig?.queue.QueueName) {
    throw new Error('queueConfig.QueueName is mandatory when locator is not provided')
  }

  const { queueUrl, queueArn } = await assertQueue(sqsClient, creationConfig.queue, {
    topicArnsWithPublishPermissionsPrefix: creationConfig.topicArnsWithPublishPermissionsPrefix,
  })
  const queueName = creationConfig.queue.QueueName

  return {
    queueUrl,
    queueArn,
    queueName,
  }
}
