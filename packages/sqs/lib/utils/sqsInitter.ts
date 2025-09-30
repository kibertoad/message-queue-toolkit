import type { QueueAttributeName, SQSClient } from '@aws-sdk/client-sqs'
import { SetQueueAttributesCommand, TagQueueCommand } from '@aws-sdk/client-sqs'
import type { DeletionConfig } from '@message-queue-toolkit/core'
import { isProduction } from '@message-queue-toolkit/core'

import type { SQSCreationConfig, SQSQueueLocatorType } from '../sqs/AbstractSqsService.ts'

import {
  assertQueue,
  deleteQueue,
  getQueueAttributes,
  resolveQueueUrlFromLocatorConfig,
} from './sqsUtils.ts'

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

  await deleteQueue(
    sqsClient,
    creationConfig.queue.QueueName,
    deletionConfig.waitForConfirmation !== false,
  )
}

export async function updateQueueAttributes(
  sqsClient: SQSClient,
  queueUrl: string,
  attributes: Partial<Record<QueueAttributeName, string>> = {},
) {
  const command = new SetQueueAttributesCommand({
    QueueUrl: queueUrl,
    Attributes: attributes,
  })
  await sqsClient.send(command)
}

export async function updateQueueTags(
  sqsClient: SQSClient,
  queueUrl: string,
  tags: Record<string, string> | undefined = {},
) {
  const command = new TagQueueCommand({
    QueueUrl: queueUrl,
    Tags: tags,
  })
  await sqsClient.send(command)
}

export async function initSqs(
  sqsClient: SQSClient,
  locatorConfig?: Partial<SQSQueueLocatorType>,
  creationConfig?: SQSCreationConfig,
) {
  // reuse existing queue only
  if (locatorConfig) {
    const queueUrl = await resolveQueueUrlFromLocatorConfig(sqsClient, locatorConfig)

    const checkResult = await getQueueAttributes(sqsClient, queueUrl, ['QueueArn'])

    if (checkResult.error === 'not_found') {
      throw new Error(`Queue with queueUrl ${locatorConfig.queueUrl} does not exist.`)
    }

    const queueArn = checkResult.result?.attributes?.QueueArn
    if (!queueArn) {
      throw new Error('Queue ARN was not set')
    }

    const splitUrl = queueUrl.split('/')
    // biome-ignore lint/style/noNonNullAssertion: It's ok
    const queueName = splitUrl[splitUrl.length - 1]!
    return { queueArn, queueUrl, queueName }
  }

  // create new queue if does not exist
  if (!creationConfig?.queue.QueueName) {
    throw new Error('queueConfig.QueueName is mandatory when locator is not provided')
  }

  // create new queue
  const { queueUrl, queueArn } = await assertQueue(sqsClient, creationConfig.queue, {
    topicArnsWithPublishPermissionsPrefix: creationConfig.topicArnsWithPublishPermissionsPrefix,
    updateAttributesIfExists: creationConfig.updateAttributesIfExists,
    forceTagUpdate: creationConfig.forceTagUpdate,
    policyConfig: creationConfig.policyConfig,
  })
  const queueName = creationConfig.queue.QueueName

  return { queueUrl, queueArn, queueName }
}
