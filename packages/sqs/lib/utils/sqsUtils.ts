import {
  CreateQueueCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs'
import type {
  CreateQueueCommandInput,
  QueueAttributeName,
  SQSClient,
  SendMessageCommandInput,
} from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { globalLogger } from '@lokalise/node-core'
import { isShallowSubset, waitAndRetry } from '@message-queue-toolkit/core'

import type { ExtraSQSCreationParams, SQSQueueLocatorType } from '../sqs/AbstractSqsService.ts'

import { generateQueuePublishForTopicPolicy } from './sqsAttributeUtils.ts'
import { updateQueueAttributes, updateQueueTags } from './sqsInitter.ts'

const AWS_QUEUE_DOES_NOT_EXIST_ERROR_NAME = 'QueueDoesNotExist'

type QueueAttributesResult = {
  attributes?: Partial<Record<QueueAttributeName, string>>
}

export async function getQueueUrl(
  sqsClient: SQSClient,
  queueName: string,
): Promise<Either<'not_found', string>> {
  try {
    const result = await sqsClient.send(
      new GetQueueUrlCommand({
        QueueName: queueName,
      }),
    )

    if (result.QueueUrl) {
      return {
        result: result.QueueUrl,
      }
    }

    return {
      error: 'not_found',
    }
  } catch (err) {
    // @ts-ignore
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function getQueueAttributes(
  sqsClient: SQSClient,
  queueUrl: string,
  attributeNames: QueueAttributeName[] = ['All'],
): Promise<Either<'not_found', QueueAttributesResult>> {
  const command = new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: attributeNames,
  })

  try {
    const response = await sqsClient.send(command)
    return {
      result: {
        attributes: response.Attributes,
      },
    }
  } catch (err) {
    // @ts-ignore
    if (err.Code === 'AWS.SimpleQueueService.NonExistentQueue') {
      return {
        // @ts-ignore
        error: 'not_found',
      }
    }
    throw err
  }
}

export async function resolveQueueUrlFromLocatorConfig(
  sqsClient: SQSClient,
  locatorConfig: Partial<SQSQueueLocatorType>,
) {
  if (locatorConfig.queueUrl) {
    return locatorConfig.queueUrl
  }

  if (!locatorConfig.queueName) {
    throw new Error('Invalid locatorConfig setup - queueUrl or queueName must be provided')
  }

  const queueUrlResult = await getQueueUrl(sqsClient, locatorConfig.queueName)

  if (queueUrlResult.error === 'not_found') {
    throw new Error(`Queue with queueName ${locatorConfig.queueName} does not exist.`)
  }

  return queueUrlResult.result
}

async function updateExistingQueue(
  sqsClient: SQSClient,
  queueUrl: string,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
) {
  const existingAttributes = await getQueueAttributes(sqsClient, queueUrl)

  if (!existingAttributes.result?.attributes) {
    throw new Error('Attributes are not set')
  }

  const queueArn = existingAttributes.result?.attributes.QueueArn
  if (!queueArn) {
    throw new Error('Queue ARN was not set')
  }

  // we will try to update existing queue if exists
  if (extraParams?.updateAttributesIfExists) {
    const updatedAttributes: Partial<Record<QueueAttributeName, string>> = {
      ...queueConfig.Attributes,
    }
    if (extraParams?.topicArnsWithPublishPermissionsPrefix) {
      updatedAttributes.Policy = generateQueuePublishForTopicPolicy(
        queueArn,
        extraParams.topicArnsWithPublishPermissionsPrefix,
      )
    }

    // Only perform update if there are new or changed values in the queue config
    if (!isShallowSubset(updatedAttributes, existingAttributes.result.attributes)) {
      await updateQueueAttributes(sqsClient, queueUrl, updatedAttributes)
    }
  }

  if (extraParams?.forceTagUpdate) {
    await updateQueueTags(sqsClient, queueUrl, queueConfig.tags)
  }

  return {
    queueUrl,
    queueArn,
    queueName: queueConfig.QueueName,
  }
}

export async function assertQueue(
  sqsClient: SQSClient,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
) {
  // biome-ignore lint/style/noNonNullAssertion: <explanation>
  const queueUrlResult = await getQueueUrl(sqsClient, queueConfig.QueueName!)
  const queueExists = !!queueUrlResult.result

  if (queueExists) {
    return updateExistingQueue(sqsClient, queueUrlResult.result, queueConfig, extraParams)
  }

  // create new queue
  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  // biome-ignore lint/style/noNonNullAssertion: <explanation>
  const newQueueUrlResult = await getQueueUrl(sqsClient, queueConfig.QueueName!)
  const newQueueExists = !!newQueueUrlResult.result

  if (!newQueueExists) {
    throw new Error(`Queue ${queueConfig.QueueName ?? ''} was not created`)
  }

  const getQueueAttributesCommand = new GetQueueAttributesCommand({
    QueueUrl: newQueueUrlResult.result,
    AttributeNames: ['QueueArn'],
  })
  const queueAttributesResponse = await sqsClient.send(getQueueAttributesCommand)
  const queueArn = queueAttributesResponse.Attributes?.QueueArn

  if (!queueArn) {
    throw new Error('Queue ARN was not set')
  }

  if (extraParams?.topicArnsWithPublishPermissionsPrefix) {
    const setTopicAttributesCommand = new SetQueueAttributesCommand({
      QueueUrl: newQueueUrlResult.result,
      Attributes: {
        Policy: generateQueuePublishForTopicPolicy(
          queueArn,
          extraParams.topicArnsWithPublishPermissionsPrefix,
        ),
      },
    })
    await sqsClient.send(setTopicAttributesCommand)
  }

  return {
    queueArn,
    queueUrl: newQueueUrlResult.result,
    queueName: queueConfig.QueueName,
  }
}

export async function deleteQueue(
  client: SQSClient,
  queueName: string,
  waitForConfirmation = true,
) {
  try {
    const queueUrlCommand = new GetQueueUrlCommand({
      QueueName: queueName,
    })
    const response = await client.send(queueUrlCommand)

    const command = new DeleteQueueCommand({
      QueueUrl: response.QueueUrl,
    })

    await client.send(command)

    if (waitForConfirmation) {
      await waitAndRetry(async () => {
        const queueList = await client.send(
          new ListQueuesCommand({
            QueueNamePrefix: queueName,
          }),
        )
        return !queueList.QueueUrls || queueList.QueueUrls.length === 0
      })
    }
  } catch (err) {
    // This is fine
    // @ts-ignore
    if (err.name === AWS_QUEUE_DOES_NOT_EXIST_ERROR_NAME) {
      return
    }

    // @ts-ignore
    globalLogger.error(`Failed to delete: ${err.message}`)
  }
}

/**
 * Calculates the size of an outgoing SQS message.
 *
 * SQS imposes a 256 KB limit on the total size of a message, which includes both the message body and any metadata (attributes).
 * This function currently computes the size based solely on the message body, as no attributes are included at this time.
 * For future updates, if message attributes are added, their sizes should also be considered.
 *
 * Reference: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html#sqs-message-attributes
 */
export function calculateOutgoingMessageSize(message: unknown) {
  const messageBody = JSON.stringify(message)
  return calculateSqsMessageBodySize(messageBody)
}

export function calculateSqsMessageBodySize(messageBody: SendMessageCommandInput['MessageBody']) {
  return messageBody ? Buffer.byteLength(messageBody, 'utf8') : 0
}
