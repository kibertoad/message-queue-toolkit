import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
  ListQueuesCommand,
} from '@aws-sdk/client-sqs'
import type { CreateQueueCommandInput, SQSClient, QueueAttributeName } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import { isShallowSubset, waitAndRetry } from '@message-queue-toolkit/core'

import type { ExtraSQSCreationParams, SQSQueueLocatorType } from '../sqs/AbstractSqsService'

import { generateQueuePublishForTopicPolicy } from './sqsAttributeUtils'
import { updateQueueAttributes } from './sqsInitter'

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
  queueLocator: SQSQueueLocatorType,
  attributeNames: QueueAttributeName[] = ['All'],
): Promise<Either<'not_found', QueueAttributesResult>> {
  const command = new GetQueueAttributesCommand({
    QueueUrl: queueLocator.queueUrl,
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

async function updateExistingQueue(
  sqsClient: SQSClient,
  queueUrl: string,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
) {
  const existingAttributes = await getQueueAttributes(sqsClient, {
    queueUrl,
  })

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
  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
  const queueUrlResult = await getQueueUrl(sqsClient, queueConfig.QueueName!)
  const queueExists = !!queueUrlResult.result

  if (queueExists) {
    return updateExistingQueue(sqsClient, queueUrlResult.result, queueConfig, extraParams)
  }

  // create new queue
  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
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
    console.log(`Failed to delete: ${err.message}`)
  }
}
