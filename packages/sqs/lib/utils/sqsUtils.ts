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

import type { ExtraSQSCreationParams } from '../sqs/AbstractSqsConsumer'
import type { SQSQueueLocatorType } from '../sqs/AbstractSqsService'

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

export async function assertQueue(
  sqsClient: SQSClient,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
) {
  // we will try to update existing queue if exists
  if (extraParams?.updateAttributesIfExists) {
    // eslint-disable-next-line  @typescript-eslint/no-non-null-assertion
    const queueExistsResult = await getQueueUrl(sqsClient, queueConfig.QueueName!)

    if (queueExistsResult.result) {
      const queueUrl = queueExistsResult.result
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

      if (!isShallowSubset(existingAttributes.result, queueConfig.Attributes)) {
        await updateQueueAttributes(sqsClient, queueUrl, queueConfig.Attributes)
      }

      return {
        queueUrl,
        queueArn,
        queueName: queueConfig.QueueName,
      }
    }
  }

  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  const getUrlCommand = new GetQueueUrlCommand({
    QueueName: queueConfig.QueueName,
  })
  const response = await sqsClient.send(getUrlCommand)

  if (!response.QueueUrl) {
    throw new Error(`Queue ${queueConfig.QueueName ?? ''} was not created`)
  }

  const getQueueAttributesCommand = new GetQueueAttributesCommand({
    QueueUrl: response.QueueUrl,
    AttributeNames: ['QueueArn'],
  })
  const queueAttributesResponse = await sqsClient.send(getQueueAttributesCommand)
  const queueArn = queueAttributesResponse.Attributes?.QueueArn

  if (!queueArn) {
    throw new Error('Queue ARN was not set')
  }

  if (extraParams?.topicArnsWithPublishPermissionsPrefix) {
    const setTopicAttributesCommand = new SetQueueAttributesCommand({
      QueueUrl: response.QueueUrl,
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
    queueUrl: response.QueueUrl,
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
