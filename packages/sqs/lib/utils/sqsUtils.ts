import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  SetQueueAttributesCommand,
} from '@aws-sdk/client-sqs'
import type { QueueAttributeName } from '@aws-sdk/client-sqs/dist-types/models/models_0'
import type { Either } from '@lokalise/node-core'

import type { ExtraSQSCreationParams } from '../sqs/AbstractSqsConsumer'
import type { SQSQueueLocatorType } from '../sqs/AbstractSqsService'

import { generateQueuePublishForTopicPolicy } from './sqsAttributeUtils'

type QueueAttributesResult = {
  attributes?: Record<string, string>
}

export async function getQueueAttributes(
  sqsClient: SQSClient,
  queueLocator: SQSQueueLocatorType,
  attributeNames?: QueueAttributeName[],
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

export async function deleteQueue(client: SQSClient, queueName: string) {
  try {
    const queueUrlCommand = new GetQueueUrlCommand({
      QueueName: queueName,
    })
    const response = await client.send(queueUrlCommand)

    const command = new DeleteQueueCommand({
      QueueUrl: response.QueueUrl,
    })

    await client.send(command)
  } catch (err) {
    // @ts-ignore
    console.log(`Failed to delete: ${err.message}`)
  }
}
