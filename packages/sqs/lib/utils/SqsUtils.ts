import type { CreateQueueCommandInput, SQSClient } from '@aws-sdk/client-sqs'
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  DeleteQueueCommand,
  PurgeQueueCommand,
} from '@aws-sdk/client-sqs'

export async function assertQueue(sqsClient: SQSClient, queueConfig: CreateQueueCommandInput) {
  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  const getUrlCommand = new GetQueueUrlCommand({
    QueueName: queueConfig.QueueName,
  })
  const response = await sqsClient.send(getUrlCommand)

  if (!response.QueueUrl) {
    throw new Error(`Queue ${queueConfig.QueueName ?? ''} was not created`)
  }

  return response.QueueUrl
}

export async function purgeQueue(client: SQSClient, queueName: string) {
  try {
    const queueUrlCommand = new GetQueueUrlCommand({
      QueueName: queueName,
    })
    const response = await client.send(queueUrlCommand)

    const purgeCommand = new PurgeQueueCommand({
      QueueUrl: response.QueueUrl,
    })
    await client.send(purgeCommand)
  } catch (err) {
    // @ts-ignore
    console.log(`Failed to purge: ${err.message}`)
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
