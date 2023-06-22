import type { SQSClient } from '@aws-sdk/client-sqs'
import { DeleteQueueCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs'

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
