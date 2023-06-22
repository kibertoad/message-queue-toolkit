import type { SQSClient } from '@aws-sdk/client-sqs'
import { DeleteQueueCommand } from '@aws-sdk/client-sqs'

export async function deleteQueue(client: SQSClient, queueName: string) {
  const command = new DeleteQueueCommand({
    QueueUrl: queueName,
  })

  try {
    await client.send(command)
  } catch (err) {}
}
