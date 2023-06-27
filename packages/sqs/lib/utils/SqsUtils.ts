import {
  CreateQueueCommand,
  CreateQueueCommandInput,
  GetQueueUrlCommand,
  SQSClient,
} from '@aws-sdk/client-sqs'

export async function assertQueue(sqsClient: SQSClient, queueConfig: CreateQueueCommandInput) {
  const command = new CreateQueueCommand(queueConfig)
  await sqsClient.send(command)

  const getUrlCommand = new GetQueueUrlCommand({
    QueueName: queueConfig.QueueName,
  })
  const response = await sqsClient.send(getUrlCommand)

  if (!response.QueueUrl) {
    throw new Error(`Queue ${queueConfig.QueueName} was not created`)
  }

  return response.QueueUrl
}
