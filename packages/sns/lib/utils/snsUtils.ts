import { CreateTopicCommand, CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'

export async function assertTopic(snsClient: SNSClient, topicOptions: CreateTopicCommandInput) {
  const command = new CreateTopicCommand(topicOptions)
  const response = await snsClient.send(command)

  if (!response.TopicArn) {
    throw new Error('No topic arn in response')
  }
  return response.TopicArn
}
