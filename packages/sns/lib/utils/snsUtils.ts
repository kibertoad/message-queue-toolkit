import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import {
  CreateTopicCommand,
  DeleteTopicCommand,
  GetTopicAttributesCommand,
} from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'

type QueueAttributesResult = {
  attributes?: Record<string, string>
}

export async function getTopicAttributes(
  snsClient: SNSClient,
  topicArn: string,
): Promise<Either<'not_found', QueueAttributesResult>> {
  const command = new GetTopicAttributesCommand({
    TopicArn: topicArn,
  })

  try {
    const response = await snsClient.send(command)
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

export async function assertTopic(snsClient: SNSClient, topicOptions: CreateTopicCommandInput) {
  const command = new CreateTopicCommand(topicOptions)
  const response = await snsClient.send(command)

  if (!response.TopicArn) {
    throw new Error('No topic arn in response')
  }
  return response.TopicArn
}

export async function deleteTopic(client: SNSClient, topicName: string) {
  try {
    const topicArn = await assertTopic(client, {
      Name: topicName,
    })

    const command = new DeleteTopicCommand({
      TopicArn: topicArn,
    })

    await client.send(command)
  } catch (err) {
    // we don't care it operation has failed
  }
}
