import type { SNSClient } from '@aws-sdk/client-sns'
import { DeleteTopicCommand } from '@aws-sdk/client-sns'

import { assertTopic } from '../../lib/utils/snsUtils'

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
    // @ts-ignore
    console.log(`Failed to delete: ${err.message}`)
  }
}
