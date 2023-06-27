import type { AsyncPublisher } from '@message-queue-toolkit/core'

import { AbstractSnsService } from './AbstractSnsService'
import { PublishCommand } from '@aws-sdk/client-sns'
import { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'

export type SNSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSnsPublisher<MessagePayloadType extends object>
  extends AbstractSnsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SNSMessageOptions>
{
  async publish(message: MessagePayloadType, options: SNSMessageOptions = {}): Promise<void> {
    try {
      const input = {
        Message: JSON.stringify(message),
        TopicArn: this.topicArn,
        ...options,
      } satisfies PublishCommandInput
      const command = new PublishCommand(input)
      await this.snsClient.send(command)
    } catch (error) {
      this.handleError(error)
      throw error
    }
  }
}
