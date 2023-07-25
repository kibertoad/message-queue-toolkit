import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type { AsyncPublisher } from '@message-queue-toolkit/core'

import type { SNSMessageOptions } from './AbstractSnsPublisher'
import { AbstractSnsServiceMultiSchema } from './AbstractSnsServiceMultiSchema'

export abstract class AbstractSnsPublisherMultiSchema<MessagePayloadSchemas extends object>
  extends AbstractSnsServiceMultiSchema<MessagePayloadSchemas>
  implements AsyncPublisher<MessagePayloadSchemas, SNSMessageOptions>
{
  async publish(message: MessagePayloadSchemas, options: SNSMessageOptions = {}): Promise<void> {
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
