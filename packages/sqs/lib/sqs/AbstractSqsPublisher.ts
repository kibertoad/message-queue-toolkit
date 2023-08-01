import type { SendMessageCommandInput } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { AsyncPublisher } from '@message-queue-toolkit/core'
import type { ZodType } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'

import { AbstractSqsService } from './AbstractSqsService'

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSqsPublisher<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    try {
      const input = {
        // SendMessageRequest
        QueueUrl: this.queueUrl,
        MessageBody: JSON.stringify(message),
        ...options,
      } satisfies SendMessageCommandInput
      const command = new SendMessageCommand(input)
      await this.sqsClient.send(command)
    } catch (error) {
      this.handleError(error)
      throw error
    }
  }

  protected resolveSchema(_message: SQSMessage): ZodType<MessagePayloadType> {
    throw new Error('Unsupported, but not used anyway')
  }
}
