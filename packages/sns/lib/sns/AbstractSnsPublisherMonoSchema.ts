import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type {
  AsyncPublisher,
  MonoSchemaQueueOptions,
  NewQueueOptions,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'

import type { ExistingSNSOptions, SNSCreationConfig, SNSDependencies } from './AbstractSnsService'
import { AbstractSnsService } from './AbstractSnsService'

export type SNSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSnsPublisherMonoSchema<MessagePayloadType extends object>
  extends AbstractSnsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SNSMessageOptions>
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>

  constructor(
    dependencies: SNSDependencies,
    options: (ExistingSNSOptions | NewQueueOptions<SNSCreationConfig>) &
      MonoSchemaQueueOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.messageSchema = options.messageSchema
  }

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

  protected resolveSchema(_message: SNS_MESSAGE_BODY_TYPE): ZodSchema<MessagePayloadType> {
    return this.messageSchema
  }
}
