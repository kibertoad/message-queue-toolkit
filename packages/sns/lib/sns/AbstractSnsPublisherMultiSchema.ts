import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type { AsyncPublisher, NewQueueOptions } from '@message-queue-toolkit/core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { ExistingSNSOptions, SNSCreationConfig, SNSDependencies } from './AbstractSnsService'
import { AbstractSnsService } from './AbstractSnsService'

export type SNSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSnsPublisherMultiSchema<MessagePayloadType extends object>
  extends AbstractSnsService<MessagePayloadType, MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SNSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  constructor(
    dependencies: SNSDependencies,
    options: (ExistingSNSOptions | NewQueueOptions<SNSCreationConfig>) & {
      messageSchemas: readonly ZodSchema<MessagePayloadType>[]
    },
  ) {
    super(dependencies, options)

    const messageSchemas = options.messageSchemas
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
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

  protected resolveSchema(message: MessagePayloadType): ZodSchema<MessagePayloadType> {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
