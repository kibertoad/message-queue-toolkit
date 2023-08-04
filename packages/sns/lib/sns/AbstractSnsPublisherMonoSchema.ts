import { PublishCommand } from '@aws-sdk/client-sns'
import type { PublishCommandInput } from '@aws-sdk/client-sns/dist-types/commands/PublishCommand'
import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  MessageInvalidFormatError,
  MessageValidationError,
  MonoSchemaQueueOptions,
  NewQueueOptions,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

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
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>

  constructor(
    dependencies: SNSDependencies,
    options: (ExistingSNSOptions | NewQueueOptions<SNSCreationConfig>) &
      MonoSchemaQueueOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  async publish(message: MessagePayloadType, options: SNSMessageOptions = {}): Promise<void> {
    try {
      this.messageSchema.parse(message)
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

  /* c8 ignore start */
  protected override resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    unknown
  > {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema() {
    return this.schemaEither
  }
  /* c8 ignore stop */
}
