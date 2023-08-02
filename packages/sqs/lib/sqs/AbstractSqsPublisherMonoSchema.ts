import type { SendMessageCommandInput } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  MessageInvalidFormatError,
  MessageValidationError,
  ExistingQueueOptions,
  MonoSchemaQueueOptions,
  NewQueueOptions,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import type { SQSDependencies, SQSQueueLocatorType } from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSqsPublisherMonoSchema<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>

  constructor(
    dependencies: SQSDependencies,
    options: (NewQueueOptions<SQSCreationConfig> | ExistingQueueOptions<SQSQueueLocatorType>) &
      MonoSchemaQueueOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.messageSchema = options.messageSchema
  }

  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    try {
      this.messageSchema.parse(message)
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

  protected resolveMessage(
  ): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema() {
    return {
      result: this.messageSchema,
    }
  }
}
