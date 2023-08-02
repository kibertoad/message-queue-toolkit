import type { SendMessageCommandInput } from '@aws-sdk/client-sqs'
import { SendMessageCommand } from '@aws-sdk/client-sqs'
import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  MessageInvalidFormatError,
  MessageValidationError,
  ExistingQueueOptions,
  NewQueueOptions,
} from '@message-queue-toolkit/core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { SQSMessage } from '../types/MessageTypes'

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import { AbstractSqsService } from './AbstractSqsService'
import type { SQSDependencies, SQSQueueLocatorType } from './AbstractSqsService'

export type SQSMessageOptions = {
  MessageGroupId?: string
  MessageDeduplicationId?: string
}

export abstract class AbstractSqsPublisherMultiSchema<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  constructor(
    dependencies: SQSDependencies,
    options: (NewQueueOptions<SQSCreationConfig> | ExistingQueueOptions<SQSQueueLocatorType>) & {
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

  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    try {
      const resolveSchemaResult = this.resolveSchema(message)
      if (resolveSchemaResult.error) {
        throw resolveSchemaResult.error
      }

      resolveSchemaResult.result.parse(message)
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
    _message: SQSMessage,
  ): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
