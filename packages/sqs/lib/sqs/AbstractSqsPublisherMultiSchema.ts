import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  MessageInvalidFormatError,
  MessageValidationError,
  ExistingQueueOptions,
  NewQueueOptions,
  MultiSchemaPublisherOptions,
  BarrierResult,
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
    options: (NewQueueOptions<SQSCreationConfig> | ExistingQueueOptions<SQSQueueLocatorType>) &
      MultiSchemaPublisherOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)

    const messageSchemas = options.messageSchemas
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
  }

  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    const messageSchemaResult = this.resolveSchema(message)
    if (messageSchemaResult.error) {
      throw messageSchemaResult.error
    }

    return this.internalPublish(message, messageSchemaResult.result, options)
  }

  /* c8 ignore start */
  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  protected resolveMessage(
    _message: SQSMessage,
  ): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override processPrehandlers(): Promise<unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override preHandlerBarrier(): Promise<BarrierResult<unknown>> {
    throw new Error('Not implemented for publisher')
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
