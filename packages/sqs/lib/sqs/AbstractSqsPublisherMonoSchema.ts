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

import type { SQSCreationConfig } from './AbstractSqsConsumer'
import type { SQSMessageOptions } from './AbstractSqsPublisherMultiSchema'
import type { SQSDependencies, SQSQueueLocatorType } from './AbstractSqsService'
import { AbstractSqsService } from './AbstractSqsService'

export abstract class AbstractSqsPublisherMonoSchema<MessagePayloadType extends object>
  extends AbstractSqsService<MessagePayloadType>
  implements AsyncPublisher<MessagePayloadType, SQSMessageOptions>
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>

  constructor(
    dependencies: SQSDependencies,
    options: (NewQueueOptions<SQSCreationConfig> | ExistingQueueOptions<SQSQueueLocatorType>) &
      MonoSchemaQueueOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)
    this.messageSchema = options.messageSchema

    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  async publish(message: MessagePayloadType, options: SQSMessageOptions = {}): Promise<void> {
    return this.internalPublish(message, this.messageSchema, options)
  }

  /* c8 ignore start */
  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

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
