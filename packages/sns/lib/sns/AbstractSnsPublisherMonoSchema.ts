import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  BarrierResult,
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

  publish(message: MessagePayloadType, options: SNSMessageOptions = {}): Promise<void> {
    return this.internalPublish(message, this.messageSchema, options)
  }

  /* c8 ignore start */
  protected override resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    unknown
  > {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveNextFunction(): () => void {
    throw new Error('Not implemented for publisher')
  }

  protected override processPrehandlers(): Promise<undefined> {
    throw new Error('Not implemented for publisher')
  }

  protected override preHandlerBarrier(): Promise<BarrierResult<undefined>> {
    throw new Error('Not implemented for publisher')
  }

  override processMessage(): Promise<Either<'retryLater', 'success'>> {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema() {
    return this.schemaEither
  }
  /* c8 ignore stop */
}
