import type { Either } from '@lokalise/node-core'
import type {
  AsyncPublisher,
  BarrierResult,
  MessageInvalidFormatError,
  MessageValidationError,
  NewQueueOptions,
} from '@message-queue-toolkit/core'
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
    const messageSchemaResult = this.resolveSchema(message)
    if (messageSchemaResult.error) {
      throw messageSchemaResult.error
    }

    return this.internalPublish(message, messageSchemaResult.result, options)
  }

  protected override resolveMessage(): Either<
    MessageInvalidFormatError | MessageValidationError,
    unknown
  > {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema(message: MessagePayloadType) {
    return this.messageSchemaContainer.resolveSchema(message)
  }

  /* c8 ignore start */
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
  /* c8 ignore stop */
}
