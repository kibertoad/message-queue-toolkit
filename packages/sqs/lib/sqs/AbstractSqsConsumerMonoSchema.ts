import type { Either } from '@lokalise/node-core'
import type {
  BarrierCallback,
  MonoSchemaConsumerOptions,
  QueueConsumer as QueueConsumer,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type {
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig,
} from './AbstractSqsConsumer'
import { AbstractSqsConsumer } from './AbstractSqsConsumer'
import type { SQSConsumerDependencies, SQSQueueLocatorType } from './AbstractSqsService'

export type NewSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  CreationConfigType extends SQSCreationConfig,
> = NewSQSConsumerOptions<CreationConfigType> & MonoSchemaConsumerOptions<MessagePayloadType>

export type ExistingSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingSQSConsumerOptions<QueueLocatorType> & MonoSchemaConsumerOptions<MessagePayloadType>

export abstract class AbstractSqsConsumerMonoSchema<
    MessagePayloadType extends object,
    QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
    CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
    ConsumerOptionsType extends
      | NewSQSConsumerOptionsMono<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptionsMono<MessagePayloadType, QueueLocatorType> =
      | NewSQSConsumerOptionsMono<MessagePayloadType, CreationConfigType>
      | ExistingSQSConsumerOptionsMono<MessagePayloadType, QueueLocatorType>,
  >
  extends AbstractSqsConsumer<
    MessagePayloadType,
    QueueLocatorType,
    CreationConfigType,
    ConsumerOptionsType
  >
  implements QueueConsumer
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>
  private readonly barrier?: BarrierCallback<MessagePayloadType>

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)

    this.messageSchema = options.messageSchema
    this.barrier = options.barrier
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected resolveSchema() {
    return this.schemaEither
  }

  protected override async internalProcessMessage(
    message: MessagePayloadType,
    messageType: string,
  ): Promise<Either<'retryLater', 'success'>> {
    const barrierResponse = this.barrier ? await this.barrier(message) : true
    return barrierResponse
      ? super.internalProcessMessage(message, messageType)
      : { error: 'retryLater' }
  }
}
