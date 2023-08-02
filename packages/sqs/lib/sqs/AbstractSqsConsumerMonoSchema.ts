import type { Either } from '@lokalise/node-core'
import type {
  MonoSchemaQueueOptions,
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
> = NewSQSConsumerOptions<CreationConfigType> & MonoSchemaQueueOptions<MessagePayloadType>

export type ExistingSQSConsumerOptionsMono<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
> = ExistingSQSConsumerOptions<QueueLocatorType> & MonoSchemaQueueOptions<MessagePayloadType>

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

  protected constructor(dependencies: SQSConsumerDependencies, options: ConsumerOptionsType) {
    super(dependencies, options)

    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected resolveSchema() {
    return this.schemaEither
  }
}
