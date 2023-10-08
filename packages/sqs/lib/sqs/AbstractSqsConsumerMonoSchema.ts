import type { Either } from '@lokalise/node-core'
import type {
  MonoSchemaQueueOptions,
  QueueConsumer as QueueConsumer,
  BarrierResult,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type {
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig,
} from './AbstractSqsConsumer'
import { AbstractSqsConsumer } from './AbstractSqsConsumer'
import type { SQSConsumerDependencies, SQSQueueLocatorType } from './AbstractSqsService'

const DEFAULT_BARRIER_RESULT = {
  isPassing: true,
  output: undefined,
} as const

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
    BarrierOutput = undefined,
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
    ConsumerOptionsType,
    BarrierOutput
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

  /**
   * Override to implement barrier pattern
   */
  protected override preHandlerBarrier(
    _message: MessagePayloadType,
    _messageType: string,
  ): Promise<BarrierResult<BarrierOutput>> {
    // @ts-ignore
    return Promise.resolve(DEFAULT_BARRIER_RESULT)
  }

  abstract override processMessage(
    message: MessagePayloadType,
    messageType: string,
    barrierOutput: BarrierOutput,
  ): Promise<Either<'retryLater', 'success'>>

  protected resolveSchema() {
    return this.schemaEither
  }
}
