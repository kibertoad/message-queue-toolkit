import type { Either } from '@lokalise/node-core'
import type {
  QueueConsumer,
  MonoSchemaQueueOptions,
  BarrierResult,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type {
  ExistingAMQPConsumerOptions,
  NewAMQPConsumerOptions,
} from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

const DEFAULT_BARRIER_RESULT = {
  isPassing: true,
  output: undefined,
} as const

export abstract class AbstractAmqpConsumerMonoSchema<
    MessagePayloadType extends object,
    BarrierOutput = undefined,
  >
  extends AbstractAmqpBaseConsumer<MessagePayloadType, BarrierOutput>
  implements QueueConsumer
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>

  constructor(
    dependencies: AMQPConsumerDependencies,
    options:
      | (NewAMQPConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>)
      | (ExistingAMQPConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>),
  ) {
    super(dependencies, options)

    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected override resolveSchema(_message: MessagePayloadType) {
    return this.schemaEither
  }

  /**
   * Override to implement barrier pattern
   */
  protected preHandlerBarrier(_message: MessagePayloadType): Promise<BarrierResult<BarrierOutput>> {
    // @ts-ignore
    return Promise.resolve(DEFAULT_BARRIER_RESULT)
  }
}
