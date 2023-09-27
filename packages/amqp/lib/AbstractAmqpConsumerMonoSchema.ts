import type { Either } from '@lokalise/node-core'
import type { QueueConsumer, MonoSchemaQueueOptions } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type {
  ExistingAMQPConsumerOptions,
  NewAMQPConsumerOptions,
} from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBaseConsumer } from './AbstractAmqpBaseConsumer'
import type { AMQPConsumerDependencies } from './AbstractAmqpService'

export abstract class AbstractAmqpConsumerMonoSchema<MessagePayloadType extends object>
  extends AbstractAmqpBaseConsumer<MessagePayloadType>
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
  protected preHandlerBarrier(_message: MessagePayloadType): Promise<boolean> {
    return Promise.resolve(true)
  }
}
