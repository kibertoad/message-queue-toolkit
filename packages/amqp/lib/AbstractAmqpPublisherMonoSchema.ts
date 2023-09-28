import type { Either } from '@lokalise/node-core'
import type {
  ExistingQueueOptions,
  MonoSchemaQueueOptions,
  NewQueueOptions,
  SyncPublisher,
} from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { AMQPLocatorType } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBasePublisher } from './AbstractAmqpBasePublisher'
import type { AMQPDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'

export abstract class AbstractAmqpPublisherMonoSchema<MessagePayloadType extends object>
  extends AbstractAmqpBasePublisher<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  private readonly messageSchema: ZodSchema<MessagePayloadType>

  constructor(
    dependencies: AMQPDependencies,
    options: (NewQueueOptions<CreateAMQPQueueOptions> | ExistingQueueOptions<AMQPLocatorType>) &
      MonoSchemaQueueOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)

    this.messageSchema = options.messageSchema
  }

  publish(message: MessagePayloadType): void {
    this.messageSchema.parse(message)

    if (this.logMessages) {
      // @ts-ignore
      const resolvedLogMessage = this.resolveMessageLog(message, message[this.messageTypeField])
      this.logMessage(resolvedLogMessage)
    }

    this.sendToQueue(message)
  }

  /* c8 ignore start */
  protected override resolveSchema(): Either<Error, ZodSchema<MessagePayloadType>> {
    throw new Error('Not implemented for publisher')
  }
  /* c8 ignore stop */
}
