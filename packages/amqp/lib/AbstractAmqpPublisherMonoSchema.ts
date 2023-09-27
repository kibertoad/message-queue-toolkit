import type { Either } from '@lokalise/node-core'
import type {
  ExistingQueueOptions,
  MessageInvalidFormatError,
  MessageValidationError,
  MonoSchemaQueueOptions,
  NewQueueOptions,
  SyncPublisher,
} from '@message-queue-toolkit/core'
import { objectToBuffer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { AMQPLocatorType } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpService } from './AbstractAmqpService'
import type { AMQPDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'

export abstract class AbstractAmqpPublisherMonoSchema<MessagePayloadType extends object>
  extends AbstractAmqpService<MessagePayloadType>
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

    try {
      this.channel.sendToQueue(this.queueName, objectToBuffer(message))
    } catch (err) {
      // Unfortunately, reliable retry mechanism can't be implemented with try-catch block,
      // as not all failures end up here. If connection is closed programmatically, it works fine,
      // but if server closes connection unexpectedly (e. g. RabbitMQ is shut down), then we don't land here
      // @ts-ignore
      if (err.message === 'Channel closed') {
        void this.reconnect()
      }
    }
  }

  /* c8 ignore start */
  protected resolveMessage(): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  protected override resolveSchema(): Either<Error, ZodSchema<MessagePayloadType>> {
    throw new Error('Not implemented for publisher')
  }

  /* c8 ignore stop */
}
