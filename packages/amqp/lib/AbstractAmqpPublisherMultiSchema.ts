import type { Either } from '@lokalise/node-core'
import type {
  ExistingQueueOptions,
  MessageInvalidFormatError,
  MessageValidationError,
  NewQueueOptions,
  SyncPublisher,
  MultiSchemaPublisherOptions,
} from '@message-queue-toolkit/core'
import { MessageSchemaContainer, objectToBuffer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { AMQPLocatorType } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpService } from './AbstractAmqpService'
import type { AMQPDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'

export abstract class AbstractAmqpPublisherMultiSchema<MessagePayloadType extends object>
  extends AbstractAmqpService<MessagePayloadType>
  implements SyncPublisher<MessagePayloadType>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayloadType>

  constructor(
    dependencies: AMQPDependencies,
    options: (NewQueueOptions<CreateAMQPQueueOptions> | ExistingQueueOptions<AMQPLocatorType>) &
      MultiSchemaPublisherOptions<MessagePayloadType>,
  ) {
    super(dependencies, options)

    const messageSchemas = options.messageSchemas
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayloadType>({
      messageSchemas,
      messageTypeField: options.messageTypeField,
    })
  }

  publish(message: MessagePayloadType): void {
    const resolveSchemaResult = this.resolveSchema(message)
    if (resolveSchemaResult.error) {
      throw resolveSchemaResult.error
    }
    resolveSchemaResult.result.parse(message)

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
        this.logger.error(`AMQP channel closed`)
        void this.reconnect()
      } else {
        throw err
      }
    }
  }

  /* c8 ignore start */
  protected resolveMessage(): Either<MessageInvalidFormatError | MessageValidationError, unknown> {
    throw new Error('Not implemented for publisher')
  }

  /* c8 ignore stop */

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
