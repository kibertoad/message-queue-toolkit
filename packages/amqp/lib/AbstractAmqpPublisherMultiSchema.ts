import type { Either } from '@lokalise/node-core'
import type {
  ExistingQueueOptions,
  NewQueueOptions,
  SyncPublisher,
  MultiSchemaPublisherOptions,
} from '@message-queue-toolkit/core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import type { AMQPLocatorType } from './AbstractAmqpBaseConsumer'
import { AbstractAmqpBasePublisher } from './AbstractAmqpBasePublisher'
import type { AMQPDependencies, CreateAMQPQueueOptions } from './AbstractAmqpService'

export abstract class AbstractAmqpPublisherMultiSchema<MessagePayloadType extends object>
  extends AbstractAmqpBasePublisher<MessagePayloadType>
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

    this.sendToQueue(message)
  }

  protected override resolveSchema(
    message: MessagePayloadType,
  ): Either<Error, ZodSchema<MessagePayloadType>> {
    return this.messageSchemaContainer.resolveSchema(message)
  }
}
