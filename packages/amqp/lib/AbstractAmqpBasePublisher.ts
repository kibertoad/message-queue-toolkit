import type { Either } from '@lokalise/node-core'
import type { MessageInvalidFormatError, MessageValidationError } from '@message-queue-toolkit/core'
import { objectToBuffer } from '@message-queue-toolkit/core'

import { AbstractAmqpService } from './AbstractAmqpService'

export abstract class AbstractAmqpBasePublisher<
  MessagePayloadType extends object,
> extends AbstractAmqpService<MessagePayloadType> {
  protected sendToQueue(message: MessagePayloadType): void {
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
}
