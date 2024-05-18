import type { Options } from 'amqplib/properties'

import { AbstractAmqpPublisher } from './AbstractAmqpPublisher'

export type AmqpQueueMessageOptions = {
  publishOptions: Options.Publish
}

const NO_PARAMS: AmqpQueueMessageOptions = {
  publishOptions: {},
}

export abstract class AbstractAmqpQueuePublisher<
  MessagePayloadType extends object,
> extends AbstractAmqpPublisher<MessagePayloadType, AmqpQueueMessageOptions> {
  protected publishInternal(message: Buffer, options: AmqpQueueMessageOptions): void {
    this.channel.sendToQueue(this.queueName, message, options.publishOptions)
  }

  publish(message: MessagePayloadType, options: AmqpQueueMessageOptions = NO_PARAMS) {
    super.publish(message, options)
  }
}
