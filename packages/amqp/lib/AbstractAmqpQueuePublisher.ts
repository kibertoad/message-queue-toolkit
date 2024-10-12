import type { Options } from 'amqplib/properties'

import type { AMQPPublisherOptions } from './AbstractAmqpPublisher'
import { AbstractAmqpPublisher } from './AbstractAmqpPublisher'
import type {
  AMQPDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
} from './AbstractAmqpService'
import { ensureAmqpQueue } from './utils/amqpQueueUtils'

export type AmqpQueueMessageOptions = {
  publishOptions: Options.Publish
}

const NO_PARAMS: AmqpQueueMessageOptions = {
  publishOptions: {},
}

export abstract class AbstractAmqpQueuePublisher<
  MessagePayloadType extends object,
> extends AbstractAmqpPublisher<
  MessagePayloadType,
  AmqpQueueMessageOptions,
  AMQPQueueCreationConfig,
  AMQPQueueLocator
> {
  protected readonly queueName: string

  constructor(
    dependencies: AMQPDependencies,
    options: AMQPPublisherOptions<MessagePayloadType, AMQPQueueCreationConfig, AMQPQueueLocator>,
  ) {
    super(dependencies, options)

    if (!options.locatorConfig?.queueName && !options.creationConfig?.queueName) {
      throw new Error('Either locatorConfig or creationConfig must provide queueName')
    }

    this.queueName = options.locatorConfig
      ? options.locatorConfig.queueName
      : options.creationConfig!.queueName
  }

  protected publishInternal(message: Buffer, options: AmqpQueueMessageOptions): void {
    this.channel.sendToQueue(this.queueName, message, options.publishOptions)
  }

  publish(message: MessagePayloadType, options: AmqpQueueMessageOptions = NO_PARAMS) {
    super.publish(message, options)
  }

  protected override createMissingEntities(): Promise<void> {
    return ensureAmqpQueue(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
