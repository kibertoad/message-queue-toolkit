import type { Options } from 'amqplib'
import type { AMQPPublisherOptions } from './AbstractAmqpPublisher.ts'
import { AbstractAmqpPublisher } from './AbstractAmqpPublisher.ts'
import type {
  AMQPDependencies,
  AMQPTopicCreationConfig,
  AMQPTopicLocator,
  AMQPTopicPublisherConfig,
} from './AbstractAmqpService.ts'
import { ensureExchange } from './utils/amqpQueueUtils.ts'

export type AMQPTopicPublisherOptions<MessagePayloadType extends object> = Omit<
  AMQPPublisherOptions<MessagePayloadType, AMQPTopicCreationConfig, AMQPTopicLocator>,
  'creationConfig'
> & {
  exchange: string
}

export type AmqpTopicMessageOptions = {
  routingKey?: string // used as topic in topic exchanges
  publishOptions?: Options.Publish
}

export abstract class AbstractAmqpTopicPublisher<
  MessagePayloadType extends object,
> extends AbstractAmqpPublisher<
  MessagePayloadType,
  AmqpTopicMessageOptions,
  AMQPTopicPublisherConfig,
  AMQPTopicPublisherConfig
> {
  constructor(
    dependencies: AMQPDependencies,
    options: AMQPTopicPublisherOptions<MessagePayloadType>,
  ) {
    super(dependencies, {
      ...options,
      creationConfig: {
        exchange: options.exchange,
        updateAttributesIfExists: false,
      },
      exchange: options.exchange,
      locatorConfig: undefined,
    })
  }

  protected publishInternal(
    message: Buffer,
    options: Omit<AmqpTopicMessageOptions, 'routingKey'> & { routingKey: string },
  ): void {
    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    this.channel.publish(this.exchange!, options.routingKey, message, options.publishOptions)
  }

  protected override resolveTopicOrQueue(): string {
    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    return this.exchange!
  }

  protected override async createMissingEntities(): Promise<void> {
    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    await ensureExchange(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
