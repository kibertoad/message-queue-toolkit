import type { Options } from 'amqplib'
import type { AMQPPublisherOptions } from './AbstractAmqpPublisher.ts'
import { AbstractAmqpPublisher } from './AbstractAmqpPublisher.ts'
import type {
  AMQPDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
} from './AbstractAmqpService.ts'
import { ensureAmqpQueue } from './utils/amqpQueueUtils.ts'

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
      : // biome-ignore lint/style/noNonNullAssertion: this is expected
        options.creationConfig!.queueName
  }

  protected publishInternal(message: Buffer, options: AmqpQueueMessageOptions): void {
    this.channel.sendToQueue(this.queueName, message, options.publishOptions)
  }

  override publish(message: MessagePayloadType, options: AmqpQueueMessageOptions = NO_PARAMS) {
    super.publish(message, options)
  }

  protected override resolveTopicOrQueue(): string {
    return this.queueName
  }

  protected override createMissingEntities(): Promise<void> {
    // biome-ignore lint/style/noNonNullAssertion: <explanation>
    return ensureAmqpQueue(this.connection!, this.channel, this.creationConfig, this.locatorConfig)
  }
}
