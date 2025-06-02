import {
  type ConsumeOptions,
  Consumer,
  type ConsumerOptions,
  type Message,
  type MessagesStream,
  jsonDeserializer,
  stringDeserializer,
} from '@platformatic/kafka'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.js'
import type { KafkaDependencies, TopicConfig } from './types.js'

export type KafkaConsumerOptions<_TopicsConfig extends TopicConfig[]> = BaseKafkaOptions &
  Omit<ConsumerOptions<string, object, string, object>, 'deserializers' | 'autocommit'> &
  Omit<ConsumeOptions<string, object, string, object>, 'topics'>

export abstract class AbstractKafkaConsumer<
  TopicsConfig extends TopicConfig[],
> extends AbstractKafkaService<TopicsConfig, KafkaConsumerOptions<TopicsConfig>> {
  private readonly consumer: Consumer<string, object, string, object>
  private consumerStream?: MessagesStream<string, object, string, object>

  constructor(dependencies: KafkaDependencies, options: KafkaConsumerOptions<TopicsConfig>) {
    super(dependencies, options)

    this.consumer = new Consumer({
      ...this.options.kafka,
      ...this.options,
      autocommit: false, // Handling commits manually
      deserializers: {
        key: stringDeserializer,
        value: jsonDeserializer,
        headerKey: stringDeserializer,
        headerValue: jsonDeserializer,
      },
    })
  }

  async init(): Promise<void> {
    if (this.consumerStream) return Promise.resolve()

    this.consumerStream = await this.consumer.consume({
      topics: [], //this.topicsConfig.map((config) => config.topic),
    })

    this.consumerStream.on('data', async (message) => {
      await this.consume(message)
    })
  }

  async close(): Promise<void> {
    await new Promise((done) => this.consumerStream?.close(done))
    this.consumerStream = undefined
    await this.consumer.close()
  }

  private consume(_message: Message<string, object, string, object>): Promise<void> {
    return Promise.resolve()
  }
}
