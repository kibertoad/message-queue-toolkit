import { stringValueSerializer } from '@lokalise/node-core'
import {
  type ConsumeOptions,
  Consumer,
  type ConsumerOptions,
  type Message,
  type MessagesStream,
  jsonDeserializer,
  stringDeserializer,
} from '@platformatic/kafka'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.ts'
import { KafkaHandlerContainer } from './handler-container/KafkaHandlerContainer.js'
import type { KafkaHandlerRouting } from './handler-container/KafkaHandlerRoutingBuilder.js'
import type { KafkaDependencies, TopicConfig } from './types.ts'

export type KafkaConsumerOptions<TopicsConfig extends TopicConfig[]> = BaseKafkaOptions &
  Omit<
    ConsumerOptions<string, object, string, object>,
    'deserializers' | 'autocommit' | 'bootstrapBrokers'
  > &
  Omit<ConsumeOptions<string, object, string, object>, 'topics'> & {
    handlers: KafkaHandlerRouting<TopicsConfig>
  }

export abstract class AbstractKafkaConsumer<
  TopicsConfig extends TopicConfig[],
> extends AbstractKafkaService<TopicsConfig, KafkaConsumerOptions<TopicsConfig>> {
  private readonly consumer: Consumer<string, object, string, object>
  private consumerStream?: MessagesStream<string, object, string, object>

  private readonly handlerContainer: KafkaHandlerContainer<TopicsConfig>

  constructor(dependencies: KafkaDependencies, options: KafkaConsumerOptions<TopicsConfig>) {
    super(dependencies, options)

    this.handlerContainer = new KafkaHandlerContainer<TopicsConfig>(
      options.handlers,
      options.messageTypeField,
    )

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
      ...this.options,
      topics: this.handlerContainer.topics,
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

  // TODO: Improve logging with logger child on constructor + add request context?
  // TODO: Message logging
  // TODO: Observability

  private async consume(message: Message<string, object, string, object>): Promise<void> {
    const handler = this.handlerContainer.resolveHandler(message.topic, message.value)
    // if there is no handler for the message, we just ignore it (simulating subscription)
    if (!handler) return message.commit()

    const parsedMessageValue = handler.schema.safeParse(message.value)
    if (!parsedMessageValue.success) {
      this.handlerError(parsedMessageValue.error, { message: stringValueSerializer(message) })
      this.handleMessageProcessed({
        message: message,
        processingResult: { status: 'error', errorReason: 'invalidMessage' },
        topic: message.topic,
      })
      return message.commit()
    }

    try {
      await handler.handler({ ...message, value: parsedMessageValue.data })
      this.handleMessageProcessed({
        message: message,
        processingResult: { status: 'consumed' },
        topic: message.topic,
      })
      return message.commit()
    } catch (error) {
      this.handlerError(error, { message: stringValueSerializer(message) })
      this.handleMessageProcessed({
        message: message,
        processingResult: { status: 'error', errorReason: 'handlerError' },
        topic: message.topic,
      })
    }
  }
}
