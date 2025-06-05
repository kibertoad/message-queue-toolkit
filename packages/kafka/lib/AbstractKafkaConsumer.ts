import { setTimeout } from 'node:timers/promises'
import { InternalError, stringValueSerializer } from '@lokalise/node-core'
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
import type { KafkaHandler } from './handler-container/index.js'
import type { KafkaDependencies, TopicConfig } from './types.ts'

export type KafkaConsumerOptions<TopicsConfig extends TopicConfig[]> = BaseKafkaOptions &
  Omit<
    ConsumerOptions<string, object, string, object>,
    'deserializers' | 'autocommit' | 'bootstrapBrokers'
  > &
  Omit<ConsumeOptions<string, object, string, object>, 'topics'> & {
    handlers: KafkaHandlerRouting<TopicsConfig>
  }

/*
TODO: Proper retry mechanism + DLQ -> https://lokalise.atlassian.net/browse/EDEXP-498
In the meantime, we will retry in memory up to 3 times
 */
const MAX_IN_MEMORY_RETRIES = 3

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
    const topics = this.handlerContainer.topics
    if (topics.length === 0) throw new Error('At least one topic must be defined')

    try {
      const { handlers, ...consumeOptions } = this.options // Handlers cannot be passed to consume method
      this.consumerStream = await this.consumer.consume({ ...consumeOptions, topics })
    } catch (error) {
      throw new InternalError({
        message: 'Consumer init failed',
        errorCode: 'KAFKA_CONSUMER_INIT_ERROR',
        cause: error,
      })
    }

    this.consumerStream.on('data', (message) => this.consume(message))
    this.consumerStream.on('error', (error) => this.handlerError(error))
  }

  async close(): Promise<void> {
    if (!this.consumerStream) return Promise.resolve()

    await new Promise((done) => this.consumerStream?.close(done))
    this.consumerStream = undefined
    await this.consumer.close()
  }

  /*
  TODO: https://lokalise.atlassian.net/browse/EDEXP-493
    - Improve logging with logger child on constructor + add request context?
    - Message logging
    - Observability
   */

  private async consume(message: Message<string, object, string, object>): Promise<void> {
    const handler = this.handlerContainer.resolveHandler(message.topic, message.value)
    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handler) return message.commit()

    const parseResult = handler.schema.safeParse(message.value)
    if (!parseResult.success) {
      this.handlerError(parseResult.error, {
        topic: message.topic,
        message: stringValueSerializer(message.value),
      })
      this.handleMessageProcessed({
        topic: message.topic,
        message: message.value,
        processingResult: { status: 'error', errorReason: 'invalidMessage' },
      })

      return message.commit()
    }

    const validatedMessage = parseResult.data

    let retries = 0
    let consumed = false
    do {
      // exponential backoff -> 2^(retry-1)
      if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

      consumed = await this.tryToConsume({ ...message, value: validatedMessage }, handler.handler)
      if (consumed) break

      retries++
    } while (retries < MAX_IN_MEMORY_RETRIES)

    if (consumed) {
      this.handleMessageProcessed({
        topic: message.topic,
        message: validatedMessage,
        processingResult: { status: 'consumed' },
      })
    } else {
      this.handleMessageProcessed({
        topic: message.topic,
        message: validatedMessage,
        processingResult: { status: 'error', errorReason: 'handlerError' },
      })
    }

    return message.commit()
  }

  private async tryToConsume<MessageValue extends object>(
    message: Message<string, MessageValue, string, object>,
    handler: KafkaHandler<MessageValue>,
  ): Promise<boolean> {
    try {
      await handler(message)
      return true
    } catch (error) {
      this.handlerError(error, {
        topic: message.topic,
        message: stringValueSerializer(message.value),
      })
    }

    return false
  }
}
