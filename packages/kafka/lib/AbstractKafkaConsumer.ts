import { randomUUID } from 'node:crypto'
import { setTimeout } from 'node:timers/promises'
import {
  InternalError,
  type TransactionObservabilityManager,
  stringValueSerializer,
} from '@lokalise/node-core'
import type { QueueConsumerDependencies } from '@message-queue-toolkit/core'
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
import { KafkaHandlerContainer } from './handler-container/KafkaHandlerContainer.ts'
import type { KafkaHandlerRouting } from './handler-container/KafkaHandlerRoutingBuilder.ts'
import type { KafkaHandler, RequestContext } from './handler-container/index.ts'
import type { KafkaDependencies, TopicConfig } from './types.ts'

export type KafkaConsumerDependencies = KafkaDependencies &
  Pick<QueueConsumerDependencies, 'transactionObservabilityManager'>

export type KafkaConsumerOptions<TopicsConfig extends TopicConfig[]> = BaseKafkaOptions &
  Omit<
    ConsumerOptions<string, object, string, string>,
    'deserializers' | 'autocommit' | 'bootstrapBrokers'
  > &
  Omit<ConsumeOptions<string, object, string, string>, 'topics'> & {
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
  private readonly consumer: Consumer<string, object, string, string>
  private consumerStream?: MessagesStream<string, object, string, string>

  private readonly transactionObservabilityManager: TransactionObservabilityManager
  private readonly handlerContainer: KafkaHandlerContainer<TopicsConfig>

  constructor(
    dependencies: KafkaConsumerDependencies,
    options: KafkaConsumerOptions<TopicsConfig>,
  ) {
    super(dependencies, options)

    this.transactionObservabilityManager = dependencies.transactionObservabilityManager
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
        headerValue: stringDeserializer,
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

  private async consume(message: Message<string, object, string, string>): Promise<void> {
    const handler = this.handlerContainer.resolveHandler(message.topic, message.value)
    // if there is no handler for the message, we ignore it (simulating subscription)
    if (!handler) return message.commit()

    /* v8 ignore next */
    const transactionId = this.resolveMessageId(message.value) ?? randomUUID()
    this.transactionObservabilityManager?.start(this.buildTransactionName(message), transactionId)

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

    const requestContext = this.getRequestContext(message)

    let retries = 0
    let consumed = false
    do {
      // exponential backoff -> 2^(retry-1)
      if (retries > 0) await setTimeout(Math.pow(2, retries - 1))

      consumed = await this.tryToConsume(
        { ...message, value: validatedMessage },
        handler.handler,
        requestContext,
      )
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

    this.transactionObservabilityManager?.stop(transactionId)

    return message.commit()
  }

  private async tryToConsume<MessageValue extends object>(
    message: Message<string, MessageValue, string, string>,
    handler: KafkaHandler<MessageValue>,
    requestContext: RequestContext,
  ): Promise<boolean> {
    try {
      await handler(message, requestContext)
      return true
    } catch (error) {
      this.handlerError(error, {
        topic: message.topic,
        message: stringValueSerializer(message.value),
      })
    }

    return false
  }

  private buildTransactionName(message: Message<string, object, string, string>) {
    const messageType = this.resolveMessageType(message.value)

    let name = `kafka:${message.topic}`
    if (messageType?.trim().length) name += `:${messageType.trim()}`

    return name
  }

  private getRequestContext(message: Message<string, object, string, string>): RequestContext {
    const reqId = message.headers.get(this.resolveHeaderRequestIdField()) ?? randomUUID()

    return {
      reqId,
      logger: this.logger.child({
        'x-request-id': reqId,
        topic: message.topic,
        messageKey: message.key,
      }),
    }
  }
}
