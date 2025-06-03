import { InternalError, stringValueSerializer } from '@lokalise/node-core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import {
  type MessageToProduce,
  type ProduceOptions,
  Producer,
  jsonSerializer,
  stringSerializer,
} from '@platformatic/kafka'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.js'
import type {
  KafkaDependencies,
  SupportedMessageValuesInput,
  SupportedMessageValuesInputForTopic,
  SupportedTopics,
  TopicConfig,
} from './types.js'

const INIT_TIMEOUT = 2000 // 2 seconds

export type KafkaPublisherOptions<TopicsConfig extends TopicConfig[]> = BaseKafkaOptions &
  Omit<ProduceOptions<string, object, string, object>, 'serializers' | 'idempotent'> & {
    topicsConfig: TopicsConfig
  }

export type KafkaMessageOptions = Omit<
  MessageToProduce<string, object, string, object>,
  'topic' | 'value'
>

export abstract class AbstractKafkaPublisher<
  TopicsConfig extends TopicConfig[],
> extends AbstractKafkaService<TopicsConfig, KafkaPublisherOptions<TopicsConfig>> {
  private readonly topicsConfig: TopicsConfig
  private readonly schemaContainers: Record<
    string,
    MessageSchemaContainer<SupportedMessageValuesInput<TopicsConfig>>
  >

  private readonly producer: Producer<string, object, string, object>
  private isInitiated: boolean

  constructor(dependencies: KafkaDependencies, options: KafkaPublisherOptions<TopicsConfig>) {
    super(dependencies, options)
    this.isInitiated = false

    this.topicsConfig = options.topicsConfig
    if (this.topicsConfig.length === 0) throw new Error('At least one topic must be defined')

    this.schemaContainers = {}
    for (const { topic, schemas } of this.topicsConfig) {
      this.schemaContainers[topic] = new MessageSchemaContainer({
        messageSchemas: schemas,
        messageTypeField: this.options.messageTypeField,
        messageDefinitions: [],
      })
    }

    this.producer = new Producer({
      ...this.options.kafka,
      ...this.options,
      idempotent: true,
      serializers: {
        key: stringSerializer,
        value: jsonSerializer,
        headerKey: stringSerializer,
        headerValue: jsonSerializer,
      },
    })
  }

  async init(): Promise<void> {
    if (this.isInitiated) return

    let timeoutId: NodeJS.Timeout | undefined

    const initPromise = this.producer.initIdempotentProducer({ ...this.options })
    const timeoutPromise = new Promise<void>((_, reject) => {
      timeoutId = setTimeout(() => reject(new Error('Producer init timed out')), INIT_TIMEOUT)
    })

    try {
      await Promise.race([initPromise, timeoutPromise])
      this.isInitiated = true
    } finally {
      clearTimeout(timeoutId)
    }
  }

  async close(): Promise<void> {
    if (!this.isInitiated) return

    await this.producer.close()
    this.isInitiated = false
  }

  async publish<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
    message: SupportedMessageValuesInputForTopic<TopicsConfig, Topic>,
    options?: KafkaMessageOptions,
  ): Promise<void> {
    const schemaResult = this.schemaContainers[topic]?.resolveSchema(message)
    if (!schemaResult) throw new Error(`Message schemas not found for topic: ${topic}`)
    if (schemaResult.error) throw schemaResult.error

    await this.init() // lazy init

    try {
      const parsedMessage = schemaResult.result.parse(message)

      // biome-ignore lint/style/noNonNullAssertion: Should always exist due to lazy init
      await this.producer!.send({ messages: [{ ...options, topic, value: parsedMessage }] })

      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'published' },
        topic,
      })
    } catch (error) {
      const errorDetails = {
        topic,
        publisher: this.constructor.name,
        message: stringValueSerializer(message),
      }
      this.handlerError(error, errorDetails)
      throw new InternalError({
        message: `Error while publishing to Kafka: ${(error as Error).message}`,
        errorCode: 'KAFKA_PUBLISH_ERROR',
        cause: error,
        details: errorDetails,
      })
    }
  }
}
