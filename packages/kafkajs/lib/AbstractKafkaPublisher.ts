import { InternalError, stringValueSerializer } from '@lokalise/node-core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import { Kafka, type Producer, type ProducerConfig, type IHeaders } from 'kafkajs'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.ts'
import type {
  KafkaDependencies,
  RequestContext,
  SupportedMessageValuesForTopic,
  SupportedMessageValuesInputForTopic,
  SupportedTopics,
  TopicConfig,
} from './types.ts'

export type KafkaPublisherOptions<TopicsConfig extends TopicConfig[]> = BaseKafkaOptions & {
  topicsConfig: TopicsConfig
  autocreateTopics?: boolean
  producerConfig?: Omit<ProducerConfig, 'allowAutoTopicCreation'>
}

export type KafkaMessageOptions = {
  key?: string
  partition?: number
  headers?: IHeaders
}

export abstract class AbstractKafkaPublisher<
  TopicsConfig extends TopicConfig[],
> extends AbstractKafkaService<TopicsConfig, KafkaPublisherOptions<TopicsConfig>> {
  private readonly schemaContainers: Record<string, MessageSchemaContainer<object>>
  private readonly kafka: Kafka
  private readonly producer: Producer
  private isInitiated: boolean

  constructor(dependencies: KafkaDependencies, options: KafkaPublisherOptions<TopicsConfig>) {
    super(dependencies, options)
    this.isInitiated = false

    const topicsConfig = options.topicsConfig
    if (topicsConfig.length === 0) throw new Error('At least one topic must be defined')

    this.schemaContainers = {}
    for (const { topic, schema } of topicsConfig) {
      this.schemaContainers[topic] = new MessageSchemaContainer({
        messageSchemas: [{ schema }],
        messageDefinitions: [],
      })
    }

    this.kafka = new Kafka({
      clientId: this.options.kafka.clientId,
      brokers: this.options.kafka.brokers,
      ssl: this.options.kafka.ssl,
      sasl: this.options.kafka.sasl,
      connectionTimeout: this.options.kafka.connectionTimeout,
      requestTimeout: this.options.kafka.requestTimeout,
      retry: this.options.kafka.retry,
    })

    this.producer = this.kafka.producer({
      ...this.options.producerConfig,
      allowAutoTopicCreation: this.options.autocreateTopics ?? false,
    })
  }

  async init(): Promise<void> {
    if (this.isInitiated) return

    try {
      await this.producer.connect()
      this.isInitiated = true
    } catch (e) {
      throw new InternalError({
        message: 'Producer init failed',
        errorCode: 'KAFKA_PRODUCER_INIT_ERROR',
        cause: e,
      })
    }
  }

  async close(): Promise<void> {
    if (!this.isInitiated) return

    await this.producer.disconnect()
    this.isInitiated = false
  }

  async publish<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
    message: SupportedMessageValuesInputForTopic<TopicsConfig, Topic>,
    requestContext?: RequestContext,
    options?: KafkaMessageOptions,
  ): Promise<void> {
    const messageProcessingStartTimestamp = Date.now()

    const schemaResult = this.schemaContainers[topic]?.resolveSchema(message as object)
    if (!schemaResult) throw new Error(`Message schemas not found for topic: ${topic}`)
    if (schemaResult.error) throw schemaResult.error

    await this.init() // lazy init

    try {
      const parsedMessage = schemaResult.result.parse(message) as SupportedMessageValuesForTopic<
        TopicsConfig,
        Topic
      >

      const headers: IHeaders = {
        ...options?.headers,
        [this.resolveHeaderRequestIdField()]: requestContext?.reqId ?? '',
      }

      await this.producer.send({
        topic,
        messages: [
          {
            key: options?.key,
            value: JSON.stringify(parsedMessage),
            headers,
            partition: options?.partition,
          },
        ],
      })

      this.handleMessageProcessed({
        message: { topic, value: parsedMessage },
        processingResult: { status: 'published' },
        messageProcessingStartTimestamp,
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
