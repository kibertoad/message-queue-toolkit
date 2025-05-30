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

export type KafkaPublisherOptions<TopicsConfig extends TopicConfig[]> =
  BaseKafkaOptions<TopicsConfig> &
    Omit<ProduceOptions<string, object, string, object>, 'serializers'>

export type KafkaMessageOptions = Omit<
  MessageToProduce<string, object, string, object>,
  'topic' | 'value'
>

export type KafkaMessageToPublish<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = {
  topic: Topic
  message: SupportedMessageValuesInputForTopic<TopicsConfig, Topic>
}

export abstract class AbstractKafkaPublisher<
  TopicsConfig extends TopicConfig[],
> extends AbstractKafkaService<TopicsConfig, KafkaPublisherOptions<TopicsConfig>> {
  private producer?: Producer<string, object, string, object>
  private readonly schemaContainers: Record<
    string,
    MessageSchemaContainer<SupportedMessageValuesInput<TopicsConfig>>
  >

  constructor(dependencies: KafkaDependencies, options: KafkaPublisherOptions<TopicsConfig>) {
    super(dependencies, options)

    this.schemaContainers = {}
    for (const { topic, schemas } of options.topicsConfig) {
      this.schemaContainers[topic] = new MessageSchemaContainer({
        messageSchemas: schemas,
        messageDefinitions: [],
        messageTypeField: this.options.messageTypeField,
      })
    }
  }

  init(): Promise<void> {
    if (this.producer) return Promise.resolve()

    this.producer = new Producer({
      ...this.options.kafka,
      ...this.options,
      serializers: {
        key: stringSerializer,
        value: jsonSerializer,
        headerKey: stringSerializer,
        headerValue: jsonSerializer,
      },
    })

    return Promise.resolve()
  }

  override async close(): Promise<void> {
    await this.producer?.close()
    this.producer = undefined
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
