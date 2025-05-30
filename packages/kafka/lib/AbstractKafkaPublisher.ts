import { InternalError, stringValueSerializer } from '@lokalise/node-core'
import { MessageSchemaContainer } from '@message-queue-toolkit/core'
import {
  type MessageToProduce,
  type ProduceOptions,
  Producer,
  jsonSerializer,
  stringSerializer,
} from '@platformatic/kafka'
import type { ZodSchema } from 'zod'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.js'
import type {
  KafkaDependencies,
  SupportedMessageValuesInput,
  SupportedMessageValuesInputForTopic,
  SupportedTopics,
  TopicConfig,
} from './types.js'

export type KafkaPublisherOptions<TopicsConfig extends TopicConfig[]> = {
  topicsConfig: TopicsConfig
} & BaseKafkaOptions<SupportedTopics<TopicsConfig>> &
  Omit<ProduceOptions<string, object, string, object>, 'autocreateTopics' | 'serializers'>

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
> extends AbstractKafkaService<
  SupportedTopics<TopicsConfig>,
  SupportedMessageValuesInput<TopicsConfig>,
  KafkaPublisherOptions<TopicsConfig>
> {
  private readonly schemaContainers: Record<
    string,
    MessageSchemaContainer<SupportedMessageValuesInput<TopicsConfig>>
  >
  private producer?: Producer<string, object, string, object>

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
      autocreateTopics: this.autocreateTopics,
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
    messages:
      | KafkaMessageToPublish<TopicsConfig, Topic>
      | KafkaMessageToPublish<TopicsConfig, Topic>[],
    options?: KafkaMessageOptions,
  ): Promise<void> {
    const messagesArray = Array.isArray(messages) ? messages : [messages]
    if (!messagesArray.length) return Promise.resolve()
    const schemaPerMessage = this.getSchemasPerMessage(messagesArray)

    await this.init() // lazy init

    try {
      const messagesToSend = messagesArray.map(({ topic, message }) => ({
        ...options,
        topic,
        value: schemaPerMessage[topic].parse(message),
      }))

      // biome-ignore lint/style/noNonNullAssertion: Should always exist due to lazy init
      await this.producer!.send({ messages: messagesToSend })

      for (const { topic, value } of messagesToSend) {
        this.handleMessageProcessed({
          message: value,
          processingResult: { status: 'published' },
          topic,
        })
      }
    } catch (error) {
      const errorDetails = {
        topics: this.topics,
        publisher: this.constructor.name,
        messages: stringValueSerializer(messages),
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

  private getSchemasPerMessage<Topic extends SupportedTopics<TopicsConfig>>(
    messages: KafkaMessageToPublish<TopicsConfig, Topic>[],
  ) {
    const schemaPerMessage = {} as Record<Topic, ZodSchema>

    for (const { topic, message } of messages) {
      const messageSchemaResult = this.schemaContainers[topic]?.resolveSchema(message)
      if (!messageSchemaResult) throw new Error(`Message schemas not found for topic: ${topic}`)
      if (messageSchemaResult.error) throw messageSchemaResult.error

      schemaPerMessage[topic] = messageSchemaResult.result
    }

    return schemaPerMessage
  }
}
