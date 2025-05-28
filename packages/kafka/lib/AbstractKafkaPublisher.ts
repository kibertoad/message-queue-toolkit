import { InternalError, stringValueSerializer } from '@lokalise/node-core'
import { type AsyncPublisher, MessageSchemaContainer } from '@message-queue-toolkit/core'
import {
  type MessageToProduce,
  type ProduceOptions,
  Producer,
  jsonSerializer,
  stringSerializer,
} from '@platformatic/kafka'
import type { ZodSchema } from 'zod'
import { AbstractKafkaService, type BaseKafkaOptions } from './AbstractKafkaService.js'
import type { KafkaDependencies } from './types.js'

export type KafkaPublisherOptions<MessagePayload extends object> = BaseKafkaOptions & {
  messageSchemas: readonly ZodSchema<MessagePayload>[]
} & Omit<ProduceOptions<string, object, string, object>, 'autocreateTopics' | 'serializers'>

export type KafkaMessageOptions = Omit<
  MessageToProduce<string, object, string, object>,
  'topic' | 'value'
>

export abstract class AbstractKafkaPublisher<MessagePayload extends object>
  extends AbstractKafkaService<MessagePayload, KafkaPublisherOptions<MessagePayload>>
  implements AsyncPublisher<MessagePayload, KafkaMessageOptions>
{
  private readonly messageSchemaContainer: MessageSchemaContainer<MessagePayload>
  private readonly topic: string
  private readonly autocreateTopics: boolean
  private producer?: Producer<string, object, string, object>

  constructor(dependencies: KafkaDependencies, options: KafkaPublisherOptions<MessagePayload>) {
    super(dependencies, options)

    const topic = this.options.creationConfig?.topic ?? this.options.locatorConfig?.topic
    if (!topic) throw new Error('Topic must be defined in creationConfig or locatorConfig')

    this.topic = topic
    this.autocreateTopics = !!this.options.creationConfig
    this.messageSchemaContainer = new MessageSchemaContainer<MessagePayload>({
      messageSchemas: this.options.messageSchemas,
      messageDefinitions: [],
      messageTypeField: this.options.messageTypeField,
    })
  }

  init(): Promise<void> {
    if (!this.producer) return Promise.resolve()

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

  async publish(message: MessagePayload, options: KafkaMessageOptions): Promise<void> {
    const messageSchemaResult = this.messageSchemaContainer.resolveSchema(message)
    if (messageSchemaResult.error) throw messageSchemaResult.error

    await this.init() // lazy initialization

    try {
      const parsedMessage = messageSchemaResult.result.parse(message)

      if (this.options.logMessages) {
        this.logger.debug(
          {
            type: this.resolveMessageType(parsedMessage),
            message: stringValueSerializer(parsedMessage),
            topic: this.topic,
          },
          'Kafka emitting message',
        )
      }

      await this.producer?.send({
        messages: [{ ...options, topic: this.topic, value: parsedMessage }],
      })

      this.handleMessageProcessed({
        message: parsedMessage,
        processingResult: { status: 'published' },
        topicName: this.topic,
      })
    } catch (e) {
      const error = e as Error
      throw new InternalError({
        message: `Error while publishing to Kafka: ${error.message}`,
        errorCode: 'KAFKA_PUBLISH_ERROR',
        cause: error,
        details: {
          topic: this.topic,
          publisher: this.constructor.name,
          message: stringValueSerializer(message),
        },
      })
    }
  }
}
