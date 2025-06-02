import type { SupportedMessageValuesForTopic, SupportedTopics, TopicConfig } from '../types.js'
import type { KafkaHandler, KafkaTopicHandlerConfig } from './KafkaTopicHandlerConfig.js'

const DEFAULT_HANDLER_KEY = Symbol('default-handler')

type Handlers<TopicsConfig extends TopicConfig[]> = Record<
  string,
  Record<string | symbol, KafkaHandler<TopicsConfig, SupportedTopics<TopicsConfig>>>
>

export class KafkaHandlerContainer<TopicsConfig extends TopicConfig[]> {
  private readonly handlers: Handlers<TopicsConfig>
  private readonly messageTypeField?: string

  constructor(
    topicHandlers: Record<
      string,
      | KafkaTopicHandlerConfig<TopicsConfig, SupportedTopics<TopicsConfig>>
      | KafkaTopicHandlerConfig<TopicsConfig, SupportedTopics<TopicsConfig>>[]
    >,
    messageTypeField?: string,
  ) {
    this.handlers = this.mapTopicHandlers(topicHandlers)
    this.messageTypeField = messageTypeField
  }

  private mapTopicHandlers(
    topicHandlers: Record<
      string,
      | KafkaTopicHandlerConfig<TopicsConfig, SupportedTopics<TopicsConfig>>
      | KafkaTopicHandlerConfig<TopicsConfig, SupportedTopics<TopicsConfig>>[]
    >,
  ): Handlers<TopicsConfig> {
    const result: Handlers<TopicsConfig> = {}
    for (const [topic, handlers] of Object.entries(topicHandlers)) {
      result[topic] = {}

      for (const { schema, handler } of Array.isArray(handlers) ? handlers : [handlers]) {
        let handlerKey = this.messageTypeField
          ? // @ts-ignore
            schema.shape[this.messageTypeField]?.value
          : undefined
        handlerKey ??= DEFAULT_HANDLER_KEY
        if (result[topic][handlerKey]) {
          throw new Error(`Duplicate handler key "${handlerKey}" for topic "${topic}"`)
        }

        result[topic][handlerKey] = handler
      }
    }

    return result
  }

  resolveHandler<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
    messageValue: SupportedMessageValuesForTopic<TopicsConfig, Topic>,
  ): KafkaHandler<TopicsConfig, Topic> | undefined {
    const handlers = this.handlers[topic]
    if (!handlers) return undefined

    let messageValueType = undefined
    if (this.messageTypeField) messageValueType = messageValue[this.messageTypeField]

    const handlerKey = messageValueType ?? DEFAULT_HANDLER_KEY
    return handlers[handlerKey]
  }
}
