import type {
  SupportedMessageValues,
  SupportedMessageValuesForTopic,
  SupportedTopics,
  TopicConfig,
} from '../types.ts'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'
import type { KafkaHandlerRouting } from './KafkaHandlerRoutingBuilder.ts'

const DEFAULT_HANDLER_KEY = Symbol('default-handler')

type Handlers<TopicsConfig extends TopicConfig[]> = Record<
  string,
  Record<string | symbol, KafkaHandlerConfig<SupportedMessageValues<TopicsConfig>>>
>

export class KafkaHandlerContainer<TopicsConfig extends TopicConfig[]> {
  private readonly handlers: Handlers<TopicsConfig>
  private readonly messageTypeField?: string

  constructor(topicHandlers: KafkaHandlerRouting<TopicsConfig>, messageTypeField?: string) {
    this.messageTypeField = messageTypeField
    this.handlers = this.mapTopicHandlers(topicHandlers)
  }

  private mapTopicHandlers(
    topicHandlerRouting: KafkaHandlerRouting<TopicsConfig>,
  ): Handlers<TopicsConfig> {
    const result: Handlers<TopicsConfig> = {}

    for (const [topic, topicHandlers] of Object.entries(topicHandlerRouting)) {
      if (!topicHandlers.length) continue
      result[topic] = {}

      for (const handler of topicHandlers) {
        let handlerKey = this.messageTypeField
          ? // @ts-ignore
            handler.schema.shape[this.messageTypeField]?.value
          : undefined
        handlerKey ??= DEFAULT_HANDLER_KEY
        if (result[topic][handlerKey]) {
          throw new Error(`Duplicate handler for topic ${topic}`)
        }

        result[topic][handlerKey] = handler
      }
    }

    return result
  }

  resolveHandler<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
    messageValue: SupportedMessageValuesForTopic<TopicsConfig, Topic>,
  ): KafkaHandlerConfig<SupportedMessageValuesForTopic<TopicsConfig, Topic>> | undefined {
    const handlers = this.handlers[topic]
    if (!handlers) return undefined

    let messageValueType: string | undefined = undefined
    if (this.messageTypeField) messageValueType = messageValue[this.messageTypeField]

    return messageValueType
      ? (handlers[messageValueType] ?? handlers[DEFAULT_HANDLER_KEY])
      : handlers[DEFAULT_HANDLER_KEY]
  }

  get topics(): SupportedTopics<TopicsConfig>[] {
    return Object.keys(this.handlers)
  }
}
