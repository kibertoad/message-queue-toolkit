import type {
  SupportedMessageValues,
  SupportedMessageValuesForTopic,
  SupportedTopics,
  TopicConfig,
} from '../types.ts'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'
import type { KafkaHandlerRouting } from './KafkaHandlerRoutingBuilder.ts'

const DEFAULT_HANDLER_KEY = Symbol('default-handler')

type Handlers<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean,
> = Record<
  string,
  Record<
    string | symbol,
    KafkaHandlerConfig<
      SupportedMessageValues<TopicsConfig>,
      ExecutionContext,
      BatchProcessingEnabled
    >
  >
>

// TODO simplify - since we don't support messageTypeField anymore, we can use Record<topic, handler> structure
export class KafkaHandlerContainer<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean,
> {
  private readonly handlers: Handlers<TopicsConfig, ExecutionContext, BatchProcessingEnabled>

  constructor(
    topicHandlers: KafkaHandlerRouting<TopicsConfig, ExecutionContext, BatchProcessingEnabled>,
  ) {
    this.handlers = this.mapTopicHandlers(topicHandlers)
  }

  private mapTopicHandlers(
    topicHandlerRouting: KafkaHandlerRouting<
      TopicsConfig,
      ExecutionContext,
      BatchProcessingEnabled
    >,
  ): Handlers<TopicsConfig, ExecutionContext, BatchProcessingEnabled> {
    const result: Handlers<TopicsConfig, ExecutionContext, BatchProcessingEnabled> = {}

    for (const [topic, topicHandlers] of Object.entries(topicHandlerRouting)) {
      if (!topicHandlers.length) continue
      result[topic] = {}

      for (const handler of topicHandlers) {
        if (result[topic]?.[DEFAULT_HANDLER_KEY]) {
          throw new Error(`Duplicate handler for topic ${topic}`)
        }
        result[topic] = {
          [DEFAULT_HANDLER_KEY]: handler,
        }
      }
    }

    return result
  }

  resolveHandler<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
  ):
    | KafkaHandlerConfig<
        SupportedMessageValuesForTopic<TopicsConfig, Topic>,
        ExecutionContext,
        BatchProcessingEnabled
      >
    | undefined {
    const handlers = this.handlers[topic]
    if (!handlers) return undefined

    return handlers[DEFAULT_HANDLER_KEY]
  }

  get topics(): SupportedTopics<TopicsConfig>[] {
    return Object.keys(this.handlers)
  }
}
