import type {
  SupportedMessageValues,
  SupportedMessageValuesForTopic,
  SupportedTopics,
  TopicConfig,
} from '../types.ts'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'

export type KafkaHandlerRouting<
  TopicsConfig extends TopicConfig[],
  MessageValue extends SupportedMessageValues<TopicsConfig> = SupportedMessageValues<TopicsConfig>,
> = Record<string, KafkaHandlerConfig<MessageValue>[]>

export class KafkaHandlerRoutingBuilder<TopicsConfig extends TopicConfig[]> {
  private readonly configs: KafkaHandlerRouting<TopicsConfig> = {}

  addConfig<
    Topic extends SupportedTopics<TopicsConfig>,
    MessageValue extends SupportedMessageValuesForTopic<TopicsConfig, Topic>,
  >(topic: Topic, config: KafkaHandlerConfig<MessageValue>): this {
    this.configs[topic] ??= []
    this.configs[topic].push(config)

    return this
  }

  build(): KafkaHandlerRouting<TopicsConfig> {
    return this.configs
  }
}
