import type { SupportedTopics, TopicConfig } from '../types.js'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.js'

export type KafkaHandlerRouting<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig> = SupportedTopics<TopicsConfig>,
> = Record<string, KafkaHandlerConfig<TopicsConfig, Topic>[]>

export class KafkaHandlerRoutingBuilder<TopicsConfig extends TopicConfig[]> {
  private readonly configs: KafkaHandlerRouting<TopicsConfig> = {}

  addConfig<Topic extends SupportedTopics<TopicsConfig>>(
    topic: Topic,
    config: KafkaHandlerConfig<TopicsConfig, Topic>,
  ): this {
    this.configs[topic] ??= []
    this.configs[topic].push(config)

    return this
  }

  build(): KafkaHandlerRouting<TopicsConfig> {
    return this.configs
  }
}
