import type { SupportedTopics, TopicConfig } from '../types.js'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.js'

export type KafkaHandlerRouting<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig> = SupportedTopics<TopicsConfig>,
> = Record<string, KafkaHandlerConfig<TopicsConfig, Topic>[]>

// export class KafkaHandlerRoutingBuilder {}
