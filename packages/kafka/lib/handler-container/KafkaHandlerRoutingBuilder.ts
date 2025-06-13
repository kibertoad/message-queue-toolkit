import type {
  SupportedMessageValues,
  SupportedMessageValuesForTopic,
  SupportedTopics,
  TopicConfig,
} from '../types.ts'
import type { KafkaHandlerConfig } from './KafkaHandlerConfig.ts'

export type KafkaHandlerRouting<
  TopicsConfig extends TopicConfig[],
  ExecutionContext,
  MessageValue extends SupportedMessageValues<TopicsConfig> = SupportedMessageValues<TopicsConfig>,
> = Record<string, KafkaHandlerConfig<MessageValue, ExecutionContext>[]>

export class KafkaHandlerRoutingBuilder<
  const TopicsConfig extends TopicConfig[],
  ExecutionContext,
> {
  private readonly configs: KafkaHandlerRouting<TopicsConfig, ExecutionContext> = {}

  addConfig<
    Topic extends SupportedTopics<TopicsConfig>,
    MessageValue extends SupportedMessageValuesForTopic<TopicsConfig, Topic>,
  >(topic: Topic, config: KafkaHandlerConfig<MessageValue, ExecutionContext>): this {
    this.configs[topic] ??= []
    this.configs[topic].push(config)

    return this
  }

  build(): KafkaHandlerRouting<TopicsConfig, ExecutionContext> {
    return this.configs
  }
}
