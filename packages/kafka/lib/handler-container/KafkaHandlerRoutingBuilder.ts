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
  BatchProcessingEnabled extends boolean,
> = Partial<{
  [Topic in SupportedTopics<TopicsConfig>]: KafkaHandlerConfig<
    SupportedMessageValuesForTopic<TopicsConfig, Topic>,
    ExecutionContext,
    BatchProcessingEnabled
  >
}>

export class KafkaHandlerRoutingBuilder<
  const TopicsConfig extends TopicConfig[],
  ExecutionContext,
  BatchProcessingEnabled extends boolean,
> {
  private readonly configs: KafkaHandlerRouting<
    TopicsConfig,
    ExecutionContext,
    BatchProcessingEnabled
  > = {}

  addConfig<
    Topic extends SupportedTopics<TopicsConfig>,
    MessageValue extends SupportedMessageValuesForTopic<TopicsConfig, Topic>,
  >(
    topic: Topic,
    config: KafkaHandlerConfig<MessageValue, ExecutionContext, BatchProcessingEnabled>,
  ): this {
    this.configs[topic] = config as KafkaHandlerConfig<
      SupportedMessageValues<TopicsConfig>,
      ExecutionContext,
      BatchProcessingEnabled
    >

    return this
  }

  build(): KafkaHandlerRouting<TopicsConfig, ExecutionContext, BatchProcessingEnabled> {
    return this.configs
  }
}
