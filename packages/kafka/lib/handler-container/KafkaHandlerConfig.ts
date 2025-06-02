import type { ZodSchema } from 'zod'
import type { SupportedMessageValuesInputForTopic, SupportedTopics, TopicConfig } from '../types.js'

export type KafkaHandler<TopicsConfig extends TopicConfig[], Topic extends string> = (
  message: SupportedMessageValuesInputForTopic<TopicsConfig, Topic>,
) => Promise<void>

export class KafkaHandlerConfig<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> {
  public readonly schema: ZodSchema<SupportedMessageValuesInputForTopic<TopicsConfig, Topic>>
  public readonly handler: KafkaHandler<TopicsConfig, Topic>

  constructor(
    schema: ZodSchema<SupportedMessageValuesInputForTopic<TopicsConfig, Topic>>,
    handler: KafkaHandler<TopicsConfig, Topic>,
  ) {
    this.schema = schema
    this.handler = handler
  }
}
