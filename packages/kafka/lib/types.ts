import type { CommonLogger } from '@lokalise/node-core'
import type { QueueDependencies } from '@message-queue-toolkit/core'
import type {ConnectionOptions, Message} from '@platformatic/kafka'
import type { ZodSchema, z } from 'zod/v4'

export interface RequestContext {
  logger: CommonLogger
  reqId: string
}

export type KafkaDependencies = QueueDependencies

export type KafkaConfig = {
  bootstrapBrokers: string[]
  clientId: string
} & ConnectionOptions

export type TopicConfig<Topic extends string = string> = {
  topic: Topic
  schema: ZodSchema<object>
}

export type SupportedTopics<TopicsConfig extends TopicConfig[]> = TopicsConfig[number]['topic']

type MessageSchemasForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = Extract<TopicsConfig[number], { topic: Topic }>['schema']
export type SupportedMessageValuesInputForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = z.input<MessageSchemasForTopic<TopicsConfig, Topic>>
export type SupportedMessageValuesForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = z.output<MessageSchemasForTopic<TopicsConfig, Topic>>

type MessageSchemas<TopicsConfig extends TopicConfig[]> = TopicsConfig[number]['schema']
export type SupportedMessageValuesInput<TopicsConfig extends TopicConfig[]> = z.input<
  MessageSchemas<TopicsConfig>
>
export type SupportedMessageValues<TopicsConfig extends TopicConfig[]> = z.output<
  MessageSchemas<TopicsConfig>
>

export type DeserializedMessage<TopicsConfig extends TopicConfig[]> = Message<string, SupportedMessageValues<TopicsConfig>, string, string>
