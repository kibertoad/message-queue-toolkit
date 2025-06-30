import type { QueueDependencies } from '@message-queue-toolkit/core'
import type { ConnectionOptions } from '@platformatic/kafka'
import type { ZodSchema } from 'zod'
import type z from 'zod/v3'

export type KafkaDependencies = Omit<QueueDependencies, 'messageMetricsManager'>

export type KafkaConfig = {
  bootstrapBrokers: string[]
  clientId: string
} & ConnectionOptions

export type TopicConfig<Topic extends string = string> = {
  topic: Topic
  schemas: ZodSchema[]
}

export type SupportedTopics<TopicsConfig extends TopicConfig[]> = TopicsConfig[number]['topic']

type MessageSchemasForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = Extract<TopicsConfig[number], { topic: Topic }>['schemas'][number]
export type SupportedMessageValuesInputForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = z.input<MessageSchemasForTopic<TopicsConfig, Topic>>
export type SupportedMessageValuesForTopic<
  TopicsConfig extends TopicConfig[],
  Topic extends SupportedTopics<TopicsConfig>,
> = z.output<MessageSchemasForTopic<TopicsConfig, Topic>>

type MessageSchemas<TopicsConfig extends TopicConfig[]> = TopicsConfig[number]['schemas'][number]
export type SupportedMessageValuesInput<TopicsConfig extends TopicConfig[]> = z.input<
  MessageSchemas<TopicsConfig>
>
export type SupportedMessageValues<TopicsConfig extends TopicConfig[]> = z.input<
  MessageSchemas<TopicsConfig>
>
