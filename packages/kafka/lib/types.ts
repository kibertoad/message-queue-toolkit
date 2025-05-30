import type { QueueDependencies } from '@message-queue-toolkit/core'
import type { ConnectionOptions } from '@platformatic/kafka'
import type { ZodSchema } from 'zod'
import type z from 'zod/v3'

export type KafkaConfig = {
  bootstrapBrokers: string[]
  clientId: string
} & ConnectionOptions

export type KafkaTopicCreatorLocator<Topic extends string> =
  | { topic: Topic; topics?: never }
  | { topic?: never; topics: readonly Topic[] }

export type KafkaDependencies = Omit<QueueDependencies, 'messageMetricsManager'>

export type TopicMessagesConfig<Topic extends string = string> = Record<Topic, ZodSchema[]>

export type ExtractTopics<T> = Extract<keyof T, string>

export type ExtractMessagePayloadsForTopic<
  TopicMessages extends TopicMessagesConfig,
  Topic extends ExtractTopics<TopicMessages>,
> = z.infer<TopicMessages[Topic][number]>

export type ExtractMessagePayloads<TopicMessages extends TopicMessagesConfig> = {
  [Topic in ExtractTopics<TopicMessages>]: ExtractMessagePayloadsForTopic<TopicMessages, Topic>
}
