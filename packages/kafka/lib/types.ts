import type { QueueDependencies } from '@message-queue-toolkit/core'
import type { ConnectionOptions } from '@platformatic/kafka'

export type KafkaConfig = {
  bootstrapBrokers: string[]
  clientId: string
} & ConnectionOptions

export type KafkaTopicCreatorLocator = { topic: string }

export type KafkaDependencies = Omit<QueueDependencies, 'messageMetricsManager'>
