import type { CommonEventDefinition } from '@message-queue-toolkit/core'

export type PubSubAwareEventDefinition = {
  pubSubTopic?: string
} & CommonEventDefinition
