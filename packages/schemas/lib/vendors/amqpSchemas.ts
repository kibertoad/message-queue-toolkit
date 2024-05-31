import type { CommonEventDefinition } from '../events/eventTypes'

export type AmqpAwareEventDefinition = {
  schemaVersion?: string
  exchange?: string // optional if used with a direct exchange
  queueName?: string // should only be specified for direct exchanges
  topic?: string // used for topic exchanges
} & CommonEventDefinition
