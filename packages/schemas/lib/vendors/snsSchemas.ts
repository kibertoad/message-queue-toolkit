import type { CommonEventDefinition } from '../events/eventTypes'

export type SnsAwareEventDefinition = {
  schemaVersion?: string
  snsTopic?: string
} & CommonEventDefinition
