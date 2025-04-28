import type { CommonEventDefinition } from '../events/eventTypes.ts'

export type SnsAwareEventDefinition = {
  schemaVersion?: string
  snsTopic?: string
} & CommonEventDefinition
