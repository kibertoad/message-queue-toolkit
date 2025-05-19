import type { CommonEventDefinition } from '../events/eventTypes.ts'

export type SnsAwareEventDefinition = {
  snsTopic?: string
} & CommonEventDefinition
