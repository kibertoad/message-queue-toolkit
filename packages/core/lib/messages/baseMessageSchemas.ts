import z from 'zod'

import { BASE_EVENT_SCHEMA } from '../events/baseEventSchemas'

// External message metadata that describe the context in which the message originated
export const EXTERNAL_MESSAGE_METADATA_SCHEMA = z
  .object({
    schemaVersion: z.string().min(1).describe('message schema version'),
    // this is always set to a service that created the message
    producedBy: z.string().min(1).describe('app/service that produced the message'),
    // this is always propagated within the message chain. For the first message in the chain it is equal to "producedBy"
    originatedFrom: z
      .string()
      .min(1)
      .describe('app/service that initiated entire workflow that led to creating this message'),
    // this is always propagated within the message chain.
    correlationId: z.string().describe('unique identifier passed to all events in workflow chain'),
  })
  .describe('external message metadata')

export const BASE_MESSAGE_SCHEMA = BASE_EVENT_SCHEMA.extend({
  // For internal domain events that did not originate within a message chain metadata field can be omitted, producer should then assume it is initiating a new chain
  metadata: EXTERNAL_MESSAGE_METADATA_SCHEMA.optional(),
})

export type BaseMessageType = z.infer<typeof BASE_MESSAGE_SCHEMA>
