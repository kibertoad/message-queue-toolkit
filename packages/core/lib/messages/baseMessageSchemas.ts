import z from 'zod'

// Core fields that describe either internal event or external message
export const BASE_MESSAGE_SCHEMA = z.object({
  id: z.string().describe('event unique identifier'),
  timestamp: z.string().datetime().describe('iso 8601 datetime'),
  type: z.literal<string>('<replace.me>').describe('event type name'),
  payload: z.optional(z.object({})).describe('event payload based on type'),
})

// Extra fields that are optional for the event processing
// For internal domain events that did not originate within a message chain these fields can be omitted, producer should assume it is initiating a new chain then
export const EXTENDED_MESSAGE_SCHEMA = BASE_MESSAGE_SCHEMA.extend({
  metadata: z
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
      correlationId: z
        .string()
        .describe('unique identifier passed to all events in workflow chain'),
    })
    .optional()
    .describe('external message metadata'),
})

export type BaseMessageType = z.infer<typeof BASE_MESSAGE_SCHEMA>
