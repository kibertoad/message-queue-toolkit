import z from 'zod'

// Core fields that describe event
export const BASE_MESSAGE_SCHEMA = z.object({
  id: z.string().describe('event unique identifier'),
  timestamp: z.string().datetime().describe('iso 8601 datetime'),
  type: z.literal<string>('<replace.me>').describe('event type name'),
  payload: z.optional(z.object({})).describe('event payload based on type'),
})

// Extra fields that are optional for the event processing
export const EXTENDED_MESSAGE_SCHEMA = BASE_MESSAGE_SCHEMA.extend({
  metadata: z
    .object({
      schemaVersion: z.string().min(1).describe('message schema version'),
      producedBy: z.string().min(1).describe('app/service that produced the message'),
      originatedFrom: z
        .string()
        .min(1)
        .describe('app/service that initiated entire workflow that led to creating this message'),
      correlationId: z
        .string()
        .describe('unique identifier passed to all events in workflow chain'),
    })
    .describe('event metadata'),
})

export type BaseMessageType = z.infer<typeof BASE_MESSAGE_SCHEMA>
