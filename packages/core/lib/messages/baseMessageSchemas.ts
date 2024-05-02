import z from 'zod'

export const BASE_MESSAGE_SCHEMA = z.object({
  id: z.string().describe('event unique identifier'),
  type: z.literal<string>('<replace.me>').describe('event type name'),
  payload: z.optional(z.object({})).describe('event payload based on type'),
})

export const EXTENDED_MESSAGE_SCHEMA = BASE_MESSAGE_SCHEMA.extend({
  metadata: z
    .object({
      schemaVersion: z.string().min(1).describe('base event schema version'),
      timestamp: z.string().datetime().describe('iso 8601 datetime'),
      producerApp: z.string().min(1).describe('app/service that produced the event'),
      originApp: z.string().min(1).describe('app/service that initiated the workflow'),
      correlationId: z
        .string()
        .describe('unique identifier passed to all events in workflow chain'),
    })
    .describe('event metadata'),
})

export type BaseMessageType = z.infer<typeof BASE_MESSAGE_SCHEMA>
