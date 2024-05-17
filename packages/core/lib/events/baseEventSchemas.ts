import { z } from 'zod'

// Core fields that describe either internal event or external message
export const BASE_EVENT_SCHEMA = z.object({
  id: z.string().describe('event unique identifier'),
  timestamp: z.date().describe('event creation date'),
  type: z.literal<string>('<replace.me>').describe('event type name'),
  payload: z.optional(z.object({})).describe('event payload based on type'),
})

export type BaseEventType = z.infer<typeof BASE_EVENT_SCHEMA>
