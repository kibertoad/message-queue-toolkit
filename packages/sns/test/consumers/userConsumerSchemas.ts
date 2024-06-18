import z from 'zod'

export const PERMISSIONS_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.enum(['add', 'remove']),
  permissions: z.array(z.string()).nonempty().describe('List of user permissions'),
  timestamp: z.string().optional(),
})

export const PERMISSIONS_ADD_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  preHandlerIncrement: z.optional(z.number()),
  messageType: z.literal('add'),
  timestamp: z.string().optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  preHandlerIncrement: z.optional(z.number()),
  messageType: z.literal('remove'),
  timestamp: z.string().optional(),
})

export type PERMISSIONS_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_MESSAGE_SCHEMA>

export type PERMISSIONS_ADD_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA>
