import { z } from 'zod/v4'

export const PERMISSIONS_ADD_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('add'),
  timestamp: z.string().datetime(),
  userIds: z.array(z.string()).optional(),
  metadata: z.object({ largeField: z.string() }).optional(),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('remove'),
  timestamp: z.string().datetime(),
  userIds: z.array(z.string()),
})

export type PERMISSIONS_ADD_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA>
