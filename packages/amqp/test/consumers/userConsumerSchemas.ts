import z from 'zod/v3'

export const PERMISSIONS_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.enum(['add', 'remove']),
  userIds: z.array(z.number()).describe('User IDs'),
  permissions: z.array(z.string()).nonempty().describe('List of user permissions'),
  timestamp: z.string().or(z.date()).optional(),
})

export const PERMISSIONS_ADD_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('add'),
  timestamp: z.string().or(z.date()).optional(),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('remove'),
  timestamp: z.string().or(z.date()).optional(),
})

export type PERMISSIONS_MESSAGE_TYPE = z.output<typeof PERMISSIONS_MESSAGE_SCHEMA>

export type PERMISSIONS_ADD_MESSAGE_TYPE = z.output<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE = z.output<typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA>
