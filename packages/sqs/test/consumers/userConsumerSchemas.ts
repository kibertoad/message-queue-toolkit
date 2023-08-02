import z from 'zod'

export const PERMISSIONS_MESSAGE_SCHEMA = z.object({
  messageType: z.enum(['add', 'remove']),
  userIds: z.array(z.number()).describe('User IDs'),
  permissions: z.array(z.string()).nonempty().describe('List of user permissions'),
})

export const PERMISSIONS_ADD_MESSAGE_SCHEMA = z.object({
  messageType: z.literal('add'),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA = z.object({
  messageType: z.literal('remove'),
})

export const OTHER_MESSAGE_SCHEMA = z.object({
  dummy: z.literal('dummy'),
})

export type PERMISSIONS_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_MESSAGE_SCHEMA>

export type PERMISSIONS_ADD_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA>
export type OTHER_MESSAGE_SCHEMA_TYPE = z.infer<typeof OTHER_MESSAGE_SCHEMA>
