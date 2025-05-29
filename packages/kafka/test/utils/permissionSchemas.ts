import z from 'zod/v3'

const BASE_SCHEMA = z.object({
  id: z.string(),
  permissions: z.array(z.string()).nonempty().describe('List of user permissions'),
})

export const PERMISSION_ADDED_SCHEMA = BASE_SCHEMA.extend({
  type: z.literal('added')
})
export type PermissionAdded = z.infer<typeof PERMISSION_ADDED_SCHEMA>


export const PERMISSION_REMOVED_SCHEMA = BASE_SCHEMA.extend({
  type: z.literal('removed')
})
export type PermissionRemoved = z.infer<typeof PERMISSION_REMOVED_SCHEMA>