import { MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA } from '@message-queue-toolkit/schemas'
import z from 'zod'

export const PERMISSIONS_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.enum(['add', 'remove']),
  userIds: z.array(z.number()).describe('User IDs'),
  permissions: z.array(z.string()).nonempty().describe('List of user permissions'),
  timestamp: z.string().optional(),
  deduplicationId: z.string().optional(),
  deduplicationOptions: MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.optional(),
})

export const PERMISSIONS_ADD_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('add'),
  timestamp: z.string().optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
  deduplicationId: z.string().optional(),
  deduplicationOptions: MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.optional(),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA = z.object({
  id: z.string(),
  messageType: z.literal('remove'),
  timestamp: z.string().optional(),
  deduplicationId: z.string().optional(),
  deduplicationOptions: MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.optional(),
})

export type PERMISSIONS_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_MESSAGE_SCHEMA>

export type PERMISSIONS_ADD_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE = z.infer<typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA>
