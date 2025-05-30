import z from 'zod/v3'
import type { TopicMessagesConfig } from '../../lib/index.js'

const BASE_SCHEMA = z.object({
  id: z.string(),
  permissions: z.array(z.string()).describe('List of user permissions'),
})

export const PERMISSION_ADDED_SCHEMA = BASE_SCHEMA.extend({
  type: z.literal('added'),
})
export type PermissionAdded = z.infer<typeof PERMISSION_ADDED_SCHEMA>

export const PERMISSION_REMOVED_SCHEMA = BASE_SCHEMA.extend({
  type: z.literal('removed'),
})
export type PermissionRemoved = z.infer<typeof PERMISSION_REMOVED_SCHEMA>

const PERMISSION_ADDED_TOPIC = 'permission-added'
const PERMISSION_REMOVED_TOPIC = 'permission-removed'
const PERMISSION_GENERAL_TOPIC = 'permission-general'
export const TOPICS = [PERMISSION_ADDED_TOPIC, PERMISSION_REMOVED_TOPIC, PERMISSION_GENERAL_TOPIC]

export const PERMISSION_TOPIC_MESSAGES_CONFIG = {
  [PERMISSION_ADDED_TOPIC]: [PERMISSION_ADDED_SCHEMA],
  [PERMISSION_REMOVED_TOPIC]: [PERMISSION_REMOVED_SCHEMA],
  [PERMISSION_GENERAL_TOPIC]: [PERMISSION_ADDED_SCHEMA, PERMISSION_REMOVED_SCHEMA],
} as const satisfies TopicMessagesConfig
