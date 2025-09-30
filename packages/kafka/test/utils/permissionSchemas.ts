import z from 'zod/v4'
import type { TopicConfig } from '../../lib/index.ts'

const BASE_PERMISSION_SCHEMA = z.object({
  id: z.string(),
  permissions: z.array(z.string()).describe('List of user permissions'),
})

export const PERMISSION_ADDED_SCHEMA = BASE_PERMISSION_SCHEMA.extend({
  type: z.literal('added'),
})
export type PermissionAdded = z.output<typeof PERMISSION_ADDED_SCHEMA>

export const PERMISSION_REMOVED_SCHEMA = BASE_PERMISSION_SCHEMA.extend({
  type: z.literal('removed'),
})
export type PermissionRemoved = z.output<typeof PERMISSION_REMOVED_SCHEMA>

export const PERMISSION_ADDED_TOPIC = 'permission-added'
export const PERMISSION_REMOVED_TOPIC = 'permission-removed'
export const PERMISSION_GENERAL_TOPIC = 'permission-general'
export const TOPICS = [PERMISSION_ADDED_TOPIC, PERMISSION_REMOVED_TOPIC, PERMISSION_GENERAL_TOPIC]

export const PERMISSION_TOPIC_MESSAGES_CONFIG = [
  { topic: PERMISSION_ADDED_TOPIC, schema: PERMISSION_ADDED_SCHEMA },
  { topic: PERMISSION_REMOVED_TOPIC, schema: PERMISSION_REMOVED_SCHEMA },
] as const satisfies TopicConfig[]
