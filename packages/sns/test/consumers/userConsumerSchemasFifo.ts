import { MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA } from '@message-queue-toolkit/schemas'
import z from 'zod/v4'

export const PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO = z.object({
  id: z.string(),
  preHandlerIncrement: z.optional(z.number()),
  messageType: z.literal('add'),
  userIds: z.string(), // Field used for MessageGroupId in FIFO topics
  timestamp: z.string().optional(),
  metadata: z.record(z.string(), z.any()).optional(),
  deduplicationId: z.string().optional(),
  deduplicationOptions: MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.optional(),
})

export const PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO = z.object({
  id: z.string(),
  preHandlerIncrement: z.optional(z.number()),
  messageType: z.literal('remove'),
  userIds: z.string(), // Field used for MessageGroupId in FIFO topics
  timestamp: z.string().optional(),
  deduplicationId: z.string().optional(),
  deduplicationOptions: MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA.optional(),
})

export type PERMISSIONS_ADD_MESSAGE_TYPE_FIFO = z.output<typeof PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO>
export type PERMISSIONS_REMOVE_MESSAGE_TYPE_FIFO = z.output<
  typeof PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO
>
