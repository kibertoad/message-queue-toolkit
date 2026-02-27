import type { TopicConfig } from '@message-queue-toolkit/kafka'
import z from 'zod/v4'

export const DIRECT_EVENT_SCHEMA = z.object({
  id: z.string(),
  event_type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})
export type DirectEvent = z.output<typeof DIRECT_EVENT_SCHEMA>

export const DIRECT_ORDER_SCHEMA = z.object({
  id: z.string(),
  customer_id: z.string(),
  amount: z.string(),
  status: z.string(),
  created_at: z.string(),
})
export type DirectOrder = z.output<typeof DIRECT_ORDER_SCHEMA>

export const DIRECT_TOPICS_CONFIG = [
  { topic: 'direct-events', schema: DIRECT_EVENT_SCHEMA },
  { topic: 'direct-orders', schema: DIRECT_ORDER_SCHEMA },
] as const satisfies TopicConfig[]
