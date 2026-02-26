import type { TopicConfig } from '@message-queue-toolkit/kafka'
import z from 'zod/v4'

const eventRowSchema = z.object({
  id: z.string(),
  event_type: z.string(),
  payload: z.record(z.string(), z.unknown()),
  created_at: z.string(),
})

const orderRowSchema = z.object({
  id: z.string(),
  customer_id: z.string(),
  amount: z.coerce.string(),
  status: z.string(),
  created_at: z.string(),
})

export const CDC_EVENT_SCHEMA = z.object({
  after: eventRowSchema.nullable().optional(),
  before: eventRowSchema.nullable().optional(),
  updated: z.string().optional(),
  resolved: z.string().optional(),
})
export type CdcEvent = z.output<typeof CDC_EVENT_SCHEMA>

export const CDC_ORDER_SCHEMA = z.object({
  after: orderRowSchema.nullable().optional(),
  before: orderRowSchema.nullable().optional(),
  updated: z.string().optional(),
  resolved: z.string().optional(),
})
export type CdcOrder = z.output<typeof CDC_ORDER_SCHEMA>

export const CDC_TOPICS_CONFIG = [
  { topic: 'events', schema: CDC_EVENT_SCHEMA },
  { topic: 'orders', schema: CDC_ORDER_SCHEMA },
] as const satisfies TopicConfig[]
