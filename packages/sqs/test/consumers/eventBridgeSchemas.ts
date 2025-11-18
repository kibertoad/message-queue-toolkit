import z from 'zod/v4'

import { createEventBridgeSchema } from '../../lib/index.ts'

/**
 * User presence payload schema
 * This is what the handler will actually receive (the 'detail' field content)
 */
export const USER_PRESENCE_DETAIL_SCHEMA = z.object({
  topicName: z.string(),
  userId: z.string(),
  organizationId: z.string(),
  presenceDefinition: z.object({
    id: z.string(),
    systemPresence: z.string(),
    mobilePresence: z.string(),
    aggregationPresence: z.string(),
    message: z.string().nullable(),
  }),
  timestamp: z.string(),
})

/**
 * User routing status payload schema
 */
export const USER_ROUTING_STATUS_DETAIL_SCHEMA = z.object({
  topicName: z.string(),
  userId: z.string(),
  organizationId: z.string(),
  routingStatus: z.object({
    id: z.string(),
    status: z.string(),
    startTime: z.string().optional(),
  }),
  timestamp: z.string(),
})

/**
 * Envelope schemas with literal detail-type values for routing.
 * These validate the full EventBridge envelope structure.
 * Note: The detail field is validated twice - once here in the envelope, and once
 * as extracted payload. This is an acceptable tradeoff to maintain proper type safety.
 */
export const USER_PRESENCE_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_PRESENCE_DETAIL_SCHEMA,
  'v2.users.{id}.presence',
)

export const USER_ROUTING_STATUS_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_ROUTING_STATUS_DETAIL_SCHEMA,
  'v2.users.{id}.routing.status',
)

export type UserPresencePayload = z.output<typeof USER_PRESENCE_DETAIL_SCHEMA>
export type UserRoutingStatusPayload = z.output<typeof USER_ROUTING_STATUS_DETAIL_SCHEMA>
export type UserPresenceEnvelope = z.output<typeof USER_PRESENCE_ENVELOPE_SCHEMA>
export type UserRoutingStatusEnvelope = z.output<typeof USER_ROUTING_STATUS_ENVELOPE_SCHEMA>

export type SupportedEventBridgePayloads = UserPresencePayload | UserRoutingStatusPayload
