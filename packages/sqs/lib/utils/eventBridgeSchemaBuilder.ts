import type { ZodType } from 'zod/v4'
import z from 'zod/v4'

/**
 * Base EventBridge envelope schema (without detail typing)
 */
const EVENT_BRIDGE_BASE_SCHEMA = z.object({
  version: z.string(),
  id: z.string(),
  'detail-type': z.string(),
  source: z.string(),
  account: z.string(),
  time: z.string(),
  region: z.string(),
  resources: z.array(z.string()),
})

/**
 * Creates a typed EventBridge event schema with a custom detail payload.
 *
 * @template T - The Zod schema type for the detail payload
 * @param detailSchema - Zod schema for the 'detail' field
 * @param detailType - Optional literal value for 'detail-type' field (for routing)
 * @returns A Zod schema for the complete EventBridge event with typed detail
 *
 * @example
 * ```typescript
 * const USER_CREATED_DETAIL = z.object({
 *   userId: z.string(),
 *   email: z.string().email(),
 *   timestamp: z.string(),
 * })
 *
 * // With generic detail-type (for test schemas)
 * const USER_CREATED_EVENT = createEventBridgeSchema(USER_CREATED_DETAIL)
 *
 * // With literal detail-type (for routing in consumers)
 * const USER_CREATED_ENVELOPE = createEventBridgeSchema(
 *   USER_CREATED_DETAIL,
 *   'app.user.created'
 * )
 * ```
 */
export function createEventBridgeSchema<T extends ZodType>(detailSchema: T, detailType?: string) {
  const baseSchema = EVENT_BRIDGE_BASE_SCHEMA.extend({
    detail: detailSchema,
  })

  if (detailType) {
    return baseSchema.extend({
      'detail-type': z.literal(detailType),
    })
  }

  return baseSchema
}

/**
 * Helper to extract just the detail type from an EventBridge schema.
 * Useful when you need the payload type for handlers.
 *
 * @example
 * ```typescript
 * const USER_CREATED_EVENT = createEventBridgeSchema(USER_CREATED_DETAIL)
 * type UserCreatedDetail = EventBridgeDetail<typeof USER_CREATED_EVENT>
 * // Same as: z.infer<typeof USER_CREATED_DETAIL>
 * ```
 */
export type EventBridgeDetail<T> = T extends z.ZodObject<infer U>
  ? U extends { detail: infer D }
    ? D extends ZodType
      ? z.infer<D>
      : never
    : never
  : never

/**
 * Creates multiple EventBridge event schemas for different detail types.
 * Useful when you have multiple event types from the same source.
 *
 * @param detailSchemas - Record of event names to detail schemas
 * @returns Record of event names to complete EventBridge schemas
 *
 * @example
 * ```typescript
 * const EVENTS = createEventBridgeSchemas({
 *   userCreated: z.object({ userId: z.string(), email: z.string() }),
 *   userUpdated: z.object({ userId: z.string(), changes: z.record(z.any()) }),
 *   userDeleted: z.object({ userId: z.string() }),
 * })
 *
 * type UserCreatedEvent = z.infer<typeof EVENTS.userCreated>
 * type UserUpdatedEvent = z.infer<typeof EVENTS.userUpdated>
 * ```
 */
export function createEventBridgeSchemas<T extends Record<string, ZodType>>(
  detailSchemas: T,
): { [K in keyof T]: ReturnType<typeof createEventBridgeSchema<T[K]>> } {
  const result = {} as { [K in keyof T]: ReturnType<typeof createEventBridgeSchema<T[K]>> }

  for (const key in detailSchemas) {
    const schema = detailSchemas[key]
    if (schema) {
      result[key] = createEventBridgeSchema(schema) as ReturnType<
        typeof createEventBridgeSchema<T[typeof key]>
      >
    }
  }

  return result
}
