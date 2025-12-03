import { z } from 'zod/v4'

/**
 * Multi-store payload reference schema.
 * Contains information about where and how the payload was stored.
 */
export const PAYLOAD_REF_SCHEMA = z.object({
  /** Unique identifier for the stored payload */
  id: z.string().min(1),
  /** Name/identifier of the store where the payload is stored */
  store: z.string().min(1),
  /** Size of the payload in bytes */
  size: z.number().int().positive(),
})

export type PayloadRef = z.output<typeof PAYLOAD_REF_SCHEMA>

/**
 * When the payload is too large to be sent in a single message, it is offloaded to a storage service and a pointer to the offloaded payload is sent instead.
 * This schema represents the reference to the payload that is sent in place of the original payload.
 *
 * The schema supports two formats:
 * 1. New format, payloadRef object with id, store, and size
 * 2. Old format, offloadedPayloadPointer and offloadedPayloadSize (for backward compatibility)
 *
 * At least one format must be present in the message.
 */
export const OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA = z
  .object({
    // New, extended payload reference (supports multi-store functionality).
    payloadRef: PAYLOAD_REF_SCHEMA.optional(),
    // Legacy payload reference, preserved for backward compatibility.
    offloadedPayloadPointer: z.string().min(1).optional(),
    offloadedPayloadSize: z.number().int().positive().optional(),
  })
  // Pass-through allows to pass message ID, type, timestamp and message-deduplication-related fields that are using dynamic keys.
  .passthrough()
  .refine(
    (data) => {
      // At least one format must be present
      return data.payloadRef !== undefined || data.offloadedPayloadPointer !== undefined
    },
    {
      message: 'Either payloadRef or offloadedPayloadPointer must be present',
    },
  )

export type OffloadedPayloadPointerPayload = z.output<
  typeof OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA
>

export function isOffloadedPayloadPointerPayload(
  value: unknown,
): value is OffloadedPayloadPointerPayload {
  if (typeof value !== 'object' || value === null) {
    return false
  }
  const payload = value as OffloadedPayloadPointerPayload
  return payload.payloadRef !== undefined || payload.offloadedPayloadPointer !== undefined
}
