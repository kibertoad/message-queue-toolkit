import { z } from 'zod/v3'

/**
 * When the payload is too large to be sent in a single message, it is offloaded to a storage service and a pointer to the offloaded payload is sent instead.
 * This schema represents the payload that is sent in place of the original payload.
 */
export const OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA = z
  .object({
    offloadedPayloadPointer: z.string().min(1),
    offloadedPayloadSize: z.number().int().positive(),
  })
  // Pass-through allows to pass message ID, type, timestamp and message-deduplication-related fields that are using dynamic keys.
  .passthrough()

export type OffloadedPayloadPointerPayload = z.infer<
  typeof OFFLOADED_PAYLOAD_POINTER_PAYLOAD_SCHEMA
>

export function isOffloadedPayloadPointerPayload(
  value: unknown,
): value is OffloadedPayloadPointerPayload {
  return (value as OffloadedPayloadPointerPayload).offloadedPayloadPointer !== undefined
}
