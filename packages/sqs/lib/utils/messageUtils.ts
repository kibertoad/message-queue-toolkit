import type { ResolvedMessage } from '@message-queue-toolkit/core'
import { isOffloadedPayloadPointerPayload } from '@message-queue-toolkit/core'

import { OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE } from '../sqs/AbstractSqsPublisher.ts'

export function resolveOutgoingMessageAttributes<MessageAttributeValue>(
  payload: unknown,
): Record<string, MessageAttributeValue> {
  const attributes: Record<string, MessageAttributeValue> = {}

  if (isOffloadedPayloadPointerPayload(payload)) {
    // Prefer payloadRef.size (new format), fall back to offloadedPayloadSize (legacy format)
    const size = payload.payloadRef?.size ?? payload.offloadedPayloadSize
    if (size === undefined) {
      throw new Error(
        'Offloaded payload is missing size information. Expected either payloadRef.size or offloadedPayloadSize.',
      )
    }
    attributes[OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE] = {
      DataType: 'Number',
      StringValue: size.toString(),
    } as MessageAttributeValue
  }

  return attributes
}

export function hasOffloadedPayload(message: ResolvedMessage): boolean {
  return !!message.attributes && OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE in message.attributes
}
