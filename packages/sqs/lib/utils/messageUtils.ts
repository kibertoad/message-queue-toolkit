import type { ResolvedMessage } from '@message-queue-toolkit/core'
import { isOffloadedPayloadPointerPayload } from '@message-queue-toolkit/core'

export const OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE = 'payloadOffloading.size'

export function resolveOutgoingMessageAttributes<MessageAttributeValue>(
  payload: unknown,
): Record<string, MessageAttributeValue> {
  const attributes: Record<string, MessageAttributeValue> = {}

  if (isOffloadedPayloadPointerPayload(payload)) {
    attributes[OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE] = {
      DataType: 'Number',
      // The SQS SDK does not provide properties to set numeric values, we have to convert it to string
      StringValue: payload.offloadedPayloadSize.toString(),
    } as MessageAttributeValue
  }

  return attributes
}

export function hasOffloadedPayload(message: ResolvedMessage): boolean {
  return !!message.attributes && OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE in message.attributes
}
