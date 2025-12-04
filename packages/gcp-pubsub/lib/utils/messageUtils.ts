export const OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE = 'offloadedPayloadSize'

export function hasOffloadedPayload(attributes?: { [key: string]: string }): boolean {
  return !!attributes && OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE in attributes
}

export function buildOffloadedPayloadAttributes(
  payload: unknown,
  attributes: Record<string, string> = {},
): Record<string, string> {
  // Check if payload has been offloaded (using payloadRef format)
  if (
    typeof payload === 'object' &&
    payload !== null &&
    'payloadRef' in payload &&
    payload.payloadRef &&
    typeof payload.payloadRef === 'object' &&
    'size' in payload.payloadRef
  ) {
    const payloadRef = payload.payloadRef as { size: number }
    attributes[OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE] = payloadRef.size.toString()
  }

  return attributes
}
