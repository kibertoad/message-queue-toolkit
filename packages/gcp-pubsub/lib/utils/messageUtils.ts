export const OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE = 'offloadedPayloadSize'

export function hasOffloadedPayload(attributes?: { [key: string]: string }): boolean {
  return !!attributes && OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE in attributes
}

export function buildOffloadedPayloadAttributes(
  payload: unknown,
  attributes: Record<string, string> = {},
): Record<string, string> {
  // Check if payload has been offloaded
  if (
    typeof payload === 'object' &&
    payload !== null &&
    'offloadedPayloadPointer' in payload &&
    'offloadedPayloadSize' in payload
  ) {
    const offloadedPayload = payload as { offloadedPayloadSize: number }
    attributes[OFFLOADED_PAYLOAD_SIZE_ATTRIBUTE] = offloadedPayload.offloadedPayloadSize.toString()
  }

  return attributes
}
