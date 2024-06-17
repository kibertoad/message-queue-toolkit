import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'

import type { PayloadStoreTypes, SerializedPayload } from '@message-queue-toolkit/core'

export class FakePayloadStore implements PayloadStoreTypes {
  private storedPayloads: Record<string, Readable> = {}

  storePayload(payload: SerializedPayload): Promise<string> {
    const key = randomUUID()
    this.storedPayloads[key] =
      typeof payload.value === 'string' ? Readable.from(payload.value) : payload.value
    return Promise.resolve(key)
  }

  retrievePayload(key: string) {
    return Promise.resolve(
      this.storedPayloads[key] ? Readable.from(this.storedPayloads[key]) : null,
    )
  }

  getAndClearPayloads() {
    const payloads = { ...this.storedPayloads }
    this.storedPayloads = {}
    return payloads
  }
}
