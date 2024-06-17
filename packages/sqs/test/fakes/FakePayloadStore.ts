import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'

import type { PayloadStore } from '@message-queue-toolkit/core'

export class FakePayloadStore implements PayloadStore {
  private storedPayloads: Record<string, Readable> = {}

  storePayload(payload: Readable): Promise<string> {
    const key = randomUUID()
    this.storedPayloads[key] = payload
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
