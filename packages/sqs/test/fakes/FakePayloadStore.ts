import { randomUUID } from 'node:crypto'

import type { PayloadStore } from '@message-queue-toolkit/core'

export class FakePayloadStore implements PayloadStore {
  private storedPayloads: Record<string, string> = {}

  storePayload(payload: string) {
    const key = randomUUID()
    this.storedPayloads[key] = payload
    return Promise.resolve(key)
  }

  retrievePayload(key: string) {
    return Promise.resolve(this.storedPayloads[key] ?? null)
  }

  getAndClearPayloads() {
    const payloads = { ...this.storedPayloads }
    this.storedPayloads = {}
    return payloads
  }
}
