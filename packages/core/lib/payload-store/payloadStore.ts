import type { Readable } from 'node:stream'

import { tmpNameSync } from 'tmp'

export interface PayloadStore {
  /** Store the payload and return a key that can be used to retrieve it later. */
  storePayload(payload: Readable, payloadSize: number): Promise<string>

  /** Retrieve the previously stored payload. */
  retrievePayload(key: string): Promise<Readable | null>
}

export type TemporaryFilePathResolver = () => string
export const defaultTemporaryFilePathResolver: TemporaryFilePathResolver = () => tmpNameSync()

export type PayloadStoreConfig = {
  /** Threshold in bytes after which the payload should be stored in the store. */
  messageSizeThreshold: number

  /** The store to use for storing the payload. */
  store: PayloadStore

  /** The resolver to use for generating temporary file paths when serializing large payloads. */
  temporaryFilePathResolver?: TemporaryFilePathResolver
}
