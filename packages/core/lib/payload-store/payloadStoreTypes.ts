import type { Readable } from 'node:stream'

export interface PayloadStoreTypes {
  /** Store the payload and return a key that can be used to retrieve it later. */
  storePayload(payload: SerializedPayload): Promise<string>

  /** Retrieve the previously stored payload. */
  retrievePayload(key: string): Promise<Readable | null>
}

export type SerializedPayload = {
  value: string | Readable
  size: number
}

export interface PayloadSerializer {
  serialize(payload: unknown): Promise<SerializedPayload>
}

export type PayloadStoreConfig = {
  /** Threshold in bytes after which the payload should be stored in the store. */
  messageSizeThreshold: number

  /** The store to use for storing the payload. */
  store: PayloadStoreTypes

  /** The serializer to use for serializing the payload. */
  serializer?: PayloadSerializer
}
