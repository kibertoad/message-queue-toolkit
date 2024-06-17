export interface PayloadStore {
  /** Store the payload and return a key that can be used to retrieve it later. */
  storePayload(payload: string): Promise<string>

  /** Retrieve the previously stored payload. */
  retrievePayload(key: string): Promise<string | null>

  /** Delete the stored payload. */
  deletePayload(key: string): Promise<void>
}

export type PayloadStoreConfig = {
  /** Threshold in bytes after which the payload should be stored in the store. */
  messageSizeThreshold: number

  /** The store to use for storing the payload. */
  store: PayloadStore
}
