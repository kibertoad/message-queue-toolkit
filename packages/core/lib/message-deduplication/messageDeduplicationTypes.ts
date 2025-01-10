export interface MessageDeduplicationKeyGenerator<MessageType extends object = object> {
  generate(message: MessageType): string
}

export interface MessageDeduplicationStore {
  storeCacheKey(key: string, value: string, ttlSeconds: number): Promise<void>
  retrieveCacheKey(key: string): Promise<string | null>
}

export type MessageDeduplicationConfig = {
  /** How many seconds to keep the deduplication key in the store */
  deduplicationWindowSeconds: number

  /** The generator to use for generating deduplication keys */
  deduplicationKeyGenerator: MessageDeduplicationKeyGenerator

  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: MessageDeduplicationStore
}
