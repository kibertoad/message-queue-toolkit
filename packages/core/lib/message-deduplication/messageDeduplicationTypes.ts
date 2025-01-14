export interface MessageDeduplicationKeyGenerator<Message extends object = object> {
  generate(message: Message): string
}

export interface MessageDeduplicationStore {
  /**
   * Stores a deduplication key in case it does not already exist.
   * Returns true if the key was stored, false if it already existed.
   */
  setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean>

  /** Retrieves value associated with deduplication key */
  getByKey(key: string): Promise<string | null>
}

export type MessageDeduplicationMessageTypeConfig = {
  /** How many seconds to keep the deduplication key in the store for a particular message type */
  deduplicationWindowSeconds: number

  /** The generator to use for generating deduplication keys for a particular message type */
  deduplicationKeyGenerator: MessageDeduplicationKeyGenerator
}

export type MessageDeduplicationConfig = {
  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: MessageDeduplicationStore

  /** The configuration for deduplication for each message type */
  messageTypeToConfigMap: Record<string, MessageDeduplicationMessageTypeConfig>
}
