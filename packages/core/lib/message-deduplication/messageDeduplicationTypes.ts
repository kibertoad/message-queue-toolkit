export interface MessageDeduplicationKeyGenerator<Message extends object = object> {
  generate(message: Message): string
}

export interface PublisherMessageDeduplicationStore {
  /**
   * Stores a deduplication key in case it does not already exist.
   * Returns true if the key was stored, false if it already existed.
   */
  setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean>

  /** Retrieves value associated with deduplication key */
  getByKey(key: string): Promise<string | null>
}

export type PublisherMessageDeduplicationMessageTypeConfig<Message extends object = object> = {
  /** How many seconds to keep the deduplication key in the store for a particular message type */
  deduplicationWindowSeconds: number

  /** The generator to use for generating deduplication keys for a particular message type */
  deduplicationKeyGenerator: MessageDeduplicationKeyGenerator<Message>
}

export type PublisherMessageDeduplicationConfig = {
  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: PublisherMessageDeduplicationStore

  /** The configuration for deduplication for each message type */
  messageTypeToConfigMap: Record<string, PublisherMessageDeduplicationMessageTypeConfig>
}

export interface ConsumerMessageDeduplicationStore {
  /**
   * Stores a deduplication key in case it does not already exist.
   * Returns true if the key was stored, false if it already existed.
   */
  setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean>

  /** Retrieves value associated with deduplication key */
  getByKey(key: string): Promise<string | null>

  /** Retrieves TTL of the deduplication key */
  getKeyTtl(key: string): Promise<number | null>

  /** Updates TTL of the deduplication key */
  updateKeyTtl(key: string, ttlSeconds: number): Promise<void>

  /** Updates TTL of the deduplication key and its value */
  updateKeyTtlAndValue(key: string, value: string, ttlSeconds: number): Promise<void>

  /** Deletes the deduplication key */
  deleteKey(key: string): Promise<void>
}

export type ConsumerMessageDeduplicationMessageTypeConfig<Message extends object = object> = {
  /** How many seconds to keep the deduplication key in the store for a particular message type after message is successfully processed */
  deduplicationWindowSeconds: number

  /** How many seconds it is expected to take to process a message of a particular type */
  maximumProcessingTimeSeconds: number

  /** The generator to use for generating deduplication keys for a particular message type */
  deduplicationKeyGenerator: MessageDeduplicationKeyGenerator<Message>
}

export type ConsumerMessageDeduplicationConfig = {
  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: ConsumerMessageDeduplicationStore

  /** The configuration for deduplication for each message type */
  messageTypeToConfigMap: Record<string, ConsumerMessageDeduplicationMessageTypeConfig>
}

export enum ConsumerMessageDeduplicationKeyStatus {
  PROCESSING = 'PROCESSING',
  PROCESSED = 'PROCESSED',
}
