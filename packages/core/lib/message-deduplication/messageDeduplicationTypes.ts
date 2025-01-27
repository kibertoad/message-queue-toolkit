import type {
  ConsumerMessageDeduplicationMessageType,
  PublisherMessageDeduplicationMessageType,
} from './messageDeduplicationSchemas'

export interface PublisherMessageDeduplicationStore {
  /**
   * Stores a deduplication key in case it does not already exist.
   * @param {string} key - deduplication key
   * @param {string} value - value to store
   * @param {number} ttlSeconds - time to live in seconds
   * @returns {boolean} - true if the key was stored, false if it already existed
   */
  setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean>

  /** Retrieves value associated with deduplication key */
  getByKey(key: string): Promise<string | null>
}

export type PublisherMessageDeduplicationMessageTypeConfig =
  PublisherMessageDeduplicationMessageType

export interface ConsumerMessageDeduplicationStore extends PublisherMessageDeduplicationStore {
  /**
   * Retrieves TTL of the deduplication key
   *
   * @param {string} key - deduplication key
   * @returns {number|null} - TTL of the deduplication key in seconds or null if the key does not exist
   */
  getKeyTtl(key: string): Promise<number | null>

  /** Sets a value for the deduplication key or updates it if it already exists */
  setOrUpdate(key: string, value: string, ttlSeconds: number): Promise<void>

  /** Deletes the deduplication key */
  deleteKey(key: string): Promise<void>
}

export type ConsumerMessageDeduplicationMessageTypeConfig = ConsumerMessageDeduplicationMessageType

export type MessageDeduplicationConfig<
  TStore extends ConsumerMessageDeduplicationStore | PublisherMessageDeduplicationStore,
  TConfig extends
    | ConsumerMessageDeduplicationMessageTypeConfig
    | PublisherMessageDeduplicationMessageTypeConfig,
> = {
  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: TStore

  /** The configuration for deduplication for each message type */
  messageTypeToConfigMap: Record<string, TConfig>
}

export enum ConsumerMessageDeduplicationKeyStatus {
  PROCESSING = 'PROCESSING',
  PROCESSED = 'PROCESSED',
}
