import type { Either } from '@lokalise/node-core'
import type { MessageDeduplicationOptions } from '@message-queue-toolkit/schemas'

export interface ReleasableLock {
  release(): Promise<void>
}

export class AcquireLockTimeoutError extends Error {}

export type AcquireLockOptions = Required<
  Pick<
    MessageDeduplicationOptions,
    'acquireTimeoutSeconds' | 'lockTimeoutSeconds' | 'refreshIntervalSeconds'
  >
>

export interface MessageDeduplicationStore {
  /**
   * Stores a deduplication key in case it does not already exist.
   * @param {string} key - deduplication key
   * @param {string} value - value to store
   * @param {number} ttlSeconds - time to live in seconds
   * @returns {Promise<boolean>} - true if the key was stored, false if it already existed
   */
  setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean>

  /**
   * Acquires locks for a given key
   * @param {string} key - deduplication key
   * @param {object} options - options used when acquiring the lock
   * @returns {Promise<Either<AcquireLockTimeoutError | Error, ReleasableLock>>} - a promise that resolves to a ReleasableLock if the lock was acquired, AcquireLockTimeoutError error if the lock could not be acquired due to timeout, or an Error if the lock could not be acquired for another reason
   */
  acquireLock(
    key: string,
    options: object,
  ): Promise<Either<AcquireLockTimeoutError | Error, ReleasableLock>>

  /**
   * Checks if a deduplication key exists in the store
   * @param {string} key - deduplication key
   * @returns {Promise<boolean>} - true if the key exists, false otherwise
   */
  keyExists(key: string): Promise<boolean>
}

export type MessageDeduplicationConfig = {
  /** The store to use for storage and retrieval of deduplication keys */
  deduplicationStore: MessageDeduplicationStore
}

export enum DeduplicationRequester {
  Consumer = 'consumer',
  Publisher = 'publisher',
}

export const DEFAULT_MESSAGE_DEDUPLICATION_OPTIONS: Required<MessageDeduplicationOptions> = {
  deduplicationWindowSeconds: 40,
  lockTimeoutSeconds: 20,
  acquireTimeoutSeconds: 20,
  refreshIntervalSeconds: 10,
}

export const noopReleasableLock: ReleasableLock = {
  release: async () => {},
}
