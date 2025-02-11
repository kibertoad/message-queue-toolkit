import type { Either } from '@lokalise/node-core'
import {
  type AcquireLockOptions,
  AcquireLockTimeoutError,
  type MessageDeduplicationStore,
  type ReleasableLock,
} from '@message-queue-toolkit/core'
import type { Redis } from 'ioredis'
import { Mutex, TimeoutError } from 'redis-semaphore'

export type RedisMessageDeduplicationStoreDependencies = {
  redis: Redis
}

export class RedisMessageDeduplicationStore implements MessageDeduplicationStore {
  private readonly redis: Redis

  constructor(dependencies: RedisMessageDeduplicationStoreDependencies) {
    this.redis = dependencies.redis
  }

  async acquireLock(
    key: string,
    options: AcquireLockOptions,
  ): Promise<Either<AcquireLockTimeoutError | Error, ReleasableLock>> {
    const mutex = this.getMutex(key, options)

    try {
      await mutex.acquire()
      return { result: mutex }
    } catch (err) {
      if (err instanceof TimeoutError) return { error: new AcquireLockTimeoutError(err.message) }
      return { error: err as Error }
    }
  }

  async setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean> {
    const result = await this.redis.set(key, value, 'EX', ttlSeconds, 'NX')

    return result === 'OK'
  }

  async keyExists(key: string): Promise<boolean> {
    const result = await this.redis.exists(key)

    return result === 1
  }

  /** For testing purposes only */
  deleteKey(key: string): Promise<number> {
    return this.redis.del(key)
  }

  /** For testing purposes only */
  getKeyTtl(key: string): Promise<number> {
    return this.redis.ttl(key)
  }

  private getMutex(key: string, options: AcquireLockOptions): Mutex {
    return new Mutex(this.redis, key, {
      acquireTimeout: options.acquireTimeoutSeconds * 1000,
      lockTimeout: options.lockTimeoutSeconds * 1000,
      refreshInterval: options.refreshIntervalSeconds * 1000,
    })
  }
}
