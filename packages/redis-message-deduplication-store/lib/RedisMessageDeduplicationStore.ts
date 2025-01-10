import type { MessageDeduplicationStore } from '@message-queue-toolkit/core'
import type { Redis } from 'ioredis'

export type RedisMessageDeduplicationStoreDependencies = {
  redis: Redis
}

export type RedisMessageDeduplicationStoreConfig = {
  keyPrefix?: string
}

export class RedisMessageDeduplicationStore implements MessageDeduplicationStore {
  private readonly redis: Redis
  private readonly config: RedisMessageDeduplicationStoreConfig

  constructor(
    dependencies: RedisMessageDeduplicationStoreDependencies,
    config: RedisMessageDeduplicationStoreConfig,
  ) {
    this.redis = dependencies.redis
    this.config = config
  }

  async storeCacheKey(key: string, value: string, ttlSeconds: number): Promise<void> {
    const cacheKey = this.getCacheKeyWithOptionalPrefix(key)

    await this.redis.set(cacheKey, value, 'EX', ttlSeconds)
  }

  retrieveCacheKey(key: string): Promise<string | null> {
    const cacheKey = this.getCacheKeyWithOptionalPrefix(key)

    return this.redis.get(cacheKey)
  }

  private getCacheKeyWithOptionalPrefix(key: string): string {
    return this.config?.keyPrefix?.length ? `${this.config.keyPrefix}:${key}` : key
  }
}
