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

  async setIfNotExists(key: string, value: string, ttlSeconds: number): Promise<boolean> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)
    const result = await this.redis.set(keyWithPrefix, value, 'EX', ttlSeconds, 'NX')

    return result === 'OK'
  }

  getByKey(key: string): Promise<string | null> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)

    return this.redis.get(keyWithPrefix)
  }

  private getKeyWithOptionalPrefix(key: string): string {
    return this.config?.keyPrefix?.length ? `${this.config.keyPrefix}:${key}` : key
  }
}
