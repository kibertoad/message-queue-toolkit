import type { PublisherMessageDeduplicationStore } from '@message-queue-toolkit/core'
import type { Redis } from 'ioredis'

export type RedisPublisherMessageDeduplicationStoreDependencies = {
  redis: Redis
}

export type RedisPublisherMessageDeduplicationStoreConfig = {
  keyPrefix?: string
}

export class RedisPublisherMessageDeduplicationStore implements PublisherMessageDeduplicationStore {
  private readonly redis: Redis
  private readonly config: RedisPublisherMessageDeduplicationStoreConfig

  constructor(
    dependencies: RedisPublisherMessageDeduplicationStoreDependencies,
    config: RedisPublisherMessageDeduplicationStoreConfig,
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
