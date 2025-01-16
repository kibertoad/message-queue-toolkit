import type { ConsumerMessageDeduplicationStore } from '@message-queue-toolkit/core'
import type { Redis } from 'ioredis'

export type RedisConsumerMessageDeduplicationStoreDependencies = {
  redis: Redis
}

export type RedisConsumerMessageDeduplicationStoreConfig = {
  keyPrefix?: string
}

export class RedisConsumerMessageDeduplicationStore implements ConsumerMessageDeduplicationStore {
  private readonly redis: Redis
  private readonly config: RedisConsumerMessageDeduplicationStoreConfig

  constructor(
    dependencies: RedisConsumerMessageDeduplicationStoreDependencies,
    config: RedisConsumerMessageDeduplicationStoreConfig,
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

  async getKeyTtl(key: string): Promise<number | null> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)

    const ttl = await this.redis.ttl(keyWithPrefix)

    return ttl >= 0 ? ttl : null
  }

  async updateKeyTtl(key: string, ttlSeconds: number): Promise<void> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)

    await this.redis.expire(keyWithPrefix, ttlSeconds)
  }

  async updateKeyTtlAndValue(key: string, value: string, ttlSeconds: number): Promise<void> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)

    await this.redis.set(keyWithPrefix, value, 'EX', ttlSeconds)
  }

  async deleteKey(key: string): Promise<void> {
    const keyWithPrefix = this.getKeyWithOptionalPrefix(key)

    await this.redis.del(keyWithPrefix)
  }

  private getKeyWithOptionalPrefix(key: string): string {
    return this.config?.keyPrefix?.length ? `${this.config.keyPrefix}:${key}` : key
  }
}
