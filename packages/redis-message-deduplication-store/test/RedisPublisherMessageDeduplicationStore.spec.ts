import { afterEach, beforeAll, describe, expect, it } from 'vitest'

import { Redis } from 'ioredis'
import { RedisPublisherMessageDeduplicationStore } from '../lib/RedisPublisherMessageDeduplicationStore'
import { cleanRedis } from './utils/cleanRedis'
import { TEST_REDIS_CONFIG } from './utils/testRedisConfig'

const KEY_PREFIX = 'test_key_prefix'

describe('RedisPublisherMessageDeduplicationStore', () => {
  const redisConfig = TEST_REDIS_CONFIG

  let redis: Redis
  let store: RedisPublisherMessageDeduplicationStore

  beforeAll(() => {
    redis = new Redis({
      host: redisConfig.host,
      db: redisConfig.db,
      port: redisConfig.port,
      username: redisConfig.username,
      password: redisConfig.password,
      connectTimeout: redisConfig.connectTimeout,
      commandTimeout: redisConfig.commandTimeout,
      tls: redisConfig.useTls ? {} : undefined,
      maxRetriesPerRequest: null,
      lazyConnect: false,
    })
    store = new RedisPublisherMessageDeduplicationStore({ redis }, { keyPrefix: KEY_PREFIX })
  })

  afterEach(async () => {
    await cleanRedis(redis)
  })

  describe('setIfNotExists', () => {
    it('in case key does not exist, it stores it and returns true', async () => {
      const key = 'test_key'
      const value = 'test_value'
      const ttlSeconds = 60

      const result = await store.setIfNotExists(key, value, ttlSeconds)

      expect(result).toBe(true)

      const storedValue = await redis.get(`${KEY_PREFIX}:${key}`)
      expect(storedValue).toBe(value)

      const storedTtl = await redis.ttl(`${KEY_PREFIX}:${key}`)
      expect(storedTtl).toBeLessThanOrEqual(ttlSeconds)
    })

    it('in case key exists, it does not store it and returns false', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(`${KEY_PREFIX}:${key}`, value, 'EX', 120)

      const result = await store.setIfNotExists(key, value, 60)

      expect(result).toBe(false)

      const storedTtl = await redis.ttl(`${KEY_PREFIX}:${key}`)
      expect(storedTtl).toBeGreaterThan(60)
    })
  })

  describe('getByKey', () => {
    it('in case key exists, it returns the value', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(`${KEY_PREFIX}:${key}`, value)

      const result = await store.getByKey(key)

      expect(result).toBe(value)
    })

    it('in case key does not exist, it returns null', async () => {
      const key = 'test_key'

      const result = await store.getByKey(key)

      expect(result).toBeNull()
    })
  })
})
