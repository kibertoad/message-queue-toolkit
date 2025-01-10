import { afterEach, beforeAll, describe, expect, it } from 'vitest'

import { Redis } from 'ioredis'
import { RedisMessageDeduplicationStore } from '../lib/RedisMessageDeduplicationStore'
import { cleanRedis } from './utils/cleanRedis'
import { TEST_REDIS_CONFIG } from './utils/testRedisConfig'

const KEY_PREFIX = 'test_key_prefix'

describe('RedisMessageDeduplicationStore', () => {
  const redisConfig = TEST_REDIS_CONFIG

  let redis: Redis
  let store: RedisMessageDeduplicationStore

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
    store = new RedisMessageDeduplicationStore({ redis }, { keyPrefix: KEY_PREFIX })
  })

  afterEach(async () => {
    await cleanRedis(redis)
  })

  describe('storeKey', () => {
    it('stores a key in Redis with provided value and ttl', async () => {
      const key = 'test_key'
      const value = 'test_value'
      const ttlSeconds = 60

      await store.storeKey(key, value, ttlSeconds)

      const storedValue = await redis.get(`${KEY_PREFIX}:${key}`)
      expect(storedValue).toBe(value)

      const storedTtl = await redis.ttl(`${KEY_PREFIX}:${key}`)
      expect(storedTtl).toBeLessThanOrEqual(ttlSeconds)
    })
  })

  describe('retrieveKey', () => {
    it('retrieves a key from Redis', async () => {
      const key = 'test_key'
      const value = 'test_value'
      const ttlSeconds = 60

      await redis.set(`${KEY_PREFIX}:${key}`, value, 'EX', ttlSeconds)

      const retrievedValue = await store.retrieveKey(key)

      expect(retrievedValue).toBe(value)
    })
  })
})
