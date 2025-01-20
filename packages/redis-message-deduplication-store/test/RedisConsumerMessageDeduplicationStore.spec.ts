import { afterEach, beforeAll, describe, expect, it } from 'vitest'

import { Redis } from 'ioredis'
import { RedisConsumerMessageDeduplicationStore } from '../lib/RedisConsumerMessageDeduplicationStore'
import { cleanRedis } from './utils/cleanRedis'
import { TEST_REDIS_CONFIG } from './utils/testRedisConfig'

const KEY_PREFIX = 'test_key_prefix'

describe('RedisConsumerMessageDeduplicationStore', () => {
  const redisConfig = TEST_REDIS_CONFIG

  let redis: Redis
  let store: RedisConsumerMessageDeduplicationStore

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
    store = new RedisConsumerMessageDeduplicationStore({ redis }, { keyPrefix: KEY_PREFIX })
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

  describe('getKeyTtl', () => {
    it('in case key exists, it returns the ttl', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(`${KEY_PREFIX}:${key}`, value, 'EX', 60)

      const result = await store.getKeyTtl(key)

      expect(result).toBeLessThanOrEqual(60)
    })

    it('in case key does not exist, it returns null', async () => {
      const key = 'test_key'

      const result = await store.getKeyTtl(key)

      expect(result).toBeNull()
    })
  })

  describe('setOrUpdate', () => {
    it('updates the ttl and value of the key if it exists', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(`${KEY_PREFIX}:${key}`, value, 'EX', 60)

      const newValue = 'new_test_value'
      await store.setOrUpdate(key, newValue, 120)

      const storedValue = await redis.get(`${KEY_PREFIX}:${key}`)
      expect(storedValue).toBe(newValue)

      const storedTtl = await redis.ttl(`${KEY_PREFIX}:${key}`)
      expect(storedTtl).toBeGreaterThan(60)
    })

    it('stores the key if it does not exist', async () => {
      const key = 'test_key'
      const value = 'test_value'

      await store.setOrUpdate(key, value, 60)

      const storedValue = await redis.get(`${KEY_PREFIX}:${key}`)
      expect(storedValue).toBe(value)

      const storedTtl = await redis.ttl(`${KEY_PREFIX}:${key}`)
      expect(storedTtl).toBeLessThanOrEqual(60)
    })
  })

  describe('deleteKey', () => {
    it('deletes the key', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(`${KEY_PREFIX}:${key}`, value)

      await store.deleteKey(key)

      const storedValue = await redis.get(`${KEY_PREFIX}:${key}`)
      expect(storedValue).toBeNull()
    })
  })
})
