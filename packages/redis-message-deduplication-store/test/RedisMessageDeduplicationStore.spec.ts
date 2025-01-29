import { afterEach, beforeAll, describe, expect, it, vi } from 'vitest'

import { AcquireLockTimeoutError } from '@message-queue-toolkit/core'
import { Redis } from 'ioredis'
import { Mutex, TimeoutError } from 'redis-semaphore'
import { RedisMessageDeduplicationStore } from '../lib/RedisMessageDeduplicationStore'
import { cleanRedis } from './utils/cleanRedis'
import { TEST_REDIS_CONFIG } from './utils/testRedisConfig'

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
    store = new RedisMessageDeduplicationStore({ redis })
  })

  afterEach(async () => {
    vi.restoreAllMocks()
    await cleanRedis(redis)
  })

  describe('setIfNotExists', () => {
    it('in case key does not exist, it stores it and returns true', async () => {
      const key = 'test_key'
      const value = 'test_value'
      const ttlSeconds = 60

      const result = await store.setIfNotExists(key, value, ttlSeconds)

      expect(result).toBe(true)

      const storedValue = await redis.get(key)
      expect(storedValue).toBe(value)

      const storedTtl = await redis.ttl(key)
      expect(storedTtl).toBeLessThanOrEqual(ttlSeconds)
    })

    it('in case key exists, it does not store it and returns false', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(key, value, 'EX', 120)

      const result = await store.setIfNotExists(key, value, 60)

      expect(result).toBe(false)

      const storedTtl = await redis.ttl(key)
      expect(storedTtl).toBeGreaterThan(60)
    })
  })

  describe('acquireLock', () => {
    it('acquires lock and returns Mutex', async () => {
      const key = 'test_key'
      const acquireLockResult = await store.acquireLock(key)

      expect(acquireLockResult.result).toBeInstanceOf(Mutex)
    })

    it('returns AcquireLockTimeoutError if lock cannot be acquired due to timeout', async () => {
      const key = 'test_key'
      vi.spyOn(Mutex.prototype, 'acquire').mockRejectedValue(new TimeoutError('Test error'))

      const acquireLockResult = await store.acquireLock(key)

      expect(acquireLockResult.error).toBeInstanceOf(AcquireLockTimeoutError)
    })

    it('returns Error if lock cannot be acquired for other reasons', async () => {
      const key = 'test_key'
      vi.spyOn(Mutex.prototype, 'acquire').mockRejectedValue(new Error('Test error'))

      const acquireLockResult = await store.acquireLock(key)

      expect(acquireLockResult.error).toBeInstanceOf(Error)
      expect(acquireLockResult.error).not.toBeInstanceOf(AcquireLockTimeoutError)
    })
  })

  describe('keyExists', () => {
    it('returns true if key exists', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(key, value, 'EX', 120)

      const result = await store.keyExists(key)

      expect(result).toBe(true)
    })

    it('returns false if key does not exist', async () => {
      const key = 'test_key'

      const result = await store.keyExists(key)

      expect(result).toBe(false)
    })
  })

  describe('deleteKey', () => {
    it('deletes key and returns number of deleted keys', async () => {
      const key = 'test_key'
      const value = 'test_value'
      await redis.set(key, value, 'EX', 120)

      const result = await store.deleteKey(key)

      expect(result).toBe(1)

      const storedValue = await redis.get(key)
      expect(storedValue).toBeNull()
    })
  })
})
