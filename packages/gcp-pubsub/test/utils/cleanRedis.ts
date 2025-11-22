import type { Redis } from 'ioredis'

export async function cleanRedis(redis: Redis): Promise<void> {
  await redis.flushdb()
}
