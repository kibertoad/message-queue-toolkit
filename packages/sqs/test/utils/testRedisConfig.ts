import type { RedisConfig } from '@lokalise/node-core'

export const TEST_REDIS_CONFIG: RedisConfig = {
  host: 'localhost',
  db: 0,
  port: 6379,
  username: undefined,
  password: 'sOmE_sEcUrE_pAsS',
  connectTimeout: undefined,
  commandTimeout: undefined,
  useTls: false,
}
