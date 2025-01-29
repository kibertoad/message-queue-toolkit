import { z } from 'zod'

export const MESSAGE_DEDUPLICATION_WINDOW_SECONDS_SCHEMA = z
  .number()
  .int()
  .gt(0)
  .describe('message deduplication window in seconds')
