import { z } from 'zod/v3'

export const MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA = z.object({
  deduplicationWindowSeconds: z
    .number()
    .int()
    .gt(0)
    .optional()
    .describe('message deduplication window in seconds'),
  lockTimeoutSeconds: z.number().int().gt(0).optional().describe('message lock timeout in seconds'),
  acquireTimeoutSeconds: z
    .number()
    .int()
    .gt(0)
    .optional()
    .describe('message lock acquire timeout in seconds'),
  refreshIntervalSeconds: z
    .number()
    .gt(0)
    .optional()
    .describe('message lock refresh interval in seconds'),
})

export type MessageDeduplicationOptions = z.infer<typeof MESSAGE_DEDUPLICATION_OPTIONS_SCHEMA>
