import { z } from 'zod'

export const MESSAGE_DEDUPLICATION_MESSAGE_TYPE_SCHEMA = z.object({
  deduplicationWindowSeconds: z.number().int().gt(0).optional(),
  maximumProcessingTimeSeconds: z.number().int().gt(0).optional(),
})
