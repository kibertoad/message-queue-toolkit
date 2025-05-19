import type { Either } from '@lokalise/node-core'
import type { z } from 'zod/v3'
import type { USER_SCHEMA, UserEvents } from '../TestMessages.ts'

let _latestData: z.infer<typeof USER_SCHEMA>

export function userCreatedHandler(
  message: z.infer<typeof UserEvents.created.consumerSchema>,
): Promise<Either<'retryLater', 'success'>> {
  _latestData = message.payload

  return Promise.resolve({ result: 'success' })
}
