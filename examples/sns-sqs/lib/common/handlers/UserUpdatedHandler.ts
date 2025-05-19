import type { Either } from '@lokalise/node-core'
import type { z } from 'zod'
import type { USER_SCHEMA, UserEvents } from '../TestMessages.ts'

let _latestData: z.infer<typeof USER_SCHEMA>

export function userUpdatedHandler(
  message: z.infer<typeof UserEvents.updated.consumerSchema>,
): Promise<Either<'retryLater', 'success'>> {
  _latestData = message.payload

  return Promise.resolve({ result: 'success' })
}
