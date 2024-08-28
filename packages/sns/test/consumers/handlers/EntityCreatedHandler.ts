import type { Either } from '@lokalise/node-core'
import type z from 'zod'

import type { TestEvents } from '../../utils/testContext'

let _latestData: string

export function entityCreatedHandler(
  message: z.infer<typeof TestEvents.created.consumerSchema>,
): Promise<Either<'retryLater', 'success'>> {
  _latestData = message.payload.newData

  return Promise.resolve({ result: 'success' })
}
