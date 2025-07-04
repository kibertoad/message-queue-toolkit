import type { Either } from '@lokalise/node-core'
import type z from 'zod/v4'

import type { TestEvents } from '../../utils/testContext.ts'

let _latestData: string

export function entityCreatedHandler(
  message: z.output<typeof TestEvents.created.consumerSchema>,
): Promise<Either<'retryLater', 'success'>> {
  _latestData = message.payload.newData

  return Promise.resolve({ result: 'success' })
}
