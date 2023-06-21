import type { Either } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import { AbstractConsumer } from '../../lib/AbstractConsumer'
import type { AMQPDependencies } from '../../lib/AbstractQueueService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumer extends AbstractConsumer<CommonMessage> {
  constructor(dependencies: AMQPDependencies, queueName = 'dummy', messageSchema: ZodType) {
    super(
      {
        queueName: queueName,
        messageSchema,
      },
      dependencies,
    )
  }

  processMessage(): Promise<Either<'retryLater', 'success'>> {
    return Promise.resolve({
      result: 'success',
    })
  }
}
