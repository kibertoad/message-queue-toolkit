import type { Either } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import { AbstractAmqpConsumer } from '../../lib/AbstractAmqpConsumer'
import type { AMQPDependencies } from '../../lib/AbstractAmqpService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumer extends AbstractAmqpConsumer<CommonMessage> {
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
