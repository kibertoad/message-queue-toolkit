import type { Either } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import { AbstractAmqpConsumer } from '../../lib/AbstractAmqpConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumer extends AbstractAmqpConsumer<CommonMessage> {
  constructor(dependencies: AMQPConsumerDependencies, queueName = 'dummy', messageSchema: ZodType) {
    super(dependencies, {
      queueName: queueName,
      messageSchema,
      messageTypeField: 'messageType',
    })
  }

  processMessage(): Promise<Either<'retryLater', 'success'>> {
    return Promise.resolve({
      result: 'success',
    })
  }
}
