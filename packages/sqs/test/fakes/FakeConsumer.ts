import type { Either } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer'
import type { SQSDependencies } from '../../lib/sqs/AbstractSqsService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumer extends AbstractSqsConsumer<CommonMessage> {
  constructor(dependencies: SQSDependencies, queueName = 'dummy', messageSchema: ZodType) {
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
