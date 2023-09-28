import type { Either } from '@lokalise/node-core'
import type { ZodType } from 'zod'

import { AbstractAmqpConsumerMonoSchema } from '../../lib/AbstractAmqpConsumerMonoSchema'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumer extends AbstractAmqpConsumerMonoSchema<CommonMessage> {
  constructor(dependencies: AMQPConsumerDependencies, queueName = 'dummy', messageSchema: ZodType) {
    super(dependencies, {
      creationConfig: {
        queueName: queueName,
        queueOptions: {
          durable: true,
          autoDelete: false,
        },
      },
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
