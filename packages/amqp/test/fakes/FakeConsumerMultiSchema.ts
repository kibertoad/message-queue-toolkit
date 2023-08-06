import type { Either } from '@lokalise/node-core'
import type { MessageHandlerConfig } from '@message-queue-toolkit/core'

import { AbstractAmqpConsumerMultiSchema } from '../../lib/AbstractAmqpConsumerMultiSchema'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumerMultiSchema extends AbstractAmqpConsumerMultiSchema<
  CommonMessage,
  FakeConsumerMultiSchema
> {
  constructor(
    dependencies: AMQPConsumerDependencies,
    queueName = 'dummy',
    handlers: MessageHandlerConfig<CommonMessage, FakeConsumerMultiSchema>[],
  ) {
    super(dependencies, {
      creationConfig: {
        queueName: queueName,
        queueOptions: {
          durable: true,
          autoDelete: false,
        },
      },
      deletionConfig: {
        deleteIfExists: true,
      },
      handlers,
      messageTypeField: 'messageType',
    })
  }

  processMessage(): Promise<Either<'retryLater', 'success'>> {
    return Promise.resolve({
      result: 'success',
    })
  }
}
