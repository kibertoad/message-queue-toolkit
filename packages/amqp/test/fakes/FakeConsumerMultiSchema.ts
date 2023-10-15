import type { Either } from '@lokalise/node-core'
import type { MessageHandlerConfig } from '@message-queue-toolkit/core'

import { AbstractAmqpConsumerMultiSchema } from '../../lib/AbstractAmqpConsumerMultiSchema'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import type { CommonMessage } from '../../lib/types/MessageTypes'

export class FakeConsumerMultiSchema<ExecutionContext> extends AbstractAmqpConsumerMultiSchema<
  CommonMessage,
  ExecutionContext
> {
  constructor(
    dependencies: AMQPConsumerDependencies,
    queueName = 'dummy',
    handlers: MessageHandlerConfig<CommonMessage, ExecutionContext>[],
    executionContext: ExecutionContext,
  ) {
    super(
      dependencies,
      {
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
      },
      executionContext,
    )
  }

  processMessage(): Promise<Either<'retryLater', 'success'>> {
    return Promise.resolve({
      result: 'success',
    })
  }
}
