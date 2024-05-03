import type { Either } from '@lokalise/node-core'
import type { BaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { SNSSQSConsumerDependencies } from '@message-queue-toolkit/sns'
import type { ZodSchema } from 'zod'

import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer'

export class FakeConsumer<T extends BaseMessageType> extends AbstractSnsSqsConsumer<T, unknown> {
  constructor(
    dependencies: SNSSQSConsumerDependencies,
    queue: string,
    topic: string,
    messageSchema: ZodSchema<T>,
  ) {
    super(
      dependencies,
      {
        handlers: new MessageHandlerConfigBuilder<T, unknown>()
          .addConfig(messageSchema, (message) => this._processMessage(message))
          .build(),
        creationConfig: {
          topic: {
            Name: topic,
          },
          queue: {
            QueueName: queue,
          },
        },
        subscriptionConfig: {
          updateAttributesIfExists: true,
        },
        messageTypeField: 'type',
        handlerSpy: true,
      },
      dependencies,
    )
  }

  private _processMessage(_message: T): Promise<Either<'retryLater', 'success'>> {
    return Promise.resolve({
      result: 'success',
    })
  }
}
