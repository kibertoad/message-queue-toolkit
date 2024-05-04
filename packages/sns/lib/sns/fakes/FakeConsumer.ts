import type { BaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { SNSSQSConsumerDependencies } from 'dist'
import type { ZodSchema } from 'zod'

import { AbstractSnsSqsConsumer } from '../AbstractSnsSqsConsumer'

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
          .addConfig(messageSchema, () =>
            Promise.resolve({
              result: 'success',
            }),
          )
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
}
