import type { BaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod'

import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'

export class CustomFakeConsumer extends AbstractAmqpQueueConsumer<BaseMessageType, unknown> {
  public static readonly QUEUE_NAME = 'dummy-queue'
  constructor(dependencies: AMQPConsumerDependencies, schema: ZodSchema) {
    super(
      dependencies,
      {
        creationConfig: {
          queueName: CustomFakeConsumer.QUEUE_NAME,
          queueOptions: {
            durable: true,
            autoDelete: false,
          },
        },
        handlerSpy: true,
        messageTypeField: 'messageType',
        handlers: new MessageHandlerConfigBuilder<BaseMessageType, unknown>()
          .addConfig(schema, () => Promise.resolve({ result: 'success' }))
          .build(),
      },
      {},
    )
  }
}
