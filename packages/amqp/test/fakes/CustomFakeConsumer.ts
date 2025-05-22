import type { PublisherBaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v3'

import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer.ts'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService.ts'

export class CustomFakeConsumer extends AbstractAmqpQueueConsumer<
  PublisherBaseMessageType,
  unknown
> {
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
        handlers: new MessageHandlerConfigBuilder<PublisherBaseMessageType, unknown>()
          .addConfig(schema, () => Promise.resolve({ result: 'success' }))
          .build(),
      },
      {},
    )
  }
}
