import type { PublisherBaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ZodSchema } from 'zod/v4'

import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer.ts'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService.ts'

/**
 * Message type used for the catch-all handler in CustomFakeConsumer.
 */
const CUSTOM_FAKE_MESSAGE_TYPE = 'custom.fake.message'

export class CustomFakeConsumer extends AbstractAmqpQueueConsumer<
  PublisherBaseMessageType,
  unknown
> {
  public static readonly QUEUE_NAME = 'dummy-queue'
  constructor(dependencies: AMQPConsumerDependencies, schema: ZodSchema<any>) {
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
        // Use literal resolver so all messages route to the same handler
        messageTypeResolver: { literal: CUSTOM_FAKE_MESSAGE_TYPE },
        handlers: new MessageHandlerConfigBuilder<PublisherBaseMessageType, unknown>()
          .addConfig(schema, () => Promise.resolve({ result: 'success' }), {
            messageType: CUSTOM_FAKE_MESSAGE_TYPE,
          })
          .build(),
      },
      {},
    )
  }
}
