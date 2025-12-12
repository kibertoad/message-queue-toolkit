import type { PublisherBaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer.ts'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService.ts'
import type { AmqpAwareEventDefinition } from '../../lib/AmqpQueuePublisherManager.ts'

export class FakeQueueConsumer extends AbstractAmqpQueueConsumer<
  PublisherBaseMessageType,
  unknown
> {
  public static readonly QUEUE_NAME = 'dummy-queue'
  constructor(dependencies: AMQPConsumerDependencies, eventDefinition: AmqpAwareEventDefinition) {
    super(
      dependencies,
      {
        creationConfig: {
          queueName: eventDefinition.queueName!,
          queueOptions: {
            durable: true,
            autoDelete: false,
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        handlerSpy: true,
        messageTypeResolver: { messageTypePath: 'type' },
        handlers: new MessageHandlerConfigBuilder<PublisherBaseMessageType, unknown>()
          .addConfig(eventDefinition.consumerSchema, () => Promise.resolve({ result: 'success' }))
          .build(),
      },
      {},
    )
  }
}
