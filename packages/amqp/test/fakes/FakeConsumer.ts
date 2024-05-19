import type { BaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import type { AmqpAwareEventDefinition } from '../../lib/AmqpQueuePublisherManager'

export class FakeConsumer extends AbstractAmqpQueueConsumer<BaseMessageType, unknown> {
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
        messageTypeField: 'type',
        handlers: new MessageHandlerConfigBuilder<BaseMessageType, unknown>()
          .addConfig(eventDefinition.consumerSchema, () => Promise.resolve({ result: 'success' }))
          .build(),
      },
      {},
    )
  }
}
