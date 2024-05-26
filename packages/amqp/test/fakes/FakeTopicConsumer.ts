import type { BaseMessageType } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import { AbstractAmqpTopicConsumer } from '../../lib/AbstractAmqpTopicConsumer'
import type { AmqpAwareEventDefinition } from '../../lib/AmqpQueuePublisherManager'

export class FakeTopicConsumer extends AbstractAmqpTopicConsumer<BaseMessageType, unknown> {
  public messageCounter = 0

  constructor(
    dependencies: AMQPConsumerDependencies,
    eventDefinition: AmqpAwareEventDefinition,
    options: {
      queueName: string
      topicPattern?: string
    },
  ) {
    if (!eventDefinition.exchange) {
      throw new Error(
        `No exchange defined for event ${eventDefinition.consumerSchema.shape.type.value}`,
      )
    }

    super(
      dependencies,
      {
        creationConfig: {
          queueName: options.queueName,
          queueOptions: {
            durable: true,
            autoDelete: false,
          },
          topicPattern: options.topicPattern,
          exchange: eventDefinition.exchange,
        },
        deletionConfig: {
          deleteIfExists: true,
        },
        handlerSpy: true,
        messageTypeField: 'type',
        handlers: new MessageHandlerConfigBuilder<BaseMessageType, unknown>()
          .addConfig(eventDefinition.consumerSchema, () => {
            this.messageCounter++
            return Promise.resolve({ result: 'success' })
          })
          .build(),
      },
      {},
    )
  }
}
