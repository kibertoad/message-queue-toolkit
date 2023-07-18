import type { QueueOptions } from '@message-queue-toolkit/core'

import type { AMQPLocatorType } from '../../lib/AbstractAmqpConsumer'
import { AbstractAmqpPublisher } from '../../lib/AbstractAmqpPublisher'
import type { AMQPDependencies, AMQPQueueConfig } from '../../lib/AbstractAmqpService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class AmqpPermissionPublisher extends AbstractAmqpPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(
    dependencies: AMQPDependencies,
    options: Partial<
      Pick<QueueOptions<PERMISSIONS_MESSAGE_TYPE, AMQPQueueConfig, AMQPLocatorType>, 'queueLocator'>
    >,
  ) {
    super(dependencies, {
      queueName: AmqpPermissionPublisher.QUEUE_NAME,
      queueConfiguration: {
        durable: true,
        autoDelete: false,
      },
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
