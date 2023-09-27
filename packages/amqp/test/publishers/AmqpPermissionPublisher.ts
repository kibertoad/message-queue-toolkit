import type {
  ExistingAMQPConsumerOptions,
  NewAMQPConsumerOptions,
} from '../../lib/AbstractAmqpBaseConsumer'
import { AbstractAmqpPublisherMonoSchema } from '../../lib/AbstractAmqpPublisherMonoSchema'
import type { AMQPDependencies } from '../../lib/AbstractAmqpService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class AmqpPermissionPublisher extends AbstractAmqpPublisherMonoSchema<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(
    dependencies: AMQPDependencies,
    options:
      | Pick<NewAMQPConsumerOptions, 'creationConfig'>
      | Pick<ExistingAMQPConsumerOptions, 'locatorConfig'> = {
      creationConfig: {
        queueName: AmqpPermissionPublisher.QUEUE_NAME,
        queueOptions: {
          durable: true,
          autoDelete: false,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
