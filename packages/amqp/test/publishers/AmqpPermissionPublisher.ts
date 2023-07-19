import type {
  NewAMQPConsumerOptions,
  ExistingAMQPConsumerOptions,
} from '../../lib/AbstractAmqpConsumer'
import { AbstractAmqpPublisher } from '../../lib/AbstractAmqpPublisher'
import type { AMQPDependencies } from '../../lib/AbstractAmqpService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class AmqpPermissionPublisher extends AbstractAmqpPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(
    dependencies: AMQPDependencies,
    options:
      | Pick<NewAMQPConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'creationConfig'>
      | Pick<ExistingAMQPConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'locatorConfig'> = {
      creationConfig: {
        queueName: AmqpPermissionPublisher.QUEUE_NAME,
        queue: {
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
