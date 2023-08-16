import type {
  ExistingAMQPConsumerOptions,
  NewAMQPConsumerOptions,
} from '../../lib/AbstractAmqpBaseConsumer'
import { AbstractAmqpPublisherMultiSchema } from '../../lib/AbstractAmqpPublisherMultiSchema'
import type { AMQPDependencies } from '../../lib/AbstractAmqpService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

type SupportedTypes = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class AmqpPermissionPublisherMultiSchema extends AbstractAmqpPublisherMultiSchema<SupportedTypes> {
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: AMQPDependencies,
    options:
      | Pick<NewAMQPConsumerOptions, 'creationConfig' | 'logMessages'>
      | Pick<ExistingAMQPConsumerOptions, 'locatorConfig' | 'logMessages'> = {
      creationConfig: {
        queueName: AmqpPermissionPublisherMultiSchema.QUEUE_NAME,
        queueOptions: {
          durable: true,
          autoDelete: false,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeField: 'messageType',
      logMessages: true,
      ...options,
    })
  }
}
