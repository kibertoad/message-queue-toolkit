import type { AMQPPublisherOptions } from '../../lib/AbstractAmqpPublisher'
import { AbstractAmqpPublisher } from '../../lib/AbstractAmqpPublisher'
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

export class AmqpPermissionPublisher extends AbstractAmqpPublisher<SupportedTypes> {
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: AMQPDependencies,
    options: Pick<
      AMQPPublisherOptions<SupportedTypes>,
      'creationConfig' | 'logMessages' | 'locatorConfig'
    > = {
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
      ...(options.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options.creationConfig ?? {
              queueName: AmqpPermissionPublisher.QUEUE_NAME,
              queueOptions: {
                durable: true,
                autoDelete: false,
              },
            },
          }),
      logMessages: options.logMessages ?? true,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeField: 'messageType',
    })
  }
}
