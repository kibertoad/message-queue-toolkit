import { AbstractPublisher } from '../../lib/AbstractPublisher'
import type { AMQPDependencies } from '../../lib/AbstractQueueService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class PermissionPublisher extends AbstractPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: AMQPDependencies) {
    super(
      {
        queueName: PermissionPublisher.QUEUE_NAME,
        messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      },
      dependencies,
    )
  }
}
