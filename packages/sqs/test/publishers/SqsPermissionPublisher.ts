import { AbstractSqsPublisher } from '../../lib/sqs/AbstractSqsPublisher'
import type { SQSDependencies } from '../../lib/sqs/AbstractSqsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class SqsPermissionPublisher extends AbstractSqsPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: SQSDependencies) {
    super(dependencies, {
      queueName: SqsPermissionPublisher.QUEUE_NAME,
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
    })
  }
}
