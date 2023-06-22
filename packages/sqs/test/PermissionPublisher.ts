import { AbstractSqsPublisher } from '../lib/sqs/AbstractSqsPublisher'
import type { SQSDependencies } from '../lib/sqs/AbstractSqsService'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'

export class PermissionPublisher extends AbstractSqsPublisher<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: SQSDependencies) {
    super(dependencies, {
      queueName: PermissionPublisher.QUEUE_NAME,
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
    })
  }
}
