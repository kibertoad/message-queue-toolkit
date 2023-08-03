import { AbstractSqsPublisherMultiSchema } from '../../lib/sqs/AbstractSqsPublisherMultiSchema'
import type { SQSDependencies } from '../../lib/sqs/AbstractSqsService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SqsPermissionPublisherMultiSchema extends AbstractSqsPublisherMultiSchema<SupportedMessages> {
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(dependencies: SQSDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionPublisherMultiSchema.QUEUE_NAME,
        },
      },
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeField: 'messageType',
    })
  }
}
