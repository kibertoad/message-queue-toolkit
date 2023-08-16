import { AbstractSqsPublisherMonoSchema } from '../../lib/sqs/AbstractSqsPublisherMonoSchema'
import type { SQSDependencies } from '../../lib/sqs/AbstractSqsService'
import type { PERMISSIONS_MESSAGE_TYPE } from '../consumers/userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from '../consumers/userConsumerSchemas'

export class SqsPermissionPublisherMonoSchema extends AbstractSqsPublisherMonoSchema<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: SQSDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionPublisherMonoSchema.QUEUE_NAME,
        },
      },
      deletionConfig: {
        deleteIfExists: false,
      },
      logMessages: true,
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
    })
  }
}
