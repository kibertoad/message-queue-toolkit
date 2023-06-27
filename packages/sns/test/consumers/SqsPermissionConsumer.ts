import type { Either } from '@lokalise/node-core'

import { AbstractSqsConsumer, SQSConsumerDependencies } from '@message-queue-toolkit/sqs'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'
import { deserializeSNSMessage } from '../../lib/sns/snsMessageDeserializer'

export class SqsPermissionConsumer extends AbstractSqsConsumer<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      queueName: SqsPermissionConsumer.QUEUE_NAME,
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      deserializer: deserializeSNSMessage,
      messageTypeField: 'messageType',
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      queueConfiguration: {},
    })
  }

  override async processMessage(
    message: PERMISSIONS_MESSAGE_TYPE,
  ): Promise<Either<'retryLater', 'success'>> {
    const matchedUserPermissions = message.userIds.reduce((acc, userId) => {
      if (userPermissionMap[userId]) {
        acc.push(userPermissionMap[userId])
      }
      return acc
    }, [] as string[][])

    if (!matchedUserPermissions || matchedUserPermissions.length < message.userIds.length) {
      // not all users were already created, we need to wait to be able to set permissions
      return {
        error: 'retryLater',
      }
    }

    // Do not do this in production, some kind of bulk insertion is needed here
    for (const userPermissions of matchedUserPermissions) {
      userPermissions.push(...message.permissions)
    }

    return {
      result: 'success',
    }
  }
}
