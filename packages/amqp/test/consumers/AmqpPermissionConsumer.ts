import type { Either } from '@lokalise/node-core'

import { AbstractAmqpConsumer } from '../../lib/AbstractAmqpConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'

export class AmqpPermissionConsumer extends AbstractAmqpConsumer<PERMISSIONS_MESSAGE_TYPE> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(dependencies: AMQPConsumerDependencies) {
    super(dependencies, {
      creationConfig: {
        queueName: AmqpPermissionConsumer.QUEUE_NAME,
        queueOptions: {
          durable: true,
          autoDelete: false,
        },
      },
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
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
