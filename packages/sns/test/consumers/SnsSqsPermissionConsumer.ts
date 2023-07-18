import type { Either } from '@lokalise/node-core'

import type {
  SNSSQSConsumerDependencies,
  NewSnsSqsConsumerOptions,
    ExistingSnsSqsConsumerOptions
} from '../../lib/sns/AbstractSnsSqsConsumer'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'
import {ExistingSQSConsumerOptions, NewSQSConsumerOptions} from "@message-queue-toolkit/sqs";

export class SnsSqsPermissionConsumer extends AbstractSnsSqsConsumer<PERMISSIONS_MESSAGE_TYPE> {
  public static CONSUMED_QUEUE_NAME = 'user_permissions'
  public static SUBSCRIBED_TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
        | Pick<NewSnsSqsConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'queueConfig'>
        | Pick<ExistingSnsSqsConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'queueLocator'> = {
      queueConfig: {
        QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME,
      }
    }
  ) {
    super(dependencies, {
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      topicConfig: {
        Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME,
      },
      subscriptionConfig: {},
      ...options,
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
