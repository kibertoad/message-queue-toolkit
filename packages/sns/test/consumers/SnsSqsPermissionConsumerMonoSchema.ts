import type { Either } from '@lokalise/node-core'
import type { BarrierResult } from '@message-queue-toolkit/core'

import type {
  SNSSQSConsumerDependencies,
  NewSnsSqsConsumerOptions,
  ExistingSnsSqsConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumerMonoSchema'
import { AbstractSnsSqsConsumerMonoSchema } from '../../lib/sns/AbstractSnsSqsConsumerMonoSchema'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'

export class SnsSqsPermissionConsumerMonoSchema extends AbstractSnsSqsConsumerMonoSchema<
  PERMISSIONS_MESSAGE_TYPE,
  number
> {
  public static CONSUMED_QUEUE_NAME = 'user_permissions'
  public static SUBSCRIBED_TOPIC_NAME = 'user_permissions'

  public preHandlerBarrierCounter: number = 0

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | Pick<NewSnsSqsConsumerOptions, 'creationConfig'>
      | Pick<ExistingSnsSqsConsumerOptions, 'locatorConfig'> = {
      creationConfig: {
        queue: {
          QueueName: SnsSqsPermissionConsumerMonoSchema.CONSUMED_QUEUE_NAME,
        },
        topic: {
          Name: SnsSqsPermissionConsumerMonoSchema.SUBSCRIBED_TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      deletionConfig: {
        deleteIfExists: true,
      },
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
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

  async preHandlerBarrier(_message: PERMISSIONS_MESSAGE_TYPE): Promise<BarrierResult<number>> {
    this.preHandlerBarrierCounter++
    return {
      isPassing: this.preHandlerBarrierCounter > 2,
      output: this.preHandlerBarrierCounter,
    }
  }
}
