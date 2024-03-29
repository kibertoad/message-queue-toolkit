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

type Options =
  | Pick<NewSnsSqsConsumerOptions, 'creationConfig'>
  | Pick<ExistingSnsSqsConsumerOptions, 'locatorConfig'>

export class SnsSqsPermissionConsumerMonoSchema extends AbstractSnsSqsConsumerMonoSchema<
  PERMISSIONS_MESSAGE_TYPE,
  undefined,
  undefined,
  number
> {
  public static CONSUMED_QUEUE_NAME = 'user_permissions'
  public static SUBSCRIBED_TOPIC_NAME = 'user_permissions'

  public preHandlerBarrierCounter: number = 0

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: Options = {
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
      handlerSpy: true,
      messageTypeField: 'messageType',
      deletionConfig: {
        deleteIfExists: true,
      },
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      subscriptionConfig: {
        updateAttributesIfExists: false,
      },
      ...(options as Omit<NewSnsSqsConsumerOptions, 'messageTypeField'>),
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
    if (this.preHandlerBarrierCounter < 3) {
      return {
        isPassing: false,
      }
    }
    return {
      isPassing: true,
      output: this.preHandlerBarrierCounter,
    }
  }
}
