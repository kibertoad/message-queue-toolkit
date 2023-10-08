import type { Either } from '@lokalise/node-core'
import type { BarrierResult } from '@message-queue-toolkit/core'

import type {
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig,
} from '../../lib/sqs/AbstractSqsConsumer'
import { AbstractSqsConsumerMonoSchema } from '../../lib/sqs/AbstractSqsConsumerMonoSchema'
import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsService'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type { PERMISSIONS_MESSAGE_TYPE } from './userConsumerSchemas'
import { PERMISSIONS_MESSAGE_SCHEMA } from './userConsumerSchemas'

export class SqsPermissionConsumerMonoSchema extends AbstractSqsConsumerMonoSchema<
  PERMISSIONS_MESSAGE_TYPE,
  string[][]
> {
  public static QUEUE_NAME = 'user_permissions'

  constructor(
    dependencies: SQSConsumerDependencies,
    options:
      | Pick<NewSQSConsumerOptions<SQSCreationConfig>, 'creationConfig' | 'logMessages'>
      | Pick<ExistingSQSConsumerOptions, 'locatorConfig' | 'logMessages'> = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerMonoSchema.QUEUE_NAME,
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
      ...options,
    })
  }

  protected override async preHandlerBarrier(
    message: PERMISSIONS_MESSAGE_TYPE,
    _messageType: string,
  ): Promise<BarrierResult<string[][]>> {
    const matchedUserPermissions = message.userIds.reduce((acc, userId) => {
      if (userPermissionMap[userId]) {
        acc.push(userPermissionMap[userId])
      }
      return acc
    }, [] as string[][])

    if (matchedUserPermissions && matchedUserPermissions.length == message.userIds.length) {
      return {
        isPassing: true,
        output: matchedUserPermissions,
      }
    }

    // not all users were already created, we need to wait to be able to set permissions
    return {
      isPassing: false,
    }
  }

  override async processMessage(
    message: PERMISSIONS_MESSAGE_TYPE,
    _messageType: string,
    matchedUserPermissions: string[][],
  ): Promise<Either<'retryLater', 'success'>> {
    // Do not do this in production, some kind of bulk insertion is needed here
    for (const userPermissions of matchedUserPermissions) {
      userPermissions.push(...message.permissions)
    }

    return {
      result: 'success',
    }
  }
}
