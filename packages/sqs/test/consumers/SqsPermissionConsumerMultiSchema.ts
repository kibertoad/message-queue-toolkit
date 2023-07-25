import type { Either } from '@lokalise/node-core'

import type {
  ExistingSQSConsumerOptions,
  NewSQSConsumerOptions,
  SQSCreationConfig,
} from '../../lib/sqs/AbstractSqsConsumer'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer'
import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsService'
import { userPermissionMap } from '../repositories/PermissionRepository'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA
} from './userConsumerSchemas'
import {AbstractSqsConsumerMultiSchema} from "../../lib/sqs/AbstractSqsConsumerMultiSchema";
import {MessageHandlerConfig, MessageHandlerConfig} from "@message-queue-toolkit/core";

export class SqsPermissionConsumerMultiSchema extends AbstractSqsConsumerMultiSchema<any> {
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSConsumerDependencies,
    options:
      | Pick<NewSQSConsumerOptions<PERMISSIONS_MESSAGE_TYPE, SQSCreationConfig>, 'creationConfig'>
      | Pick<ExistingSQSConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'locatorConfig'> = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerMultiSchema.QUEUE_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageSchema: PERMISSIONS_MESSAGE_SCHEMA,
      messageTypeField: 'messageType',
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      ...options,
    }, {
      handlers: [
        new MessageHandlerConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (message, context: SqsPermissionConsumerMultiSchema) => {
              return {
                result: 'success'
              }
            }
        ),
        new MessageHandlerConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            async (message, context: SqsPermissionConsumerMultiSchema) => {
              message
              return {
                result: 'success'
              }
            }
        )
      ]
    })
  }
}
