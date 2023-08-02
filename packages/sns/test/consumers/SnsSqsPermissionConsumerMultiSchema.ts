import {MessageHandlerConfig, MessageHandlerConfigBuilder} from '@message-queue-toolkit/core'
import type {
  SNSSQSConsumerDependencies,
  NewSnsSqsConsumerOptions,
  ExistingSnsSqsConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumerMonoSchema'
import { AbstractSnsSqsConsumerMultiSchema } from '../../lib/sns/AbstractSnsSqsConsumerMultiSchema'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE
} from './userConsumerSchemas'
import {PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA} from "./userConsumerSchemas";

type SupportedEvents = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SnsSqsPermissionConsumerMultiSchema extends AbstractSnsSqsConsumerMultiSchema<PERMISSIONS_MESSAGE_TYPE, SnsSqsPermissionConsumerMultiSchema> {
  public static CONSUMED_QUEUE_NAME = 'user_permissions'
  public static SUBSCRIBED_TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | Pick<NewSnsSqsConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'creationConfig'>
      | Pick<ExistingSnsSqsConsumerOptions<PERMISSIONS_MESSAGE_TYPE>, 'locatorConfig'> = {
      creationConfig: {
        queue: {
          QueueName: SnsSqsPermissionConsumerMultiSchema.CONSUMED_QUEUE_NAME,
        },
        topic: {
          Name: SnsSqsPermissionConsumerMultiSchema.SUBSCRIBED_TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      handlers: new MessageHandlerConfigBuilder<SupportedEvents, SnsSqsPermissionConsumerMultiSchema>()
          .addConfig(
          PERMISSIONS_ADD_MESSAGE_SCHEMA,
          async (message, context) => {
            return {
              result: 'success',
            }
          },
        )
          .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
          async (message, context) => {
            message
            return {
              result: 'success',
            }
          },
        ).build(),
      messageTypeField: 'messageType',
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      subscriptionConfig: {},
      ...options,
    })
  }
}
