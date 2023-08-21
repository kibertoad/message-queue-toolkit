import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { SQSCreationConfig } from '../../lib/sqs/AbstractSqsConsumer'
import type {
  ExistingSQSConsumerOptionsMultiSchema,
  NewSQSConsumerOptionsMultiSchema,
} from '../../lib/sqs/AbstractSqsConsumerMultiSchema'
import { AbstractSqsConsumerMultiSchema } from '../../lib/sqs/AbstractSqsConsumerMultiSchema'
import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsService'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SqsPermissionConsumerMultiSchema extends AbstractSqsConsumerMultiSchema<
  SupportedMessages,
  SqsPermissionConsumerMultiSchema
> {
  public addCounter = 0
  public addBarrierCounter = 0
  public removeCounter = 0
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSConsumerDependencies,
    options:
      | Pick<
          NewSQSConsumerOptionsMultiSchema<
            SupportedMessages,
            SqsPermissionConsumerMultiSchema,
            SQSCreationConfig
          >,
          'creationConfig' | 'logMessages'
        >
      | Pick<
          ExistingSQSConsumerOptionsMultiSchema<
            SupportedMessages,
            SqsPermissionConsumerMultiSchema
          >,
          'locatorConfig' | 'logMessages'
        > = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerMultiSchema.QUEUE_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      messageTypeField: 'messageType',
      deletionConfig: {
        deleteIfExists: true,
      },
      consumerOverrides: {
        terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
      },
      ...options,
      handlers: new MessageHandlerConfigBuilder<
        SupportedMessages,
        SqsPermissionConsumerMultiSchema
      >()
        .addConfig(
          PERMISSIONS_ADD_MESSAGE_SCHEMA,
          async (_message, _context) => {
            this.addCounter++
            return {
              result: 'success',
            }
          },
          {
            preHandlerBarrier: (_message) => {
              this.addBarrierCounter++
              return Promise.resolve(this.addBarrierCounter > 0)
            },
          },
        )
        .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, async (_message, _context) => {
          this.removeCounter++
          return {
            result: 'success',
          }
        })
        .build(),
    })
  }

  resetCounters(): void {
    this.removeCounter = 0
    this.addCounter = 0
    this.addBarrierCounter = 0
  }
}
