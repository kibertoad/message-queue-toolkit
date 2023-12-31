import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { BarrierResult } from '@message-queue-toolkit/core'

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

type SqsPermissionConsumerMultiSchemaOptions = (
  | Pick<
      NewSQSConsumerOptionsMultiSchema<
        SupportedMessages,
        SqsPermissionConsumerMultiSchema,
        SQSCreationConfig
      >,
      'creationConfig' | 'logMessages'
    >
  | Pick<
      ExistingSQSConsumerOptionsMultiSchema<SupportedMessages, SqsPermissionConsumerMultiSchema>,
      'locatorConfig' | 'logMessages'
    >
) & {
  addPreHandlerBarrier?: (message: SupportedMessages) => Promise<BarrierResult<number>>
}

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}

export class SqsPermissionConsumerMultiSchema extends AbstractSqsConsumerMultiSchema<
  SupportedMessages,
  ExecutionContext
> {
  public addCounter = 0
  public removeCounter = 0
  public static QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSConsumerDependencies,
    options: SqsPermissionConsumerMultiSchemaOptions = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerMultiSchema.QUEUE_NAME,
        },
      },
    },
  ) {
    super(
      dependencies,
      {
        messageTypeField: 'messageType',
        handlerSpy: true,
        deletionConfig: {
          deleteIfExists: true,
        },
        consumerOverrides: {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        ...options,
        handlers: new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (_message, context, barrierOutput) => {
              if (options.addPreHandlerBarrier && !barrierOutput) {
                return { error: 'retryLater' }
              }
              this.addCounter += context.incrementAmount
              return {
                result: 'success',
              }
            },
            {
              preHandlerBarrier: options.addPreHandlerBarrier,
            },
          )
          .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, async (_message, context) => {
            this.removeCounter += context.incrementAmount
            return {
              result: 'success',
            }
          })
          .build(),
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
