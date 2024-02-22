import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { PrehandlerResult } from '@message-queue-toolkit/core/dist/lib/queues/HandlerContainer'
import type { SQSCreationConfig } from '@message-queue-toolkit/sqs'
import type { NewSQSConsumerOptionsMultiSchema } from '@message-queue-toolkit/sqs/dist/lib/sqs/AbstractSqsConsumerMultiSchema'

import type {
  SNSSQSConsumerDependencies,
  NewSnsSqsConsumerOptions,
  ExistingSnsSqsConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumerMonoSchema'
import type { NewSnsSqsConsumerOptionsMulti } from '../../lib/sns/AbstractSnsSqsConsumerMultiSchema'
import { AbstractSnsSqsConsumerMultiSchema } from '../../lib/sns/AbstractSnsSqsConsumerMultiSchema'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas'

type SupportedEvents = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  prehandlerCount: number
}

export class SnsSqsPermissionConsumerMultiSchema extends AbstractSnsSqsConsumerMultiSchema<
  SupportedEvents,
  ExecutionContext,
  PrehandlerOutput
> {
  public static CONSUMED_QUEUE_NAME = 'user_permissions_multi'
  public static SUBSCRIBED_TOPIC_NAME = 'user_permissions_multi'

  public addCounter = 0
  public addBarrierCounter = 0
  public removeCounter = 0
  public prehandlerCounter = 0

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | Pick<NewSnsSqsConsumerOptions, 'creationConfig'>
      | Pick<ExistingSnsSqsConsumerOptions, 'locatorConfig'> = {
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
    super(
      dependencies,
      {
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<SupportedEvents, ExecutionContext>()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (_message, context, _barrierOutput: number) => {
              this.addCounter += context.incrementAmount
              return {
                result: 'success',
              }
            },
            {
              prehandlers: [
                (
                  message: SupportedEvents,
                  context: ExecutionContext,
                  prehandlerOutput: Partial<PrehandlerOutput>,
                  next: (result: PrehandlerResult) => void,
                ) => {
                  if (message.prehandlerIncrement) {
                    this.prehandlerCounter += message.prehandlerIncrement
                  }
                  next({
                    result: 'success',
                  })
                },
              ],
              preHandlerBarrier: async (_message, context) => {
                this.addBarrierCounter += context.incrementAmount
                if (this.addBarrierCounter < 3) {
                  return {
                    isPassing: false,
                  }
                }

                return {
                  isPassing: true,
                  output: this.addBarrierCounter,
                }
              },
            },
          )
          .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, async (_message, context) => {
            this.removeCounter += context.incrementAmount
            return {
              result: 'success',
            }
          })
          .build(),
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
        // FixMe this casting shouldn't be necessary
        ...(options as Pick<
          NewSnsSqsConsumerOptionsMulti<SupportedEvents, ExecutionContext, PrehandlerOutput>,
          'creationConfig' | 'logMessages'
        >),
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
