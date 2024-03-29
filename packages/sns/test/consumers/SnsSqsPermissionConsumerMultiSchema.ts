import type { Either } from '@lokalise/node-core'
import {
  type BarrierResult,
  MessageHandlerConfigBuilder,
  type Prehandler,
  type PrehandlingOutputs,
} from '@message-queue-toolkit/core'
import type { PrehandlerResult } from '@message-queue-toolkit/core/dist/lib/queues/HandlerContainer'

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

type SnsSqsPermissionConsumerMultiSchemaOptions = (
  | Pick<NewSnsSqsConsumerOptions, 'creationConfig' | 'deletionConfig'>
  | Pick<ExistingSnsSqsConsumerOptions, 'locatorConfig'>
) & {
  addPreHandlerBarrier?: (
    message: SupportedMessages,
    _executionContext: ExecutionContext,
    prehandlerOutput: PrehandlerOutput,
  ) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedMessages,
    context: ExecutionContext,
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PrehandlerOutput>[]
}

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  prehandlerCount: number
}

export class SnsSqsPermissionConsumerMultiSchema extends AbstractSnsSqsConsumerMultiSchema<
  SupportedMessages,
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
    options: SnsSqsPermissionConsumerMultiSchemaOptions = {
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
    const defaultRemoveHandler = async (
      _message: SupportedMessages,
      context: ExecutionContext,
      _prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return {
        result: 'success',
      }
    }

    super(
      dependencies,
      {
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (_message, context, _prehandlingOutputs) => {
              this.addCounter += context.incrementAmount
              return {
                result: 'success',
              }
            },
            {
              prehandlers: [
                (
                  message: SupportedMessages,
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
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options.removeHandlerOverride ?? defaultRemoveHandler,
            {
              prehandlers: options.removePreHandlers,
            },
          )
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
          NewSnsSqsConsumerOptionsMulti<SupportedMessages, ExecutionContext, PrehandlerOutput>,
          'creationConfig' | 'logMessages'
        >),
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
