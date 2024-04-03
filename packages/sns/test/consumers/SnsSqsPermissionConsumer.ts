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
  SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  prehandlerCount: number
}

type SnsSqsPermissionConsumerOptions = Pick<
  SNSSQSConsumerOptions<SupportedMessages, ExecutionContext, PrehandlerOutput>,
  'creationConfig' | 'locatorConfig' | 'deletionConfig' | 'deadLetterQueue'
> & {
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

export class SnsSqsPermissionConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
> {
  public static readonly CONSUMED_QUEUE_NAME = 'user_permissions_multi'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'user_permissions_multi'

  public addCounter = 0
  public addBarrierCounter = 0
  public removeCounter = 0
  public prehandlerCounter = 0

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SnsSqsPermissionConsumerOptions = {
      creationConfig: {
        queue: {
          QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME,
        },
        topic: {
          Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME,
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
        deletionConfig: options.deletionConfig ?? {
          deleteIfExists: true,
        },
        deadLetterQueue: options.deadLetterQueue,
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options.creationConfig ?? {
                queue: { QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME },
                topic: { Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME },
              },
            }),
        messageTypeField: 'messageType',
        consumerOverrides: {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
      },
      {
        incrementAmount: 1,
      },
    )
  }

  get subscriptionProps() {
    return {
      topicArn: this.topicArn,
      queueUrl: this.queueUrl,
      subscriptionArn: this.subscriptionArn,
      deadLetterQueueUrl: this.deadLetterQueueUrl,
    }
  }
}
