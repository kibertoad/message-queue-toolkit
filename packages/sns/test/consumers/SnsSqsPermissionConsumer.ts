import type { Either } from '@lokalise/node-core'
import type {
  BarrierResult,
  PreHandlingOutputs,
  Prehandler,
  PrehandlerResult,
} from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

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
type PreHandlerOutput = {
  preHandlerCount: number
}

type SnsSqsPermissionConsumerOptions = Pick<
  SNSSQSConsumerOptions<SupportedMessages, ExecutionContext, PreHandlerOutput>,
  | 'creationConfig'
  | 'locatorConfig'
  | 'deletionConfig'
  | 'deadLetterQueue'
  | 'consumerOverrides'
  | 'maxRetryDuration'
  | 'payloadStoreConfig'
> & {
  addPreHandlerBarrier?: (
    message: SupportedMessages,
    _executionContext: ExecutionContext,
    preHandlerOutput: PreHandlerOutput,
  ) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PreHandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PreHandlerOutput>[]
}

export class SnsSqsPermissionConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PreHandlerOutput
> {
  public static readonly CONSUMED_QUEUE_NAME = 'user_permissions_multi'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'user_permissions_multi'

  public addCounter = 0
  public addBarrierCounter = 0
  public removeCounter = 0
  public preHandlerCounter = 0

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
    const defaultRemoveHandler = (
      _message: SupportedMessages,
      context: ExecutionContext,
      _preHandlingOutputs: PreHandlingOutputs<PreHandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return Promise.resolve({ result: 'success' })
    }

    super(
      dependencies,
      {
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PreHandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            (_message, context, _preHandlingOutputs) => {
              this.addCounter += context.incrementAmount
              return Promise.resolve({ result: 'success' })
            },
            {
              preHandlers: [
                (
                  message: SupportedMessages,
                  _context: ExecutionContext,
                  _preHandlerOutput: Partial<PreHandlerOutput>,
                  next: (result: PrehandlerResult) => void,
                ) => {
                  if (message.preHandlerIncrement) {
                    this.preHandlerCounter += message.preHandlerIncrement
                  }
                  next({
                    result: 'success',
                  })
                },
              ],
              preHandlerBarrier: (_message, context) => {
                this.addBarrierCounter += context.incrementAmount
                if (this.addBarrierCounter < 3) {
                  return Promise.resolve({ isPassing: false })
                }

                return Promise.resolve({
                  isPassing: true,
                  output: this.addBarrierCounter,
                })
              },
            },
          )
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options.removeHandlerOverride ?? defaultRemoveHandler,
            {
              preHandlers: options.removePreHandlers,
            },
          )
          .build(),
        deletionConfig: options.deletionConfig ?? {
          deleteIfExists: true,
        },
        payloadStoreConfig: options.payloadStoreConfig,
        consumerOverrides: options.consumerOverrides ?? {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        deadLetterQueue: options.deadLetterQueue,
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options.creationConfig ?? {
                queue: {
                  QueueName: SnsSqsPermissionConsumer.CONSUMED_QUEUE_NAME,
                },
                topic: { Name: SnsSqsPermissionConsumer.SUBSCRIBED_TOPIC_NAME },
              },
            }),
        messageTypeField: 'messageType',
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
        maxRetryDuration: options.maxRetryDuration,
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
      queueName: this.queueName,
      subscriptionArn: this.subscriptionArn,
      deadLetterQueueUrl: this.deadLetterQueueUrl,
    }
  }
}
