import type { Either } from '@lokalise/node-core'
import type { BarrierResult, PreHandlingOutputs, Prehandler } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type {
  SNSSQSConsumerDependencies,
  SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer.ts'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE_FIFO,
  PERMISSIONS_REMOVE_MESSAGE_TYPE_FIFO,
} from './userConsumerSchemasFifo.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO,
} from './userConsumerSchemasFifo.ts'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE_FIFO | PERMISSIONS_REMOVE_MESSAGE_TYPE_FIFO
type ExecutionContext = {
  incrementAmount: number
}
type PreHandlerOutput = {
  preHandlerCount: number
}

type SnsSqsPermissionConsumerFifoOptions = Partial<
  Extract<
    SNSSQSConsumerOptions<SupportedMessages, ExecutionContext, PreHandlerOutput>,
    { fifoQueue: true }
  >
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
  addHandlerOverride?: (
    message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PreHandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PreHandlerOutput>[]
}

export class SnsSqsPermissionConsumerFifo extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PreHandlerOutput
> {
  public static readonly CONSUMED_QUEUE_NAME = 'user_permissions_fifo.fifo'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'user_permissions_fifo.fifo'

  public addCounter = 0
  public removeCounter = 0
  public processedMessagesIds: Set<string> = new Set()

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SnsSqsPermissionConsumerFifoOptions = {
      creationConfig: {
        queue: {
          QueueName: SnsSqsPermissionConsumerFifo.CONSUMED_QUEUE_NAME,
          Attributes: {
            FifoQueue: 'true',
            ContentBasedDeduplication: 'false',
          },
        },
        topic: {
          Name: SnsSqsPermissionConsumerFifo.SUBSCRIBED_TOPIC_NAME,
          Attributes: {
            FifoTopic: 'true',
            ContentBasedDeduplication: 'false',
          },
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

    const defaultAddHandler = (
      message: SupportedMessages,
      context: ExecutionContext,
      barrierOutput: PreHandlingOutputs<PreHandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      if (options.addPreHandlerBarrier && !barrierOutput) {
        return Promise.resolve({ error: 'retryLater' })
      }
      this.addCounter += context.incrementAmount
      this.processedMessagesIds.add(message.id)
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
            PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO,
            options.addHandlerOverride ?? defaultAddHandler,
            {
              preHandlers: [
                (_message, _context, preHandlerOutput, next) => {
                  preHandlerOutput.preHandlerCount = (preHandlerOutput.preHandlerCount || 0) + 1
                  next({
                    result: 'success',
                  })
                },
              ],
              preHandlerBarrier: options.addPreHandlerBarrier,
            },
          )
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO,
            options.removeHandlerOverride ?? defaultRemoveHandler,
            {
              preHandlers: options.removePreHandlers,
            },
          )
          .build(),
        deletionConfig: options.deletionConfig ?? {
          deleteIfExists: false,
        },
        payloadStoreConfig: options.payloadStoreConfig,
        consumerOverrides: options.consumerOverrides ?? {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        deadLetterQueue: options.deadLetterQueue,
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig, creationConfig: options.creationConfig as any }
          : {
              creationConfig: options.creationConfig ?? {
                queue: {
                  QueueName: SnsSqsPermissionConsumerFifo.CONSUMED_QUEUE_NAME,
                  Attributes: {
                    FifoQueue: 'true',
                    ContentBasedDeduplication: 'false',
                  },
                },
                topic: {
                  Name: SnsSqsPermissionConsumerFifo.SUBSCRIBED_TOPIC_NAME,
                  Attributes: {
                    FifoTopic: 'true',
                    ContentBasedDeduplication: 'false',
                  },
                },
              },
            }),
        fifoQueue: true,
        messageTypeResolver: { messageTypePath: 'messageType' },
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
        maxRetryDuration: options.maxRetryDuration,
        concurrentConsumersAmount: options.concurrentConsumersAmount,
        barrierSleepCheckIntervalInMsecs: options.barrierSleepCheckIntervalInMsecs,
        barrierVisibilityExtensionIntervalInMsecs:
          options.barrierVisibilityExtensionIntervalInMsecs,
        barrierVisibilityTimeoutInSeconds: options.barrierVisibilityTimeoutInSeconds,
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
