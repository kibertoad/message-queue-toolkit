import type { Either } from '@lokalise/node-core'
import type { BarrierResult, PreHandlingOutputs, Prehandler } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type {
  SQSConsumerDependencies,
  SQSConsumerOptions,
} from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas.ts'

export type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

type SqsPermissionConsumerFifoOptions = Partial<
  Extract<
    SQSConsumerOptions<SupportedMessages, ExecutionContext, PrehandlerOutput>,
    { fifoQueue: true }
  >
> & {
  addPreHandlerBarrier?: (
    message: SupportedMessages,
    _executionContext: ExecutionContext,
    preHandlerOutput: PrehandlerOutput,
  ) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  addHandlerOverride?: (
    message: SupportedMessages,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PrehandlerOutput>[]
  concurrentConsumersAmount?: number
}

type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  messageId: string
}

export class SqsPermissionConsumerFifo extends AbstractSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
> {
  public addCounter = 0
  public removeCounter = 0
  public processedMessagesIds: Set<string> = new Set()
  public static readonly QUEUE_NAME = 'user_permissions_fifo.fifo'

  constructor(
    dependencies: SQSConsumerDependencies,
    options: SqsPermissionConsumerFifoOptions = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumerFifo.QUEUE_NAME,
          Attributes: {
            FifoQueue: 'true',
            ContentBasedDeduplication: 'false',
          },
        },
      },
    },
  ) {
    const defaultRemoveHandler = (
      _message: SupportedMessages,
      context: ExecutionContext,
      _preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return Promise.resolve({
        result: 'success',
      })
    }
    const defaultAddHandler = (
      message: SupportedMessages,
      context: ExecutionContext,
      barrierOutput: PreHandlingOutputs<PrehandlerOutput, number>,
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
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options.creationConfig ?? {
                queue: {
                  QueueName: SqsPermissionConsumerFifo.QUEUE_NAME,
                  Attributes: {
                    FifoQueue: 'true',
                    ContentBasedDeduplication: 'false',
                  },
                },
              },
            }),
        fifoQueue: true,
        logMessages: options.logMessages,
        deletionConfig: options.deletionConfig ?? {
          deleteIfExists: true,
        },
        deadLetterQueue: options.deadLetterQueue,
        messageTypeField: 'messageType',
        handlerSpy: true,
        consumerOverrides: options.consumerOverrides ?? {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        concurrentConsumersAmount: options.concurrentConsumersAmount,
        maxRetryDuration: options.maxRetryDuration,
        barrierSleepCheckIntervalInMsecs: options.barrierSleepCheckIntervalInMsecs,
        barrierVisibilityExtensionIntervalInMsecs:
          options.barrierVisibilityExtensionIntervalInMsecs,
        barrierVisibilityTimeoutInSeconds: options.barrierVisibilityTimeoutInSeconds,
        payloadStoreConfig: options.payloadStoreConfig,
        messageDeduplicationConfig: options.messageDeduplicationConfig,
        enableConsumerDeduplication: options.enableConsumerDeduplication,
        messageDeduplicationIdField: 'deduplicationId',
        messageDeduplicationOptionsField: 'deduplicationOptions',
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            options.addHandlerOverride ?? defaultAddHandler,
            {
              preHandlerBarrier: options.addPreHandlerBarrier,
              preHandlers: [
                (message, _context, preHandlerOutput, next) => {
                  preHandlerOutput.messageId = message.id
                  next({
                    result: 'success',
                  })
                },
              ],
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
      },
      {
        incrementAmount: 1,
      },
    )
  }

  public get queueProps() {
    return {
      name: this.queueName,
      url: this.queueUrl,
      arn: this.queueArn,
      isFifo: this.isFifoQueue,
    }
  }

  public get dlqUrl() {
    return this.deadLetterQueueUrl ?? ''
  }
}
