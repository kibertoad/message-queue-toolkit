import { setTimeout } from 'node:timers/promises'
import type { Either } from '@lokalise/node-core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { BarrierResult, PreHandlingOutputs, Prehandler } from '@message-queue-toolkit/core'

import type { SQSConsumerDependencies, SQSConsumerOptions } from '../../lib/sqs/AbstractSqsConsumer'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas'

export type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

type SqsPermissionConsumerOptions = Pick<
  SQSConsumerOptions<SupportedMessages, ExecutionContext, PrehandlerOutput>,
  | 'creationConfig'
  | 'locatorConfig'
  | 'logMessages'
  | 'deletionConfig'
  | 'deadLetterQueue'
  | 'consumerOverrides'
  | 'maxRetryDuration'
  | 'payloadStoreConfig'
  | 'consumerMessageDeduplicationConfig'
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
  removePreHandlers?: Prehandler<SupportedMessages, ExecutionContext, PrehandlerOutput>[]
  concurrentConsumersAmount?: number
  delayConsumerProcessingMs?: number // Custom option used for tests only. It delays the processing of the message by the consumer by the specified amount of milliseconds
}

type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  messageId: string
}

export class SqsPermissionConsumer extends AbstractSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
> {
  public addCounter = 0
  public removeCounter = 0
  public processedMessagesIds: Set<string> = new Set()
  public static readonly QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSConsumerDependencies,
    options: SqsPermissionConsumerOptions = {
      creationConfig: {
        queue: {
          QueueName: SqsPermissionConsumer.QUEUE_NAME,
        },
      },
    },
  ) {
    const defaultRemoveHandler = async (
      _message: SupportedMessages,
      context: ExecutionContext,
      _preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      if (options.delayConsumerProcessingMs) {
        await setTimeout(options.delayConsumerProcessingMs)
      }
      return Promise.resolve({
        result: 'success',
      })
    }

    super(
      dependencies,
      {
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options.creationConfig ?? {
                queue: { QueueName: SqsPermissionConsumer.QUEUE_NAME },
              },
            }),
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
        payloadStoreConfig: options.payloadStoreConfig,
        consumerMessageDeduplicationConfig: options.consumerMessageDeduplicationConfig,
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (message, context, barrierOutput) => {
              if (options.delayConsumerProcessingMs) {
                await setTimeout(options.delayConsumerProcessingMs)
              }
              if (message.metadata?.forceConsumerToThrow) {
                return Promise.reject(new Error('Forced error'))
              }
              if (options.addPreHandlerBarrier && !barrierOutput) {
                return Promise.resolve({ error: 'retryLater' })
              }
              this.addCounter += context.incrementAmount
              this.processedMessagesIds.add(message.id)
              return Promise.resolve({ result: 'success' })
            },
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
    }
  }

  public get dlqUrl() {
    return this.deadLetterQueueUrl ?? ''
  }
}
