import type { Either } from '@lokalise/node-core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { BarrierResult, Prehandler, PreHandlingOutputs } from '@message-queue-toolkit/core'

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

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

type SqsPermissionConsumerOptions = Pick<
  SQSConsumerOptions<SupportedMessages, ExecutionContext, PrehandlerOutput>,
  'creationConfig' | 'locatorConfig' | 'logMessages' | 'deletionConfig' | 'deadLetterQueue'
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
      return {
        result: 'success',
      }
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
        consumerOverrides: {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PrehandlerOutput
        >()
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
