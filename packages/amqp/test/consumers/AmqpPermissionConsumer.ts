import type { Either } from '@lokalise/node-core'
import type { BarrierResult, PreHandlingOutputs, Prehandler } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { AMQPConsumerOptions } from '../../lib/AbstractAmqpConsumer.ts'
import { AbstractAmqpQueueConsumer } from '../../lib/AbstractAmqpQueueConsumer.ts'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService.ts'

import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from './userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from './userConsumerSchemas.ts'

type SupportedEvents = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
  incrementAmount: number
}
type PrehandlerOutput = {
  preHandlerCount: number
}

type AmqpPermissionConsumerOptions = Pick<
  AMQPConsumerOptions<SupportedEvents, ExecutionContext, PrehandlerOutput>,
  'creationConfig' | 'locatorConfig' | 'logMessages' | 'deadLetterQueue' | 'maxRetryDuration'
> & {
  addPreHandlerBarrier?: (message: SupportedEvents) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedEvents,
    context: ExecutionContext,
    preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedEvents, ExecutionContext, PrehandlerOutput>[]
}

export class AmqpPermissionConsumer extends AbstractAmqpQueueConsumer<
  SupportedEvents,
  ExecutionContext,
  PrehandlerOutput
> {
  public static readonly QUEUE_NAME = 'user_permissions_multi'

  public addCounter = 0
  public removeCounter = 0

  constructor(dependencies: AMQPConsumerDependencies, options?: AmqpPermissionConsumerOptions) {
    const defaultRemoveHandler = (
      _message: SupportedEvents,
      context: ExecutionContext,
      _preHandlingOutputs: PreHandlingOutputs<PrehandlerOutput, number>,
    ): Promise<Either<'retryLater', 'success'>> => {
      this.removeCounter += context.incrementAmount
      return Promise.resolve({
        result: 'success',
      })
    }

    super(
      dependencies,
      {
        ...(options?.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options?.creationConfig ?? {
                queueName: AmqpPermissionConsumer.QUEUE_NAME,
                queueOptions: {
                  durable: true,
                  autoDelete: false,
                },
              },
            }),
        deadLetterQueue: options?.deadLetterQueue,
        logMessages: options?.logMessages,
        handlerSpy: true,
        messageTypeResolver: { messageTypePath: 'messageType' },
        deletionConfig: { deleteIfExists: true },
        maxRetryDuration: options?.maxRetryDuration,
        handlers: new MessageHandlerConfigBuilder<
          SupportedEvents,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            (_message, context, barrierOutput) => {
              if (options?.addPreHandlerBarrier && !barrierOutput) {
                throw new Error('barrier is not working')
              }
              this.addCounter += context.incrementAmount
              return Promise.resolve({
                result: 'success',
              })
            },
            {
              preHandlerBarrier: options?.addPreHandlerBarrier,
            },
          )
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options?.removeHandlerOverride ?? defaultRemoveHandler,
            {
              preHandlers: options?.removePreHandlers,
            },
          )
          .build(),
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
