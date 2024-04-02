import type { Either } from '@lokalise/node-core'
import type { BarrierResult, Prehandler, PrehandlingOutputs } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { AMQPConsumerOptions } from '../../lib/AbstractAmqpConsumer'
import { AbstractAmqpConsumer } from '../../lib/AbstractAmqpConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'

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

type AmqpPermissionConsumerOptions = Pick<
  AMQPConsumerOptions<SupportedEvents, ExecutionContext, PrehandlerOutput>,
  'creationConfig' | 'locatorConfig' | 'logMessages' | 'deadLetterQueue'
> & {
  addPreHandlerBarrier?: (message: SupportedEvents) => Promise<BarrierResult<number>>
  removeHandlerOverride?: (
    _message: SupportedEvents,
    context: ExecutionContext,
    prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, number>,
  ) => Promise<Either<'retryLater', 'success'>>
  removePreHandlers?: Prehandler<SupportedEvents, ExecutionContext, PrehandlerOutput>[]
}

export class AmqpPermissionConsumer extends AbstractAmqpConsumer<
  SupportedEvents,
  ExecutionContext,
  PrehandlerOutput
> {
  public static readonly QUEUE_NAME = 'user_permissions_multi'

  public addCounter = 0
  public removeCounter = 0

  constructor(dependencies: AMQPConsumerDependencies, options?: AmqpPermissionConsumerOptions) {
    const defaultRemoveHandler = async (
      _message: SupportedEvents,
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
        messageTypeField: 'messageType',
        deletionConfig: {
          deleteIfExists: true,
        },
        handlers: new MessageHandlerConfigBuilder<
          SupportedEvents,
          ExecutionContext,
          PrehandlerOutput
        >()
          .addConfig(
            PERMISSIONS_ADD_MESSAGE_SCHEMA,
            async (_message, context, barrierOutput) => {
              if (options?.addPreHandlerBarrier && !barrierOutput) {
                throw new Error('barrier is not working')
              }
              this.addCounter += context.incrementAmount
              return {
                result: 'success',
              }
            },
            {
              preHandlerBarrier: options?.addPreHandlerBarrier,
            },
          )
          .addConfig(
            PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
            options?.removeHandlerOverride ?? defaultRemoveHandler,
            {
              prehandlers: options?.removePreHandlers,
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
