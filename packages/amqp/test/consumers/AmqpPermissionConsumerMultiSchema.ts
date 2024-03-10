import type { Either } from '@lokalise/node-core'
import type { BarrierResult, Prehandler, PrehandlingOutputs } from '@message-queue-toolkit/core'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { NewAMQPConsumerOptions } from '../../lib/AbstractAmqpBaseConsumer'
import { AbstractAmqpConsumerMultiSchema } from '../../lib/AbstractAmqpConsumerMultiSchema'
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

export class AmqpPermissionConsumerMultiSchema extends AbstractAmqpConsumerMultiSchema<
  SupportedEvents,
  ExecutionContext,
  PrehandlerOutput
> {
  public static QUEUE_NAME = 'user_permissions_multi'

  public addCounter = 0
  public removeCounter = 0

  constructor(
    dependencies: AMQPConsumerDependencies,
    options: Partial<
      NewAMQPConsumerOptions<SupportedEvents, ExecutionContext, PrehandlerOutput>
    > & {
      addPreHandlerBarrier?: (message: SupportedEvents) => Promise<BarrierResult<number>>
      removeHandlerOverride?: (
        _message: SupportedEvents,
        context: ExecutionContext,
        prehandlingOutputs: PrehandlingOutputs<PrehandlerOutput, number>,
      ) => Promise<Either<'retryLater', 'success'>>
      removePreHandlers?: Prehandler<SupportedEvents, ExecutionContext, PrehandlerOutput>[]
    },
  ) {
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
        handlerSpy: true,
        creationConfig: {
          queueName: AmqpPermissionConsumerMultiSchema.QUEUE_NAME,
          queueOptions: {
            durable: true,
            autoDelete: false,
          },
        },
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
                return { error: 'retryLater' }
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
        messageTypeField: 'messageType',
        ...options,
      },
      {
        incrementAmount: 1,
      },
    )
  }
}
