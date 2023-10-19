import type { BarrierResult } from '@message-queue-toolkit/core'
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

export class AmqpPermissionConsumerMultiSchema extends AbstractAmqpConsumerMultiSchema<
  SupportedEvents,
  ExecutionContext
> {
  public static QUEUE_NAME = 'user_permissions_multi'

  public addCounter = 0
  public removeCounter = 0

  constructor(
    dependencies: AMQPConsumerDependencies,
    options?: Partial<NewAMQPConsumerOptions> & {
      addPreHandlerBarrier?: (message: SupportedEvents) => Promise<BarrierResult<number>>
    },
  ) {
    super(
      dependencies,
      {
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
        handlers: new MessageHandlerConfigBuilder<SupportedEvents, ExecutionContext>()
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
          .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, async (_message, context) => {
            this.removeCounter += context.incrementAmount
            return {
              result: 'success',
            }
          })
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
