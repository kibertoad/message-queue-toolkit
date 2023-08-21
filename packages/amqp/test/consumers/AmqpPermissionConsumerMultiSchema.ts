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

export class AmqpPermissionConsumerMultiSchema extends AbstractAmqpConsumerMultiSchema<
  SupportedEvents,
  AmqpPermissionConsumerMultiSchema
> {
  public static QUEUE_NAME = 'user_permissions_multi'

  public addCounter = 0
  public addBarrierCounter = 0
  public removeCounter = 0

  constructor(dependencies: AMQPConsumerDependencies, options?: Partial<NewAMQPConsumerOptions>) {
    super(dependencies, {
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
        AmqpPermissionConsumerMultiSchema
      >()
        .addConfig(
          PERMISSIONS_ADD_MESSAGE_SCHEMA,
          async (_message, _context) => {
            this.addCounter++
            return {
              result: 'success',
            }
          },
          {
            preHandlerBarrier: (_message) => {
              this.addBarrierCounter++
              return Promise.resolve(this.addBarrierCounter > 0)
            },
          },
        )
        .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, async (_message, _context) => {
          this.removeCounter++
          return {
            result: 'success',
          }
        })
        .build(),
      messageTypeField: 'messageType',
      ...options,
    })
  }
}
