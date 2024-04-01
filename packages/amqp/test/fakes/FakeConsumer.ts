import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import z from 'zod'

import { AbstractAmqpConsumer } from '../../lib/AbstractAmqpConsumer'
import type { AMQPConsumerDependencies } from '../../lib/AbstractAmqpService'

export const COMMON_MESSAGE_SCHEMA = z.object({
  messageType: z.string(),
})

export type CommonMessage = z.infer<typeof COMMON_MESSAGE_SCHEMA>
export class FakeConsumer extends AbstractAmqpConsumer<CommonMessage, unknown> {
  constructor(dependencies: AMQPConsumerDependencies) {
    super(
      dependencies,
      {
        creationConfig: {
          queueName: 'dummy',
          queueOptions: {
            durable: true,
            autoDelete: false,
          },
        },
        messageTypeField: 'messageType',
        handlers: new MessageHandlerConfigBuilder<CommonMessage, unknown>()
          .addConfig(COMMON_MESSAGE_SCHEMA, () => Promise.resolve({ result: 'success' }))
          .build(),
      },
      {},
    )
  }
}
