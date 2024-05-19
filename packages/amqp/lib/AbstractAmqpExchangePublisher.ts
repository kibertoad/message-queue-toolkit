import type * as Buffer from 'node:buffer'

import { objectToBuffer } from '@message-queue-toolkit/core'
import type { Options } from 'amqplib/properties'

import type { AMQPPublisherOptions } from './AbstractAmqpPublisher'
import { AbstractAmqpPublisher } from './AbstractAmqpPublisher'
import type { AMQPDependencies } from './AbstractAmqpService'

export type AMQPExchangePublisherOptions<MessagePayloadType extends object> = Omit<
  AMQPPublisherOptions<MessagePayloadType>,
  'creationConfig'
> & {
  exchange: string
}

export type AmqpExchangeMessageOptions = {
  routingKey: string
  publishOptions: Options.Publish
}

export abstract class AbstractAmqpExchangePublisher<
  MessagePayloadType extends object,
> extends AbstractAmqpPublisher<MessagePayloadType, AmqpExchangeMessageOptions> {
  constructor(
    dependencies: AMQPDependencies,
    options: AMQPExchangePublisherOptions<MessagePayloadType>,
  ) {
    super(dependencies, {
      ...options,
      // FixMe exchange publisher doesn't need queue at all
      creationConfig: {
        queueName: 'dummy',
        queueOptions: {},
        updateAttributesIfExists: false,
      },
      exchange: options.exchange,
      locatorConfig: undefined,
    })
  }

  protected publishInternal(message: Buffer, options: AmqpExchangeMessageOptions): void {
    this.channel.publish(
      this.exchange!,
      options.routingKey,
      objectToBuffer(message),
      options.publishOptions,
    )
  }

  protected createMissingEntities(): Promise<void> {
    this.logger.warn('Missing entity creation is not implemented')
    return Promise.resolve()
  }
}
