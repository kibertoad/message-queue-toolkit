import type { BaseEventType } from '@message-queue-toolkit/core'

import type {
  AmqpExchangeMessageOptions,
  AMQPExchangePublisherOptions,
} from './AbstractAmqpExchangePublisher'
import { AbstractAmqpExchangePublisher } from './AbstractAmqpExchangePublisher'
import type { AbstractAmqpPublisher, AMQPPublisherOptions } from './AbstractAmqpPublisher'
import type { AmqpQueueMessageOptions } from './AbstractAmqpQueuePublisher'
import { AbstractAmqpQueuePublisher } from './AbstractAmqpQueuePublisher'
import type { AMQPDependencies } from './AbstractAmqpService'

export type AmqpPublisherFactory<
  T extends AbstractAmqpPublisher<M, MessageOptions>,
  M extends BaseEventType,
  MessageOptions,
  PublisherOptions extends Omit<AMQPPublisherOptions<M>, 'creationConfig'>,
> = {
  buildPublisher(dependencies: AMQPDependencies, options: PublisherOptions): T
}

export class CommonAmqpQueuePublisher<
  M extends BaseEventType = BaseEventType,
> extends AbstractAmqpQueuePublisher<M> {}

export class CommonAmqpExchangePublisher<
  M extends BaseEventType = BaseEventType,
> extends AbstractAmqpExchangePublisher<M> {}

export class CommonAmqpQueuePublisherFactory<M extends BaseEventType = BaseEventType>
  implements
    AmqpPublisherFactory<
      CommonAmqpQueuePublisher<M>,
      M,
      AmqpQueueMessageOptions,
      AMQPPublisherOptions<M>
    >
{
  buildPublisher(
    dependencies: AMQPDependencies,
    options: AMQPPublisherOptions<M>,
  ): CommonAmqpQueuePublisher<M> {
    return new CommonAmqpQueuePublisher(dependencies, options)
  }
}

export class CommonAmqpExchangePublisherFactory<M extends BaseEventType = BaseEventType>
  implements
    AmqpPublisherFactory<
      CommonAmqpExchangePublisher<M>,
      M,
      AmqpExchangeMessageOptions,
      AMQPExchangePublisherOptions<M>
    >
{
  buildPublisher(
    dependencies: AMQPDependencies,
    options: AMQPExchangePublisherOptions<M>,
  ): CommonAmqpExchangePublisher<M> {
    return new CommonAmqpExchangePublisher(dependencies, options)
  }
}
