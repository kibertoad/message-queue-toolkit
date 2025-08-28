import type { CommonCreationConfigType, PublisherBaseEventType } from '@message-queue-toolkit/core'

import type { AbstractAmqpPublisher, AMQPPublisherOptions } from './AbstractAmqpPublisher.ts'
import type { AmqpQueueMessageOptions } from './AbstractAmqpQueuePublisher.ts'
import { AbstractAmqpQueuePublisher } from './AbstractAmqpQueuePublisher.ts'
import type {
  AMQPDependencies,
  AMQPQueueCreationConfig,
  AMQPQueueLocator,
} from './AbstractAmqpService.ts'
import type {
  AMQPTopicPublisherOptions,
  AmqpTopicMessageOptions,
} from './AbstractAmqpTopicPublisher.ts'
import { AbstractAmqpTopicPublisher } from './AbstractAmqpTopicPublisher.ts'

export type AmqpPublisherFactory<
  T extends AbstractAmqpPublisher<M, MessageOptions, CommonCreationConfigType, object>,
  M extends PublisherBaseEventType,
  MessageOptions,
  PublisherOptions extends Omit<
    AMQPPublisherOptions<M, CommonCreationConfigType, object>,
    'creationConfig'
  >,
> = {
  buildPublisher(dependencies: AMQPDependencies, options: PublisherOptions): T
}

export class CommonAmqpQueuePublisher<
  M extends PublisherBaseEventType = PublisherBaseEventType,
> extends AbstractAmqpQueuePublisher<M> {}

export class CommonAmqpTopicPublisher<
  M extends PublisherBaseEventType = PublisherBaseEventType,
> extends AbstractAmqpTopicPublisher<M> {}

export class CommonAmqpQueuePublisherFactory<
  M extends PublisherBaseEventType = PublisherBaseEventType,
> implements
    AmqpPublisherFactory<
      CommonAmqpQueuePublisher<M>,
      M,
      AmqpQueueMessageOptions,
      AMQPPublisherOptions<M, AMQPQueueCreationConfig, AMQPQueueLocator>
    >
{
  buildPublisher(
    dependencies: AMQPDependencies,
    options: AMQPPublisherOptions<M, AMQPQueueCreationConfig, AMQPQueueLocator>,
  ): CommonAmqpQueuePublisher<M> {
    return new CommonAmqpQueuePublisher(dependencies, options)
  }
}

export class CommonAmqpTopicPublisherFactory<
  M extends PublisherBaseEventType = PublisherBaseEventType,
> implements
    AmqpPublisherFactory<
      CommonAmqpTopicPublisher<M>,
      M,
      AmqpTopicMessageOptions,
      AMQPTopicPublisherOptions<M>
    >
{
  buildPublisher(
    dependencies: AMQPDependencies,
    options: AMQPTopicPublisherOptions<M>,
  ): CommonAmqpTopicPublisher<M> {
    return new CommonAmqpTopicPublisher(dependencies, options)
  }
}
