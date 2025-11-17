import type { PublisherBaseEventType, QueuePublisherOptions } from '@message-queue-toolkit/core'

import { AbstractPubSubPublisher } from './AbstractPubSubPublisher.ts'
import type {
  PubSubCreationConfig,
  PubSubDependencies,
  PubSubQueueLocatorType,
} from './AbstractPubSubService.ts'

export type PubSubPublisherFactory<
  T extends AbstractPubSubPublisher<M>,
  M extends PublisherBaseEventType,
> = {
  buildPublisher(
    dependencies: PubSubDependencies,
    options: QueuePublisherOptions<PubSubCreationConfig, PubSubQueueLocatorType, M>,
  ): T
}

export class CommonPubSubPublisher<
  M extends PublisherBaseEventType = PublisherBaseEventType,
> extends AbstractPubSubPublisher<M> {}

export class CommonPubSubPublisherFactory<M extends PublisherBaseEventType = PublisherBaseEventType>
  implements PubSubPublisherFactory<CommonPubSubPublisher<M>, M>
{
  buildPublisher(
    dependencies: PubSubDependencies,
    options: QueuePublisherOptions<PubSubCreationConfig, PubSubQueueLocatorType, M>,
  ): CommonPubSubPublisher<M> {
    return new CommonPubSubPublisher(dependencies, options)
  }
}
