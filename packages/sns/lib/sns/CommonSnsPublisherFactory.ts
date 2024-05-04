import type { BaseEventType } from '@message-queue-toolkit/core'

import type { SNSPublisherOptions } from './AbstractSnsPublisher'
import { AbstractSnsPublisher } from './AbstractSnsPublisher'
import type { SNSDependencies } from './AbstractSnsService'

export type SnsPublisherFactory<T extends AbstractSnsPublisher<M>, M extends BaseEventType> = {
  buildPublisher(dependencies: SNSDependencies, options: SNSPublisherOptions<M>): T
}

export class CommonSnsPublisher<
  M extends BaseEventType = BaseEventType,
> extends AbstractSnsPublisher<M> {}

export class CommonSnsPublisherFactory<M extends BaseEventType = BaseEventType>
  implements SnsPublisherFactory<CommonSnsPublisher<M>, M>
{
  buildPublisher(
    dependencies: SNSDependencies,
    options: SNSPublisherOptions<M>,
  ): CommonSnsPublisher<M> {
    return new CommonSnsPublisher(dependencies, options)
  }
}
