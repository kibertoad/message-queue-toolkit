import { CommonMetadataFiller, EventRegistry } from '@message-queue-toolkit/core'
import type { AllPublisherMessageSchemas } from '@message-queue-toolkit/schemas'
import { SnsPublisherManager } from '@message-queue-toolkit/sns'
import type { CommonSnsPublisher } from '@message-queue-toolkit/sns'
import { CommonSnsPublisherFactory } from '@message-queue-toolkit/sns'
import { errorReporter, logger, snsClient, stsClient } from './Dependencies.ts'
import { UserEvents, type UserEventsType } from './TestMessages.ts'

const isTest = true

type PublisherTypes = AllPublisherMessageSchemas<UserEventsType>

export const publisherManager = new SnsPublisherManager<
  CommonSnsPublisher<PublisherTypes>,
  UserEventsType
>(
  {
    errorReporter,
    logger,
    eventRegistry: new EventRegistry(Object.values(UserEvents)),
    snsClient,
    stsClient,
  },
  {
    metadataFiller: new CommonMetadataFiller({
      serviceId: 'service',
    }),
    publisherFactory: new CommonSnsPublisherFactory(),
    newPublisherOptions: {
      handlerSpy: true,
      messageIdField: 'id',
      messageTypeResolver: { messageTypePath: 'type' },
      deletionConfig: {
        deleteIfExists: isTest, // only enable this in tests
        // and ensure that the owning side is doing the deletion.
        // if it is enabled both on consumer and publisher side, you are likely to experience confusing behaviour
      },
      creationConfig: {
        updateAttributesIfExists: true,
      },
    },
  },
)
