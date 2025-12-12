import type { PubSubMessageOptions } from '../../lib/pubsub/AbstractPubSubPublisher.ts'
import { AbstractPubSubPublisher } from '../../lib/pubsub/AbstractPubSubPublisher.ts'
import type { PubSubDependencies } from '../../lib/pubsub/AbstractPubSubService.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas.ts'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

type PubSubPermissionPublisherOptions = {
  creationConfig?: {
    topic: {
      name: string
    }
  }
  locatorConfig?: {
    topicName: string
  }
  logMessages?: boolean
  deletionConfig?: {
    deleteIfExists: boolean
  }
  payloadStoreConfig?: any
  enablePublisherDeduplication?: boolean
}

export class PubSubPermissionPublisher extends AbstractPubSubPublisher<SupportedMessages> {
  public static TOPIC_NAME = 'user_permissions'

  constructor(
    dependencies: PubSubDependencies,
    options: PubSubPermissionPublisherOptions = {
      creationConfig: {
        topic: {
          name: PubSubPermissionPublisher.TOPIC_NAME,
        },
      },
    },
  ) {
    super(dependencies, {
      ...options,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeResolver: { messageTypePath: 'messageType' },
      handlerSpy: true,
    })
  }

  override async publish(
    message: SupportedMessages,
    options?: PubSubMessageOptions,
  ): Promise<void> {
    return await super.publish(message, options)
  }
}
