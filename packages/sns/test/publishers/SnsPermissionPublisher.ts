import {
  AbstractSnsPublisher,
  type SNSPublisherOptions,
} from '../../lib/sns/AbstractSnsPublisher.ts'
import type { SNSDependencies } from '../../lib/sns/AbstractSnsService.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas.ts'

type SupportedTypes = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SnsPermissionPublisher extends AbstractSnsPublisher<SupportedTypes> {
  public static readonly TOPIC_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SNSDependencies,
    options?: Pick<
      SNSPublisherOptions<SupportedTypes>,
      | 'creationConfig'
      | 'locatorConfig'
      | 'payloadStoreConfig'
      | 'messageDeduplicationConfig'
      | 'enablePublisherDeduplication'
    >,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options?.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              topic: { Name: SnsPermissionPublisher.TOPIC_NAME },
            },
          }),
      deletionConfig: {
        deleteIfExists: false,
      },
      payloadStoreConfig: options?.payloadStoreConfig,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      handlerSpy: true,
      messageTypeField: 'messageType',
      messageDeduplicationConfig: options?.messageDeduplicationConfig,
      enablePublisherDeduplication: options?.enablePublisherDeduplication,
      messageDeduplicationIdField: 'deduplicationId',
      messageDeduplicationOptionsField: 'deduplicationOptions',
    })
  }

  get topicArnProp(): string {
    return this.topicArn
  }
}
