import {
  AbstractSnsPublisher,
  type SNSPublisherOptions,
} from '../../lib/sns/AbstractSnsPublisher.ts'
import type { SNSDependencies } from '../../lib/sns/AbstractSnsService.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE_FIFO,
  PERMISSIONS_REMOVE_MESSAGE_TYPE_FIFO,
} from '../consumers/userConsumerSchemasFifo.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO,
} from '../consumers/userConsumerSchemasFifo.ts'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE_FIFO | PERMISSIONS_REMOVE_MESSAGE_TYPE_FIFO

export class SnsPermissionPublisherFifo extends AbstractSnsPublisher<SupportedMessages> {
  public static readonly TOPIC_NAME = 'user_permissions_fifo.fifo'

  constructor(
    dependencies: SNSDependencies,
    options?: Partial<SNSPublisherOptions<SupportedMessages>>,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              topic: {
                Name: SnsPermissionPublisherFifo.TOPIC_NAME,
                Attributes: {
                  FifoTopic: 'true',
                  ContentBasedDeduplication: 'false',
                },
              },
            },
          }),
      fifoTopic: true,
      deletionConfig: options?.deletionConfig ?? {
        deleteIfExists: false,
      },
      payloadStoreConfig: options?.payloadStoreConfig,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA_FIFO, PERMISSIONS_REMOVE_MESSAGE_SCHEMA_FIFO],
      handlerSpy: true,
      messageTypeField: 'messageType',
      messageDeduplicationConfig: options?.messageDeduplicationConfig,
      enablePublisherDeduplication: options?.enablePublisherDeduplication,
      messageDeduplicationIdField: 'deduplicationId',
      messageDeduplicationOptionsField: 'deduplicationOptions',
      messageGroupIdField: options?.messageGroupIdField,
      defaultMessageGroupId: options?.defaultMessageGroupId,
    })
  }

  get topicArnProp(): string {
    return this.topicArn
  }
}
