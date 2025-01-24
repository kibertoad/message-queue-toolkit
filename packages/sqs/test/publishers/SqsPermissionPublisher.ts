import type { QueuePublisherOptions } from '@message-queue-toolkit/core'

import { AbstractSqsPublisher } from '../../lib/sqs/AbstractSqsPublisher'
import type {
  SQSCreationConfig,
  SQSDependencies,
  SQSQueueLocatorType,
} from '../../lib/sqs/AbstractSqsService'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SqsPermissionPublisher extends AbstractSqsPublisher<SupportedMessages> {
  public static readonly QUEUE_NAME = 'user_permissions_multi'

  constructor(
    dependencies: SQSDependencies,
    options?: Pick<
      QueuePublisherOptions<SQSCreationConfig, SQSQueueLocatorType, SupportedMessages>,
      | 'creationConfig'
      | 'locatorConfig'
      | 'deletionConfig'
      | 'logMessages'
      | 'payloadStoreConfig'
      | 'publisherMessageDeduplicationConfig'
    >,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              queue: {
                QueueName: SqsPermissionPublisher.QUEUE_NAME,
              },
            },
          }),
      logMessages: options?.logMessages,
      deletionConfig: options?.deletionConfig ?? {
        deleteIfExists: false,
      },
      handlerSpy: true,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeField: 'messageType',
      payloadStoreConfig: options?.payloadStoreConfig,
      publisherMessageDeduplicationConfig: options?.publisherMessageDeduplicationConfig,
    })
  }

  public get queueProps() {
    return {
      name: this.queueName,
      url: this.queueUrl,
      arn: this.queueArn,
    }
  }
}
