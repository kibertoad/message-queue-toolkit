import type { SQSPublisherOptions } from '../../lib/sqs/AbstractSqsPublisher.ts'
import { AbstractSqsPublisher } from '../../lib/sqs/AbstractSqsPublisher.ts'
import type { SQSDependencies } from '../../lib/sqs/AbstractSqsService.ts'
import type {
  PERMISSIONS_ADD_MESSAGE_TYPE,
  PERMISSIONS_REMOVE_MESSAGE_TYPE,
} from '../consumers/userConsumerSchemas.ts'
import {
  PERMISSIONS_ADD_MESSAGE_SCHEMA,
  PERMISSIONS_REMOVE_MESSAGE_SCHEMA,
} from '../consumers/userConsumerSchemas.ts'

type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE

export class SqsPermissionPublisherFifo extends AbstractSqsPublisher<SupportedMessages> {
  public static readonly QUEUE_NAME = 'user_permissions_fifo.fifo'

  constructor(
    dependencies: SQSDependencies,
    options?: Partial<SQSPublisherOptions<SupportedMessages>>,
  ) {
    super(dependencies, {
      ...(options?.locatorConfig
        ? { locatorConfig: options.locatorConfig }
        : {
            creationConfig: options?.creationConfig ?? {
              queue: {
                QueueName: SqsPermissionPublisherFifo.QUEUE_NAME,
                Attributes: {
                  FifoQueue: 'true',
                  ContentBasedDeduplication: 'false',
                },
              },
            },
          }),
      fifoQueue: true,
      logMessages: options?.logMessages,
      deletionConfig: options?.deletionConfig ?? {
        deleteIfExists: false,
      },
      handlerSpy: true,
      messageSchemas: [PERMISSIONS_ADD_MESSAGE_SCHEMA, PERMISSIONS_REMOVE_MESSAGE_SCHEMA],
      messageTypeResolver: { messageTypePath: 'messageType' },
      messageGroupIdField: options?.messageGroupIdField,
      defaultMessageGroupId: options?.defaultMessageGroupId,
      payloadStoreConfig: options?.payloadStoreConfig,
      messageDeduplicationConfig: options?.messageDeduplicationConfig,
      enablePublisherDeduplication: options?.enablePublisherDeduplication,
      messageDeduplicationIdField: 'deduplicationId',
      messageDeduplicationOptionsField: 'deduplicationOptions',
    })
  }

  public get queueProps() {
    return {
      name: this.queueName,
      url: this.queueUrl,
      arn: this.queueArn,
      isFifo: this.isFifoQueue,
    }
  }
}
