import type { SQSClient, CreateQueueRequest } from '@aws-sdk/client-sqs'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes'
import { deleteSqs, initSqs } from '../utils/sqsInitter'

import type { SQSCreationConfig } from './AbstractSqsConsumer'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueConfig = CreateQueueRequest

export type SQSQueueLocatorType = {
  queueUrl: string
}

export abstract class AbstractSqsService<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  SQSOptionsType extends
    | NewQueueOptions<CreationConfigType>
    | ExistingQueueOptions<QueueLocatorType> =
    | NewQueueOptions<CreationConfigType>
    | ExistingQueueOptions<QueueLocatorType>,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  SQSMessage,
  DependenciesType,
  CreationConfigType,
  QueueLocatorType,
  SQSOptionsType
> {
  protected readonly sqsClient: SQSClient
  // @ts-ignore
  public queueUrl: string
  // @ts-ignore
  public queueName: string
  // @ts-ignore
  public queueArn: string

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)

    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }
    const { queueUrl, queueName, queueArn } = await initSqs(
      this.sqsClient,
      this.locatorConfig,
      this.creationConfig,
    )

    this.queueArn = queueArn
    this.queueUrl = queueUrl
    this.queueName = queueName
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
