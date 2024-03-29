import type { SQSClient } from '@aws-sdk/client-sqs'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes'
import { deleteSqs, initSqs } from '../utils/sqsInitter'

import type { SQSCreationConfig } from './AbstractSqsConsumer'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueLocatorType = {
  queueUrl: string
}

export type QueueProperties = {
  url: string
  arn: string
  name: string
}

export abstract class AbstractSqsService<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  SQSOptionsType extends QueueOptions<CreationConfigType, QueueLocatorType> = QueueOptions<
    CreationConfigType,
    QueueLocatorType
  >,
  DependenciesType extends SQSDependencies = SQSDependencies,
  PrehandlerOutput = unknown,
  ExecutionContext = unknown,
> extends AbstractQueueService<
  MessagePayloadType,
  SQSMessage,
  DependenciesType,
  CreationConfigType,
  QueueLocatorType,
  SQSOptionsType,
  PrehandlerOutput,
  ExecutionContext
> {
  protected readonly sqsClient: SQSClient

  // @ts-ignore
  private _queue: QueueProperties

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)
    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }
    this._queue = await initSqs(this.sqsClient, this.locatorConfig, this.creationConfig)
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}

  public get queue(): QueueProperties {
    return this._queue
  }
}
