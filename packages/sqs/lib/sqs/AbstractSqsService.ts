import type { SQSClient } from '@aws-sdk/client-sqs'
import type { CreateQueueRequest } from '@aws-sdk/client-sqs/dist-types/models/models_0'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import { assertQueue, getQueueAttributes } from '../utils/SqsUtils'

import type { SQSCreationConfig } from './AbstractSqsConsumer'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueAWSConfig = CreateQueueRequest
export type SQSQueueConfig = {
  tags?: Record<string, string>
  Attributes?: {
    DelaySeconds?: number
    MaximumMessageSize?: number
    MessageRetentionPeriod?: number
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    Policy?: any
    ReceiveMessageWaitTimeSeconds?: number
    VisibilityTimeout?: number
    RedrivePolicy?: string
    RedriveAllowPolicy?: string
  }
}

export type SQSQueueLocatorType = {
  queueUrl: string
}

export class AbstractSqsService<
  MessagePayloadType extends object,
  QueueLocatorType extends SQSQueueLocatorType = SQSQueueLocatorType,
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  SQSOptionsType extends
    | NewQueueOptions<MessagePayloadType, CreationConfigType>
    | ExistingQueueOptions<MessagePayloadType, QueueLocatorType> =
    | NewQueueOptions<MessagePayloadType, CreationConfigType>
    | ExistingQueueOptions<MessagePayloadType, QueueLocatorType>,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
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

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)

    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    // reuse existing queue only
    if (this.locatorConfig) {
      const checkResult = await getQueueAttributes(this.sqsClient, this.locatorConfig)
      if (checkResult.error === 'not_found') {
        throw new Error(`Queue with queueUrl ${this.locatorConfig.queueUrl} does not exist.`)
      }

      this.queueUrl = this.locatorConfig.queueUrl

      const splitUrl = this.queueUrl.split('/')
      this.queueName = splitUrl[splitUrl.length - 1]
      return
    }

    // create new queue if does not exist
    if (!this.creationConfig?.queue.QueueName) {
      throw new Error('queueConfig.QueueName is mandatory when locator is not provided')
    }

    this.queueUrl = await assertQueue(this.sqsClient, this.creationConfig.queue)
    this.queueName = this.creationConfig.queue.QueueName
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
