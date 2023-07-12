import type { SQSClient } from '@aws-sdk/client-sqs'
import type { CreateQueueRequest } from '@aws-sdk/client-sqs/dist-types/models/models_0'
import type {
  CommonQueueLocator,
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import {assertQueue, getQueueAttributes} from '../utils/SqsUtils'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueAWSConfig = Omit<CreateQueueRequest, 'QueueName'>
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
  SQSOptionsType extends QueueOptions<MessagePayloadType, SQSQueueAWSConfig, SQSQueueLocatorType> = QueueOptions<
    MessagePayloadType,
    SQSQueueAWSConfig,
    SQSQueueLocatorType
  >,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  DependenciesType,
  SQSQueueAWSConfig,
  SQSQueueLocatorType,
  SQSOptionsType
> {
  protected readonly sqsClient: SQSClient
  // @ts-ignore
  public queueUrl: string

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)

    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    // reuse existing queue only
    if (this.queueLocator) {
      const checkResult = await getQueueAttributes(this.sqsClient, this.queueLocator)
      if (checkResult.error === 'not_found') {
        throw new Error(`Queue with queueUrl ${this.queueLocator.queueUrl} does not exist.`)
      }

      this.queueUrl = this.queueLocator.queueUrl
      return
    }

    // create new queue if does not exist
    this.queueUrl = await assertQueue(this.sqsClient, {
      QueueName: this.queueName,
      ...this.queueConfiguration,
    })
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
