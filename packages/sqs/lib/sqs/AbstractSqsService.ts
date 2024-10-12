import type { CreateQueueRequest, SQSClient } from '@aws-sdk/client-sqs'
import type { QueueDependencies, QueueOptions } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SQSMessage } from '../types/MessageTypes'
import { deleteSqs, initSqs } from '../utils/sqsInitter'

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
export const SQS_MESSAGE_MAX_SIZE = 256 * 1024 // 256KB

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type ExtraSQSCreationParams = {
  topicArnsWithPublishPermissionsPrefix?: string
  updateAttributesIfExists?: boolean
  forceTagUpdate?: boolean
}

export type SQSCreationConfig = {
  queue: CreateQueueRequest
  updateAttributesIfExists?: boolean
  forceTagUpdate?: boolean
} & ExtraSQSCreationParams

export type SQSQueueLocatorType = {
  queueUrl: string
}

export abstract class AbstractSqsService<
  MessagePayloadType extends object,
  QueueLocatorType extends object = SQSQueueLocatorType,
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
  protected queueName: string
  // @ts-ignore
  protected queueUrl: string
  // @ts-ignore
  protected queueArn: string

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)
    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }
    const { queueName, queueUrl, queueArn } = await initSqs(
      this.sqsClient,
      this.locatorConfig,
      this.creationConfig,
    )
    this.queueName = queueName
    this.queueUrl = queueUrl
    this.queueArn = queueArn
    this.isInitted = true
  }

  public override close(): Promise<void> {
    this.isInitted = false
    return Promise.resolve()
  }
}
