import type { CreateQueueRequest, SQSClient } from '@aws-sdk/client-sqs'
import type { QueueDependencies, QueueOptions } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'
import type { SQSMessage } from '../types/MessageTypes.ts'
import { deleteSqs, initSqs } from '../utils/sqsInitter.ts'

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
export const SQS_MESSAGE_MAX_SIZE = 256 * 1024 // 256KB
export const SQS_RESOURCE_ANY = Symbol('any')
export const SQS_RESOURCE_CURRENT_QUEUE = Symbol('current_queue')

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type ExtraSQSCreationParams = {
  topicArnsWithPublishPermissionsPrefix?: string
  updateAttributesIfExists?: boolean
  forceTagUpdate?: boolean
  policyConfig?: SQSPolicyConfig
}

type SQSPolicyStatement = {
  Effect?: string
  Principal?: string
  Action?: string[]
}

export type SQSPolicyConfig = {
  resource: string | typeof SQS_RESOURCE_ANY | typeof SQS_RESOURCE_CURRENT_QUEUE
  statements?: SQSPolicyStatement | SQSPolicyStatement[]
}

export type SQSCreationConfig = {
  queue: CreateQueueRequest
  updateAttributesIfExists?: boolean
  forceTagUpdate?: boolean
  policyConfig?: SQSPolicyConfig
} & ExtraSQSCreationParams

export type SQSQueueLocatorType =
  | {
      queueUrl: string
      queueName?: never
    }
  | {
      queueName: string
      queueUrl?: never
    }

export type SQSQueueConfig = {
  /**
   * Indicates whether this is a FIFO queue.
   * FIFO queues must have names ending with .fifo suffix.
   * When true, MessageGroupId is required for all messages.
   */
  fifoQueue?: boolean
}

export type SQSOptions<
  CreationConfigType extends SQSCreationConfig = SQSCreationConfig,
  QueueLocatorType extends object = SQSQueueLocatorType,
> = QueueOptions<CreationConfigType, QueueLocatorType> & SQSQueueConfig

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

  // @ts-expect-error
  protected queueName: string
  // @ts-expect-error
  protected queueUrl: string
  // @ts-expect-error
  protected queueArn: string
  protected readonly isFifoQueue: boolean

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)
    this.sqsClient = dependencies.sqsClient
    this.isFifoQueue = (options as SQSQueueConfig).fifoQueue ?? false
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }
    const { queueName, queueUrl, queueArn } = await initSqs(
      this.sqsClient,
      this.locatorConfig,
      this.creationConfig,
      this.isFifoQueue,
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
