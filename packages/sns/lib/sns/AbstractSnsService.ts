import type { CreateTopicCommandInput, SNSClient, Tag } from '@aws-sdk/client-sns'
import type { QueueDependencies, QueueOptions } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'
import { deleteSns, initSns } from '../utils/snsInitter'

// https://docs.aws.amazon.com/general/latest/gr/sns.html
export const SNS_MESSAGE_MAX_SIZE = 256 * 1024 // 256KB

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
}

export type SNSTopicAWSConfig = CreateTopicCommandInput
export type SNSTopicConfig = {
  tags?: Tag[]
  DataProtectionPolicy?: string
  // ToDo if correct for sns
  Attributes?: {
    DeliveryPolicy?: string
    DisplayName?: string
    Policy?: string
    SignatureVersion?: number
    TracingConfig?: string
    FifoTopic?: boolean
    ContentBasedDeduplication?: boolean
  }
}

export type ExtraSNSCreationParams = {
  queueUrlsWithSubscribePermissionsPrefix?: string | readonly string[]
  allowedSourceOwner?: string
}

export type SNSCreationConfig = {
  topic?: SNSTopicAWSConfig
  updateAttributesIfExists?: boolean
} & ExtraSNSCreationParams

export type SNSTopicLocatorType = {
  topicArn?: string
  topicName?: string
}

export type SNSOptions = QueueOptions<SNSCreationConfig, SNSTopicLocatorType>

export abstract class AbstractSnsService<
  MessagePayloadType extends object,
  MessageEnvelopeType extends object = SNS_MESSAGE_BODY_TYPE,
  SNSOptionsType extends SNSOptions = SNSOptions,
  DependenciesType extends SNSDependencies = SNSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  MessageEnvelopeType,
  DependenciesType,
  SNSCreationConfig,
  SNSTopicLocatorType,
  SNSOptionsType
> {
  protected readonly snsClient: SNSClient
  // @ts-ignore
  protected topicArn: string

  constructor(dependencies: DependenciesType, options: SNSOptionsType) {
    super(dependencies, options)

    this.snsClient = dependencies.snsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSns(this.snsClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initResult.topicArn
    this.isInitted = true
  }

  public override close(): Promise<void> {
    this.isInitted = false
    return Promise.resolve()
  }
}
