import type { CreateTopicCommandInput, SNSClient, Tag } from '@aws-sdk/client-sns'
import type { QueueDependencies, QueueOptions } from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { STSClient } from '@aws-sdk/client-sts'
import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes.ts'
import { deleteSns, initSns } from '../utils/snsInitter.ts'

// https://docs.aws.amazon.com/general/latest/gr/sns.html
export const SNS_MESSAGE_MAX_SIZE = 256 * 1024 // 256KB

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
  stsClient: STSClient
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
  forceTagUpdate?: boolean
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
  protected readonly stsClient: STSClient
  // @ts-ignore
  protected topicArn: string

  constructor(dependencies: DependenciesType, options: SNSOptionsType) {
    super(dependencies, options)

    this.snsClient = dependencies.snsClient
    this.stsClient = dependencies.stsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSns(this.snsClient, this.stsClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initSns(
      this.snsClient,
      this.stsClient,
      this.locatorConfig,
      this.creationConfig,
    )
    this.topicArn = initResult.topicArn
    this.isInitted = true
  }

  public override close(): Promise<void> {
    this.isInitted = false
    return Promise.resolve()
  }
}
