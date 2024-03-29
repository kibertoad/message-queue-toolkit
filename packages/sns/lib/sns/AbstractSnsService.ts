import type { SNSClient, CreateTopicCommandInput, Tag } from '@aws-sdk/client-sns'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'
import { deleteSns, initSns } from '../utils/snsInitter'

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
}

// TODO
export type SNSConsumerDependencies = SNSDependencies & QueueConsumerDependencies

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
  queueUrlsWithSubscribePermissionsPrefix?: string
}

export type SNSCreationConfig = {
  topic: SNSTopicAWSConfig
  updateAttributesIfExists?: boolean
} & ExtraSNSCreationParams

export type SNSQueueLocatorType = {
  topicArn: string
}

export type SNSOptions = QueueOptions<SNSCreationConfig, SNSQueueLocatorType>

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
  SNSQueueLocatorType,
  SNSOptionsType
> {
  protected readonly snsClient: SNSClient
  // @ts-ignore
  private _topicArn: string

  constructor(dependencies: DependenciesType, options: SNSOptionsType) {
    super(dependencies, options)

    this.snsClient = dependencies.snsClient
  }

  public async init() {
    if (this.deletionConfig && this.creationConfig) {
      await deleteSns(this.snsClient, this.deletionConfig, this.creationConfig)
    }

    const initResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this._topicArn = initResult.topicArn
  }

  public override close(): Promise<void> {
    return Promise.resolve()
  }

  get topicArn(): string {
    return this._topicArn
  }
}
