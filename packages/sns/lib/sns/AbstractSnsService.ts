import type { SNSClient, CreateTopicCommandInput, Tag } from '@aws-sdk/client-sns'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
  NewQueueOptionsMultiSchema,
  ExistingQueueOptionsMultiSchema,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import type { SNS_MESSAGE_BODY_TYPE } from '../types/MessageTypes'

import {deleteSns, initSns} from './SnsInitter'
import {deleteSqs} from "@message-queue-toolkit/sqs/dist/lib/sqs/sqsInitter";

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
}

export type SNSQueueLocatorType = {
  topicArn: string
}

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

export type NewSNSOptions = NewQueueOptions<SNSCreationConfig>

export type ExistingSNSOptions = ExistingQueueOptions<SNSQueueLocatorType>

export type NewSNSOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
> = NewQueueOptionsMultiSchema<MessagePayloadSchemas, SNSCreationConfig, ExecutionContext>

export type ExistingSNSOptionsMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
> = ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, SNSQueueLocatorType, ExecutionContext>

export type SNSCreationConfig = {
  topic: SNSTopicAWSConfig
}

export abstract class AbstractSnsService<
  MessagePayloadType extends object,
  MessageEnvelopeType extends object = SNS_MESSAGE_BODY_TYPE,
  SNSOptionsType extends
    | ExistingQueueOptions<SNSQueueLocatorType>
    | NewQueueOptions<SNSCreationConfig> = ExistingSNSOptions | NewQueueOptions<SNSCreationConfig>,
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
  public topicArn: string

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
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
