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

import { initSns } from './SnsInitter'

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

export type NewSNSOptions<MessagePayloadType extends object> = NewQueueOptions<
  MessagePayloadType,
  SNSCreationConfig
>

export type ExistingSNSOptions<MessagePayloadType extends object> = ExistingQueueOptions<
  MessagePayloadType,
  SNSQueueLocatorType
>

export type NewSNSOptionsMultiSchema<MessagePayloadSchemas extends object> =
  NewQueueOptionsMultiSchema<MessagePayloadSchemas, SNSCreationConfig>

export type ExistingSNSOptionsMultiSchema<MessagePayloadSchemas extends object> =
  ExistingQueueOptionsMultiSchema<MessagePayloadSchemas, SNSQueueLocatorType>

export type SNSCreationConfig = {
  topic: SNSTopicAWSConfig
}

export class AbstractSnsService<
  MessagePayloadType extends object,
  SNSOptionsType extends
    | ExistingQueueOptions<MessagePayloadType, SNSQueueLocatorType>
    | NewQueueOptions<MessagePayloadType, SNSCreationConfig> =
    | ExistingSNSOptions<MessagePayloadType>
    | NewQueueOptions<MessagePayloadType, SNSCreationConfig>,
  DependenciesType extends SNSDependencies = SNSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
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
    const initResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initResult.topicArn
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
