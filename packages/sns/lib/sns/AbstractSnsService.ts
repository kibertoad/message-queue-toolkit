import type { SNSClient, CreateTopicCommandInput, Tag } from '@aws-sdk/client-sns'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  NewQueueOptions,
  ExistingQueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import { assertTopic, getTopicAttributes } from '../utils/snsUtils'

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
    if (this.locatorConfig) {
      const checkResult = await getTopicAttributes(this.snsClient, this.locatorConfig.topicArn)
      if (checkResult.error === 'not_found') {
        throw new Error(`Topic with topicArn ${this.locatorConfig.topicArn} does not exist.`)
      }

      this.topicArn = this.locatorConfig.topicArn
      return
    }

    // create new topic if it does not exist
    if (!this.creationConfig) {
      throw new Error(
        'When locatorConfig for the topic is not specified, creationConfig of the topic is mandatory',
      )
    }
    this.topicArn = await assertTopic(this.snsClient, this.creationConfig.topic)
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
