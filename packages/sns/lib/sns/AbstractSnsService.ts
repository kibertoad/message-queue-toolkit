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
  SNSTopicAWSConfig
>

export type ExistingSNSOptions<MessagePayloadType extends object> = ExistingQueueOptions<
  MessagePayloadType,
  SNSQueueLocatorType
>

export class AbstractSnsService<
  MessagePayloadType extends object,
  SNSOptionsType extends
    | ExistingQueueOptions<MessagePayloadType, SNSQueueLocatorType>
    | NewQueueOptions<
        MessagePayloadType,
        SNSTopicAWSConfig
      > = ExistingSNSOptions<MessagePayloadType>,
  DependenciesType extends SNSDependencies = SNSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  DependenciesType,
  SNSTopicAWSConfig,
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
    if (this.queueLocator) {
      const checkResult = await getTopicAttributes(this.snsClient, this.queueLocator.topicArn)
      if (checkResult.error === 'not_found') {
        throw new Error(`Topic with topicArn ${this.queueLocator.topicArn} does not exist.`)
      }

      this.topicArn = this.queueLocator.topicArn
      return
    }

    // create new topic if it does not exist
    if (!this.queueConfig) {
      throw new Error(
        'When queueLocator for the topic is not specified, queueConfig of the topic is mandatory',
      )
    }
    this.topicArn = await assertTopic(this.snsClient, this.queueConfig)
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
