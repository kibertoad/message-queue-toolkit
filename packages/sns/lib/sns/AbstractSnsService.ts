import type { SNSClient, CreateTopicCommandInput } from '@aws-sdk/client-sns'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

import { assertTopic } from '../utils/snsUtils'

export type SNSDependencies = QueueDependencies & {
  snsClient: SNSClient
}

export type SNSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SNSTopicAWSConfig = Omit<CreateTopicCommandInput, 'Name'>
export type SQSQueueConfig = {
  tags?: Record<string, string>
  // ToDo if correct for sns
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

export class AbstractSnsService<
  MessagePayloadType extends object,
  SNSOptionsType extends QueueOptions<MessagePayloadType, SNSTopicAWSConfig> = QueueOptions<
    MessagePayloadType,
    SNSTopicAWSConfig
  >,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  DependenciesType,
  SNSTopicAWSConfig,
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
    this.topicArn = await assertTopic(this.snsClient, {
      Name: this.queueName,
      ...this.queueConfiguration,
    })
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
