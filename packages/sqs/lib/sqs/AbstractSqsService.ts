import type { SQSClient } from '@aws-sdk/client-sqs'
import { CreateQueueCommand, GetQueueUrlCommand } from '@aws-sdk/client-sqs'
import type { CreateQueueRequest } from '@aws-sdk/client-sqs/dist-types/models/models_0'
import type {
  QueueConsumerDependencies,
  QueueDependencies,
  QueueOptions,
} from '@message-queue-toolkit/core'
import { AbstractQueueService } from '@message-queue-toolkit/core'

export type SQSDependencies = QueueDependencies & {
  sqsClient: SQSClient
}

export type SQSConsumerDependencies = SQSDependencies & QueueConsumerDependencies

export type SQSQueueAWSConfig = Omit<CreateQueueRequest, 'QueueName'>
export type SQSQueueConfig = {
  tags?: Record<string, string>
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

export class AbstractSqsService<
  MessagePayloadType extends object,
  SQSOptionsType extends QueueOptions<MessagePayloadType, SQSQueueAWSConfig> = QueueOptions<
    MessagePayloadType,
    SQSQueueAWSConfig
  >,
  DependenciesType extends SQSDependencies = SQSDependencies,
> extends AbstractQueueService<
  MessagePayloadType,
  DependenciesType,
  SQSQueueAWSConfig,
  SQSOptionsType
> {
  protected readonly sqsClient: SQSClient
  // @ts-ignore
  public queueUrl: string

  constructor(dependencies: DependenciesType, options: SQSOptionsType) {
    super(dependencies, options)

    this.sqsClient = dependencies.sqsClient
  }

  public async init() {
    const command = new CreateQueueCommand({
      QueueName: this.queueName,
    })
    await this.sqsClient.send(command)

    const getUrlCommand = new GetQueueUrlCommand({
      QueueName: this.queueName,
      ...this.queueConfiguration,
    })
    const response = await this.sqsClient.send(getUrlCommand)

    if (!response.QueueUrl) {
      throw new Error(`Queue ${this.queueName} was not created`)
    }

    this.queueUrl = response.QueueUrl
  }

  // eslint-disable-next-line @typescript-eslint/require-await
  public override async close(): Promise<void> {}
}
