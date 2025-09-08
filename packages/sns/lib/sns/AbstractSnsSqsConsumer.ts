import type { SNSClient } from '@aws-sdk/client-sns'
import type { STSClient } from '@aws-sdk/client-sts'
import type {
  SQSConsumerDependencies,
  SQSConsumerOptions,
  SQSCreationConfig,
  SQSMessage,
  SQSQueueLocatorType,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer, deleteSqs } from '@message-queue-toolkit/sqs'
import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter.ts'
import { readSnsMessage } from '../utils/snsMessageReader.ts'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber.ts'
import type { SNSCreationConfig, SNSOptions, SNSTopicLocatorType } from './AbstractSnsService.ts'

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
  stsClient: STSClient
}
export type SNSSQSCreationConfig = Omit<SQSCreationConfig, 'policyConfig'> & SNSCreationConfig

export type SNSSQSQueueLocatorType = Partial<SQSQueueLocatorType> &
  SNSTopicLocatorType & {
    subscriptionArn?: string
  }

export type SNSSQSConsumerOptions<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = SQSConsumerOptions<
  MessagePayloadType,
  ExecutionContext,
  PrehandlerOutput,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType
> &
  SNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export abstract class AbstractSnsSqsConsumer<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput = undefined,
> extends AbstractSqsConsumer<
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  SNSSQSCreationConfig,
  SNSSQSQueueLocatorType,
  SNSSQSConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  private readonly stsClient: STSClient

  // @ts-expect-error
  protected topicArn: string
  // @ts-expect-error
  protected subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SNSSQSConsumerOptions<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext,
  ) {
    super(
      dependencies,
      {
        ...options,
      },
      executionContext,
    )

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
    this.stsClient = dependencies.stsClient
  }

  override async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig && this.subscriptionConfig) {
      await deleteSnsSqs(
        this.sqsClient,
        this.snsClient,
        this.stsClient,
        this.deletionConfig,
        this.creationConfig.queue,
        this.creationConfig.topic,
        this.subscriptionConfig,
        undefined,
        this.locatorConfig,
      )
    } else if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.stsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
      { logger: this.logger },
    )
    this.queueName = initSnsSqsResult.queueName
    this.queueUrl = initSnsSqsResult.queueUrl
    this.topicArn = initSnsSqsResult.topicArn
    this.subscriptionArn = initSnsSqsResult.subscriptionArn

    await this.initDeadLetterQueue()
  }

  protected override resolveMessage(message: SQSMessage) {
    const result = readSnsMessage(message, this.errorResolver)
    if (result.result) {
      return result
    }

    // if it not an SNS message, then it is a SQS message
    return super.resolveMessage(message)
  }

  protected override resolveSchema(messagePayload: MessagePayloadSchemas) {
    return this._messageSchemaContainer.resolveSchema(messagePayload)
  }
}
