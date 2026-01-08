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
      {
        logger: this.logger,
        onResourcesReady: (result) => {
          // Update values that may have been empty when resourcesReady was false
          this.topicArn = result.topicArn
          this.queueUrl = result.queueUrl
          this.subscriptionArn = result.subscriptionArn
          this.queueName = result.queueName
          // Initialize DLQ now that resources are ready
          this.initDeadLetterQueue().catch((err) => {
            this.logger.error({
              message: 'Failed to initialize dead letter queue after resources became ready',
              error: err,
            })
          })
        },
      },
    )

    // Always assign topicArn and queueName (always valid in both blocking and non-blocking modes)
    this.topicArn = initSnsSqsResult.topicArn
    this.queueName = initSnsSqsResult.queueName

    // Only assign queueUrl and subscriptionArn if resources are ready,
    // or if they have valid values (non-blocking mode with locatorConfig provides valid values)
    if (initSnsSqsResult.resourcesReady || initSnsSqsResult.queueUrl) {
      this.queueUrl = initSnsSqsResult.queueUrl
      this.subscriptionArn = initSnsSqsResult.subscriptionArn
    }

    // Only initialize DLQ if resources are ready and queueUrl is available
    if (initSnsSqsResult.resourcesReady && initSnsSqsResult.queueUrl) {
      await this.initDeadLetterQueue()
    }
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
