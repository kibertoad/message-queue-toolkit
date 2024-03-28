import type { SNSClient } from '@aws-sdk/client-sns'
import type {
  SQSConsumerDependencies,
  SQSConsumerOptions,
  SQSCreationConfig,
  SQSMessage,
  SQSQueueLocatorType,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer, deleteSqs } from '@message-queue-toolkit/sqs'

import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter'
import { readSnsMessage } from '../utils/snsMessageReader'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber'

import type { SNSCreationConfig, SNSOptions, SNSQueueLocatorType } from './AbstractSnsService'

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
}
export type SNSSQSCreationConfig = SQSCreationConfig & SNSCreationConfig

export type SNSSQSQueueLocatorType = SQSQueueLocatorType &
  SNSQueueLocatorType & {
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
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

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
  }

  override async init(): Promise<void> {
    if (this.deletionConfig && this.creationConfig && this.subscriptionConfig) {
      await deleteSnsSqs(
        this.sqsClient,
        this.snsClient,
        this.deletionConfig,
        this.creationConfig.queue,
        this.creationConfig.topic,
        this.subscriptionConfig,
      )
    } else if (this.deletionConfig && this.creationConfig) {
      await deleteSqs(this.sqsClient, this.deletionConfig, this.creationConfig)
    }

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
      {
        logger: this.logger,
      },
    )
    this.queueUrl = initSnsSqsResult.queueUrl
    this.topicArn = initSnsSqsResult.topicArn
    this.subscriptionArn = initSnsSqsResult.subscriptionArn
  }

  protected override resolveMessage(message: SQSMessage) {
    return readSnsMessage(message, this.errorResolver)
  }

  protected override resolveSchema(messagePayload: MessagePayloadSchemas) {
    return this.messageSchemaContainer.resolveSchema(messagePayload)
  }
}
