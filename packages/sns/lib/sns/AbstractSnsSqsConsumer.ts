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
  protected topicArn: string
  // @ts-ignore
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
