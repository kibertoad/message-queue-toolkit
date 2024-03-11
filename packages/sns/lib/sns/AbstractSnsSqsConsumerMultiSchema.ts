import type { SNSClient } from '@aws-sdk/client-sns'
import type { MultiSchemaConsumerOptions } from '@message-queue-toolkit/core'
import type { SQSCreationConfig, SQSMessage } from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumerMultiSchema, deleteSqs } from '@message-queue-toolkit/sqs'

import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter'
import { readSnsMessage } from '../utils/snsMessageReader'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber'

import type { SNSCreationConfig } from './AbstractSnsService'
import type {
  ExistingSnsSqsConsumerOptions,
  NewSnsSqsConsumerOptions,
  SNSSQSConsumerDependencies,
  SNSSQSQueueLocatorType,
} from './AbstractSnsSqsConsumerMonoSchema'

export type ExistingSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = ExistingSnsSqsConsumerOptions &
  MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export type NewSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput,
> = NewSnsSqsConsumerOptions &
  MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>

export abstract class AbstractSnsSqsConsumerMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
  PrehandlerOutput,
> extends AbstractSqsConsumerMultiSchema<
  MessagePayloadSchemas,
  ExecutionContext,
  PrehandlerOutput,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  NewSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext, PrehandlerOutput>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: NewSnsSqsConsumerOptionsMulti<
      MessagePayloadSchemas,
      ExecutionContext,
      PrehandlerOutput
    >,
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
