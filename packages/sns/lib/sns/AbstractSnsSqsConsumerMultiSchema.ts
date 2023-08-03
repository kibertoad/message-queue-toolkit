import type { SNSClient } from '@aws-sdk/client-sns'
import type { MultiSchemaConsumerOptions } from '@message-queue-toolkit/core'
import type { SQSCreationConfig, SQSMessage } from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumerMultiSchema } from '@message-queue-toolkit/sqs'
import { deleteSqs } from '@message-queue-toolkit/sqs/dist/lib/sqs/sqsInitter'

import type { SNSCreationConfig } from './AbstractSnsService'
import type {
  ExistingSnsSqsConsumerOptions,
  NewSnsSqsConsumerOptions,
  SNSSQSConsumerDependencies,
  SNSSQSQueueLocatorType,
} from './AbstractSnsSqsConsumerMonoSchema'
import { deleteSnsSqs, initSnsSqs } from './SnsInitter'
import type { SNSSubscriptionOptions } from './SnsSubscriber'
import { readSnsMessage } from './snsMessageReader'

export type ExistingSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
> = ExistingSnsSqsConsumerOptions & MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>

export type NewSnsSqsConsumerOptionsMulti<
  MessagePayloadType extends object,
  ExecutionContext,
> = NewSnsSqsConsumerOptions & MultiSchemaConsumerOptions<MessagePayloadType, ExecutionContext>

export abstract class AbstractSnsSqsConsumerMultiSchema<
  MessagePayloadSchemas extends object,
  ExecutionContext,
> extends AbstractSqsConsumerMultiSchema<
  MessagePayloadSchemas,
  ExecutionContext,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  | NewSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
  | ExistingSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | NewSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>
      | ExistingSnsSqsConsumerOptionsMulti<MessagePayloadSchemas, ExecutionContext>,
  ) {
    super(dependencies, {
      ...options,
    })

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
