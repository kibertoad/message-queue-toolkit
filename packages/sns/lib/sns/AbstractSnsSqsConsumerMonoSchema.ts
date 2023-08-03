import type { SNSClient } from '@aws-sdk/client-sns'
import type { Either } from '@lokalise/node-core'
import type { MonoSchemaQueueOptions } from '@message-queue-toolkit/core'
import type {
  SQSConsumerDependencies,
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
  SQSQueueLocatorType,
  SQSCreationConfig,
  SQSMessage,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer, deleteSqs } from '@message-queue-toolkit/sqs'
import type { ZodSchema } from 'zod'

import { deleteSnsSqs, initSnsSqs } from '../utils/snsInitter'
import { readSnsMessage } from '../utils/snsMessageReader'
import type { SNSSubscriptionOptions } from '../utils/snsSubscriber'

import type {
  ExistingSNSOptions,
  NewSNSOptions,
  SNSCreationConfig,
  SNSQueueLocatorType,
} from './AbstractSnsService'

export type NewSnsSqsConsumerOptions = NewSQSConsumerOptions<
  SQSCreationConfig & SNSCreationConfig
> &
  NewSNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type NewSnsSqsConsumerOptionsMono<MessagePayloadType extends object> =
  NewSnsSqsConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>

export type ExistingSnsSqsConsumerOptions = ExistingSQSConsumerOptions<SNSSQSQueueLocatorType> &
  ExistingSNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type ExistingSnsSqsConsumerOptionsMono<MessagePayloadType extends object> =
  ExistingSnsSqsConsumerOptions & MonoSchemaQueueOptions<MessagePayloadType>

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
}

export type SNSSQSQueueLocatorType = SQSQueueLocatorType &
  SNSQueueLocatorType & {
    subscriptionArn?: string
  }

export abstract class AbstractSnsSqsConsumerMonoSchema<
  MessagePayloadType extends object,
> extends AbstractSqsConsumer<
  MessagePayloadType,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  NewSnsSqsConsumerOptions | ExistingSnsSqsConsumerOptionsMono<MessagePayloadType>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  private readonly messageSchema: ZodSchema<MessagePayloadType>
  private readonly schemaEither: Either<Error, ZodSchema<MessagePayloadType>>
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options:
      | NewSnsSqsConsumerOptionsMono<MessagePayloadType>
      | ExistingSnsSqsConsumerOptionsMono<MessagePayloadType>,
  ) {
    super(dependencies, {
      ...options,
    })

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
    this.messageSchema = options.messageSchema
    this.schemaEither = {
      result: this.messageSchema,
    }
  }

  protected override resolveSchema() {
    return this.schemaEither
  }

  protected override resolveMessage(message: SQSMessage) {
    return readSnsMessage(message, this.errorResolver)
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
}
