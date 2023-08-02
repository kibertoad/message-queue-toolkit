import type { SNSClient } from '@aws-sdk/client-sns'
import type { MonoSchemaQueueOptions } from '@message-queue-toolkit/core'
import type {
  SQSConsumerDependencies,
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
  SQSQueueLocatorType,
  SQSCreationConfig,
  SQSMessage,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'
import type { ZodSchema } from 'zod'

import type {
  ExistingSNSOptions,
  NewSNSOptions,
  SNSCreationConfig,
  SNSQueueLocatorType,
} from './AbstractSnsService'
import { initSns, initSnsSqs } from './SnsInitter'
import type { SNSSubscriptionOptions } from './SnsSubscriber'
import { deserializeSNSMessage } from './snsMessageDeserializer'

export type NewSnsSqsConsumerOptions<MessagePayloadType extends object> = NewSQSConsumerOptions<
  MessagePayloadType,
  SQSCreationConfig & SNSCreationConfig
> &
  NewSNSOptions & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type NewSnsSqsConsumerOptionsMono<MessagePayloadType extends object> =
  NewSnsSqsConsumerOptions<MessagePayloadType> & MonoSchemaQueueOptions<MessagePayloadType>

export type ExistingSnsSqsConsumerOptions<MessagePayloadType extends object> =
  ExistingSQSConsumerOptions<MessagePayloadType, SNSSQSQueueLocatorType> &
    ExistingSNSOptions & {
      subscriptionConfig?: SNSSubscriptionOptions
    }

export type ExistingSnsSqsConsumerOptionsMono<MessagePayloadType extends object> =
  ExistingSnsSqsConsumerOptions<MessagePayloadType> & MonoSchemaQueueOptions<MessagePayloadType>

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
  | NewSnsSqsConsumerOptions<MessagePayloadType>
  | ExistingSnsSqsConsumerOptionsMono<MessagePayloadType>
> {
  private readonly subscriptionConfig?: SNSSubscriptionOptions
  private readonly snsClient: SNSClient
  private readonly messageSchema: ZodSchema<MessagePayloadType>
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
      deserializer: options.deserializer ?? deserializeSNSMessage,
    })

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
    this.messageSchema = options.messageSchema
  }

  protected resolveSchema(_message: SQSMessage): ZodSchema<MessagePayloadType> {
    return this.messageSchema
  }

  async init(): Promise<void> {
    await super.init()

    const initSnsResult = await initSns(this.snsClient, this.locatorConfig, this.creationConfig)
    this.topicArn = initSnsResult.topicArn

    const initSnsSqsResult = await initSnsSqs(
      this.sqsClient,
      this.snsClient,
      this.locatorConfig,
      this.creationConfig,
      this.subscriptionConfig,
    )
    this.subscriptionArn = initSnsSqsResult.subscriptionArn
  }
}
