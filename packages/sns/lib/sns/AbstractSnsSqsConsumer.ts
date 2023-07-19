import type { SNSClient } from '@aws-sdk/client-sns'
import type {
  SQSConsumerDependencies,
  NewSQSConsumerOptions,
  ExistingSQSConsumerOptions,
  SQSQueueLocatorType,
  SQSCreationConfig,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'

import { assertTopic, getTopicAttributes } from '../utils/snsUtils'

import type {
  ExistingSNSOptions,
  NewSNSOptions,
  SNSCreationConfig,
  SNSQueueLocatorType,
} from './AbstractSnsService'
import type { SNSSubscriptionOptions } from './SnsSubscriber'
import { subscribeToTopic } from './SnsSubscriber'
import { deserializeSNSMessage } from './snsMessageDeserializer'

export type NewSnsSqsConsumerOptions<MessagePayloadType extends object> = NewSQSConsumerOptions<
  MessagePayloadType,
  SQSCreationConfig & SNSCreationConfig
> &
  NewSNSOptions<MessagePayloadType> & {
    subscriptionConfig?: SNSSubscriptionOptions
  }

export type ExistingSnsSqsConsumerOptions<MessagePayloadType extends object> =
  ExistingSQSConsumerOptions<MessagePayloadType, SNSSQSQueueLocatorType> &
    ExistingSNSOptions<MessagePayloadType> & {
      subscriptionConfig?: SNSSubscriptionOptions
    }

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
}

export type SNSSQSQueueLocatorType = SQSQueueLocatorType &
  SNSQueueLocatorType & {
    subscriptionArn?: string
  }

export abstract class AbstractSnsSqsConsumer<
  MessagePayloadType extends object,
> extends AbstractSqsConsumer<
  MessagePayloadType,
  SNSSQSQueueLocatorType,
  SNSCreationConfig & SQSCreationConfig,
  NewSnsSqsConsumerOptions<MessagePayloadType> | ExistingSnsSqsConsumerOptions<MessagePayloadType>
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
      | NewSnsSqsConsumerOptions<MessagePayloadType>
      | ExistingSnsSqsConsumerOptions<MessagePayloadType>,
  ) {
    super(dependencies, {
      ...options,
      deserializer: options.deserializer ?? deserializeSNSMessage,
    })

    this.subscriptionConfig = options.subscriptionConfig
    this.snsClient = dependencies.snsClient
  }

  async init(): Promise<void> {
    await super.init()

    // reuse existing queue only
    if (this.locatorConfig) {
      const checkResult = await getTopicAttributes(this.snsClient, this.locatorConfig.topicArn)
      if (checkResult.error === 'not_found') {
        throw new Error(`Topic with topicArn ${this.locatorConfig.topicArn} does not exist.`)
      }

      this.topicArn = this.locatorConfig.topicArn
    }
    // create new topic if it does not exist
    else {
      if (!this.creationConfig) {
        throw new Error(
          'If queueLocator.subscriptionArn is not specified, subscribedToTopic parameter is mandatory, as there will be an attempt to create the missing topic',
        )
      }

      this.topicArn = await assertTopic(this.snsClient, this.creationConfig?.topic)
    }

    if (!this.locatorConfig?.subscriptionArn) {
      if (!this.creationConfig?.topic) {
        throw new Error(
          'If locatorConfig.subscriptionArn is not specified, creationConfig.topic parameter is mandatory, as there will be an attempt to create the missing topic',
        )
      }
      if (!this.creationConfig?.queue) {
        throw new Error(
          'If locatorConfig.subscriptionArn is not specified, creationConfig.queue parameter is mandatory, as there will be an attempt to create the missing queue',
        )
      }
      if (!this.subscriptionConfig) {
        throw new Error(
          'If locatorConfig.subscriptionArn is not specified, subscriptionConfig parameter is mandatory, as there will be an attempt to create the missing subscription',
        )
      }

      const { subscriptionArn } = await subscribeToTopic(
        this.sqsClient,
        this.snsClient,
        this.creationConfig.queue,
        this.creationConfig.topic,
        this.subscriptionConfig,
      )
      if (!subscriptionArn) {
        throw new Error('Failed to subscribe')
      }
      this.subscriptionArn = subscriptionArn
    } else {
      this.subscriptionArn = this.locatorConfig.subscriptionArn
    }
  }
}
