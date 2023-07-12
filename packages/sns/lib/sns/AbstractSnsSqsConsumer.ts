import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import type {
  SQSConsumerDependencies,
  SQSConsumerOptions,
  SQSQueueLocatorType,
} from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'

import { assertTopic, getTopicAttributes } from '../utils/snsUtils'

import type { SNSQueueLocatorType } from './AbstractSnsService'
import { subscribeToTopic } from './SnsSubscriber'
import { deserializeSNSMessage } from './snsMessageDeserializer'

export type SnsSqsConsumerOptions<MessagePayloadType extends object> = SQSConsumerOptions<
  MessagePayloadType,
  SNSSQSQueueLocatorType
> & {
  subscribedToTopic?: CreateTopicCommandInput
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
  SnsSqsConsumerOptions<MessagePayloadType>
> {
  private readonly subscribedToTopic?: CreateTopicCommandInput
  private readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string
  // @ts-ignore
  public subscriptionArn: string

  protected constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SnsSqsConsumerOptions<MessagePayloadType>,
  ) {
    super(dependencies, {
      ...options,
      deserializer: options.deserializer ?? deserializeSNSMessage,
    })

    this.subscribedToTopic = options.subscribedToTopic
    this.snsClient = dependencies.snsClient
  }

  async init(): Promise<void> {
    await super.init()

    // reuse existing queue only
    if (this.queueLocator) {
      const checkResult = await getTopicAttributes(this.snsClient, this.queueLocator.topicArn)
      if (checkResult.error === 'not_found') {
        throw new Error(`Topic with topicArn ${this.queueLocator.topicArn} does not exist.`)
      }

      this.topicArn = this.queueLocator.topicArn
    }
    // create new topic if it does not exist
    else {
      if (!this.subscribedToTopic) {
        throw new Error(
          'If queueLocator.subscriptionArn is not specified, subscribedToTopic parameter is mandatory, as there will be an attempt to create the missing topic',
        )
      }

      this.topicArn = await assertTopic(this.snsClient, this.subscribedToTopic)
    }

    if (!this.queueLocator?.subscriptionArn) {
      if (!this.subscribedToTopic) {
        throw new Error(
          'If queueLocator.subscriptionArn is not specified, subscribedToTopic parameter is mandatory, as there will be an attempt to create the missing topic',
        )
      }

      const { subscriptionArn } = await subscribeToTopic(
        this.sqsClient,
        this.snsClient,
        {
          QueueName: this.queueName,
          ...this.queueConfiguration,
        },
        this.subscribedToTopic,
      )
      if (!subscriptionArn) {
        throw new Error('Failed to subscribe')
      }
      this.subscriptionArn = subscriptionArn
    } else {
      this.subscriptionArn = this.queueLocator.subscriptionArn
    }
  }
}
