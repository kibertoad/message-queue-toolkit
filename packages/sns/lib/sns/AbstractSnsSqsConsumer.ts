import type { CreateTopicCommandInput, SNSClient } from '@aws-sdk/client-sns'
import type { SQSConsumerDependencies, SQSConsumerOptions } from '@message-queue-toolkit/sqs'
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'

import { assertTopic } from '../utils/snsUtils'

import { subscribeToTopic } from './SnsSubscriber'
import { deserializeSNSMessage } from './snsMessageDeserializer'

export type SnsSqsConsumerOptions<MessagePayloadType extends object> =
  SQSConsumerOptions<MessagePayloadType> & {
    subscribedToTopic: CreateTopicCommandInput
  }

export type SNSSQSConsumerDependencies = SQSConsumerDependencies & {
  snsClient: SNSClient
}

export abstract class AbstractSnsSqsConsumer<
  MessagePayloadType extends object,
> extends AbstractSqsConsumer<MessagePayloadType, SnsSqsConsumerOptions<MessagePayloadType>> {
  private readonly subscribedToTopic: CreateTopicCommandInput
  private readonly snsClient: SNSClient
  // @ts-ignore
  public topicArn: string

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

    this.topicArn = await assertTopic(this.snsClient, this.subscribedToTopic)

    await subscribeToTopic(
      this.sqsClient,
      this.snsClient,
      {
        QueueName: this.queueName,
        ...this.queueConfiguration,
      },
      this.subscribedToTopic,
    )
  }
}
