import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { SNSSQSConsumerDependencies } from '../../lib/sns/AbstractSnsSqsConsumer'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer'

import type { ConsumerMessageSchema } from '@message-queue-toolkit/schemas'
import { TestEvents } from '../utils/testContext'
import { entityCreatedHandler } from './handlers/EntityCreatedHandler'
import { entityUpdatedHandler } from './handlers/EntityUpdatedHandler'

export type SupportedMessages = ConsumerMessageSchema<
  typeof TestEvents.created | typeof TestEvents.updated
>
type ExecutionContext = {}

type PreHandlerOutput = {}

export class CreateLocateConfigMixConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PreHandlerOutput
> {
  public static readonly CONSUMED_QUEUE_NAME = 'create_locate_queue'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'create_locate_topic'

  constructor(dependencies: SNSSQSConsumerDependencies) {
    super(
      dependencies,
      {
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<
          SupportedMessages,
          ExecutionContext,
          PreHandlerOutput
        >()
          .addConfig(TestEvents.created, entityCreatedHandler, {})
          .addConfig(TestEvents.updated, entityUpdatedHandler, {})
          .build(),
        messageTypeField: 'type',
        creationConfig: {
          queue: {
            QueueName: CreateLocateConfigMixConsumer.CONSUMED_QUEUE_NAME,
          },
          topic: {
            Name: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
          },
        },
        // locatorConfig: {
        //   topicName: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
        // },
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
      },
      {
        incrementAmount: 1,
      },
    )
  }

  get subscriptionProps() {
    return {
      topicArn: this.topicArn,
      queueUrl: this.queueUrl,
      queueName: this.queueName,
      subscriptionArn: this.subscriptionArn,
      deadLetterQueueUrl: this.deadLetterQueueUrl,
    }
  }
}
