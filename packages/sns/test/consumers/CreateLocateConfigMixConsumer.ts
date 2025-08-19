import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ConsumerMessageSchema } from '@message-queue-toolkit/schemas'
import type { SNSSQSConsumerDependencies } from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { TestEvents } from '../utils/testContext.ts'
import { entityCreatedHandler } from './handlers/EntityCreatedHandler.ts'
import { entityUpdatedHandler } from './handlers/EntityUpdatedHandler.ts'

export type SupportedMessages = ConsumerMessageSchema<
  typeof TestEvents.created | typeof TestEvents.updated
>
// biome-ignore lint/complexity/noBannedTypes: Expected
type ExecutionContext = {}

// biome-ignore lint/complexity/noBannedTypes: Expected
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
        },
        locatorConfig: {
          topicName: CreateLocateConfigMixConsumer.SUBSCRIBED_TOPIC_NAME,
        },
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
