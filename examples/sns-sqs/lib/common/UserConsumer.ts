import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ConsumerMessageSchema } from '@message-queue-toolkit/schemas'
import { AbstractSnsSqsConsumer, type SNSSQSConsumerDependencies } from '@message-queue-toolkit/sns'
import { UserEvents } from './TestMessages.ts'
import { userCreatedHandler } from './handlers/UserCreatedHandler.ts'
import { userUpdatedHandler } from './handlers/UserUpdatedHandler.ts'

type SupportedMessages = ConsumerMessageSchema<
  typeof UserEvents.created | typeof UserEvents.updated
>

// biome-ignore lint/complexity/noBannedTypes: to be expanded later
type ExecutionContext = {}

const isTest = true

export class UserConsumer extends AbstractSnsSqsConsumer<SupportedMessages, ExecutionContext> {
  public static readonly CONSUMED_QUEUE_NAME = 'user-my_service'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'user'

  constructor(dependencies: SNSSQSConsumerDependencies) {
    super(
      dependencies,
      {
        handlerSpy: true,
        handlers: new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
          .addConfig(UserEvents.created, userCreatedHandler, {})
          .addConfig(UserEvents.updated, userUpdatedHandler, {})
          .build(),
        messageTypeField: 'type',
        // Consumer creates its own queue
        creationConfig: {
          queue: {
            QueueName: UserConsumer.CONSUMED_QUEUE_NAME,
          },
        },
        deletionConfig: {
          deleteIfExists: isTest,
        },
        locatorConfig: {
          // Topic is created by a publisher, consumer relies on it already existing.
          // Note that in order for this to work correctly you need to ensure that
          // publisher gets initialized first. If consumer will initialize first,
          // publisher may delete already existing topic and subscription and break the setup
          topicName: UserConsumer.SUBSCRIBED_TOPIC_NAME,
        },
        // consumer creates its own subscription
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
      },
      {},
    )
  }
}
