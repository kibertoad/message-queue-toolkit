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
        creationConfig: {
          queue: {
            QueueName: UserConsumer.CONSUMED_QUEUE_NAME,
          },
        },
        locatorConfig: {
          topicName: UserConsumer.SUBSCRIBED_TOPIC_NAME,
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
}
