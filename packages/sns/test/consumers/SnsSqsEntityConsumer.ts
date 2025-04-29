import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { ConsumerMessageSchema } from '@message-queue-toolkit/schemas'

import type {
  SNSSQSConsumerDependencies,
  SNSSQSConsumerOptions,
} from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { AbstractSnsSqsConsumer } from '../../lib/sns/AbstractSnsSqsConsumer.ts'
import { TestEvents } from '../utils/testContext.ts'

import { entityCreatedHandler } from './handlers/EntityCreatedHandler.ts'
import { entityUpdatedHandler } from './handlers/EntityUpdatedHandler.ts'

type SupportedMessages = ConsumerMessageSchema<
  typeof TestEvents.created | typeof TestEvents.updated
>

type ExecutionContext = {
  incrementAmount: number
}
type PreHandlerOutput = {
  preHandlerCount: number
}

type SnsSqsPermissionConsumerOptions = Pick<
  SNSSQSConsumerOptions<SupportedMessages, ExecutionContext, PreHandlerOutput>,
  | 'creationConfig'
  | 'locatorConfig'
  | 'deletionConfig'
  | 'deadLetterQueue'
  | 'consumerOverrides'
  | 'maxRetryDuration'
>

export class SnsSqsEntityConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext,
  PreHandlerOutput
> {
  public static readonly CONSUMED_QUEUE_NAME = 'entities_queue'
  public static readonly SUBSCRIBED_TOPIC_NAME = 'dummy'

  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SnsSqsPermissionConsumerOptions = {
      creationConfig: {
        queue: {
          QueueName: SnsSqsEntityConsumer.CONSUMED_QUEUE_NAME,
        },
        topic: {
          Name: SnsSqsEntityConsumer.SUBSCRIBED_TOPIC_NAME,
        },
      },
    },
  ) {
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
        deletionConfig: options.deletionConfig ?? {
          deleteIfExists: true,
        },
        consumerOverrides: options.consumerOverrides ?? {
          terminateVisibilityTimeout: true, // this allows to retry failed messages immediately
        },
        ...(options.locatorConfig
          ? { locatorConfig: options.locatorConfig }
          : {
              creationConfig: options.creationConfig ?? {
                queue: { QueueName: SnsSqsEntityConsumer.CONSUMED_QUEUE_NAME },
                topic: { Name: SnsSqsEntityConsumer.SUBSCRIBED_TOPIC_NAME },
              },
            }),
        messageTypeField: 'type',
        subscriptionConfig: {
          updateAttributesIfExists: false,
        },
        maxRetryDuration: options.maxRetryDuration,
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
