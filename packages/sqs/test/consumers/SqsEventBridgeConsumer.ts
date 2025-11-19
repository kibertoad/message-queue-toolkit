import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'

import type {
  SupportedEventBridgeEnvelopes,
  UserPresenceEnvelope,
  UserRoutingStatusEnvelope,
} from './eventBridgeSchemas.ts'
import {
  USER_PRESENCE_ENVELOPE_SCHEMA,
  USER_ROUTING_STATUS_ENVELOPE_SCHEMA,
} from './eventBridgeSchemas.ts'

/**
 * Execution context for EventBridge consumer tests
 */
export type EventBridgeTestContext = {
  userPresenceMessages: UserPresenceEnvelope[]
  userRoutingStatusMessages: UserRoutingStatusEnvelope[]
}

/**
 * EventBridge consumer demonstrating non-standard event format support.
 *
 * Key configurations:
 * - messageTypeField: 'detail-type' (instead of default 'type')
 * - messageTimestampField: 'time' (instead of default 'timestamp')
 *
 * How it works:
 * - Full EventBridge envelope is validated and passed to handlers
 * - Handlers receive the complete envelope and can access message.detail directly
 */
export class SqsEventBridgeConsumer extends AbstractSqsConsumer<
  SupportedEventBridgeEnvelopes,
  EventBridgeTestContext
> {
  public static readonly QUEUE_NAME = 'eventbridge_events'

  constructor(dependencies: SQSConsumerDependencies, executionContext: EventBridgeTestContext) {
    super(
      dependencies,
      {
        // Standard SQS configuration
        creationConfig: {
          queue: {
            QueueName: SqsEventBridgeConsumer.QUEUE_NAME,
          },
        },
        deletionConfig: {
          deleteIfExists: true,
        },

        // EventBridge-specific field mappings
        messageTypeField: 'detail-type', // EventBridge uses 'detail-type' instead of 'type'
        messageIdField: 'id', // Standard field, same as default
        messageTimestampField: 'time', // EventBridge uses 'time' instead of 'timestamp'

        // Enable handler spy for testing
        handlerSpy: true,

        // Handler configuration
        // Handlers receive the full EventBridge envelope and can access message.detail
        handlers: new MessageHandlerConfigBuilder<
          SupportedEventBridgeEnvelopes,
          EventBridgeTestContext
        >()
          .addConfig(USER_PRESENCE_ENVELOPE_SCHEMA, (message, context) => {
            // Handler receives the full EventBridge envelope
            context.userPresenceMessages.push(message)
            return Promise.resolve({
              result: 'success' as const,
            })
          })
          .addConfig(USER_ROUTING_STATUS_ENVELOPE_SCHEMA, (message, context) => {
            context.userRoutingStatusMessages.push(message)
            return Promise.resolve({
              result: 'success' as const,
            })
          })
          .build(),
      },
      executionContext,
    )
  }

  public get queueProps() {
    return {
      name: this.queueName,
      url: this.queueUrl,
      arn: this.queueArn,
    }
  }
}
