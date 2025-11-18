import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

import type { SQSConsumerDependencies } from '../../lib/sqs/AbstractSqsConsumer.ts'
import { AbstractSqsConsumer } from '../../lib/sqs/AbstractSqsConsumer.ts'

import type {
  SupportedEventBridgePayloads,
  UserPresencePayload,
  UserRoutingStatusPayload,
} from './eventBridgeSchemas.ts'
import {
  USER_PRESENCE_DETAIL_SCHEMA,
  USER_PRESENCE_ENVELOPE_SCHEMA,
  USER_ROUTING_STATUS_DETAIL_SCHEMA,
  USER_ROUTING_STATUS_ENVELOPE_SCHEMA,
} from './eventBridgeSchemas.ts'

/**
 * Execution context for EventBridge consumer tests
 */
export type EventBridgeTestContext = {
  userPresenceMessages: UserPresencePayload[]
  userRoutingStatusMessages: UserRoutingStatusPayload[]
}

/**
 * EventBridge consumer demonstrating non-standard event format support.
 *
 * Key configurations:
 * - messageTypeField: 'detail-type' (instead of default 'type')
 * - messageTimestampField: 'time' (instead of default 'timestamp')
 * - messagePayloadField: 'detail' (extracts nested payload for validation and handler)
 * - skipMissingTimestampValidation: true (EventBridge events don't need additional timestamp)
 *
 * How it works:
 * - messagePayloadField extracts the 'detail' field from the EventBridge envelope
 * - Schemas validate the extracted 'detail' content
 * - Handlers receive the validated 'detail' payload
 */
export class SqsEventBridgeConsumer extends AbstractSqsConsumer<
  SupportedEventBridgePayloads,
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

        // Payload extraction configuration
        messagePayloadField: 'detail', // Extract 'detail' field and pass to handler
        messageTypeFromFullMessage: true, // Look for 'detail-type' in root, not in extracted payload
        skipMissingTimestampValidation: true, // Don't auto-add timestamp field

        // Enable handler spy for testing
        handlerSpy: true,

        // Handler configuration
        // Three-param addConfig: (envelopeSchema, payloadSchema, handler)
        // - envelopeSchema: Used for routing (matches 'detail-type')
        // - payloadSchema: Used for validation of extracted 'detail'
        // - handler: Receives validated 'detail' payload
        handlers: new MessageHandlerConfigBuilder<
          SupportedEventBridgePayloads,
          EventBridgeTestContext
        >()
          .addConfig(
            USER_PRESENCE_ENVELOPE_SCHEMA,
            USER_PRESENCE_DETAIL_SCHEMA,
            (message, context) => {
              // Handler receives the 'detail' field content (extracted by messagePayloadField)
              context.userPresenceMessages.push(message)
              return Promise.resolve({
                result: 'success' as const,
              })
            },
          )
          .addConfig(
            USER_ROUTING_STATUS_ENVELOPE_SCHEMA,
            USER_ROUTING_STATUS_DETAIL_SCHEMA,
            (message, context) => {
              context.userRoutingStatusMessages.push(message)
              return Promise.resolve({
                result: 'success' as const,
              })
            },
          )
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
