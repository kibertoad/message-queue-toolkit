import type { MessageTypeResolverConfig } from '@message-queue-toolkit/core'

/**
 * Pre-built message type resolver configurations for AWS SQS.
 *
 * These resolvers handle common AWS patterns where message types are stored
 * in specific fields of the message body.
 */

/**
 * EventBridge detail-type field name.
 * EventBridge events use 'detail-type' as the event type discriminator.
 */
export const EVENT_BRIDGE_DETAIL_TYPE_FIELD = 'detail-type'

/**
 * EventBridge timestamp field name.
 * EventBridge events use 'time' instead of the default 'timestamp' field.
 * Use this constant for `messageTimestampField` configuration.
 *
 * @example
 * ```typescript
 * {
 *   messageTypeResolver: EVENT_BRIDGE_TYPE_RESOLVER,
 *   messageTimestampField: EVENT_BRIDGE_TIMESTAMP_FIELD,
 * }
 * ```
 */
export const EVENT_BRIDGE_TIMESTAMP_FIELD = 'time'

/**
 * Pre-built resolver for AWS EventBridge events delivered to SQS.
 *
 * EventBridge events have a specific envelope structure where the event type
 * is stored in the `detail-type` field. This resolver extracts that field
 * for message routing.
 *
 * @see https://docs.aws.amazon.com/eventbridge/latest/userguide/aws-events.html
 *
 * @example
 * ```typescript
 * import { EVENT_BRIDGE_TYPE_RESOLVER } from '@message-queue-toolkit/sqs'
 *
 * class MyConsumer extends AbstractSqsConsumer {
 *   constructor(deps: SQSConsumerDependencies) {
 *     super(deps, {
 *       messageTypeResolver: EVENT_BRIDGE_TYPE_RESOLVER,
 *       handlers: new MessageHandlerConfigBuilder()
 *         .addConfig(userCreatedSchema, handler)
 *         .build(),
 *     }, context)
 *   }
 * }
 * ```
 *
 * @example EventBridge event structure
 * ```json
 * {
 *   "version": "0",
 *   "id": "12345678-1234-1234-1234-123456789012",
 *   "detail-type": "Order Created",
 *   "source": "com.myapp.orders",
 *   "account": "123456789012",
 *   "time": "2024-01-15T10:30:00Z",
 *   "region": "us-east-1",
 *   "resources": [],
 *   "detail": {
 *     "orderId": "order-456",
 *     "amount": 99.99
 *   }
 * }
 * ```
 */
export const EVENT_BRIDGE_TYPE_RESOLVER: MessageTypeResolverConfig = {
  messageTypePath: EVENT_BRIDGE_DETAIL_TYPE_FIELD,
}

/**
 * Creates an EventBridge resolver that normalizes detail-type values.
 *
 * EventBridge detail-type values are typically human-readable strings like
 * "Order Created" or "User Signed Up". This resolver allows you to normalize
 * them to your internal naming convention (e.g., "order.created").
 *
 * @param typeMap - Map of EventBridge detail-types to internal message types
 * @param options - Optional configuration
 * @param options.fallbackToOriginal - If true, unmapped types are passed through (default: false)
 * @returns MessageTypeResolverConfig for use in consumer options
 *
 * @example
 * ```typescript
 * import { createEventBridgeResolverWithMapping } from '@message-queue-toolkit/sqs'
 *
 * const resolver = createEventBridgeResolverWithMapping({
 *   'Order Created': 'order.created',
 *   'Order Updated': 'order.updated',
 *   'Order Cancelled': 'order.cancelled',
 * })
 *
 * class MyConsumer extends AbstractSqsConsumer {
 *   constructor(deps: SQSConsumerDependencies) {
 *     super(deps, {
 *       messageTypeResolver: resolver,
 *       handlers: new MessageHandlerConfigBuilder()
 *         .addConfig(orderCreatedSchema, handler, { messageType: 'order.created' })
 *         .build(),
 *     }, context)
 *   }
 * }
 * ```
 */
export function createEventBridgeResolverWithMapping(
  typeMap: Record<string, string>,
  options?: { fallbackToOriginal?: boolean },
): MessageTypeResolverConfig {
  return {
    resolver: ({ messageData }) => {
      const data = messageData as { 'detail-type'?: string }
      const detailType = data[EVENT_BRIDGE_DETAIL_TYPE_FIELD]

      if (detailType === undefined || detailType === null) {
        throw new Error(
          `Unable to resolve message type: '${EVENT_BRIDGE_DETAIL_TYPE_FIELD}' field not found in message`,
        )
      }

      const mappedType = typeMap[detailType]
      if (mappedType) {
        return mappedType
      }

      if (options?.fallbackToOriginal) {
        return detailType
      }

      throw new Error(
        `Unable to resolve message type: detail-type '${detailType}' is not mapped. Available mappings: ${Object.keys(typeMap).join(', ')}`,
      )
    },
  }
}
