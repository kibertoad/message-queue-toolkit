import type { MessageTypeResolverConfig } from '@message-queue-toolkit/core'

/**
 * Pre-built message type resolver configurations for GCP Pub/Sub.
 *
 * These resolvers handle common GCP patterns where the message type is stored
 * in message attributes rather than the message body.
 *
 * Note: Pub/Sub message attributes are flat key-value pairs (no nested objects),
 * so we use direct property access rather than dot-notation path traversal.
 */

/**
 * CloudEvents attribute prefix used in Pub/Sub binary content mode.
 * @see https://github.com/googleapis/google-cloudevents/blob/main/docs/spec/pubsub.md
 */
export const CLOUD_EVENTS_ATTRIBUTE_PREFIX = 'ce-'

/**
 * CloudEvents type attribute name in Pub/Sub binary content mode.
 * Example: `ce-type: "com.example.someevent"`
 */
export const CLOUD_EVENTS_TYPE_ATTRIBUTE = 'ce-type'

/**
 * CloudEvents time attribute name in Pub/Sub binary content mode.
 * Example: `ce-time: "2024-01-15T10:30:00Z"`
 * Note: This is in message attributes, not the message body.
 */
export const CLOUD_EVENTS_TIME_ATTRIBUTE = 'ce-time'

/**
 * CloudEvents timestamp field name in structured content mode.
 * CloudEvents structured mode uses 'time' instead of 'timestamp' in the message body.
 * Use this constant for `messageTimestampField` configuration when consuming
 * CloudEvents in structured format.
 *
 * @example
 * ```typescript
 * // For CloudEvents in structured content mode (type in message body)
 * {
 *   messageTypeResolver: { messageTypePath: 'type' },
 *   messageTimestampField: CLOUD_EVENTS_TIMESTAMP_FIELD,
 * }
 * ```
 */
export const CLOUD_EVENTS_TIMESTAMP_FIELD = 'time'

/**
 * Cloud Storage notification event type attribute.
 * @see https://cloud.google.com/storage/docs/pubsub-notifications
 */
export const GCS_EVENT_TYPE_ATTRIBUTE = 'eventType'

/**
 * Standard GCS event types.
 * @see https://cloud.google.com/storage/docs/pubsub-notifications#events
 */
export const GCS_EVENT_TYPES = {
  OBJECT_FINALIZE: 'OBJECT_FINALIZE',
  OBJECT_DELETE: 'OBJECT_DELETE',
  OBJECT_ARCHIVE: 'OBJECT_ARCHIVE',
  OBJECT_METADATA_UPDATE: 'OBJECT_METADATA_UPDATE',
} as const

/**
 * Creates a resolver configuration that extracts message type from a message attribute.
 *
 * Use this when the message type is stored in Pub/Sub message attributes rather than
 * the message body. This is common for GCP service notifications (Cloud Storage,
 * Cloud Build, etc.) and CloudEvents.
 *
 * @param attributeName - The attribute key name (e.g., 'eventType', 'ce-type')
 * @returns MessageTypeResolverConfig for use in consumer/publisher options
 *
 * @example
 * ```typescript
 * // For Cloud Storage notifications
 * {
 *   messageTypeResolver: createAttributeResolver('eventType'),
 * }
 *
 * // For CloudEvents in binary mode
 * {
 *   messageTypeResolver: createAttributeResolver('ce-type'),
 * }
 * ```
 */
export function createAttributeResolver(attributeName: string): MessageTypeResolverConfig {
  return {
    resolver: ({ messageAttributes }) => {
      const attrs = messageAttributes as Record<string, unknown> | undefined
      const type = attrs?.[attributeName] as string | undefined | null
      if (type === undefined || type === null) {
        throw new Error(
          `Unable to resolve message type: attribute '${attributeName}' not found in message attributes`,
        )
      }
      return type
    },
  }
}

/**
 * Creates a resolver configuration that maps attribute values to internal message types.
 *
 * Use this when you want to normalize external event types to your internal naming convention.
 *
 * @param attributeName - The attribute key name
 * @param typeMap - Map of external types to internal types
 * @param options - Optional configuration
 * @param options.fallbackToOriginal - If true, unmapped types are passed through (default: false)
 * @returns MessageTypeResolverConfig for use in consumer/publisher options
 *
 * @example
 * ```typescript
 * // Map Cloud Storage events to internal types
 * {
 *   messageTypeResolver: createAttributeResolverWithMapping('eventType', {
 *     'OBJECT_FINALIZE': 'storage.object.created',
 *     'OBJECT_DELETE': 'storage.object.deleted',
 *     'OBJECT_ARCHIVE': 'storage.object.archived',
 *     'OBJECT_METADATA_UPDATE': 'storage.object.metadataUpdated',
 *   }),
 * }
 * ```
 */
export function createAttributeResolverWithMapping(
  attributeName: string,
  typeMap: Record<string, string>,
  options?: { fallbackToOriginal?: boolean },
): MessageTypeResolverConfig {
  return {
    resolver: ({ messageAttributes }) => {
      const attrs = messageAttributes as Record<string, unknown> | undefined
      const type = attrs?.[attributeName] as string | undefined | null
      if (type === undefined || type === null) {
        throw new Error(
          `Unable to resolve message type: attribute '${attributeName}' not found in message attributes`,
        )
      }
      const mappedType = typeMap[type]
      if (mappedType) {
        return mappedType
      }
      if (options?.fallbackToOriginal) {
        return type
      }
      throw new Error(
        `Unable to resolve message type: attribute value '${type}' is not mapped. Available mappings: ${Object.keys(typeMap).join(', ')}`,
      )
    },
  }
}

/**
 * Pre-built resolver for CloudEvents in Pub/Sub binary content mode.
 *
 * Extracts the event type from the `ce-type` message attribute.
 *
 * @see https://github.com/googleapis/google-cloudevents/blob/main/docs/spec/pubsub.md
 *
 * @example
 * ```typescript
 * class MyConsumer extends AbstractPubSubConsumer {
 *   constructor(deps: PubSubConsumerDependencies) {
 *     super(deps, {
 *       messageTypeResolver: CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER,
 *       handlers: new MessageHandlerConfigBuilder()
 *         .addConfig(schema, handler, { messageType: 'com.example.someevent' })
 *         .build(),
 *     }, context)
 *   }
 * }
 * ```
 */
export const CLOUD_EVENTS_BINARY_MODE_TYPE_RESOLVER: MessageTypeResolverConfig =
  createAttributeResolver(CLOUD_EVENTS_TYPE_ATTRIBUTE)

/**
 * Pre-built resolver for Google Cloud Storage notifications.
 *
 * Extracts the event type from the `eventType` message attribute and maps it
 * to a normalized internal type.
 *
 * GCS Event Types:
 * - `OBJECT_FINALIZE` → `gcs.object.finalized`
 * - `OBJECT_DELETE` → `gcs.object.deleted`
 * - `OBJECT_ARCHIVE` → `gcs.object.archived`
 * - `OBJECT_METADATA_UPDATE` → `gcs.object.metadataUpdated`
 *
 * @see https://cloud.google.com/storage/docs/pubsub-notifications
 *
 * @example
 * ```typescript
 * class GcsNotificationConsumer extends AbstractPubSubConsumer {
 *   constructor(deps: PubSubConsumerDependencies) {
 *     super(deps, {
 *       messageTypeResolver: GCS_NOTIFICATION_TYPE_RESOLVER,
 *       handlers: new MessageHandlerConfigBuilder()
 *         .addConfig(schema, handler, { messageType: 'gcs.object.finalized' })
 *         .build(),
 *     }, context)
 *   }
 * }
 * ```
 */
export const GCS_NOTIFICATION_TYPE_RESOLVER: MessageTypeResolverConfig =
  createAttributeResolverWithMapping(
    GCS_EVENT_TYPE_ATTRIBUTE,
    {
      [GCS_EVENT_TYPES.OBJECT_FINALIZE]: 'gcs.object.finalized',
      [GCS_EVENT_TYPES.OBJECT_DELETE]: 'gcs.object.deleted',
      [GCS_EVENT_TYPES.OBJECT_ARCHIVE]: 'gcs.object.archived',
      [GCS_EVENT_TYPES.OBJECT_METADATA_UPDATE]: 'gcs.object.metadataUpdated',
    },
    { fallbackToOriginal: true },
  )

/**
 * Pre-built resolver for Google Cloud Storage notifications (raw types).
 *
 * Same as `GCS_NOTIFICATION_TYPE_RESOLVER` but returns the raw GCS event type
 * without mapping (e.g., `OBJECT_FINALIZE` instead of `gcs.object.finalized`).
 *
 * @see https://cloud.google.com/storage/docs/pubsub-notifications
 *
 * @example
 * ```typescript
 * class GcsNotificationConsumer extends AbstractPubSubConsumer {
 *   constructor(deps: PubSubConsumerDependencies) {
 *     super(deps, {
 *       messageTypeResolver: GCS_NOTIFICATION_RAW_TYPE_RESOLVER,
 *       handlers: new MessageHandlerConfigBuilder()
 *         .addConfig(schema, handler, { messageType: 'OBJECT_FINALIZE' })
 *         .build(),
 *     }, context)
 *   }
 * }
 * ```
 */
export const GCS_NOTIFICATION_RAW_TYPE_RESOLVER: MessageTypeResolverConfig =
  createAttributeResolver(GCS_EVENT_TYPE_ATTRIBUTE)
