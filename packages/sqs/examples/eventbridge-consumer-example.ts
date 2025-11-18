/**
 * Example: Consuming EventBridge-style events with SQS
 *
 * This example demonstrates how to consume non-standard message formats
 * like AWS EventBridge events, where:
 * - Metadata fields (id, type, timestamp) are at different locations
 * - The actual payload is nested in a field (e.g., 'detail')
 * - Field names don't match the default structure
 */

import { SQSClient } from '@aws-sdk/client-sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import { AbstractSqsConsumer, createEventBridgeSchema } from '@message-queue-toolkit/sqs'
import z from 'zod/v4'

// ============================================================================
// Step 1: Define your payload (detail) schemas
// ============================================================================

/**
 * Define the 'detail' field schemas
 * These are what your handlers will receive
 */
const USER_PRESENCE_DETAIL_SCHEMA = z.object({
  topicName: z.string(),
  userId: z.string(),
  organizationId: z.string(),
  presenceDefinition: z.object({
    id: z.string(),
    systemPresence: z.string(),
    mobilePresence: z.string(),
    aggregationPresence: z.string(),
    message: z.string().nullable(),
  }),
  timestamp: z.string(),
})

const USER_ROUTING_STATUS_DETAIL_SCHEMA = z.object({
  topicName: z.string(),
  userId: z.string(),
  organizationId: z.string(),
  routingStatus: z.object({
    id: z.string(),
    status: z.string(),
    startTime: z.string().optional(),
  }),
  timestamp: z.string(),
})

// ============================================================================
// Step 2: Create envelope schemas for routing
// ============================================================================

/**
 * Create EventBridge envelope schemas with literal detail-type values.
 * These are used for routing messages to the correct handler.
 *
 * The second parameter is the literal detail-type value that will be matched
 * for routing purposes.
 */
const USER_PRESENCE_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_PRESENCE_DETAIL_SCHEMA,
  'v2.users.{id}.presence',
)

const USER_ROUTING_STATUS_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_ROUTING_STATUS_DETAIL_SCHEMA,
  'v2.users.{id}.routing.status',
)

// Type definitions
type UserPresencePayload = z.output<typeof USER_PRESENCE_DETAIL_SCHEMA>
type UserRoutingStatusPayload = z.output<typeof USER_ROUTING_STATUS_DETAIL_SCHEMA>
type SupportedPayloads = UserPresencePayload | UserRoutingStatusPayload

type EventBridgeContext = {
  userPresenceMessages: UserPresencePayload[]
  userRoutingStatusMessages: UserRoutingStatusPayload[]
}

// ============================================================================
// Step 3: Create a consumer with EventBridge-specific configuration
// ============================================================================

class EventBridgeConsumer extends AbstractSqsConsumer<SupportedPayloads, EventBridgeContext> {
  constructor(sqsClient: SQSClient, queueUrl: string, executionContext: EventBridgeContext) {
    super(
      {
        sqsClient,
        consumerErrorResolver: {
          // @ts-expect-error Irrelevant for the example
          // biome-ignore lint/suspicious/noExplicitAny: Example code
          processError: (error: any) => error as Error,
        },
        errorReporter: {
          // biome-ignore lint/suspicious/noConsole: Irrelevant for the example
          report: (error) => console.error('Error:', error),
        },
        // @ts-expect-error Irrelevant for the example
        logger: console,
        transactionObservabilityManager: undefined,
      },
      {
        locatorConfig: {
          queueUrl,
        },

        // ======================================================================
        // KEY CONFIGURATION: Map EventBridge fields to toolkit expectations
        // ======================================================================

        // EventBridge uses 'detail-type' instead of 'type'
        messageTypeField: 'detail-type',

        // EventBridge uses 'id' for message ID (same as default)
        messageIdField: 'id',

        // EventBridge uses 'time' instead of 'timestamp'
        messageTimestampField: 'time',

        // IMPORTANT: Extract the 'detail' field as the payload
        // This means handlers will receive the content of 'detail', not the full envelope
        messagePayloadField: 'detail',

        // Look for 'detail-type' in the root message (not in the extracted payload)
        // This is required when using envelope schemas for routing
        messageTypeFromFullMessage: true,

        // Extract timestamp from the root message for metadata/logging
        // The 'time' field is in the envelope, not in the extracted 'detail' payload
        messageTimestampFromFullMessage: true,

        // ======================================================================
        // Handler configuration with envelope and payload schemas
        // ======================================================================
        //
        // Using 3-parameter addConfig:
        // - First param: Envelope schema (for routing with literal detail-type)
        // - Second param: Payload schema (for validating extracted detail)
        // - Third param: Handler (receives validated detail payload)

        handlers: new MessageHandlerConfigBuilder<SupportedPayloads, EventBridgeContext>()
          .addConfig(
            USER_PRESENCE_ENVELOPE_SCHEMA,
            USER_PRESENCE_DETAIL_SCHEMA,
            (payload: UserPresencePayload, context: EventBridgeContext) => {
              context.userPresenceMessages.push(payload)
              return Promise.resolve({ result: 'success' as const })
            },
          )
          .addConfig(
            USER_ROUTING_STATUS_ENVELOPE_SCHEMA,
            USER_ROUTING_STATUS_DETAIL_SCHEMA,
            (payload: UserRoutingStatusPayload, context: EventBridgeContext) => {
              context.userRoutingStatusMessages.push(payload)
              return Promise.resolve({ result: 'success' as const })
            },
          )
          .build(),
      },
      executionContext,
    )
  }
}

// ============================================================================
// Step 4: Example EventBridge event structure
// ============================================================================

/**
 * This is what the actual EventBridge event looks like in SQS.
 * Note the structure:
 * - Metadata is at root level: id, detail-type, time
 * - Payload is nested in 'detail' field
 *
 * Processing flow with two-schema approach:
 * 1. Envelope schema matches 'detail-type' for routing
 * 2. messagePayloadField: 'detail' extracts the detail field
 * 3. Payload schema validates the extracted detail field
 * 4. Handler receives the validated detail field content
 */
const exampleEventBridgeEvent = {
  version: '0',
  id: '123e4567-e89b-12d3-a456-426614174000',
  'detail-type': 'v2.users.abcdef12-3456-7890-abcd-ef1234567890.presence',
  source: 'genesys.cloud',
  account: '111222333444',
  time: '2025-11-18T12:34:56.789Z',
  region: 'us-east-1',
  resources: [],
  detail: {
    // This is what the handler receives
    topicName: 'v2.users.abcdef12-3456-7890-abcd-ef1234567890.presence',
    userId: 'abcdef12-3456-7890-abcd-ef1234567890',
    organizationId: 'org12345-6789-abcd-ef01-234567890abc',
    presenceDefinition: {
      id: '1',
      systemPresence: 'AVAILABLE',
      mobilePresence: 'OFFLINE',
      aggregationPresence: 'AVAILABLE',
      message: null,
    },
    timestamp: '2025-11-18T12:34:56.789Z',
  },
}

// ============================================================================
// Step 5: Usage example
// ============================================================================

async function _main() {
  const sqsClient = new SQSClient({ region: 'us-east-1' })
  const queueUrl = 'https://sqs.us-east-1.amazonaws.com/123456789012/eventbridge-queue'

  const context: EventBridgeContext = {
    userPresenceMessages: [],
    userRoutingStatusMessages: [],
  }

  const consumer = new EventBridgeConsumer(sqsClient, queueUrl, context)

  await consumer.start()

  // Consumer is now listening for EventBridge events
  // Handlers will receive the 'detail' field content directly
}

// ============================================================================
// Key Benefits
// ============================================================================

/**
 * 1. **Type-safe routing**: Envelope schemas with literal detail-type values ensure
 *    correct message routing at compile time
 *
 * 2. **Clean payload validation**: Payload schemas validate only the data handlers need
 *
 * 3. **No message transformation needed**: The toolkit handles extraction automatically
 *
 * 4. **Type-safe handlers**: Handlers receive properly typed payload objects
 *
 * 5. **Metadata still accessible**: Deduplication, logging, etc. use root-level fields
 *
 * 6. **Clean handler code**: No need to manually extract 'detail' in every handler
 *
 * 7. **Standard toolkit features work**: Retries, DLQ, deduplication, etc. all work
 */

// ============================================================================
// Comparison: Single Schema vs Two-Schema Approach
// ============================================================================

/**
 * SINGLE SCHEMA APPROACH (for simple cases):
 *
 * ```typescript
 * // When payload and message are the same structure
 * .addConfig(
 *   SIMPLE_MESSAGE_SCHEMA,
 *   async (message, context) => {
 *     console.log(message.userId)
 *     return { result: 'success' as const }
 *   }
 * )
 * ```
 *
 * TWO-SCHEMA APPROACH (for EventBridge-style envelopes):
 *
 * ```typescript
 * // Envelope schema for routing, payload schema for validation
 * .addConfig(
 *   USER_PRESENCE_ENVELOPE_SCHEMA,  // Routes using 'detail-type'
 *   USER_PRESENCE_DETAIL_SCHEMA,     // Validates extracted 'detail'
 *   async (detail, context) => {
 *     console.log(detail.userId)     // Handler receives detail only
 *     return { result: 'success' as const }
 *   }
 * )
 * ```
 *
 * The two-schema approach separates concerns:
 * - Envelope schema: Full message structure + literal type values for routing
 * - Payload schema: Extracted payload structure for validation and handler typing
 */

export { EventBridgeConsumer, exampleEventBridgeEvent }
