/**
 * Example: Consuming EventBridge-style events with SQS
 *
 * This example demonstrates how to consume AWS EventBridge events, where:
 * - Field names differ from toolkit defaults ('detail-type' vs 'type', 'time' vs 'timestamp')
 * - The business payload is nested in the 'detail' field
 * - Handlers receive the full EventBridge envelope and access nested data directly
 */

import { SQSClient } from '@aws-sdk/client-sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import { AbstractSqsConsumer, createEventBridgeSchema } from '@message-queue-toolkit/sqs'
import z from 'zod/v4'

// ============================================================================
// Step 1: Define your detail field schemas
// ============================================================================

/**
 * Define the 'detail' field schemas.
 * These describe the business payload nested within EventBridge events.
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
// Step 2: Create EventBridge envelope schemas
// ============================================================================

/**
 * Create full EventBridge envelope schemas with literal detail-type values.
 * These schemas:
 * - Validate the complete EventBridge message structure
 * - Include literal 'detail-type' values for routing
 * - Are what handlers receive (the full envelope)
 */
const USER_PRESENCE_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_PRESENCE_DETAIL_SCHEMA,
  'v2.users.{id}.presence',
)

const USER_ROUTING_STATUS_ENVELOPE_SCHEMA = createEventBridgeSchema(
  USER_ROUTING_STATUS_DETAIL_SCHEMA,
  'v2.users.{id}.routing.status',
)

// Type definitions - handlers receive the full envelope
type UserPresenceEnvelope = z.output<typeof USER_PRESENCE_ENVELOPE_SCHEMA>
type UserRoutingStatusEnvelope = z.output<typeof USER_ROUTING_STATUS_ENVELOPE_SCHEMA>
type SupportedEvents = UserPresenceEnvelope | UserRoutingStatusEnvelope

type EventBridgeContext = {
  userPresenceMessages: UserPresenceEnvelope[]
  userRoutingStatusMessages: UserRoutingStatusEnvelope[]
}

// ============================================================================
// Step 3: Create a consumer with EventBridge-specific configuration
// ============================================================================

class EventBridgeConsumer extends AbstractSqsConsumer<SupportedEvents, EventBridgeContext> {
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
        messageTypeResolver: { messageTypePath: 'detail-type' },

        // EventBridge uses 'id' for message ID (same as default)
        messageIdField: 'id',

        // EventBridge uses 'time' instead of 'timestamp'
        messageTimestampField: 'time',

        // ======================================================================
        // Handler configuration
        // ======================================================================
        //
        // Handlers receive the full EventBridge envelope (validated by the schema)
        // and access the 'detail' field directly

        handlers: new MessageHandlerConfigBuilder<SupportedEvents, EventBridgeContext>()
          .addConfig(USER_PRESENCE_ENVELOPE_SCHEMA, (message, context) => {
            // Handler receives the full EventBridge envelope
            // Type is inferred from USER_PRESENCE_ENVELOPE_SCHEMA
            // Access the detail field directly
            context.userPresenceMessages.push(message)
            // biome-ignore lint/suspicious/noConsole: Example code
            console.log('User presence:', message.detail.userId, message.detail.presenceDefinition)
            return Promise.resolve({ result: 'success' as const })
          })
          .addConfig(USER_ROUTING_STATUS_ENVELOPE_SCHEMA, (message, context) => {
            // Handler receives the full EventBridge envelope
            // Type is inferred from USER_ROUTING_STATUS_ENVELOPE_SCHEMA
            // Access the detail field directly
            context.userRoutingStatusMessages.push(message)
            // biome-ignore lint/suspicious/noConsole: Example code
            console.log('Routing status:', message.detail.userId, message.detail.routingStatus)
            return Promise.resolve({ result: 'success' as const })
          })
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
 * - Business payload is nested in 'detail' field
 *
 * Processing flow:
 * 1. Envelope schema validates the complete message structure
 * 2. Literal 'detail-type' value is used for routing to the correct handler
 * 3. Handler receives the full validated EventBridge envelope
 * 4. Handler accesses message.detail to get the business payload
 */
const exampleEventBridgeEvent = {
  version: '0',
  id: '123e4567-e89b-12d3-a456-426614174000',
  'detail-type': 'v2.users.{id}.presence',
  source: 'genesys.cloud',
  account: '111222333444',
  time: '2025-11-18T12:34:56.789Z',
  region: 'us-east-1',
  resources: [],
  detail: {
    // This is what the handler receives
    topicName: 'v2.users.{id}.presence',
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
  // Handlers will receive the full EventBridge envelope
}

// ============================================================================
// Key Benefits
// ============================================================================

/**
 * 1. **Type-safe routing**: Literal detail-type values in schemas ensure
 *    correct message routing at compile time
 *
 * 2. **Full validation**: Envelope schema validates the entire EventBridge structure
 *
 * 3. **Simple configuration**: Just map field names - no payload extraction needed
 *
 * 4. **Type-safe handlers**: Handlers receive properly typed envelope objects with
 *    TypeScript knowing the exact structure of message.detail
 *
 * 5. **Full message access**: Handlers can access both envelope metadata (id, time, source)
 *    and business payload (detail) when needed
 *
 * 6. **Explicit code**: message.detail access is visible in handler code
 *
 * 7. **Standard toolkit features work**: Retries, DLQ, deduplication, etc. all work
 */

// ============================================================================
// Comparison: Standard Messages vs EventBridge Events
// ============================================================================

/**
 * STANDARD MESSAGE APPROACH (flat structure):
 *
 * ```typescript
 * const SIMPLE_MESSAGE_SCHEMA = z.object({
 *   type: z.literal('user.created'),
 *   userId: z.string(),
 *   email: z.string()
 * })
 *
 * .addConfig(
 *   SIMPLE_MESSAGE_SCHEMA,
 *   async (message, context) => {
 *     console.log(message.userId)  // Direct access
 *     return { result: 'success' as const }
 *   }
 * )
 * ```
 *
 * EVENTBRIDGE APPROACH (nested structure):
 *
 * ```typescript
 * const USER_PRESENCE_ENVELOPE = createEventBridgeSchema(
 *   USER_PRESENCE_DETAIL,
 *   'v2.users.{id}.presence'  // Literal for routing
 * )
 *
 * .addConfig(
 *   USER_PRESENCE_ENVELOPE,
 *   async (message, context) => {
 *     console.log(message.detail.userId)  // Access nested payload
 *     console.log(message.time)            // Access envelope metadata if needed
 *     return { result: 'success' as const }
 *   }
 * )
 * ```
 *
 * The EventBridge approach:
 * - Validates the full EventBridge envelope structure
 * - Uses literal 'detail-type' values for routing
 * - Handlers receive complete message with type-safe access to nested payload
 */

export { EventBridgeConsumer, exampleEventBridgeEvent }
