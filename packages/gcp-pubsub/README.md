# @message-queue-toolkit/gcp-pubsub
 
Google Cloud Pub/Sub implementation for the message-queue-toolkit. Provides a robust, type-safe abstraction for publishing and consuming messages from Google Cloud Pub/Sub topics and subscriptions.

## Table of Contents

- [Installation](#installation)
- [Features](#features)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
  - [Publisher](#publisher)
  - [Consumer](#consumer)
- [Configuration](#configuration)
  - [Resource Management](#resource-management)
    - [Updating Existing Resources](#updating-existing-resources)
    - [Resource Deletion](#resource-deletion)
  - [Topic Creation](#topic-creation)
  - [Subscription Configuration](#subscription-configuration)
  - [Locator Config (Production)](#locator-config-production)
  - [Publisher Options](#publisher-options)
  - [Consumer Options](#consumer-options)
- [Advanced Features](#advanced-features)
  - [Custom Message Field Names](#custom-message-field-names)
  - [Payload Offloading](#payload-offloading)
  - [Message Deduplication](#message-deduplication)
  - [Dead Letter Queue](#dead-letter-queue)
    - [Processing DLQ Messages with AbstractPubSubDlqConsumer](#processing-dlq-messages-with-abstractpubsubdlqconsumer)
  - [Message Ordering](#message-ordering)
  - [Message Retry Logic](#message-retry-logic)
  - [Message Handlers](#message-handlers)
  - [Pre-handlers and Barriers](#pre-handlers-and-barriers)
  - [Handler Spies](#handler-spies)
  - [Consumer Flow Control](#consumer-flow-control)
  - [Multiple Message Types](#multiple-message-types)
- [Error Handling](#error-handling)
- [Testing](#testing)
  - [TestPubSubPublisher](#testpubsubpublisher)
  - [Integration Tests with Emulator](#integration-tests-with-emulator)
  - [Unit Tests with Handler Spies](#unit-tests-with-handler-spies)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Links](#links)

## Installation

```bash
npm install @message-queue-toolkit/gcp-pubsub @google-cloud/pubsub zod
```

**Peer Dependencies:**
- `@google-cloud/pubsub` - Google Cloud Pub/Sub client
- `zod` - Schema validation

## Features

- ✅ **Type-safe message handling** with Zod schema validation
- ✅ **Publisher** for publishing messages to topics
- ✅ **Consumer** for consuming messages from subscriptions
- ✅ **Automatic retry logic** with exponential backoff
- ✅ **Dead Letter Queue (DLQ)** support
- ✅ **Message deduplication** (publisher and consumer level)
- ✅ **Payload offloading** for large messages (>10MB, GCS integration)
- ✅ **Message ordering** with ordering keys
- ✅ **Exactly-once delivery** support
- ✅ **Handler spies** for testing
- ✅ **Pre-handlers and barriers** for complex message processing
- ✅ **Flow control** for throughput management
- ✅ **Automatic topic/subscription creation** with validation

## Core Concepts

### Google Pub/Sub Architecture

Google Pub/Sub follows a strict topic/subscription model:

```text
Publisher → Topic → Subscription → Consumer
```

**Key concepts:**
- **Topics**: Named resources to which messages are published
- **Subscriptions**: Named resources representing message streams from a topic
- ❌ You CANNOT publish directly to subscriptions
- ❌ You CANNOT consume directly from topics
- ✅ One topic can have multiple subscriptions (fan-out)
- ✅ One subscription per consumer (or consumer group)

**Prerequisites:**
- Google Cloud Platform account
- Pub/Sub API enabled
- Appropriate IAM permissions

### Publishers

Publishers send messages to Pub/Sub topics. They handle:
- Message validation against Zod schemas
- Automatic serialization
- Optional deduplication (preventing duplicate sends)
- Optional payload offloading (for messages > 10MB)
- Message ordering (via ordering keys)

### Consumers

Consumers receive and process messages from Pub/Sub subscriptions. They handle:
- Message deserialization and validation
- Routing to appropriate handlers based on message type
- Automatic retry with exponential backoff
- Dead letter queue integration
- Optional deduplication (preventing duplicate processing)
- Message ordering guarantees
- Flow control for throughput management

### Message Schemas

Messages are validated using Zod schemas. Each message must have:
- A unique message type field (discriminator for routing) - configurable via `messageTypeField` (required)
- A message ID field (for tracking and deduplication) - configurable via `messageIdField` (default: `'id'`)
- A timestamp field (added automatically if missing) - configurable via `messageTimestampField` (default: `'timestamp'`)

**Note:** All field names are configurable, allowing you to adapt the library to your existing message schemas without modification.

## Quick Start

### Publisher

```typescript
import { PubSub } from '@google-cloud/pubsub'
import { AbstractPubSubPublisher } from '@message-queue-toolkit/gcp-pubsub'
import { z } from 'zod'

const pubSubClient = new PubSub({
  projectId: 'my-project',
  keyFilename: '/path/to/credentials.json',
})

// Define your message schema
const UserEventSchema = z.object({
  id: z.string(),
  messageType: z.literal('user.created'),
  timestamp: z.string().datetime(),
  userId: z.string(),
  email: z.string().email(),
})

type UserEvent = z.infer<typeof UserEventSchema>

class UserEventPublisher extends AbstractPubSubPublisher<UserEvent> {
  constructor() {
    super(
      {
        pubSubClient,
        logger,
        errorReporter,
      },
      {
        creationConfig: {
          topic: {
            name: 'user-events',
            options: {
              enableMessageOrdering: true, // Optional
            },
          },
        },
        messageSchemas: [UserEventSchema],
        messageTypeField: 'messageType',
        logMessages: true,
      }
    )
  }
}

// Usage
const publisher = new UserEventPublisher()
await publisher.init()

await publisher.publish({
  id: '123',
  messageType: 'user.created',
  timestamp: new Date().toISOString(),
  userId: 'user-456',
  email: 'user@example.com',
})
```

### Consumer

```typescript
import { PubSub } from '@google-cloud/pubsub'
import { AbstractPubSubConsumer, MessageHandlerConfigBuilder } from '@message-queue-toolkit/gcp-pubsub'

class UserEventConsumer extends AbstractPubSubConsumer<UserEvent, ExecutionContext> {
  constructor() {
    super(
      {
        pubSubClient,
        logger,
        errorReporter,
        consumerErrorResolver,
      },
      {
        creationConfig: {
          topic: {
            name: 'user-events',
          },
          subscription: {
            name: 'user-events-processor',
            options: {
              ackDeadlineSeconds: 60,
              enableMessageOrdering: true,
            },
          },
        },
        messageTypeField: 'messageType',
        handlers: new MessageHandlerConfigBuilder<UserEvent, ExecutionContext>()
          .addConfig(
            UserEventSchema,
            async (message, context) => {
              // Process the message
              console.log('Processing user:', message.userId)
              await saveToDatabase(message)
              return { result: 'success' }
            }
          )
          .build(),
      },
      {} // execution context
    )
  }
}

// Usage
const consumer = new UserEventConsumer()
await consumer.init()
await consumer.start() // Starts consuming messages
```

## Configuration

### Resource Management

#### Updating Existing Resources

When using `creationConfig`, you can control whether existing resources should be updated with new configuration:

```typescript
{
  creationConfig: {
    topic: {
      name: 'my-topic',
      options: {
        messageRetentionDuration: { seconds: 604800 },
      },
    },
    updateAttributesIfExists: true,  // Update existing resources (default: false)
  },
}
```

**Behavior:**
- `updateAttributesIfExists: false` (default): If topic/subscription exists, uses it as-is without updates
- `updateAttributesIfExists: true`: If topic/subscription exists, updates its metadata with new options
- Applies to both topics and subscriptions
- Useful for managing configuration changes across environments

#### Resource Deletion

For testing and development, you can configure automatic resource deletion:

```typescript
import { deletePubSub } from '@message-queue-toolkit/gcp-pubsub'

{
  deletionConfig: {
    deleteIfExists: true,              // Enable automatic deletion
    forceDeleteInProduction: false,    // Safety: prevent production deletion (default: false)
    waitForConfirmation: true,         // Poll until deletion confirmed (default: true)
  },
  creationConfig: {
    topic: { name: 'test-topic' },
    subscription: { name: 'test-subscription' },
  },
}
```

**Deletion Behavior:**
- Only deletes if both `deleteIfExists: true` and `creationConfig` are provided
- Deletes subscription first, then topic (proper order)
- Throws error if trying to delete in production without `forceDeleteInProduction: true`
- `waitForConfirmation: true`: Polls to confirm deletion completed (recommended)
- `waitForConfirmation: false`: Returns immediately after deletion request

**Production Safety:**

The library checks `process.env.NODE_ENV` to determine if running in production:
- `NODE_ENV === 'production'` → Production mode (deletion requires explicit override)
- Any other value → Development/test mode (deletion allowed)

**Important:** The environment check uses a cached scope from `@lokalise/node-core`. If you change `NODE_ENV` at runtime (e.g., in tests), you must call `reloadConfig()`:

```typescript
import { reloadConfig } from '@message-queue-toolkit/core'

// In tests - changing NODE_ENV at runtime
process.env.NODE_ENV = 'production'
reloadConfig()  // Required to pick up the change

// In normal usage - NODE_ENV set before process starts
// No reloadConfig() needed, environment is read on initialization
```

**Examples:**

```typescript
// Development - automatic cleanup
process.env.NODE_ENV = 'development'
{
  deletionConfig: {
    deleteIfExists: true,  // OK in development
  },
}

// Production - requires explicit override
process.env.NODE_ENV = 'production'
{
  deletionConfig: {
    deleteIfExists: true,
    forceDeleteInProduction: true,  // Required in production
  },
}
```

**Use Cases:**
- Integration tests: Clean up resources between test runs
- CI/CD pipelines: Ensure fresh environment for each build
- Development: Reset state without manual cleanup

### Topic Creation

When using `creationConfig`, the topic will be created automatically if it doesn't exist:

```typescript
{
  creationConfig: {
    topic: {
      name: 'my-topic',
      options: {
        messageRetentionDuration: {
          seconds: 604800, // 7 days
        },
        messageStoragePolicy: {
          allowedPersistenceRegions: ['us-central1'],
        },
        enableMessageOrdering: true,
        kmsKeyName: 'projects/my-project/locations/us/keyRings/my-ring/cryptoKeys/my-key',
      },
    },
  },
}
```

### Subscription Configuration

For consumers, configure the subscription:

```typescript
{
  creationConfig: {
    topic: {
      name: 'my-topic',
    },
    subscription: {
      name: 'my-subscription',
      options: {
        ackDeadlineSeconds: 60,
        retainAckedMessages: false,
        messageRetentionDuration: {
          seconds: 604800,
        },
        enableMessageOrdering: true,
        enableExactlyOnceDelivery: true,
        deadLetterPolicy: {
          deadLetterTopic: 'projects/my-project/topics/my-dlq',
          maxDeliveryAttempts: 5,
        },
        filter: 'attributes.priority="high"', // Message filtering
      },
    },
  },
}
```

### Locator Config (Production)

When using `locatorConfig`, you connect to existing resources without creating them:

```typescript
{
  locatorConfig: {
    topicName: 'existing-topic',
    subscriptionName: 'existing-subscription', // For consumers
  },
}
```

### Publisher Options

```typescript
{
  // Required - Message Schema Configuration
  messageSchemas: [Schema1, Schema2],  // Array of Zod schemas
  messageTypeField: 'messageType',     // Field containing message type discriminator

  // Topic Configuration (one of these required)
  creationConfig: {
    topic: { name: 'my-topic', options: { /* ... */ } },
    updateAttributesIfExists: false,   // Update existing resources (default: false)
  },
  locatorConfig: { /* ... */ },        // Use existing topic

  // Optional - Resource Deletion (testing/development)
  deletionConfig: {
    deleteIfExists: true,              // Enable automatic deletion
    forceDeleteInProduction: false,    // Safety: prevent production deletion
    waitForConfirmation: true,         // Poll until deletion confirmed
  },

  // Optional - Message Field Configuration
  messageIdField: 'id',                       // Field containing message ID (default: 'id')
  messageTimestampField: 'timestamp',         // Field containing timestamp (default: 'timestamp')
  messageDeduplicationIdField: 'deduplicationId',     // Field for deduplication ID (default: 'deduplicationId')
  messageDeduplicationOptionsField: 'deduplicationOptions', // Field for deduplication options (default: 'deduplicationOptions')

  // Optional - Features
  logMessages: false,                  // Log all published messages
  handlerSpy: true,                    // Enable handler spy for testing

  // Optional - Deduplication
  enablePublisherDeduplication: false, // Enable store-based deduplication
  messageDeduplicationConfig: {
    store: redisStore,                 // Redis-based deduplication store
    deduplicationIdField: 'id',        // Field to use for deduplication
  },

  // Optional - Payload Offloading
  payloadStoreConfig: {
    store: gcsStore,                   // GCS-based payload store
    messageSizeThreshold: PUBSUB_MESSAGE_MAX_SIZE, // 10 MB
  },
}
```

### Consumer Options

```typescript
{
  // Required - Message Handling Configuration
  handlers: MessageHandlerConfigBuilder.build(), // Message handlers configuration
  messageTypeField: 'messageType',               // Field containing message type discriminator

  // Topic and Subscription Configuration (one of these required)
  creationConfig: {
    topic: { name: 'my-topic' },
    subscription: { name: 'my-subscription', options: { /* ... */ } },
    updateAttributesIfExists: false,   // Update existing resources (default: false)
  },
  locatorConfig: { /* ... */ },

  // Optional - Resource Deletion (testing/development)
  deletionConfig: {
    deleteIfExists: true,              // Enable automatic deletion
    forceDeleteInProduction: false,    // Safety: prevent production deletion
    waitForConfirmation: true,         // Poll until deletion confirmed
  },

  // Optional - Message Field Configuration
  messageIdField: 'id',                       // Field containing message ID (default: 'id')
  messageTimestampField: 'timestamp',         // Field containing timestamp (default: 'timestamp')
  messageDeduplicationIdField: 'deduplicationId',     // Field for deduplication ID (default: 'deduplicationId')
  messageDeduplicationOptionsField: 'deduplicationOptions', // Field for deduplication options (default: 'deduplicationOptions')

  // Optional - Retry Configuration
  maxRetryDuration: 345600,            // 4 days in seconds (default)

  // Optional - Dead Letter Queue
  deadLetterQueue: {
    deadLetterPolicy: {
      maxDeliveryAttempts: 5,          // Move to DLQ after 5 failed attempts (5-100)
    },
    creationConfig: {
      topic: { name: 'my-dlq-topic' }, // Create DLQ topic
    },
    // OR
    locatorConfig: {
      topicName: 'existing-dlq-topic', // Use existing DLQ topic
    },
  },

  // Optional - Consumer Behavior
  consumerOverrides: {
    flowControl: {
      maxMessages: 100,                // Max concurrent messages
      maxBytes: 10 * 1024 * 1024,     // Max bytes in memory
    },
    batching: {
      maxMessages: 10,                 // Pull messages in batches
      maxMilliseconds: 100,            // Max wait time for batch
    },
  },

  // Optional - Deduplication
  enableConsumerDeduplication: false,
  messageDeduplicationConfig: {
    store: redisStore,
    deduplicationIdField: 'id',
    deduplicationLockTimeout: 20000,   // Lock timeout in milliseconds
  },

  // Optional - Payload Offloading
  payloadStoreConfig: {
    store: gcsStore,
  },

  // Optional - Other
  logMessages: false,
  handlerSpy: true,
}
```

## Advanced Features

### Custom Message Field Names

All message field names are configurable, allowing you to adapt the library to your existing message schemas:

```typescript
// Your existing message schema with custom field names
const CustomMessageSchema = z.object({
  messageId: z.string(),                     // Custom ID field
  eventType: z.literal('order.created'),     // Custom type field
  createdAt: z.string().datetime(),          // Custom timestamp field
  txId: z.string(),                          // Custom deduplication ID
  txOptions: z.object({                      // Custom deduplication options
    deduplicationWindowSeconds: z.number().optional(),
  }).optional(),
  orderId: z.string(),
  amount: z.number(),
})

// Configure the publisher to use your custom field names
class OrderPublisher extends AbstractPubSubPublisher<CustomMessage> {
  constructor() {
    super(
      { pubSubClient, logger, errorReporter },
      {
        messageSchemas: [CustomMessageSchema],

        // Map library's internal fields to your custom fields
        messageIdField: 'messageId',                    // Default: 'id'
        messageTypeField: 'eventType',                  // Required
        messageTimestampField: 'createdAt',             // Default: 'timestamp'
        messageDeduplicationIdField: 'txId',            // Default: 'deduplicationId'
        messageDeduplicationOptionsField: 'txOptions',  // Default: 'deduplicationOptions'

        creationConfig: {
          topic: { name: 'orders-topic' },
        },
      }
    )
  }
}

// Use with your custom schema
await publisher.publish({
  messageId: 'msg-123',       // Library will use this for tracking
  eventType: 'order.created', // Library will use this for routing
  createdAt: new Date().toISOString(), // Library will use this for retry tracking
  txId: 'tx-456',            // Library will use this for deduplication
  orderId: 'order-789',
  amount: 99.99,
})
```

**Benefits:**
- ✅ No need to modify existing message schemas
- ✅ Maintain consistency with your domain model
- ✅ Gradual migration from legacy systems
- ✅ Works with all features (retry, deduplication, offloading)

### Payload Offloading

For messages larger than 10 MB, store the payload externally (e.g., Google Cloud Storage):

```typescript
import { Storage } from '@google-cloud/storage'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import { PUBSUB_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/gcp-pubsub'

const storage = new Storage({ projectId: 'my-project' })
const payloadStore = new GCSPayloadStore(
  { gcsStorage: storage },
  { bucketName: 'my-payload-bucket' }
)

// Publisher configuration
class LargeMessagePublisher extends AbstractPubSubPublisher<MyMessage> {
  constructor() {
    super(dependencies, {
      creationConfig: {
        topic: { name: 'large-messages' },
      },
      messageSchemas: [MyMessageSchema],
      messageTypeField: 'type',
      payloadStoreConfig: {
        store: payloadStore,
        messageSizeThreshold: PUBSUB_MESSAGE_MAX_SIZE, // 10 MB
      },
    })
  }
}

// Large message is automatically offloaded
await publisher.publish({
  id: '123',
  messageType: 'document.processed',
  largeData: hugeArrayOfData,  // If total size > 10 MB, stored in GCS
})
```

**How it works:**
1. Publisher checks message size before sending
2. If size exceeds `messageSizeThreshold`, stores payload in GCS
3. Replaces payload with pointer: `{ _offloadedPayload: { bucketName, key, size } }`
4. Sends pointer message to Pub/Sub
5. Consumer detects pointer, fetches payload from GCS
6. Processes message with full payload

**Note:** Consumer automatically retrieves offloaded payloads - no special configuration needed! Payload cleanup is the responsibility of the store (e.g., GCS lifecycle policies).

### Message Deduplication

Prevent duplicate message publishing or processing:

#### Publisher-Level Deduplication

Prevents sending the same message multiple times:

```typescript
import { InMemoryDeduplicationStore } from '@message-queue-toolkit/core'
// or
import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'

const deduplicationStore = new RedisMessageDeduplicationStore(redisClient)

// Publisher configuration
{
  enablePublisherDeduplication: true,
  messageDeduplicationIdField: 'deduplicationId',
  messageDeduplicationConfig: {
    store: deduplicationStore,
  },
}

// Publishing with deduplication
await publisher.publish({
  id: '123',
  messageType: 'user.created',
  deduplicationId: 'user-456-creation',  // Unique key for deduplication
  deduplicationOptions: {
    deduplicationWindowSeconds: 60,      // Prevent duplicates for 60 seconds
  },
  userId: 'user-456',
})

// Second publish with same deduplicationId within 60s is skipped
await publisher.publish({
  id: '124',
  messageType: 'user.created',
  deduplicationId: 'user-456-creation',  // Duplicate - won't be sent
  userId: 'user-456',
})
```

#### Consumer-Level Deduplication

Prevents processing the same message multiple times:

```typescript
{
  enableConsumerDeduplication: true,
  messageDeduplicationIdField: 'deduplicationId',
  messageDeduplicationConfig: {
    store: deduplicationStore,
  },
}

// Message configuration
{
  deduplicationId: 'unique-operation-id',
  deduplicationOptions: {
    deduplicationWindowSeconds: 3600,  // 1 hour
    lockTimeoutSeconds: 20,            // Lock duration while processing
    acquireTimeoutSeconds: 20,         // Max wait time to acquire lock
    refreshIntervalSeconds: 10,        // Lock refresh interval
  },
}
```

**How it works:**
1. Consumer receives message
2. Checks deduplication store for duplicate
3. If duplicate found (within window), skips processing
4. If not duplicate, acquires exclusive lock
5. Processes message
6. Releases lock and marks as processed
7. Subsequent messages with same ID are skipped

### Dead Letter Queue

Dead Letter Queues capture messages that cannot be processed after multiple attempts. The library supports configuring DLQ in two ways:

#### Method 1: Create DLQ Topic Automatically

```typescript
{
  creationConfig: {
    topic: { name: 'my-topic' },
    subscription: { name: 'my-subscription' },
  },
  deadLetterQueue: {
    deadLetterPolicy: {
      maxDeliveryAttempts: 5,  // Send to DLQ after 5 failed attempts (5-100)
    },
    creationConfig: {
      topic: { name: 'my-dlq-topic' },  // Creates topic if it doesn't exist
    },
  },
}
```

#### Method 2: Use Existing DLQ Topic

```typescript
{
  creationConfig: {
    topic: { name: 'my-topic' },
    subscription: { name: 'my-subscription' },
  },
  deadLetterQueue: {
    deadLetterPolicy: {
      maxDeliveryAttempts: 5,
    },
    locatorConfig: {
      topicName: 'existing-dlq-topic',  // Must exist, or init() will throw
    },
  },
}
```

**How it works:**
1. Message fails processing (handler returns error or throws)
2. Message becomes available again (after ack deadline)
3. Consumer receives message again (delivery attempt increments)
4. Pub/Sub tracks delivery attempts = 1 + (NACKs + ack deadline exceeded)
5. After `maxDeliveryAttempts` attempts, Pub/Sub automatically forwards message to DLQ topic
6. DLQ messages can be inspected, reprocessed, or deleted

**Important Notes:**
- `maxDeliveryAttempts` must be between 5 and 100
- DLQ is handled natively by Google Pub/Sub (no manual forwarding needed)
- When message is forwarded to DLQ, it's wrapped with metadata attributes:
  - `CloudPubSubDeadLetterSourceDeliveryCount`: Number of delivery attempts
  - `CloudPubSubDeadLetterSourceSubscription`: Source subscription name
  - `CloudPubSubDeadLetterSourceSubscriptionProject`: Source project
  - `CloudPubSubDeadLetterSourceTopicPublishTime`: Original publish timestamp
- Create a subscription on the DLQ topic to process dead-lettered messages
- Ensure Pub/Sub service account has permissions on the DLQ topic

#### Processing DLQ Messages with AbstractPubSubDlqConsumer

The library provides `AbstractPubSubDlqConsumer`, a convenience class for consuming messages from a DLQ topic. Unlike regular consumers that route messages by type, DLQ consumers accept any message structure since dead-lettered messages can come from various failed processing scenarios.

```typescript
import { AbstractPubSubDlqConsumer, type DlqMessage } from '@message-queue-toolkit/gcp-pubsub'

class MyDlqConsumer extends AbstractPubSubDlqConsumer<MyContext> {
  constructor(dependencies: PubSubConsumerDependencies, context: MyContext) {
    super(
      dependencies,
      {
        creationConfig: {
          topic: { name: 'my-dlq-topic' },
          subscription: { name: 'my-dlq-subscription' },
        },
        handlerSpy: true,  // Optional: for testing
        handler: async (message, context) => {
          // message is typed as DlqMessage (has 'id' field plus any other fields)
          console.log('DLQ message received:', message.id)

          // Log the dead letter for investigation
          await context.logger.error('Dead letter received', { message })

          // Optionally reprocess or store for manual review
          await context.deadLetterRepository.save(message)

          return { result: 'success' }
        },
      },
      context,
    )
  }
}

// Usage
const dlqConsumer = new MyDlqConsumer(dependencies, myContext)
await dlqConsumer.start()
```

**Key differences from AbstractPubSubConsumer:**
- Does NOT require `messageTypeField` (accepts all message types)
- Uses a passthrough schema that accepts any message with an `id` field
- Simplified handler configuration (single handler for all messages)
- The `DlqMessage` type includes `id: string` and passes through all other fields

**When to use:**
- Processing messages that failed validation or deserialization
- Logging and alerting on dead-lettered messages
- Implementing manual review workflows
- Re-routing messages to other systems for investigation

### Message Retry Logic

The library implements intelligent retry logic with exponential backoff:

```typescript
{
  maxRetryDuration: 345600,  // 4 days in seconds (default)
}
```

**Retry Flow:**

1. **Handler returns `{ error: 'retryLater' }`** or **throws an error**
2. Consumer checks if message should be retried:
   - Calculates how long the message has been retrying
   - If within `maxRetryDuration`, re-queues message (nacks it)
   - If exceeded, sends to DLQ (if configured) or marks as failed

3. **Exponential Backoff:**
   ```text
   Attempt 1: Message nacked, redelivered by Pub/Sub
   Attempt 2: Message nacked, redelivered by Pub/Sub
   Attempt 3: Message nacked, redelivered by Pub/Sub
   ...
   After maxDeliveryAttempts: Sent to DLQ
   ```

**Handler Return Types:**

```typescript
type HandlerResult = Either<'retryLater', 'success'>

// Success - message is acknowledged
return { result: 'success' }

// Retry - message is nacked, will be retried
return { error: 'retryLater' }

// Error thrown - automatically retries
throw new Error('Database connection failed')
```

### Message Ordering

Enable ordered delivery of messages with the same ordering key:

```typescript
// Publisher configuration
{
  creationConfig: {
    topic: {
      name: 'ordered-events',
      options: {
        enableMessageOrdering: true,
      },
    },
  },
}

// Publish with ordering key
await publisher.publish(message, {
  orderingKey: 'user-123', // All messages with this key are delivered in order
})

// Consumer configuration
{
  creationConfig: {
    subscription: {
      options: {
        enableMessageOrdering: true,
      },
    },
  },
}
```

**Ordering guarantees:**
- ✅ Messages with the same ordering key are delivered in order
- ✅ Messages are processed exactly once (when combined with exactly-once delivery)
- ❌ No ordering guarantee across different ordering keys

### Message Handlers

Handlers process messages based on their type. Messages are routed to the appropriate handler using the discriminator field (configurable via `messageTypeField`):

```typescript
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

const handlers = new MessageHandlerConfigBuilder<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
>()
  .addConfig(
    UserCreatedSchema,
    async (message, context, preHandlingOutputs) => {
      // Access execution context
      await context.userService.createUser(message.userId)

      // Access pre-handler outputs
      console.log('Pre-handler result:', preHandlingOutputs.preHandlerOutput)
      console.log('Barrier result:', preHandlingOutputs.barrierOutput)

      return { result: 'success' }
    },
    {
      // Optional: Pre-handlers (run before main handler)
      preHandlers: [
        (message, context, output, next) => {
          console.log('Pre-processing message:', message.id)
          output.processedAt = Date.now()
          next({ result: 'success' })
        },
      ],

      // Optional: Barrier (controls whether message should be processed)
      preHandlerBarrier: async (message, context, preHandlerOutput) => {
        const isReady = await context.userService.isSystemReady()
        return {
          isPassing: isReady,
          output: { systemStatus: 'ready' },
        }
      },

      // Optional: Custom message log formatter
      messageLogFormatter: (message) => ({
        userId: message.userId,
        action: 'create',
      }),
    }
  )
  .addConfig(UserUpdatedSchema, handleUserUpdated)
  .build()
```

### Pre-handlers and Barriers

#### Pre-handlers

Pre-handlers are middleware functions that run before the main message handler, allowing you to:
- Enrich the execution context with additional data
- Set up scoped resources (child loggers, database transactions)
- Validate prerequisites
- Transform message data
- Implement cross-cutting concerns (logging, metrics, caching)

The output from pre-handlers is passed to both the barrier and the main handler, enabling a powerful data flow pattern.

**Type Signature:**

```typescript
type Prehandler<Message, Context, Output> = (
  message: Message,
  context: Context,
  output: Output,
  next: (result: PrehandlerResult) => void
) => void
```

**Common Use Cases:**

##### 1. Child Logger Resolution

Create message-specific loggers with contextual information:

```typescript
type PrehandlerOutput = {
  logger: Logger
}

const preHandlers: Prehandler<UserMessage, ExecutionContext, PrehandlerOutput>[] = [
  (message, context, output, next) => {
    // Create child logger with message context
    output.logger = context.logger.child({
      messageId: message.id,
      messageType: message.messageType,
      userId: message.userId,
      correlationId: message.correlationId,
    })

    output.logger.info('Message processing started')
    next({ result: 'success' })
  },
]

// In your handler
const handler = async (message, context, preHandlingOutputs) => {
  const logger = preHandlingOutputs.preHandlerOutput.logger

  logger.info('Processing user update') // Automatically includes message context
  logger.error({ error: someError }, 'Failed to update user')

  return { result: 'success' }
}
```

##### 2. User Data and Permissions Resolution

Fetch and cache user information needed by the handler:

```typescript
type PrehandlerOutput = {
  user: User
  permissions: string[]
  organizationId: string
}

const preHandlers: Prehandler<OrderMessage, ExecutionContext, PrehandlerOutput>[] = [
  // Fetch user data
  async (message, context, output, next) => {
    try {
      const user = await context.userRepository.findById(message.userId)
      if (!user) {
        next({ error: new Error(`User ${message.userId} not found`) })
        return
      }
      output.user = user
      next({ result: 'success' })
    } catch (error) {
      next({ error })
    }
  },

  // Resolve permissions
  async (message, context, output, next) => {
    try {
      output.permissions = await context.permissionService.getPermissions(output.user.id)
      output.organizationId = output.user.organizationId
      next({ result: 'success' })
    } catch (error) {
      next({ error })
    }
  },
]

// In your handler - user data is already fetched
const handler = async (message, context, preHandlingOutputs) => {
  const { user, permissions, organizationId } = preHandlingOutputs.preHandlerOutput

  // Check permissions
  if (!permissions.includes('orders:create')) {
    throw new Error('Insufficient permissions')
  }

  // Use pre-fetched data
  await context.orderService.createOrder({
    orderId: message.orderId,
    userId: user.id,
    organizationId,
    userEmail: user.email, // Already available, no need to fetch again
  })

  return { result: 'success' }
}
```

#### Barriers

Barriers are async functions that determine whether a message should be processed immediately or retried later. They are essential for handling message dependencies and ensuring prerequisites are met.

**Type Signature:**

```typescript
type BarrierCallback<Message, Context, PrehandlerOutput, BarrierOutput> = (
  message: Message,
  context: Context,
  preHandlerOutput: PrehandlerOutput
) => Promise<BarrierResult<BarrierOutput>>

type BarrierResult<Output> = {
  isPassing: boolean    // true = process now, false = retry later
  output: Output        // Additional data passed to the handler
}
```

**Common Use Cases:**

##### 1. Message Ordering Dependencies

Ensure messages are processed in the correct order when they arrive out of sequence:

```typescript
// Scenario: Process order.updated only after order.created
const preHandlerBarrier = async (message: OrderUpdatedMessage, context, preHandlerOutput) => {
  // Check if the order exists (created event was processed)
  const orderExists = await context.orderRepository.exists(message.orderId)

  if (!orderExists) {
    context.logger.warn('Order not found, retrying later', {
      orderId: message.orderId,
      messageId: message.id,
    })

    return {
      isPassing: false,
      output: { reason: 'order_not_created_yet' },
    }
  }

  return {
    isPassing: true,
    output: { orderExists: true },
  }
}

// Message will be automatically retried until order.created is processed
```

##### 2. Business Workflow Prerequisites

Implement complex business logic gates:

```typescript
// Scenario: Process payment only after KYC verification is complete
const preHandlerBarrier = async (
  message: PaymentMessage,
  context,
  preHandlerOutput
) => {
  const { user } = preHandlerOutput // From pre-handler

  // Check KYC status
  const kycStatus = await context.kycService.getStatus(user.id)

  if (kycStatus !== 'approved') {
    context.logger.info('KYC not approved, retrying later', {
      userId: user.id,
      kycStatus,
    })

    return {
      isPassing: false,
      output: {
        reason: 'kyc_pending',
        kycStatus,
        retriedAt: new Date(),
      },
    }
  }

  // Check account balance
  const balance = await context.accountService.getBalance(user.id)
  if (balance < message.amount) {
    context.logger.info('Insufficient balance, retrying later', {
      userId: user.id,
      balance,
      required: message.amount,
    })

    return {
      isPassing: false,
      output: {
        reason: 'insufficient_balance',
        balance,
        required: message.amount,
      },
    }
  }

  return {
    isPassing: true,
    output: {
      kycApproved: true,
      currentBalance: balance,
    },
  }
}

const handler = async (message, context, preHandlingOutputs) => {
  const { kycApproved, currentBalance } = preHandlingOutputs.barrierOutput

  // Safe to process payment - all prerequisites met
  await context.paymentService.processPayment({
    userId: message.userId,
    amount: message.amount,
    currentBalance, // From barrier
  })

  return { result: 'success' }
}
```

**Configuration:**

```typescript
new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext, PrehandlerOutput>()
  .addConfig(
    MessageSchema,
    handler,
    {
      preHandlers: [userDataPreHandler, permissionsPreHandler],
      preHandlerBarrier: orderDependencyBarrier,
    }
  )
  .build()
```

**Important Notes:**

- **Barriers return `isPassing: false`** → Message is automatically retried (nacked)
- **Barriers throw errors** → Message follows normal error handling (retry or DLQ)
- **Barrier output** → Available in handler via `preHandlingOutputs.barrierOutput`
- **Retry limits apply** → Messages exceeding `maxRetryDuration` will be sent to DLQ even if barrier keeps returning false

### Handler Spies

Handler spies solve the fundamental challenge of testing asynchronous message-based systems.

**The Problem:**

Testing message queues is complex because:
1. **Asynchronous processing** - Messages are published and consumed asynchronously with unpredictable timing
2. **Indirect interactions** - Business logic may trigger message publishing without explicit calls to the publisher
3. **Non-deterministic order** - Messages may be processed in different orders across test runs
4. **Hard to verify** - Traditional mocking/stubbing doesn't work well for async pub/sub patterns

**The Solution:**

Handler spies provide a way to wait for and inspect messages during tests without having to:
- Poll the topic/subscription directly
- Add artificial delays (`setTimeout`)
- Mock the entire message infrastructure
- Modify production code for testing

#### Configuration

```typescript
// Enable handler spy for publisher and/or consumer
const publisher = new UserEventsPublisher(pubSubClient, {
  handlerSpy: true,  // Track published messages
})

const consumer = new UserEventsConsumer(pubSubClient, {
  handlerSpy: true,  // Track consumed messages
})
```

#### Example: Testing Message Publishing and Consumption

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

describe('User Events Flow', () => {
  let publisher: UserEventsPublisher
  let consumer: UserEventsConsumer

  beforeEach(async () => {
    publisher = new UserEventsPublisher(pubSubClient, { handlerSpy: true })
    consumer = new UserEventsConsumer(pubSubClient, { handlerSpy: true })

    await publisher.init()
    await consumer.start()
  })

  afterEach(async () => {
    await consumer.close()
    await publisher.close()
  })

  it('processes user.created event', async () => {
    // Act: Publish message
    await publisher.publish({
      id: 'msg-123',
      messageType: 'user.created',
      userId: 'user-456',
      email: 'test@example.com',
    })

    // Assert: Wait for message to be tracked by publisher spy
    const publishedMessage = await publisher.handlerSpy.waitForMessageWithId(
      'msg-123',
      'published',
      5000 // 5 second timeout
    )

    expect(publishedMessage).toMatchObject({
      id: 'msg-123',
      userId: 'user-456',
      email: 'test@example.com',
    })

    // Assert: Wait for message to be consumed
    const consumedMessage = await consumer.handlerSpy.waitForMessageWithId(
      'msg-123',
      'consumed',
      10000 // 10 second timeout
    )

    expect(consumedMessage.userId).toBe('user-456')
  })

  it('checks message without waiting', async () => {
    await publisher.publish({
      id: 'msg-789',
      messageType: 'user.deleted',
      userId: 'user-123',
    })

    // Wait briefly for async processing
    await new Promise(resolve => setTimeout(resolve, 100))

    // Check without waiting
    const result = consumer.handlerSpy.checkMessage(
      (msg) => msg.id === 'msg-789'
    )

    if (result) {
      expect(result.message.userId).toBe('user-123')
      expect(result.processingResult.status).toBe('consumed')
    } else {
      throw new Error('Message not found')
    }
  })
})
```

#### Handler Spy API Reference

```typescript
interface HandlerSpy<Message> {
  // Wait for message by ID (with timeout)
  waitForMessageWithId(
    messageId: string,
    state: 'consumed' | 'published' | 'retryLater',
    timeout?: number // Default: 15000ms
  ): Promise<Message>

  // Wait for message matching predicate (with timeout)
  waitForMessage(
    predicate: (message: Message) => boolean,
    state: 'consumed' | 'published' | 'retryLater',
    timeout?: number // Default: 15000ms
  ): Promise<Message>

  // Check if message exists without waiting
  checkMessage(
    predicate: (message: Message) => boolean
  ): { message: Message; processingResult: ProcessingResult } | undefined

  // Get all tracked messages (circular buffer, limited size)
  getAllMessages(): Array<{ message: Message; processingResult: ProcessingResult }>
}
```

**Best Practices:**

1. **Always set timeouts** - Tests can hang indefinitely if messages don't arrive
2. **Use specific predicates** - Avoid overly broad matchers that could match wrong messages
3. **Clean up between tests** - Reset handler spies or recreate publishers/consumers
4. **Use in integration tests** - Handler spies are most valuable for integration tests, not unit tests
5. **Don't use in production** - Handler spies add memory overhead (circular buffer of messages)

### Consumer Flow Control

Control message throughput:

```typescript
{
  consumerOverrides: {
    flowControl: {
      maxMessages: 100,      // Max concurrent messages
      maxBytes: 10 * 1024 * 1024, // Max bytes in memory
    },
    batching: {
      maxMessages: 10,       // Pull messages in batches
      maxMilliseconds: 100,  // Max wait time for batch
    },
  },
}
```

### Multiple Message Types

Handle different message types in one consumer:

```typescript
const UserCreatedSchema = z.object({
  messageType: z.literal('user.created'),
  userId: z.string(),
})

const UserDeletedSchema = z.object({
  messageType: z.literal('user.deleted'),
  userId: z.string(),
})

type UserEvent = z.infer<typeof UserCreatedSchema> | z.infer<typeof UserDeletedSchema>

handlers: new MessageHandlerConfigBuilder<UserEvent, Context>()
  .addConfig(UserCreatedSchema, async (message) => {
    console.log('User created:', message.userId)
    return { result: 'success' }
  })
  .addConfig(UserDeletedSchema, async (message) => {
    console.log('User deleted:', message.userId)
    return { result: 'success' }
  })
  .build()
```

## Error Handling

### Handler Returns

```typescript
type HandlerResult = Either<'retryLater', 'success'>

async (message) => {
  try {
    await processMessage(message)
    return { result: 'success' } // Message ACKed
  } catch (error) {
    if (isRetryable(error)) {
      return { error: 'retryLater' } // Message NACKed, will be retried
    }
    throw error // Message NACKed, will be retried
  }
}
```

### Terminal Errors and DLQ Behavior

When a message cannot be processed (invalid format, schema validation failure, handler error, or max retry duration exceeded), the consumer handles it based on whether a Dead Letter Queue is configured:

**With DLQ configured:**
- Message is NACKed
- Pub/Sub tracks delivery attempts
- After `maxDeliveryAttempts`, message is automatically forwarded to DLQ
- This is the recommended approach for production systems

**Without DLQ configured:**
- Message is ACKed (acknowledged) to prevent infinite redelivery
- A warning is logged indicating the message was acknowledged without DLQ
- The message is effectively dropped
- This prevents poison messages from blocking the subscription

```typescript
// Without DLQ - invalid messages are acknowledged to prevent infinite redelivery
{
  creationConfig: {
    topic: { name: 'my-topic' },
    subscription: { name: 'my-subscription' },
  },
  // No deadLetterQueue configured
  // Invalid messages will be ACKed with a warning log
}

// With DLQ - invalid messages go to DLQ after max attempts
{
  creationConfig: {
    topic: { name: 'my-topic' },
    subscription: { name: 'my-subscription' },
  },
  deadLetterQueue: {
    deadLetterPolicy: { maxDeliveryAttempts: 5 },
    creationConfig: { topic: { name: 'my-dlq-topic' } },
  },
}
```

**Best Practice:** Always configure a DLQ in production to capture and analyze failed messages.

### Error Resolver

```typescript
import { PubSubConsumerErrorResolver } from '@message-queue-toolkit/gcp-pubsub'

const consumerErrorResolver = new PubSubConsumerErrorResolver()

// Or custom implementation
class CustomErrorResolver implements ErrorResolver {
  processError(error: Error): void {
    // Send to Sentry, log, etc.
    console.error('Consumer error:', error)
  }
}
```

## Testing

The library is designed to be testable:

### TestPubSubPublisher

`TestPubSubPublisher` is a testing utility for publishing arbitrary messages to Pub/Sub topics **without validation**. This is useful for:

- Testing how consumers handle invalid or malformed messages
- Simulating edge cases that would be rejected by schema validation
- Integration testing without needing to create full publisher implementations

**Features:**
- Publish any JSON-serializable payload without Zod schema validation
- No message deduplication checks
- No payload offloading
- Supports Pub/Sub ordering keys and attributes
- Can extract topic name from existing consumer or publisher instances

```typescript
import { TestPubSubPublisher } from '@message-queue-toolkit/gcp-pubsub'

// Create test publisher
const testPublisher = new TestPubSubPublisher(pubSubClient)

// Publish to a topic by name
await testPublisher.publish(
  { any: 'data', without: 'validation' },
  { topicName: 'my-topic' }
)

// Publish to the same topic as an existing consumer
await testPublisher.publish(
  { invalid: 'message', missing: 'required fields' },
  { consumer: myConsumer }
)

// Publish to the same topic as an existing publisher
await testPublisher.publish(
  { test: 'data' },
  { publisher: myPublisher }
)

// With ordering key and attributes
await testPublisher.publish(
  { test: 'message' },
  {
    topicName: 'my-topic',
    orderingKey: 'order-1',
    attributes: { key: 'value' }
  }
)
```

**Example: Testing Invalid Message Handling**

```typescript
import { TestPubSubPublisher } from '@message-queue-toolkit/gcp-pubsub'

describe('Consumer handles invalid messages', () => {
  let testPublisher: TestPubSubPublisher
  let consumer: MyConsumer

  beforeEach(async () => {
    testPublisher = new TestPubSubPublisher(pubSubClient)
    consumer = new MyConsumer(dependencies)
    await consumer.start()
  })

  it('rejects messages with invalid schema', async () => {
    // Publish a message that doesn't match the consumer's expected schema
    await testPublisher.publish(
      {
        id: 'test-1',
        messageType: 'unknown.type',  // Invalid message type
        data: 'invalid'
      },
      { consumer }
    )

    // Consumer should handle the invalid message gracefully
    // (e.g., nack it, send to DLQ after max attempts)
  })

  it('handles messages missing required fields', async () => {
    await testPublisher.publish(
      { incomplete: 'message' },  // Missing id, messageType, timestamp
      { consumer }
    )
  })
})
```

**Important:**
- The `consumer` or `publisher` must be initialized before passing to `publish()` (call `start()` or `init()` first)
- This utility is for testing only - do not use in production code

### Integration Tests with Emulator

```bash
# Start emulator (included in docker-compose)
docker compose up -d pubsub-emulator
```

#### Test Configuration

**Important:** Integration tests should run sequentially to avoid race conditions with shared Pub/Sub emulator resources. Configure Vitest to disable file parallelism:

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    fileParallelism: false,  // Run test files sequentially
    pool: 'threads',
    poolOptions: {
      threads: { singleThread: true },
    },
  },
})
```

#### Test Pattern: Per-Test Isolation

For reliable integration tests, create fresh consumer/publisher instances for each test with explicit resource cleanup. The correct order is:
1. Create instances first (so we know which resources to delete)
2. Delete existing resources
3. Start/init the instances (which recreates the resources)

```typescript
import { describe, it, expect, beforeAll, beforeEach, afterEach, afterAll } from 'vitest'
import { PubSub } from '@google-cloud/pubsub'
import { deletePubSubTopicAndSubscription } from '@message-queue-toolkit/gcp-pubsub'

describe('UserEventsConsumer', () => {
  let pubSubClient: PubSub
  let publisher: UserEventsPublisher
  let consumer: UserEventsConsumer

  beforeAll(async () => {
    pubSubClient = new PubSub({
      projectId: 'test-project',
      apiEndpoint: 'localhost:8085',  // Emulator
    })
  })

  beforeEach(async () => {
    // 1. Create instances first
    consumer = new UserEventsConsumer({ pubSubClient, logger, errorReporter })
    publisher = new UserEventsPublisher({ pubSubClient, logger, errorReporter })

    // 2. Delete resources after creating instances but before start/init
    await deletePubSubTopicAndSubscription(
      pubSubClient,
      UserEventsConsumer.TOPIC_NAME,
      UserEventsConsumer.SUBSCRIPTION_NAME,
    )

    // 3. Start/init (this creates fresh resources)
    await consumer.start()
    await publisher.init()
  })

  afterEach(async () => {
    await consumer.close()
    await publisher.close()
  })

  it('processes user.created message', async () => {
    await publisher.publish({
      id: '123',
      messageType: 'user.created',
      userId: 'user-456',
      email: 'test@example.com',
    })

    // Wait for message to be processed
    await consumer.handlerSpy.waitForMessageWithId('123', 'consumed')

    // Verify side effects
    expect(userService.createUser).toHaveBeenCalledWith('user-456', 'test@example.com')
  })

  it('retries failed messages', async () => {
    let attempts = 0
    userService.createUser.mockImplementation(() => {
      attempts++
      if (attempts < 3) throw new Error('Temporary failure')
      return Promise.resolve()
    })

    await publisher.publish({
      id: '124',
      messageType: 'user.created',
      userId: 'user-789',
      email: 'test2@example.com',
    })

    await consumer.handlerSpy.waitForMessageWithId('124', 'consumed')

    expect(attempts).toBe(3)
  })
})
```

**Key Points:**
- **Run tests sequentially** - Set `fileParallelism: false` in vitest.config.ts to prevent race conditions
- **Create instances first** - Create consumer/publisher before deleting resources so you know which topic/subscription names to delete
- **Delete then start** - Delete resources after creating instances, then call start/init to recreate them fresh
- **Close in afterEach** - Always close instances to release subscription listeners
- **Use handlerSpy** - Wait for message processing with `waitForMessageWithId` instead of arbitrary delays

### Unit Tests with Handler Spies

```typescript
it('publishes message', async () => {
  await publisher.publish({
    id: '123',
    messageType: 'user.created',
    userId: 'user-456',
    email: 'test@example.com',
  })

  const publishedMessage = await publisher.handlerSpy.waitForMessageWithId('123', 'published')

  expect(publishedMessage).toMatchObject({
    id: '123',
    userId: 'user-456',
  })
})
```

## API Reference

### AbstractPubSubPublisher

**Constructor Options:**
- `messageSchemas`: Array of Zod schemas for messages
- `messageTypeField`: Field name containing message type
- `creationConfig` / `locatorConfig`: Topic configuration
- `logMessages`: Enable message logging
- `payloadStoreConfig`: Payload offloading configuration
- `enablePublisherDeduplication`: Enable deduplication
- `messageDeduplicationConfig`: Deduplication store config

**Methods:**
- `init()`: Initialize publisher (create/locate topic)
- `publish(message, options?)`: Publish a message
- `close()`: Close publisher
- `handlerSpy`: Access spy for testing

**Publish Options:**
- `orderingKey`: String for message ordering
- `attributes`: Custom message attributes

### AbstractPubSubConsumer

**Constructor Options:**
- `handlers`: Message handler configuration
- `messageTypeField`: Field name containing message type
- `creationConfig` / `locatorConfig`: Topic + subscription configuration
- `logMessages`: Enable message logging
- `payloadStoreConfig`: Payload retrieval configuration
- `enableConsumerDeduplication`: Enable deduplication
- `messageDeduplicationConfig`: Deduplication store config
- `deadLetterQueue`: DLQ configuration
- `maxRetryDuration`: Max retry time in seconds
- `consumerOverrides`: Flow control settings

**Methods:**
- `init()`: Initialize consumer (create/locate resources)
- `start()`: Start consuming messages
- `close()`: Stop consumer and close connections
- `handlerSpy`: Access spy for testing

## Best Practices

1. **Use message ordering** for related events (same user, same entity)
2. **Enable exactly-once delivery** for critical workflows
3. **Set appropriate ACK deadlines** (60s is a good default)
4. **Implement idempotent handlers** (at-least-once delivery)
5. **Use deduplication** for critical operations
6. **Configure DLQ** for poison message handling
7. **Monitor subscription backlog** in GCP console
8. **Use payload offloading** for large messages
9. **Test with emulator** before deploying
10. **Set appropriate flow control** limits based on your processing capacity

## Troubleshooting

### Messages not being consumed

- Check subscription exists and is attached to the topic
- Verify ACK deadline is sufficient for processing
- Check flow control limits aren't too restrictive
- Ensure consumer is started (`await consumer.start()`)

### Messages going to DLQ

- Check `maxDeliveryAttempts` configuration
- Review handler error logs
- Verify message format matches schema
- Check retry duration hasn't been exceeded

### Memory issues

- Reduce `flowControl.maxMessages`
- Reduce `flowControl.maxBytes`
- Enable payload offloading for large messages

### Emulator issues

- Ensure emulator is running on port 8085
- Set `PUBSUB_EMULATOR_HOST=localhost:8085` environment variable
- Or configure `apiEndpoint: 'localhost:8085'` in PubSub client

## License

MIT

## Contributing

Contributions are welcome! Please see the main repository for guidelines.

## Links

- [Main Repository](https://github.com/kibertoad/message-queue-toolkit)
- [Core Package](https://www.npmjs.com/package/@message-queue-toolkit/core)
- [GCS Payload Store](https://www.npmjs.com/package/@message-queue-toolkit/gcs-payload-store)
- [Redis Deduplication Store](https://www.npmjs.com/package/@message-queue-toolkit/redis-message-deduplication-store)
- [Google Cloud Pub/Sub Documentation](https://cloud.google.com/pubsub/docs)
