# @message-queue-toolkit/sqs

AWS SQS (Simple Queue Service) implementation for the message-queue-toolkit. Provides a robust, type-safe abstraction
for publishing and consuming messages from both standard and FIFO SQS queues.

## Table of Contents

- [Installation](#installation)
- [Features](#features)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
  - [Standard Queue Publisher](#standard-queue-publisher)
  - [Standard Queue Consumer](#standard-queue-consumer)
  - [FIFO Queue Publisher](#fifo-queue-publisher)
  - [FIFO Queue Consumer](#fifo-queue-consumer)
- [Configuration](#configuration)
  - [Queue Creation](#queue-creation)
  - [Queue Locator](#queue-locator)
  - [Publisher Options](#publisher-options)
  - [Consumer Options](#consumer-options)
- [Advanced Features](#advanced-features)
  - [Custom Message Field Names](#custom-message-field-names)
  - [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
  - [Message Retry Logic](#message-retry-logic)
  - [Message Deduplication](#message-deduplication)
  - [Payload Offloading](#payload-offloading)
  - [Message Handlers](#message-handlers)
  - [Pre-handlers and Barriers](#pre-handlers-and-barriers)
  - [Handler Spies](#handler-spies)
- [Non-Standard Message Formats](#non-standard-message-formats)
  - [EventBridge Events](#eventbridge-events)
  - [Custom Message Structures](#custom-message-structures)
- [FIFO Queues](#fifo-queues)
  - [FIFO Queue Requirements](#fifo-queue-requirements)
  - [Message Ordering](#message-ordering)
  - [Message Groups](#message-groups)
  - [FIFO-Specific Configuration](#fifo-specific-configuration)
- [Policy Configuration](#policy-configuration)
- [Testing](#testing)
- [API Reference](#api-reference)

## Installation

```bash
npm install @message-queue-toolkit/sqs @message-queue-toolkit/core
```

**Peer Dependencies:**
- `@aws-sdk/client-sqs` - AWS SDK for SQS
- `zod` - Schema validation

## Features

- ✅ **Type-safe message handling** with Zod schema validation
- ✅ **Standard and FIFO queue support**
- ✅ **Automatic retry logic** with exponential backoff
- ✅ **Dead Letter Queue (DLQ)** support
- ✅ **Message deduplication** (publisher and consumer level)
- ✅ **Payload offloading** for large messages (S3 integration)
- ✅ **Concurrent consumers** for high throughput
- ✅ **Policy-based access control**
- ✅ **Handler spies** for testing
- ✅ **Pre-handlers and barriers** for complex message processing
- ✅ **Automatic queue creation** with validation

## Core Concepts

### Publishers

Publishers send messages to SQS queues. They handle:
- Message validation against Zod schemas
- Automatic serialization
- Optional deduplication (preventing duplicate sends)
- Optional payload offloading (for messages > 256KB)
- FIFO-specific concerns (MessageGroupId, MessageDeduplicationId)

### Consumers

Consumers receive and process messages from SQS queues. They handle:
- Message deserialization and validation
- Routing to appropriate handlers based on message type
- Automatic retry with exponential backoff
- Dead letter queue integration
- Optional deduplication (preventing duplicate processing)
- FIFO ordering guarantees

### Message Schemas

Messages are validated using Zod schemas. Each message must have:
- A unique message type field (discriminator for routing) - configurable via `messageTypeResolver` (required)
- A message ID field (for tracking and deduplication) - configurable via `messageIdField` (default: `'id'`)
- A timestamp field (added automatically if missing) - configurable via `messageTimestampField` (default: `'timestamp'`)

**Note:** All field names are configurable, allowing you to adapt the library to your existing message schemas without modification.

## Quick Start

### Standard Queue Publisher

```typescript
import { AbstractSqsPublisher } from '@message-queue-toolkit/sqs'
import { SQSClient } from '@aws-sdk/client-sqs'
import z from 'zod'

// Define your message schemas
const UserCreatedSchema = z.object({
  id: z.string(),
  messageType: z.literal('user.created'),
  userId: z.string(),
  email: z.string().email(),
  timestamp: z.string().optional(),
})

const UserUpdatedSchema = z.object({
  id: z.string(),
  messageType: z.literal('user.updated'),
  userId: z.string(),
  changes: z.record(z.unknown()),
  timestamp: z.string().optional(),
})

type UserCreated = z.infer<typeof UserCreatedSchema>
type UserUpdated = z.infer<typeof UserUpdatedSchema>
type SupportedMessages = UserCreated | UserUpdated

// Create your publisher class
class UserEventsPublisher extends AbstractSqsPublisher<SupportedMessages> {
  constructor(sqsClient: SQSClient) {
    super(
      {
        sqsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
      },
      {
        messageSchemas: [UserCreatedSchema, UserUpdatedSchema],
        messageTypeResolver: { messageTypePath: 'messageType' },
        creationConfig: {
          queue: {
            QueueName: 'user-events-queue',
          },
        },
        deletionConfig: {
          deleteIfExists: false,
        },
      }
    )
  }
}

// Use the publisher
const sqsClient = new SQSClient({ region: 'us-east-1' })
const publisher = new UserEventsPublisher(sqsClient)

await publisher.init()

await publisher.publish({
  id: '123',
  messageType: 'user.created',
  userId: 'user-456',
  email: 'user@example.com',
})

await publisher.close()
```

### Standard Queue Consumer

```typescript
import { AbstractSqsConsumer } from '@message-queue-toolkit/sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { Either } from '@lokalise/node-core'

type ExecutionContext = {
  userService: UserService
}

class UserEventsConsumer extends AbstractSqsConsumer<
  SupportedMessages,
  ExecutionContext
> {
  constructor(sqsClient: SQSClient, userService: UserService) {
    super(
      {
        sqsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
        consumerErrorResolver: {
          resolveError: () => ({ resolve: 'retryLater' as const }),
        },
        transactionObservabilityManager: {
          start: () => {},
          stop: () => {},
        },
      },
      {
        messageTypeResolver: { messageTypePath: 'messageType' },
        handlers: new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
          .addConfig(
            UserCreatedSchema,
            async (message, context): Promise<Either<'retryLater', 'success'>> => {
              await context.userService.createUser(message.userId, message.email)
              return { result: 'success' }
            }
          )
          .addConfig(
            UserUpdatedSchema,
            async (message, context): Promise<Either<'retryLater', 'success'>> => {
              await context.userService.updateUser(message.userId, message.changes)
              return { result: 'success' }
            }
          )
          .build(),
        creationConfig: {
          queue: {
            QueueName: 'user-events-queue',
          },
        },
        deletionConfig: {
          deleteIfExists: false,
        },
      },
      { userService } // Execution context
    )
  }
}

// Use the consumer
const consumer = new UserEventsConsumer(sqsClient, userService)
await consumer.start() // Initializes and starts consuming

// Later, to stop
await consumer.close()
```

### FIFO Queue Publisher

FIFO (First-In-First-Out) queues guarantee that messages are processed exactly once and in order within a message group.

```typescript
class UserEventsFifoPublisher extends AbstractSqsPublisher<SupportedMessages> {
  constructor(sqsClient: SQSClient) {
    super(
      {
        sqsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
      },
      {
        messageSchemas: [UserCreatedSchema, UserUpdatedSchema],
        messageTypeResolver: { messageTypePath: 'messageType' },
        fifoQueue: true, // Enable FIFO mode

        // Option 1: Use a field from the message as MessageGroupId
        messageGroupIdField: 'userId',

        // Option 2: Use a default MessageGroupId for all messages
        // defaultMessageGroupId: 'user-events',

        creationConfig: {
          queue: {
            QueueName: 'user-events-queue.fifo', // Must end with .fifo
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false', // or 'true' for automatic deduplication
            },
          },
        },
      }
    )
  }
}

// Publishing to FIFO queue
const fifoPublisher = new UserEventsFifoPublisher(sqsClient)
await fifoPublisher.init()

// Messages with the same userId will be processed in order
await fifoPublisher.publish({
  id: '123',
  messageType: 'user.created',
  userId: 'user-456', // Used as MessageGroupId
  email: 'user@example.com',
})

await fifoPublisher.publish({
  id: '124',
  messageType: 'user.updated',
  userId: 'user-456', // Same group - processed after the first message
  changes: { name: 'John Doe' },
})

// You can also explicitly provide MessageGroupId
await fifoPublisher.publish(
  {
    id: '125',
    messageType: 'user.created',
    userId: 'user-789',
    email: 'other@example.com',
  },
  {
    MessageGroupId: 'custom-group-id',
    MessageDeduplicationId: 'unique-dedup-id', // Optional
  }
)
```

### FIFO Queue Consumer

```typescript
class UserEventsFifoConsumer extends AbstractSqsConsumer<
  SupportedMessages,
  ExecutionContext
> {
  constructor(sqsClient: SQSClient, userService: UserService) {
    super(
      {
        sqsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
        consumerErrorResolver: {
          resolveError: () => ({ resolve: 'retryLater' as const }),
        },
        transactionObservabilityManager: {
          start: () => {},
          stop: () => {},
        },
      },
      {
        fifoQueue: true, // Enable FIFO mode
        messageTypeResolver: { messageTypePath: 'messageType' },
        handlers: new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
          .addConfig(UserCreatedSchema, handleUserCreated)
          .addConfig(UserUpdatedSchema, handleUserUpdated)
          .build(),
        creationConfig: {
          queue: {
            QueueName: 'user-events-queue.fifo',
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
              VisibilityTimeout: '30',
            },
          },
        },
        // Optional: Configure concurrent consumers for parallel processing of different groups
        concurrentConsumersAmount: 3, // Process 3 different message groups in parallel
      },
      { userService }
    )
  }
}
```

## Configuration

### Queue Creation

When using `creationConfig`, the queue will be created automatically if it doesn't exist:

```typescript
{
  creationConfig: {
    queue: {
      QueueName: 'my-queue',
      Attributes: {
        // Standard Queue attributes
        VisibilityTimeout: '30',           // Seconds a message is invisible after being received
        MessageRetentionPeriod: '345600',  // 4 days (in seconds)
        ReceiveMessageWaitTimeSeconds: '20', // Long polling duration

        // FIFO Queue attributes (only for .fifo queues)
        FifoQueue: 'true',                 // Must be 'true' for FIFO queues
        ContentBasedDeduplication: 'false', // Automatic deduplication based on message body
        DeduplicationScope: 'queue',       // 'queue' or 'messageGroup'
        FifoThroughputLimit: 'perQueue',   // 'perQueue' or 'perMessageGroupId'

        // Encryption
        KmsMasterKeyId: 'alias/aws/sqs',   // KMS key for encryption

        // Other attributes
        DelaySeconds: '0',                 // Default delay for all messages
        MaximumMessageSize: '262144',      // 256 KB (default maximum)
      },
      tags: {
        Environment: 'production',
        Team: 'backend',
      },
    },
    updateAttributesIfExists: true,  // Update attributes if queue exists
    forceTagUpdate: false,           // Force tag update even if unchanged

    // Policy configuration (see Policy Configuration section)
    policyConfig: {
      resource: 'arn:aws:sqs:us-east-1:123456789012:my-queue',
      statements: [
        {
          Effect: 'Allow',
          Principal: '*',
          Action: ['sqs:SendMessage'],
        },
      ],
    },
  },
}
```

### Queue Locator

When using `locatorConfig`, you connect to an existing queue without creating it:

```typescript
{
  locatorConfig: {
    // Option 1: By queue URL
    queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',

    // Option 2: By queue name (URL will be resolved)
    // queueName: 'my-queue',
  },
}
```

### Publisher Options

```typescript
{
  // Required - Message Schema Configuration
  messageSchemas: [Schema1, Schema2],  // Array of Zod schemas
  messageTypeResolver: { messageTypePath: 'messageType' },     // Field containing message type discriminator

  // Queue Configuration (one of these required)
  creationConfig: { /* ... */ },       // Create queue if doesn't exist
  locatorConfig: { /* ... */ },        // Use existing queue

  // Optional - FIFO Configuration
  fifoQueue: false,                    // Set to true for FIFO queues
  messageGroupIdField: 'userId',       // Field to use as MessageGroupId
  defaultMessageGroupId: 'default',    // Default MessageGroupId if field not present

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
    deduplicationStore: redisStore,    // Redis-based deduplication store
  },

  // Optional - Payload Offloading
  payloadStoreConfig: {
    payloadStore: s3Store,             // S3-based payload store
    maxPayloadSize: 256 * 1024,        // 256 KB
  },

  // Optional - Deletion
  deletionConfig: {
    deleteIfExists: false,             // Delete queue on init
    waitForConfirmation: true,         // Wait for deletion to complete
    forceDeleteInProduction: false,    // Allow deletion in production
  },
}
```

### Consumer Options

```typescript
{
  // Required - Message Handling Configuration
  handlers: MessageHandlerConfigBuilder.build(), // Message handlers configuration
  messageTypeResolver: { messageTypePath: 'messageType' },               // Field containing message type discriminator

  // Queue Configuration (one of these required)
  creationConfig: { /* ... */ },
  locatorConfig: { /* ... */ },

  // Optional - FIFO Configuration
  fifoQueue: false,                    // Set to true for FIFO queues

  // Optional - Message Field Configuration
  messageIdField: 'id',                       // Field containing message ID (default: 'id')
  messageTimestampField: 'timestamp',         // Field containing timestamp (default: 'timestamp')
  messageDeduplicationIdField: 'deduplicationId',     // Field for deduplication ID (default: 'deduplicationId')
  messageDeduplicationOptionsField: 'deduplicationOptions', // Field for deduplication options (default: 'deduplicationOptions')

  // Optional - Concurrency
  concurrentConsumersAmount: 1,        // Number of concurrent consumer instances

  // Optional - Retry Configuration
  maxRetryDuration: 345600,            // 4 days in seconds (default)

  // Optional - Dead Letter Queue
  deadLetterQueue: {
    creationConfig: {
      queue: {
        QueueName: 'my-queue-dlq',
        // For FIFO queues, DLQ must also be FIFO
        Attributes: {
          FifoQueue: 'true',           // Match source queue type
        },
      },
    },
    redrivePolicy: {
      maxReceiveCount: 3,              // Move to DLQ after 3 receive attempts
    },
  },

  // Optional - Consumer Behavior
  consumerOverrides: {
    batchSize: 10,                     // Messages per receive (1-10)
    pollingWaitTimeMs: 0,              // Time between polls
    terminateVisibilityTimeout: true,  // Reset visibility on error
    heartbeatInterval: 300,            // Heartbeat interval in seconds
  },

  // Optional - Deduplication
  enableConsumerDeduplication: false,
  messageDeduplicationConfig: {
    deduplicationStore: redisStore,
  },

  // Optional - Payload Offloading
  payloadStoreConfig: {
    payloadStore: s3Store,
  },

  // Optional - Other
  logMessages: false,
  handlerSpy: true,
  deletionConfig: { /* ... */ },
}
```

## Advanced Features

### Custom Message Field Names

All message field names are configurable, allowing you to adapt the library to your existing message schemas:

```typescript
// Your existing message schema with custom field names
const CustomMessageSchema = z.object({
  messageId: z.string(),           // Custom ID field
  eventType: z.literal('order.created'), // Custom type field
  createdAt: z.string(),           // Custom timestamp field
  txId: z.string(),                // Custom deduplication ID
  txOptions: z.object({            // Custom deduplication options
    deduplicationWindowSeconds: z.number().optional(),
  }).optional(),
  orderId: z.string(),
  amount: z.number(),
})

// Configure the publisher to use your custom field names
class OrderPublisher extends AbstractSqsPublisher<CustomMessage> {
  constructor(sqsClient: SQSClient) {
    super(
      { sqsClient, logger: console, errorReporter: { report: console.error } },
      {
        messageSchemas: [CustomMessageSchema],

        // Map library's internal fields to your custom fields
        messageIdField: 'messageId',                    // Default: 'id'
        messageTypeResolver: { messageTypePath: 'eventType' },                  // Required
        messageTimestampField: 'createdAt',             // Default: 'timestamp'
        messageDeduplicationIdField: 'txId',            // Default: 'deduplicationId'
        messageDeduplicationOptionsField: 'txOptions',  // Default: 'deduplicationOptions'

        creationConfig: {
          queue: { QueueName: 'orders-queue' },
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

### Dead Letter Queue (DLQ)

Dead Letter Queues capture messages that cannot be processed after multiple attempts:

```typescript
{
  deadLetterQueue: {
    creationConfig: {
      queue: {
        QueueName: 'my-queue-dlq',
        // For FIFO source queues, DLQ must also be FIFO
        Attributes: {
          FifoQueue: 'true',  // Match source queue type
          MessageRetentionPeriod: '1209600', // 14 days
        },
      },
    },
    redrivePolicy: {
      maxReceiveCount: 3,  // Send to DLQ after 3 failed attempts
    },
  },
}
```

**How it works:**
1. Message fails processing (handler returns error or throws)
2. Message becomes visible again (visibility timeout expires)
3. Consumer receives message again (receive count increments)
4. After `maxReceiveCount` attempts, SQS automatically moves message to DLQ
5. DLQ messages can be inspected, reprocessed, or deleted

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
   - If within `maxRetryDuration`, re-queues message
   - If exceeded, sends to DLQ (if configured) or marks as failed

3. **Exponential Backoff (Standard Queues):**
   ```
   Attempt 1: 2^0 = 1 second delay
   Attempt 2: 2^1 = 2 seconds delay
   Attempt 3: 2^2 = 4 seconds delay
   Attempt 4: 2^3 = 8 seconds delay
   ...
   Max: 900 seconds (15 minutes) per AWS limits
   ```

4. **FIFO Queues:**
   - No delay support (AWS limitation)
   - Messages retry immediately
   - Order preserved within message group

**Handler Return Types:**

```typescript
type HandlerResult = Either<'retryLater', 'success'>

// Success - message is deleted from queue
return { result: 'success' }

// Retry - message is re-queued with delay
return { error: 'retryLater' }

// Error thrown - automatically retries
throw new Error('Database connection failed')
```

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
    deduplicationStore,
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
})

// Second publish with same deduplicationId within 60s is skipped
await publisher.publish({
  id: '124',
  messageType: 'user.created',
  deduplicationId: 'user-456-creation',  // Duplicate - won't be sent
})
```

#### Consumer-Level Deduplication

Prevents processing the same message multiple times:

```typescript
{
  enableConsumerDeduplication: true,
  messageDeduplicationIdField: 'deduplicationId',
  messageDeduplicationConfig: {
    deduplicationStore,
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

### Payload Offloading

For messages larger than 256 KB, store the payload externally (e.g., S3):

```typescript
import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'

const payloadStore = new S3PayloadStore({
  s3Client,
  bucketName: 'my-message-payloads',
})

// Publisher configuration
{
  payloadStoreConfig: {
    payloadStore,
    maxPayloadSize: 256 * 1024,  // 256 KB threshold
  },
}

// Large message is automatically offloaded
await publisher.publish({
  id: '123',
  messageType: 'document.processed',
  largeData: hugeArrayOfData,  // If total size > 256 KB, stored in S3
})
```

**How it works:**
1. Publisher checks message size before sending
2. If size exceeds `maxPayloadSize`, stores payload in S3
3. Replaces payload with pointer: `{ _offloadedPayload: { bucketName, key, size } }`
4. Sends pointer message to SQS
5. Consumer detects pointer, fetches payload from S3
6. Processes message with full payload

**Note:** Payload cleanup is the responsibility of the store (e.g., S3 lifecycle policies).

### Message Handlers

Handlers process messages based on their type. Messages are routed to the appropriate handler using the discriminator field (configurable via `messageTypeResolver`):

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

##### 3. Database Transaction Management

Set up scoped database transactions:

```typescript
type PrehandlerOutput = {
  transaction: DatabaseTransaction
}

const preHandlers = [
  async (message, context, output, next) => {
    const transaction = await context.database.beginTransaction()
    output.transaction = transaction

    try {
      next({ result: 'success' })
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  },
]

const handler = async (message, context, preHandlingOutputs) => {
  const { transaction } = preHandlingOutputs.preHandlerOutput

  try {
    await context.userRepository.create(message.userData, { transaction })
    await context.auditRepository.log(message.action, { transaction })

    await transaction.commit()
    return { result: 'success' }
  } catch (error) {
    await transaction.rollback()
    throw error
  }
}
```

##### 4. Caching and Deduplication

Implement custom caching logic:

```typescript
type PrehandlerOutput = {
  cachedData?: ProductData
  cacheHit: boolean
}

const preHandlers = [
  async (message, context, output, next) => {
    const cacheKey = `product:${message.productId}`
    const cached = await context.cache.get(cacheKey)

    if (cached) {
      output.cachedData = cached
      output.cacheHit = true
      context.logger.info('Cache hit', { productId: message.productId })
    } else {
      output.cacheHit = false
    }

    next({ result: 'success' })
  },
]

const handler = async (message, context, preHandlingOutputs) => {
  const { cachedData, cacheHit } = preHandlingOutputs.preHandlerOutput

  if (cacheHit && cachedData) {
    // Use cached data
    return processWithCache(cachedData)
  }

  // Fetch fresh data
  const data = await context.productService.fetch(message.productId)
  await context.cache.set(`product:${message.productId}`, data, { ttl: 3600 })

  return processWithCache(data)
}
```

##### 5. Metrics and Monitoring

Track message processing metrics:

```typescript
type PrehandlerOutput = {
  startTime: number
  metricsLabels: Record<string, string>
}

const preHandlers = [
  (message, context, output, next) => {
    output.startTime = Date.now()
    output.metricsLabels = {
      messageType: message.messageType,
      userId: message.userId,
      source: message.source || 'unknown',
    }

    context.metrics.increment('messages.received', output.metricsLabels)
    next({ result: 'success' })
  },
]

const handler = async (message, context, preHandlingOutputs) => {
  const { startTime, metricsLabels } = preHandlingOutputs.preHandlerOutput

  try {
    await processMessage(message)

    const duration = Date.now() - startTime
    context.metrics.histogram('message.processing.duration', duration, metricsLabels)
    context.metrics.increment('messages.processed', { ...metricsLabels, status: 'success' })

    return { result: 'success' }
  } catch (error) {
    context.metrics.increment('messages.processed', { ...metricsLabels, status: 'error' })
    throw error
  }
}
```

**Configuration:**

```typescript
new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext, PrehandlerOutput>()
  .addConfig(
    MessageSchema,
    handler,
    {
      preHandlers: [
        loggerPreHandler,
        userDataPreHandler,
        permissionsPreHandler,
      ],
    }
  )
  .build()
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

##### 2. External Resource Availability

Wait for external systems to be ready:

```typescript
// Scenario: Process message only when third-party API is available
const preHandlerBarrier = async (message, context, preHandlerOutput) => {
  try {
    // Check if external service is healthy
    const isHealthy = await context.externalApiClient.healthCheck()

    if (!isHealthy) {
      context.logger.info('External API unhealthy, retrying later')
      return {
        isPassing: false,
        output: { reason: 'external_api_unavailable' },
      }
    }

    // Check rate limit
    const rateLimitOk = await context.rateLimiter.checkLimit(message.userId)
    if (!rateLimitOk) {
      context.logger.info('Rate limit exceeded, retrying later')
      return {
        isPassing: false,
        output: { reason: 'rate_limit_exceeded' },
      }
    }

    return {
      isPassing: true,
      output: { apiAvailable: true },
    }
  } catch (error) {
    context.logger.error({ error }, 'Barrier check failed')
    return {
      isPassing: false,
      output: { reason: 'barrier_error', error },
    }
  }
}
```

##### 3. Business Workflow Prerequisites

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

##### 4. Multi-Message Dependencies

Wait for multiple related messages to be processed:

```typescript
// Scenario: Process shipment only after all items are packed
const preHandlerBarrier = async (
  message: ShipmentMessage,
  context,
  preHandlerOutput
) => {
  const orderId = message.orderId

  // Check if all items are packed
  const orderItems = await context.orderRepository.getItems(orderId)
  const packedItems = await context.packingRepository.getPackedItems(orderId)

  const allItemsPacked = orderItems.every(item =>
    packedItems.some(packed => packed.itemId === item.id)
  )

  if (!allItemsPacked) {
    const pendingItems = orderItems.filter(item =>
      !packedItems.some(packed => packed.itemId === item.id)
    )

    context.logger.info('Not all items packed, retrying later', {
      orderId,
      totalItems: orderItems.length,
      packedItems: packedItems.length,
      pendingItems: pendingItems.map(i => i.id),
    })

    return {
      isPassing: false,
      output: {
        reason: 'items_not_packed',
        pendingItemsCount: pendingItems.length,
      },
    }
  }

  return {
    isPassing: true,
    output: {
      allItemsPacked: true,
      totalWeight: packedItems.reduce((sum, item) => sum + item.weight, 0),
    },
  }
}
```

##### 5. Time-Based Gating

Delay processing until a specific time:

```typescript
// Scenario: Process scheduled messages only after their scheduled time
const preHandlerBarrier = async (message: ScheduledMessage, context, preHandlerOutput) => {
  const scheduledTime = new Date(message.scheduledFor)
  const now = new Date()

  if (now < scheduledTime) {
    const delayMs = scheduledTime.getTime() - now.getTime()
    context.logger.info('Message scheduled for future, retrying later', {
      messageId: message.id,
      scheduledFor: scheduledTime,
      delayMs,
    })

    return {
      isPassing: false,
      output: {
        reason: 'scheduled_for_future',
        scheduledFor: scheduledTime,
      },
    }
  }

  return {
    isPassing: true,
    output: {
      scheduledFor: scheduledTime,
      actualProcessingTime: now,
    },
  }
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

- **Barriers return `isPassing: false`** → Message is automatically retried with exponential backoff
- **Barriers throw errors** → Message follows normal error handling (retry or DLQ)
- **Barrier output** → Available in handler via `preHandlingOutputs.barrierOutput`
- **Retry limits apply** → Messages exceeding `maxRetryDuration` will be sent to DLQ even if barrier keeps returning false
- **FIFO queues** → Barriers are especially important for FIFO queues to handle out-of-order delivery within message groups

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
- Poll the queue directly
- Add artificial delays (`setTimeout`)
- Mock the entire message infrastructure
- Modify production code for testing

#### Configuration

```typescript
// Enable handler spy for publisher and/or consumer
const publisher = new UserEventsPublisher(sqsClient, {
  handlerSpy: true,  // Track published messages
})

const consumer = new UserEventsConsumer(sqsClient, {
  handlerSpy: true,  // Track consumed messages
})
```

#### Example 1: Testing Direct Message Publishing

```typescript
import { describe, it, expect, beforeEach } from 'vitest'

describe('UserEventsPublisher', () => {
  let publisher: UserEventsPublisher

  beforeEach(async () => {
    publisher = new UserEventsPublisher(sqsClient, { handlerSpy: true })
    await publisher.init()
  })

  it('publishes user.created event', async () => {
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
  })
})
```

#### Example 2: Testing Indirect Message Publishing via API

This example demonstrates testing business logic that publishes messages internally:

```typescript
import { describe, it, expect, beforeEach } from 'vitest'
import request from 'supertest'

// Your API endpoint that creates a user and publishes an event
class UserController {
  constructor(
    private userRepository: UserRepository,
    private eventPublisher: UserEventsPublisher
  ) {}

  async createUser(req, res) {
    const user = await this.userRepository.create(req.body)

    // Publish event internally - not directly exposed to the test
    await this.eventPublisher.publish({
      id: crypto.randomUUID(),
      messageType: 'user.created',
      userId: user.id,
      email: user.email,
    })

    res.status(201).json(user)
  }
}

describe('User Creation Flow', () => {
  let app: Express
  let publisher: UserEventsPublisher
  let consumer: UserEventsConsumer

  beforeEach(async () => {
    // Set up publisher with handler spy
    publisher = new UserEventsPublisher(sqsClient, { handlerSpy: true })
    await publisher.init()

    // Set up consumer with handler spy
    consumer = new UserEventsConsumer(sqsClient, userService, { handlerSpy: true })
    await consumer.start()

    // Create API with real publisher
    app = createApp({ eventPublisher: publisher })
  })

  it('publishes event when user is created via API', async () => {
    // Act: Make API call (no direct interaction with publisher)
    const response = await request(app)
      .post('/api/users')
      .send({
        email: 'newuser@example.com',
        name: 'John Doe',
      })

    expect(response.status).toBe(201)
    const createdUserId = response.body.id

    // Assert: Wait for message to be published (by internal business logic)
    const publishedMessage = await publisher.handlerSpy.waitForMessage(
      (msg) => msg.userId === createdUserId && msg.messageType === 'user.created',
      'published',
      5000
    )

    expect(publishedMessage).toMatchObject({
      messageType: 'user.created',
      userId: createdUserId,
      email: 'newuser@example.com',
    })

    // Assert: Wait for message to be consumed and processed
    const consumedMessage = await consumer.handlerSpy.waitForMessage(
      (msg) => msg.userId === createdUserId,
      'consumed',
      10000 // Allow more time for async processing
    )

    expect(consumedMessage.userId).toBe(createdUserId)

    // Verify side effects in your user service
    expect(userService.onUserCreated).toHaveBeenCalledWith(createdUserId)
  })

  it('handles complex multi-step workflows', async () => {
    // Create user via API
    const createResponse = await request(app)
      .post('/api/users')
      .send({ email: 'user@example.com', name: 'Jane Doe' })

    const userId = createResponse.body.id

    // Wait for user.created event
    await consumer.handlerSpy.waitForMessage(
      (msg) => msg.userId === userId && msg.messageType === 'user.created',
      'consumed'
    )

    // Update user via API (triggers another event)
    await request(app)
      .patch(`/api/users/${userId}`)
      .send({ name: 'Jane Smith' })

    // Wait for user.updated event
    const updatedMessage = await consumer.handlerSpy.waitForMessage(
      (msg) => msg.userId === userId && msg.messageType === 'user.updated',
      'consumed',
      5000
    )

    expect(updatedMessage.changes).toMatchObject({ name: 'Jane Smith' })
  })
})
```

#### Example 3: Non-Waiting Checks

For scenarios where you don't want to wait:

```typescript
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
```

#### Example 4: Inspecting All Messages

Useful for debugging or verifying batch operations:

```typescript
it('processes batch of user events', async () => {
  // Publish multiple messages
  for (let i = 0; i < 10; i++) {
    await publisher.publish({
      id: `msg-${i}`,
      messageType: 'user.created',
      userId: `user-${i}`,
      email: `user${i}@example.com`,
    })
  }

  // Wait for the last message
  await consumer.handlerSpy.waitForMessageWithId('msg-9', 'consumed')

  // Inspect all processed messages
  const allMessages = consumer.handlerSpy.getAllMessages()

  expect(allMessages.length).toBeGreaterThanOrEqual(10)

  const successfulMessages = allMessages.filter(
    ({ processingResult }) => processingResult.status === 'consumed'
  )

  expect(successfulMessages.length).toBe(10)
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

## Non-Standard Message Formats

The toolkit supports consuming messages that don't follow the standard message structure (with `type`, `id`, and `timestamp` fields at the root level). This is particularly useful for consuming events from external systems like AWS EventBridge, CloudWatch Events, or custom event sources.

### EventBridge Events

AWS EventBridge events have a different structure than standard toolkit messages. They use:
- `detail-type` instead of `type` (routing field at root level)
- `time` instead of `timestamp`
- `detail` for the actual payload (nested)
- `id` for the message ID (same as default)

Handlers receive the full EventBridge envelope and can access the nested `detail` field directly.

#### Using the EventBridge Schema Builder

The toolkit provides helper functions to create properly typed EventBridge schemas:

```typescript
import {
  AbstractSqsConsumer, createEventBridgeSchema,
} from '@message-queue-toolkit/sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import z from 'zod'

// Step 1: Define your detail (payload) schema
const USER_PRESENCE_DETAIL = z.object({
  userId: z.string(),
  status: z.enum(['AVAILABLE', 'AWAY', 'BUSY', 'OFFLINE']),
  timestamp: z.string(),
})

// Step 2: Create envelope schema with literal detail-type for routing
const USER_PRESENCE_ENVELOPE = createEventBridgeSchema(
  USER_PRESENCE_DETAIL,
  'user.presence.changed'  // Literal value for routing
)

// Step 3: Infer the envelope type (what handlers receive)
type UserPresenceEvent = z.infer<typeof USER_PRESENCE_ENVELOPE>
// {
//   version: string
//   id: string
//   'detail-type': 'user.presence.changed'  // Literal type for routing
//   source: string
//   account: string
//   time: string
//   region: string
//   resources: string[]
//   detail: {
//     userId: string
//     status: 'AVAILABLE' | 'AWAY' | 'BUSY' | 'OFFLINE'
//     timestamp: string
//   }
// }
```

#### Creating an EventBridge Consumer

```typescript
class EventBridgeConsumer extends AbstractSqsConsumer<UserPresenceEvent, ExecutionContext> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: { QueueName: 'eventbridge-events' },
      },

      // Configure field mappings for EventBridge
      messageTypeResolver: { messageTypePath: 'detail-type' },     // EventBridge uses 'detail-type'
      messageIdField: 'id',                // Standard, same as default
      messageTimestampField: 'time',       // EventBridge uses 'time'

      // Handlers receive the full EventBridge envelope
      handlers: new MessageHandlerConfigBuilder<UserPresenceEvent, ExecutionContext>()
        .addConfig(
          USER_PRESENCE_ENVELOPE,          // Schema validates full envelope
          async (message, context) => {
            // message is the full EventBridge envelope
            // Access the detail field directly
            console.log('User status changed:', message.detail.userId, message.detail.status)
            return { result: 'success' as const }
          },
        )
        .build(),
    })
  }
}
```

**Key Points:**
- The envelope schema validates the full EventBridge message structure
- The literal `detail-type` value in the schema is used for routing
- Handlers receive the complete validated envelope
- Access nested payload via `message.detail`

#### Multiple EventBridge Event Types

Handle multiple EventBridge event types with distinct detail-type values:

```typescript
import { createEventBridgeSchema } from '@message-queue-toolkit/sqs'

// Step 1: Define detail (payload) schemas
const USER_PRESENCE_DETAIL = z.object({
  userId: z.string(),
  status: z.string(),
  timestamp: z.string(),
})

const USER_ROUTING_STATUS_DETAIL = z.object({
  userId: z.string(),
  routingStatus: z.object({
    id: z.string(),
    status: z.string(),
  }),
  timestamp: z.string(),
})

// Step 2: Create envelope schemas with literal detail-type values
const USER_PRESENCE_ENVELOPE = createEventBridgeSchema(
  USER_PRESENCE_DETAIL,
  'v2.users.{id}.presence'  // Literal routing value
)

const USER_ROUTING_STATUS_ENVELOPE = createEventBridgeSchema(
  USER_ROUTING_STATUS_DETAIL,
  'v2.users.{id}.routing.status'  // Literal routing value
)

// Step 3: Union type for all event envelopes
type SupportedEventBridgeEvents =
  | z.infer<typeof USER_PRESENCE_ENVELOPE>
  | z.infer<typeof USER_ROUTING_STATUS_ENVELOPE>

// Step 4: Consumer with multiple handlers
class MultiEventConsumer extends AbstractSqsConsumer<SupportedEventBridgeEvents, ExecutionContext> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: { queue: { QueueName: 'multi-event-queue' } },

      messageTypeResolver: { messageTypePath: 'detail-type' },
      messageTimestampField: 'time',

      handlers: new MessageHandlerConfigBuilder<SupportedEventBridgeEvents, ExecutionContext>()
        .addConfig(
          USER_PRESENCE_ENVELOPE,
          async (message, context) => {
            // message.detail is typed as UserPresenceDetail
            console.log(`User ${message.detail.userId} status: ${message.detail.status}`)
            return { result: 'success' as const }
          }
        )
        .addConfig(
          USER_ROUTING_STATUS_ENVELOPE,
          async (message, context) => {
            // message.detail is typed as UserRoutingStatusDetail
            console.log(`User ${message.detail.userId} routing: ${message.detail.routingStatus.status}`)
            return { result: 'success' as const }
          }
        )
        .build(),
    })
  }
}
```

#### Complete EventBridge Example

```typescript
import { SQSClient } from '@aws-sdk/client-sqs'
import {
  AbstractSqsConsumer,
  createEventBridgeSchema,
  SqsConsumerErrorResolver,
  type SQSConsumerDependencies
} from '@message-queue-toolkit/sqs'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import z from 'zod'

// 1. Define the detail payload schema
const USER_CREATED_DETAIL = z.object({
  userId: z.string(),
  email: z.string().email(),
  name: z.string(),
  createdAt: z.string(),
})

// 2. Create envelope schema with literal detail-type for routing
const USER_CREATED_ENVELOPE = createEventBridgeSchema(
  USER_CREATED_DETAIL,
  'user.created'  // Literal value for routing
)

// 3. Type for the event envelope (what handlers receive)
type UserCreatedEvent = z.infer<typeof USER_CREATED_ENVELOPE>

// 4. Create consumer
class UserEventConsumer extends AbstractSqsConsumer<UserCreatedEvent> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: { QueueName: 'user-events' },
      },

      // EventBridge field mappings
      messageTypeResolver: { messageTypePath: 'detail-type' },
      messageTimestampField: 'time',

      handlers: new MessageHandlerConfigBuilder<UserCreatedEvent>()
        .addConfig(
          USER_CREATED_ENVELOPE,
          async (message) => {
            // message is the full EventBridge envelope
            // Access the detail field directly
            console.log(`New user created: ${message.detail.name} (${message.detail.email})`)
            await saveUserToDatabase(message.detail)
            return { result: 'success' as const }
          },
        )
        .build(),
    })
  }
}

// 5. Use the consumer
const sqsClient = new SQSClient({ region: 'us-east-1' })
const consumer = new UserEventConsumer({
  sqsClient,
  consumerErrorResolver: new SqsConsumerErrorResolver(),
  errorReporter: { report: (error) => console.error(error) },
  logger: console,
  transactionObservabilityManager: undefined,
})
await consumer.start()

// Example EventBridge event structure (for testing):
const exampleEvent = {
  version: '0',
  id: '12345678-1234-1234-1234-123456789012',
  'detail-type': 'user.created',
  source: 'my.application',
  account: '123456789012',
  time: '2025-01-15T12:00:00Z',
  region: 'us-east-1',
  resources: [],
  detail: {
    userId: 'user-123',
    email: 'user@example.com',
    name: 'John Doe',
    createdAt: '2025-01-15T12:00:00Z',
  },
} satisfies UserCreatedEvent
```

#### Pre-built EventBridge Type Resolver

Instead of manually configuring `{ messageTypePath: 'detail-type' }`, you can use the pre-built `EVENT_BRIDGE_TYPE_RESOLVER`:

```typescript
import {
  EVENT_BRIDGE_TYPE_RESOLVER,
  EVENT_BRIDGE_TIMESTAMP_FIELD,
  createEventBridgeResolverWithMapping,
} from '@message-queue-toolkit/sqs'

// Simple usage - extracts from 'detail-type' field
class EventBridgeConsumer extends AbstractSqsConsumer<EventBridgeEvent> {
  constructor(deps: SQSConsumerDependencies) {
    super(deps, {
      messageTypeResolver: EVENT_BRIDGE_TYPE_RESOLVER,
      messageTimestampField: EVENT_BRIDGE_TIMESTAMP_FIELD,  // 'time'
      handlers: new MessageHandlerConfigBuilder()
        .addConfig(schema, handler)
        .build(),
    })
  }
}

// With type mapping - normalize EventBridge detail-types to internal types
const resolver = createEventBridgeResolverWithMapping({
  'Order Created': 'order.created',      // "Order Created" → "order.created"
  'Order Updated': 'order.updated',
  'Order Cancelled': 'order.cancelled',
}, { fallbackToOriginal: true })  // Optional: pass through unmapped types

class MappedEventBridgeConsumer extends AbstractSqsConsumer<EventBridgeEvent> {
  constructor(deps: SQSConsumerDependencies) {
    super(deps, {
      messageTypeResolver: resolver,
      messageTimestampField: EVENT_BRIDGE_TIMESTAMP_FIELD,
      handlers: new MessageHandlerConfigBuilder()
        .addConfig(schema, handler, { messageType: 'order.created' })
        .build(),
    })
  }
}
```

### Custom Message Structures

For other non-standard message formats, you can configure the field mappings:

```typescript
// Define your custom message schema
const CUSTOM_MESSAGE_SCHEMA = z.object({
  eventType: z.literal('order.created'),  // Routing field
  correlationId: z.string(),
  occurredAt: z.string(),
  data: z.object({
    orderId: z.string(),
    amount: z.number(),
  }),
})

type CustomMessage = z.infer<typeof CUSTOM_MESSAGE_SCHEMA>

class CustomConsumer extends AbstractSqsConsumer<CustomMessage> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: { queue: { QueueName: 'custom-queue' } },

      // Map your custom field names
      messageTypeResolver: { messageTypePath: 'eventType' },        // Instead of 'type'
      messageIdField: 'correlationId',      // Instead of 'id'
      messageTimestampField: 'occurredAt',  // Instead of 'timestamp'

      handlers: new MessageHandlerConfigBuilder<CustomMessage>()
        .addConfig(CUSTOM_MESSAGE_SCHEMA, async (message) => {
          // Handler receives the full validated message
          // Access nested data via message.data
          console.log(`Order ${message.data.orderId}: $${message.data.amount}`)
          return { result: 'success' as const }
        })
        .build(),
    })
  }
}
```

**Configuration Options:**

- `messageTypeResolver` (required) - Configuration containing the message type for routing
- `messageIdField` (optional, default: `'id'`) - Field name containing the message ID
- `messageTimestampField` (optional, default: `'timestamp'`) - Field name containing the timestamp

## FIFO Queues

FIFO (First-In-First-Out) queues provide message ordering and exactly-once processing.

### FIFO Queue Requirements

1. **Queue name must end with `.fifo`**
   ```typescript
   QueueName: 'my-queue.fifo'  // ✅ Valid
   QueueName: 'my-queue'       // ❌ Invalid for FIFO
   ```

2. **FifoQueue attribute must be 'true'**
   ```typescript
   Attributes: {
     FifoQueue: 'true',
   }
   ```

3. **MessageGroupId required for all messages**
   ```typescript
   // Option 1: From message field
   messageGroupIdField: 'userId'

   // Option 2: Default value
   defaultMessageGroupId: 'default-group'

   // Option 3: Explicit in publish call
   await publisher.publish(message, {
     MessageGroupId: 'custom-group',
   })
   ```

4. **DLQ must also be FIFO**
   ```typescript
   deadLetterQueue: {
     creationConfig: {
       queue: {
         QueueName: 'my-queue-dlq.fifo',  // Must be FIFO
         Attributes: {
           FifoQueue: 'true',
         },
       },
     },
   }
   ```

### Message Ordering

**Ordering guarantees:**
- ✅ Messages within the same MessageGroupId are delivered in order
- ✅ Messages are processed exactly once (no duplicates)
- ❌ No ordering guarantee across different MessageGroupIds

**Example:**

```typescript
// All messages for user-123 are processed in order
await publisher.publish({ id: '1', userId: 'user-123', action: 'create' })
await publisher.publish({ id: '2', userId: 'user-123', action: 'update' })
await publisher.publish({ id: '3', userId: 'user-123', action: 'delete' })

// Messages for user-456 can be processed in parallel
await publisher.publish({ id: '4', userId: 'user-456', action: 'create' })
```

**Processing order:**
```
Message 1 (user-123) → Message 2 (user-123) → Message 3 (user-123)
Message 4 (user-456) can process in parallel with above
```

### FIFO Retry Behavior

**IMPORTANT**: FIFO queues have special retry behavior to preserve message ordering.

When a handler returns `{ error: 'retryLater' }` for a FIFO queue:

1. **The consumer throws an error** instead of republishing the message
2. **AWS SQS handles the retry** using its native retry mechanism
3. **Message order is preserved** within the MessageGroupId
4. The message returns to the **same position** in the queue after visibility timeout

**Why this matters:**

```typescript
// ❌ Standard queue: Republishes message to back of queue (breaks order)
// Message 1 → Message 2 → Message 3
// If Message 1 retries: Message 2 → Message 3 → Message 1 (wrong!)

// ✅ FIFO queue: Throws error, AWS retries in place (preserves order)
// Message 1 → Message 2 → Message 3
// If Message 1 retries: Message 1 (retry) → Message 2 → Message 3 (correct!)
```

**Configuration:**

```typescript
class MyFifoConsumer extends AbstractSqsConsumer<MessageType, Context> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: {
          QueueName: 'my-queue.fifo',
          Attributes: {
            FifoQueue: 'true',
            VisibilityTimeout: '30',  // Retry delay
          },
        },
      },
      handlers: new MessageHandlerConfigBuilder()
        .addConfig(SCHEMA, async (message) => {
          // Return retryLater to trigger AWS SQS native retry
          if (shouldRetry) {
            return { error: 'retryLater' }
          }
          return { result: 'success' }
        })
        .build(),
    })
  }
}
```

**Barrier Sleep Mechanism (FIFO Queues):**

When using barriers with FIFO queues, the consumer implements a smart sleep-and-check mechanism to avoid unnecessary AWS retries:

1. **Handler or barrier returns `{ error: 'retryLater' }`**
2. **Consumer enters sleep loop** instead of immediately throwing error
3. **Periodically re-checks barrier** (every `barrierSleepCheckIntervalInMsecs`, default: 5 seconds)
4. **If barrier passes**: Process message successfully and exit
5. **If max retry duration exceeded** (`maxRetryDuration`, default: 4 days): Send message to DLQ
6. **Visibility timeout extended** every 30 seconds during sleep to prevent message reclaim

**Configuration with barrier sleep:**

```typescript
class MyFifoConsumer extends AbstractSqsConsumer<MessageType, Context> {
  constructor(dependencies: SQSConsumerDependencies) {
    super(dependencies, {
      creationConfig: {
        queue: {
          QueueName: 'my-queue.fifo',
          Attributes: {
            FifoQueue: 'true',
            VisibilityTimeout: '3600',  // 1 hour - must be greater than heartbeatInterval
          },
        },
      },
      maxRetryDuration: 345600,                   // 4 days in seconds (default)
      barrierSleepCheckIntervalInMsecs: 5000,     // 5 seconds (default)

      // Consumer configuration - controls visibility timeout management
      consumerOverrides: {
        heartbeatInterval: 300,  // Extends visibility every 5 minutes (default)
        // Must be less than VisibilityTimeout
        // Prevents AWS from reclaiming the message during long processing or barrier sleep
      },

      handlers: new MessageHandlerConfigBuilder()
        .addConfig(
          SCHEMA,
          async (message) => {
            return { result: 'success' }
          },
          {
            preHandlerBarrier: async (message, context) => {
              // Check if prerequisite is met
              const prerequisiteMet = await checkPrerequisite(message)
              return { isPassing: prerequisiteMet }
            }
          }
        )
        .build(),
    })
  }
}
```

**Retry flow for FIFO queues (with barrier sleep):**
1. Handler or barrier returns `{ error: 'retryLater' }`
2. Consumer enters sleep loop for up to `maxRetryDuration` (default: 4 days)
3. Every `barrierSleepCheckIntervalInMsecs` (default: 5 sec), consumer:
   - Re-checks barrier
   - Extends visibility timeout (every 30 seconds)
4. If barrier passes: Process message and mark as consumed
5. If `maxRetryDuration` exceeded: Send message to DLQ with error `"FIFO queue: Retry duration exceeded. Moving message to DLQ."`

**Retry flow for FIFO queues (without barrier - immediate retry):**
1. Handler returns `{ error: 'retryLater' }` but no barrier configured
2. Consumer immediately throws error: `"FIFO queue: Triggering AWS SQS retry to preserve message order"`
3. AWS SQS makes message invisible for `VisibilityTimeout` duration
4. After timeout, message becomes visible again **at the same position**
5. Consumer processes it again

**Retry duration limit:**
- The `maxRetryDuration` setting still applies (default: 4 days)
- After exceeding retry duration, message moves to DLQ
- Error message: `"FIFO queue: Retry duration exceeded. Moving message to DLQ"`

**Understanding VisibilityTimeout and heartbeatInterval:**

The visibility timeout mechanism prevents multiple consumers from processing the same message:

- **`VisibilityTimeout` (queue attribute)**: Initial time (in seconds) that a message is hidden from other consumers after being received. This is the starting timeout when a message is first picked up.

- **`heartbeatInterval` (consumerOverrides option)**: Interval (in seconds) at which the consumer automatically extends the visibility timeout using `ChangeMessageVisibilityCommand`. This keeps the message "locked" to the current consumer during long processing.

**How they work together:**

1. **Message received**: AWS SQS hides message for `VisibilityTimeout` seconds
2. **During processing**: Consumer automatically extends timeout every `heartbeatInterval` seconds
3. **If processing exceeds VisibilityTimeout without extension**: AWS makes message visible again (allowing retry)
4. **Requirement**: `heartbeatInterval` < `VisibilityTimeout` (otherwise extension happens after message already became visible)

**For FIFO queues with barrier sleep:**
- The barrier sleep mechanism can explicitly extend visibility timeout every 30 seconds
- Extensions set visibility to 90 seconds **from now** (not cumulative) before each sleep chunk
- Extension happens BEFORE sleeping (not after) to prevent first-check vulnerability
- The 90-second value (3x the check interval) provides a 60-second safety buffer against timing drift
- However, it smartly defers to `heartbeatInterval` when set to avoid redundant API calls:
  - If `heartbeatInterval` <= 30 seconds: rely on sqs-consumer's automatic extension (no explicit extension)
  - If `heartbeatInterval` > 30 seconds or not set: explicitly extend every 30 seconds during sleep
- This optimization prevents unnecessary ChangeMessageVisibility API calls
- Set `VisibilityTimeout` high enough for expected barrier wait time (e.g., 3600 seconds / 1 hour)

**Best practices:**
- Set `VisibilityTimeout` high enough to accommodate the barrier sleep period (consider `maxRetryDuration`)
- Use `heartbeatInterval` < `VisibilityTimeout` (default: 300 seconds / 5 minutes)
- For long-running barriers: increase `VisibilityTimeout` (not `heartbeatInterval`)
- Use `maxRetryDuration` to control total retry time for both FIFO and standard queues
- Adjust `barrierSleepCheckIntervalInMsecs` based on how quickly prerequisites typically become available
- For FIFO queues: barriers are rechecked periodically instead of republishing messages
- For standard queues: messages are republished with exponential backoff delay

### Message Groups

Message groups enable parallel processing while maintaining order:

**Scenario: E-commerce orders**

```typescript
// Each customer's orders are processed in order
await publisher.publish({
  orderId: 'order-1',
  customerId: 'customer-A',
  action: 'created',
}, {
  MessageGroupId: 'customer-A',  // Group by customer
})

await publisher.publish({
  orderId: 'order-2',
  customerId: 'customer-A',
  action: 'paid',
}, {
  MessageGroupId: 'customer-A',  // Same group - processed in order
})

await publisher.publish({
  orderId: 'order-3',
  customerId: 'customer-B',
  action: 'created',
}, {
  MessageGroupId: 'customer-B',  // Different group - parallel processing
})
```

**Group Assignment:**
- AWS SQS automatically assigns groups to consumers
- Assignment is dynamic (groups can move between consumers)
- Assignment is sticky (SQS prefers consistency)
- You cannot control which consumer processes which group

**Parallel Processing:**

```typescript
{
  concurrentConsumersAmount: 5,  // 5 consumers can process 5 groups in parallel
}
```

**Best Practices:**

1. **Design groups for parallelism**
   ```typescript
   // ✅ Good: Many groups = high parallelism
   MessageGroupId: `customer-${customerId}`

   // ❌ Bad: One group = no parallelism
   MessageGroupId: 'all-customers'
   ```

2. **Balance group sizes**
   - Avoid one group having 90% of messages
   - Aim for relatively balanced message distribution

3. **Size concurrent consumers appropriately**
   ```typescript
   // If you have 10 active customer groups at peak
   concurrentConsumersAmount: 10
   ```

### FIFO-Specific Configuration

```typescript
// Publisher
{
  fifoQueue: true,

  // Choose one or more:
  messageGroupIdField: 'userId',        // Use field from message
  defaultMessageGroupId: 'default',     // Fallback value
  // or provide in publish call
}

// Consumer
{
  fifoQueue: true,
  concurrentConsumersAmount: 3,  // Process 3 groups in parallel

  // Note: Retry behavior is different for FIFO
  // - No DelaySeconds support (AWS limitation)
  // - Messages retry immediately
  // - Order is preserved
}

// Queue Configuration
{
  creationConfig: {
    queue: {
      QueueName: 'my-queue.fifo',
      Attributes: {
        FifoQueue: 'true',

        // Optional: Automatic deduplication based on message body
        ContentBasedDeduplication: 'false',  // or 'true'

        // Optional: Deduplication scope
        DeduplicationScope: 'queue',  // or 'messageGroup'

        // Optional: Throughput limit
        FifoThroughputLimit: 'perQueue',  // or 'perMessageGroupId'
      },
    },
  },
}
```

**FIFO vs Standard Queues:**

| Feature | Standard Queue | FIFO Queue |
|---------|---------------|------------|
| Ordering | Best-effort | Guaranteed within group |
| Delivery | At-least-once | Exactly-once |
| Throughput | Unlimited | 3,000 msg/s (per queue) or 300 msg/s (per group) |
| Retry Delay | Exponential backoff | Immediate (no delay) |
| Message Groups | N/A | Required (MessageGroupId) |
| Naming | Any name | Must end with `.fifo` |

## Policy Configuration

Configure queue access policies for SNS topic subscriptions or cross-account access:

### Allow SNS Topic to Send Messages

```typescript
import { SQS_RESOURCE_CURRENT_QUEUE } from '@message-queue-toolkit/sqs'

{
  creationConfig: {
    queue: { QueueName: 'my-queue' },
    policyConfig: {
      resource: SQS_RESOURCE_CURRENT_QUEUE,  // Use queue's ARN
      statements: {
        Effect: 'Allow',
        Principal: '*',
        Action: ['sqs:SendMessage'],
      },
    },
  },
}
```

### Allow Specific SNS Topics

```typescript
{
  creationConfig: {
    queue: { QueueName: 'my-queue' },
    topicArnsWithPublishPermissionsPrefix: 'arn:aws:sns:us-east-1:123456789012:my-topic',
  },
}
```

### Custom Policy

```typescript
{
  creationConfig: {
    queue: { QueueName: 'my-queue' },
    policyConfig: {
      resource: 'arn:aws:sqs:us-east-1:123456789012:my-queue',
      statements: [
        {
          Effect: 'Allow',
          Principal: { AWS: 'arn:aws:iam::111111111111:root' },
          Action: ['sqs:SendMessage', 'sqs:ReceiveMessage'],
        },
        {
          Effect: 'Deny',
          Principal: '*',
          Action: ['sqs:DeleteQueue'],
        },
      ],
    },
  },
}
```

## Testing

The library is designed to be testable:

### TestSqsPublisher - Testing Utility for Invalid Messages

`TestSqsPublisher` is a specialized testing utility that allows you to publish arbitrary messages to SQS queues **without any validation**. This is useful for:

- Testing error handling with invalid/malformed messages
- Testing edge cases without schema constraints
- Integration testing with arbitrary test data
- Verifying consumer behavior with non-standard messages

**⚠️ IMPORTANT: This is a testing utility only. Do not use in production code.**

#### Features

- ✅ **No validation** - Bypasses Zod schemas, deduplication, and payload offloading
- ✅ **Flexible queue targeting** - Accepts `queueUrl`, `queueName`, consumer, or publisher instances
- ✅ **FIFO support** - Works with both standard and FIFO queues
- ✅ **Simple API** - One publisher instance can publish to any queue

#### Basic Usage

```typescript
import { TestSqsPublisher } from '@message-queue-toolkit/sqs'
import { SQSClient } from '@aws-sdk/client-sqs'

const sqsClient = new SQSClient({ region: 'us-east-1' })
const testPublisher = new TestSqsPublisher(sqsClient)

// Publish to a queue by URL
await testPublisher.publish(
  { any: 'data', without: 'validation' },
  { queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue' }
)

// Publish to a queue by name
await testPublisher.publish(
  { incomplete: 'message' },
  { queueName: 'my-queue' }
)
```

#### Publishing to Consumer's Queue

Extract the queue URL from an existing consumer:

```typescript
const consumer = new UserEventsConsumer(sqsClient, userService)
await consumer.init()

const testPublisher = new TestSqsPublisher(sqsClient)

// Publish directly to the consumer's queue
await testPublisher.publish(
  {
    invalid: 'schema',
    missing: 'required fields',
  },
  { consumer }
)
```

#### Publishing to Publisher's Queue

Extract the queue URL from an existing publisher:

```typescript
const regularPublisher = new UserEventsPublisher(sqsClient)
await regularPublisher.init()

const testPublisher = new TestSqsPublisher(sqsClient)

// Publish to the same queue without validation
await testPublisher.publish(
  { totally: 'arbitrary', data: true },
  { publisher: regularPublisher }
)
```

#### Testing Invalid Message Handling

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

describe('Consumer Error Handling', () => {
  let sqsClient: SQSClient
  let consumer: UserEventsConsumer
  let testPublisher: TestSqsPublisher

  beforeEach(async () => {
    sqsClient = new SQSClient({
      endpoint: 'http://localhost:4566',  // LocalStack
      region: 'us-east-1',
    })

    consumer = new UserEventsConsumer(sqsClient, userService, {
      handlerSpy: true,
    })
    await consumer.start()

    testPublisher = new TestSqsPublisher(sqsClient)
  })

  afterEach(async () => {
    await consumer.close()
  })

  it('handles message with missing required fields', async () => {
    // Publish invalid message
    await testPublisher.publish(
      {
        // Missing: id, messageType, timestamp
        someData: 'incomplete',
      },
      { consumer }  // Publish to consumer's queue
    )

    // Wait briefly for processing
    await new Promise(resolve => setTimeout(resolve, 1000))

    // Verify consumer handled the error gracefully
    // (e.g., sent to DLQ, logged error, etc.)
    expect(errorLogger.errors).toContainEqual(
      expect.objectContaining({
        message: expect.stringContaining('Invalid message format'),
      })
    )
  })

  it('handles message with wrong data types', async () => {
    await testPublisher.publish(
      {
        id: 123,  // Should be string
        messageType: null,  // Should be string literal
        userId: ['not', 'a', 'string'],  // Should be string
      },
      { consumer }
    )

    await new Promise(resolve => setTimeout(resolve, 1000))

    // Verify error handling
    expect(validationErrors).toHaveLength(1)
  })

  it('handles completely malformed messages', async () => {
    await testPublisher.publish(
      {
        totally: 'random',
        nested: { deeply: { data: [1, 2, 3] } },
        unexpected: true,
      },
      { consumer }
    )

    await new Promise(resolve => setTimeout(resolve, 1000))

    // Message should be rejected and sent to DLQ
    expect(dlqMessages).toHaveLength(1)
  })
})
```

#### FIFO Queue Testing

```typescript
it('publishes to FIFO queue with MessageGroupId', async () => {
  const testPublisher = new TestSqsPublisher(sqsClient)

  await testPublisher.publish(
    { test: 'fifo-message' },
    {
      queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo',
      MessageGroupId: 'test-group',
      MessageDeduplicationId: 'unique-id-123',
    }
  )

  // Verify message was published
  const result = await consumer.handlerSpy.waitForMessage(
    (msg) => msg.test === 'fifo-message',
    'consumed'
  )

  expect(result).toBeDefined()
})
```

#### API Reference

```typescript
class TestSqsPublisher {
  constructor(sqsClient: SQSClient)

  /**
   * Publish a message to an SQS queue without validation
   * @param payload - Any JSON-serializable object
   * @param options - Queue targeting and FIFO options
   */
  async publish(
    payload: any,
    options:
      | { queueUrl: string; MessageGroupId?: string; MessageDeduplicationId?: string }
      | { queueName: string; MessageGroupId?: string; MessageDeduplicationId?: string }
      | { consumer: AbstractSqsConsumer<any, any, any, any, any>; MessageGroupId?: string; MessageDeduplicationId?: string }
      | { publisher: AbstractSqsPublisher<any>; MessageGroupId?: string; MessageDeduplicationId?: string }
  ): Promise<void>
}
```

**When to use `TestSqsPublisher`:**
- ✅ Testing error handling and edge cases
- ✅ Integration tests with invalid data
- ✅ Testing consumer resilience
- ✅ Debugging message processing issues

**When NOT to use `TestSqsPublisher`:**
- ❌ Production code
- ❌ Publishing valid messages (use regular publishers)
- ❌ Messages that need validation, deduplication, or offloading

### Integration Tests

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { SQSClient } from '@aws-sdk/client-sqs'

describe('UserEventsConsumer', () => {
  let sqsClient: SQSClient
  let publisher: UserEventsPublisher
  let consumer: UserEventsConsumer

  beforeEach(async () => {
    sqsClient = new SQSClient({
      endpoint: 'http://localhost:4566',  // LocalStack
      region: 'us-east-1',
    })

    publisher = new UserEventsPublisher(sqsClient)
    consumer = new UserEventsConsumer(sqsClient, userService)

    await publisher.init()
    await consumer.start()
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

### AbstractSqsPublisher

```typescript
class AbstractSqsPublisher<MessagePayloadType extends object> {
  constructor(
    dependencies: SQSDependencies,
    options: SQSPublisherOptions<MessagePayloadType>
  )

  async init(): Promise<void>
  async close(): Promise<void>

  async publish(
    message: MessagePayloadType,
    options?: SQSMessageOptions
  ): Promise<void>

  readonly handlerSpy: HandlerSpy<MessagePayloadType>
}
```

### AbstractSqsConsumer

```typescript
class AbstractSqsConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined
> {
  constructor(
    dependencies: SQSConsumerDependencies,
    options: SQSConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext
  )

  async init(): Promise<void>
  async start(): Promise<void>
  async close(abort?: boolean): Promise<void>

  readonly handlerSpy: HandlerSpy<MessagePayloadType>
}
```

### Types

```typescript
// Message options for publishing
type SQSMessageOptions = {
  MessageGroupId?: string         // Required for FIFO queues
  MessageDeduplicationId?: string // Optional for FIFO queues
}

// Handler result
type HandlerResult = Either<'retryLater', 'success'>

// Dependencies
type SQSDependencies = {
  sqsClient: SQSClient
  logger: Logger
  errorReporter: ErrorReporter
  messageMetricsManager?: MessageMetricsManager
}

// Consumer dependencies (extends SQSDependencies)
type SQSConsumerDependencies = SQSDependencies & {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}
```

### Utility Functions

```typescript
// Queue validation
function isFifoQueueName(queueName: string): boolean
function validateFifoQueueName(queueName: string, isFifoQueue: boolean): void
function validateFifoQueueConfiguration(
  queueName: string,
  attributes?: Record<string, string>,
  isFifoQueue?: boolean
): void

// Queue operations
async function assertQueue(
  sqsClient: SQSClient,
  queueConfig: CreateQueueCommandInput,
  extraParams?: ExtraSQSCreationParams,
  isFifoQueue?: boolean
): Promise<{ queueUrl: string; queueArn: string; queueName: string }>

async function deleteQueue(
  client: SQSClient,
  queueName: string,
  waitForConfirmation?: boolean
): Promise<void>

async function getQueueUrl(
  sqsClient: SQSClient,
  queueName: string
): Promise<Either<'not_found', string>>

async function getQueueAttributes(
  sqsClient: SQSClient,
  queueUrl: string,
  attributeNames?: QueueAttributeName[]
): Promise<Either<'not_found', { attributes?: Record<string, string> }>>

// Message size calculation
function calculateOutgoingMessageSize(message: unknown): number
```

## License

MIT

## Contributing

Contributions are welcome! Please see the main repository for guidelines.

## Links

- [Main Repository](https://github.com/kibertoad/message-queue-toolkit)
- [Core Package](https://www.npmjs.com/package/@message-queue-toolkit/core)
- [AWS SQS Documentation](https://docs.aws.amazon.com/sqs/)
- [FIFO Queue Documentation](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues.html)
