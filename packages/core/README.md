# @message-queue-toolkit/core
 
Core library for message-queue-toolkit. Provides foundational abstractions, utilities, and base classes for building message queue publishers and consumers.

## Table of Contents

- [Installation](#installation)
- [Overview](#overview)
- [Core Concepts](#core-concepts)
  - [Message Schemas](#message-schemas)
  - [Message Type Resolution](#message-type-resolution)
  - [Handler Configuration](#handler-configuration)
  - [Pre-handlers and Barriers](#pre-handlers-and-barriers)
  - [Handler Spies](#handler-spies)
- [Key Classes](#key-classes)
  - [AbstractQueueService](#abstractqueueservice)
  - [MessageHandlerConfigBuilder](#messagehandlerconfigbuilder)
  - [HandlerContainer](#handlercontainer)
  - [MessageSchemaContainer](#messageschemacontainer)
  - [AbstractPublisherManager](#abstractpublishermanager)
  - [DomainEventEmitter](#domaineventemitter)
- [Utilities](#utilities)
  - [Error Classes](#error-classes)
  - [Message Deduplication](#message-deduplication)
  - [Payload Offloading](#payload-offloading)
- [API Reference](#api-reference)
- [Links](#links)

## Installation

```bash
npm install @message-queue-toolkit/core zod
```

**Peer Dependencies:**
- `zod` - Schema validation

## Overview

The core package provides the foundational building blocks used by all protocol-specific implementations (SQS, SNS, AMQP, Kafka, GCP Pub/Sub). It includes:

- **Base Classes**: Abstract classes for publishers and consumers
- **Handler System**: Type-safe message routing and handling
- **Validation**: Zod schema validation and message parsing
- **Utilities**: Retry logic, date handling, environment utilities
- **Testing**: Handler spies for testing async message flows
- **Extensibility**: Interfaces for payload stores, deduplication stores, and metrics

## Core Concepts

### Message Schemas

Messages are validated using Zod schemas. The library uses configurable field names:

- **`messageTypeField`** or **`messageTypeResolver`**: Configuration for resolving the message type discriminator (see [Message Type Resolution](#message-type-resolution))
- **`messageIdField`** (default: `'id'`): Field containing the message ID
- **`messageTimestampField`** (default: `'timestamp'`): Field containing the timestamp

```typescript
import { z } from 'zod'

const UserCreatedSchema = z.object({
  id: z.string(),
  type: z.literal('user.created'),  // Used for routing
  timestamp: z.string().datetime(),
  userId: z.string(),
  email: z.string().email(),
})

type UserCreated = z.infer<typeof UserCreatedSchema>
```

### Message Type Resolution

#### What is Message Type?

The **message type** is a discriminator field that identifies what kind of event or command a message represents. It's used for:

1. **Routing**: Directing messages to the appropriate handler based on their type
2. **Schema validation**: Selecting the correct Zod schema to validate the message
3. **Observability**: Tracking metrics and logs per message type

In a typical event-driven architecture, a single queue or topic may receive multiple types of messages. For example, a `user-events` queue might receive `user.created`, `user.updated`, and `user.deleted` events. The message type tells the consumer which handler should process each message.

#### Configuration Options

The library supports two configuration approaches:

##### Option 1: `messageTypeField` (Simple)

Use when the message type is a field at the root level of the parsed message body:

```typescript
{
  messageTypeField: 'type',  // Extracts type from message.type
}
```

##### Option 2: `messageTypeResolver` (Flexible)

Use for complex scenarios where the type needs to be extracted from message attributes, nested fields, requires transformation, or when all messages are of the same type.

```typescript
import type { MessageTypeResolverConfig } from '@message-queue-toolkit/core'

// Literal: All messages treated as the same type
const literalConfig: MessageTypeResolverConfig = {
  literal: 'order.created',
}

// Field path: Extract from a root-level field (equivalent to messageTypeField)
const pathConfig: MessageTypeResolverConfig = {
  messageTypePath: 'type',
}

// Custom resolver: Full flexibility
const resolverConfig: MessageTypeResolverConfig = {
  resolver: ({ messageData, messageAttributes }) => {
    // Your custom logic here
    return 'resolved.type'
  },
}
```

**Important:** The resolver function must always return a valid string. If the type cannot be determined, either return a default type or throw an error with a descriptive message.

#### Real-World Examples by Platform

##### AWS SQS (Plain)

When publishing your own events directly to SQS, you control the message format:

```typescript
// Message format you control
{
  "id": "msg-123",
  "type": "order.created",  // Your type field
  "timestamp": "2024-01-15T10:30:00Z",
  "payload": {
    "orderId": "order-456",
    "amount": 99.99
  }
}

// Configuration
{
  messageTypeField: 'type',
}
```

##### AWS EventBridge → SQS

EventBridge events have a specific structure with `detail-type`:

```typescript
// EventBridge event structure delivered to SQS
{
  "version": "0",
  "id": "12345678-1234-1234-1234-123456789012",
  "detail-type": "Order Created",  // EventBridge uses detail-type
  "source": "com.myapp.orders",
  "account": "123456789012",
  "time": "2024-01-15T10:30:00Z",
  "region": "us-east-1",
  "detail": {
    "orderId": "order-456",
    "amount": 99.99
  }
}

// Configuration
{
  messageTypeField: 'detail-type',
}

// Or with resolver for normalization
{
  messageTypeResolver: {
    resolver: ({ messageData }) => {
      const data = messageData as { 'detail-type'?: string; source?: string }
      const detailType = data['detail-type']
      if (!detailType) throw new Error('detail-type is required')
      // Optionally normalize: "Order Created" → "order.created"
      return detailType.toLowerCase().replace(/ /g, '.')
    },
  },
}
```

##### AWS SNS → SQS

SNS messages wrapped in SQS have the actual payload in the `Message` field (handled automatically by the library after unwrapping):

```typescript
// After SNS envelope unwrapping, you get your original message
{
  "id": "msg-123",
  "type": "user.signup.completed",
  "userId": "user-789",
  "email": "user@example.com"
}

// Configuration
{
  messageTypeField: 'type',
}
```

##### Apache Kafka

Kafka typically uses topic-based routing, but you may still need message types within a topic:

```typescript
// Kafka message value (JSON)
{
  "eventType": "inventory.reserved",
  "eventId": "evt-123",
  "timestamp": 1705312200000,
  "data": {
    "sku": "PROD-001",
    "quantity": 5
  }
}

// Configuration
{
  messageTypeField: 'eventType',
}

// Or using Kafka headers (via custom resolver)
{
  messageTypeResolver: {
    resolver: ({ messageData, messageAttributes }) => {
      // Kafka headers are passed as messageAttributes
      if (messageAttributes?.['ce_type']) {
        return messageAttributes['ce_type'] as string  // CloudEvents header
      }
      const data = messageData as { eventType?: string }
      if (!data.eventType) throw new Error('eventType required')
      return data.eventType
    },
  },
}
```

##### Google Cloud Pub/Sub (Your Own Events)

When you control the message format in Pub/Sub:

```typescript
// Your message (base64-decoded from data field)
{
  "type": "payment.processed",
  "paymentId": "pay-123",
  "amount": 150.00,
  "currency": "USD"
}

// Configuration
{
  messageTypeField: 'type',
}
```

##### Google Cloud Pub/Sub (Cloud Storage Notifications)

Cloud Storage notifications put the event type in message **attributes**, not the data payload:

```typescript
// Pub/Sub message structure for Cloud Storage notifications
{
  "data": "eyJraW5kIjoic3RvcmFnZSMgb2JqZWN0In0=",  // Base64-encoded object metadata
  "attributes": {
    "eventType": "OBJECT_FINALIZE",  // Type is HERE, not in data!
    "bucketId": "my-bucket",
    "objectId": "path/to/file.jpg",
    "objectGeneration": "1705312200000"
  },
  "messageId": "123456789",
  "publishTime": "2024-01-15T10:30:00Z"
}

// Configuration - must use resolver to access attributes
{
  messageTypeResolver: {
    resolver: ({ messageAttributes }) => {
      const eventType = messageAttributes?.eventType as string
      if (!eventType) {
        throw new Error('eventType attribute required for Cloud Storage notifications')
      }
      // Map GCS event types to your internal types
      const typeMap: Record<string, string> = {
        'OBJECT_FINALIZE': 'storage.object.created',
        'OBJECT_DELETE': 'storage.object.deleted',
        'OBJECT_ARCHIVE': 'storage.object.archived',
        'OBJECT_METADATA_UPDATE': 'storage.object.metadataUpdated',
      }
      return typeMap[eventType] ?? eventType
    },
  },
}
```

##### Google Cloud Pub/Sub (Eventarc / CloudEvents)

Eventarc delivers events in CloudEvents format:

```typescript
// CloudEvents structured format
{
  "specversion": "1.0",
  "type": "google.cloud.storage.object.v1.finalized",  // CloudEvents type
  "source": "//storage.googleapis.com/projects/_/buckets/my-bucket",
  "id": "1234567890",
  "time": "2024-01-15T10:30:00Z",
  "datacontenttype": "application/json",
  "data": {
    "bucket": "my-bucket",
    "name": "path/to/file.jpg",
    "contentType": "image/jpeg"
  }
}

// Configuration
{
  messageTypeField: 'type',  // CloudEvents type is at root level
}

// Or with mapping to simpler types
{
  messageTypeResolver: {
    resolver: ({ messageData }) => {
      const data = messageData as { type?: string }
      const ceType = data.type
      if (!ceType) throw new Error('CloudEvents type required')
      // Map verbose CloudEvents types to simpler names
      if (ceType.includes('storage.object') && ceType.includes('finalized')) {
        return 'storage.object.created'
      }
      if (ceType.includes('storage.object') && ceType.includes('deleted')) {
        return 'storage.object.deleted'
      }
      return ceType
    },
  },
}
```

##### Single-Type Queues (Any Platform)

When a queue/subscription only ever receives one type of message, use `literal`:

```typescript
// Dedicated queue for order.created events only
{
  messageTypeResolver: {
    literal: 'order.created',
  },
}
```

This is useful for:
- Dedicated queues/subscriptions filtered to a single event type
- Legacy systems where messages don't have a type field
- Simple integrations where you know exactly what you're receiving

### Handler Configuration

Use `MessageHandlerConfigBuilder` to configure handlers for different message types:

```typescript
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

const handlers = new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
  .addConfig(
    UserCreatedSchema,
    async (message, context, preHandlingOutputs) => {
      await context.userService.createUser(message.userId)
      return { result: 'success' }
    }
  )
  .addConfig(
    UserUpdatedSchema,
    async (message, context, preHandlingOutputs) => {
      await context.userService.updateUser(message.userId, message.changes)
      return { result: 'success' }
    }
  )
  .build()
```

### Pre-handlers and Barriers

**Pre-handlers** are middleware functions that run before the main handler:

```typescript
const preHandlers = [
  (message, context, output, next) => {
    // Enrich context, validate prerequisites, etc.
    output.logger = context.logger.child({ messageId: message.id })
    next({ result: 'success' })
  },
]
```

**Barriers** control whether a message should be processed or retried later:

```typescript
const preHandlerBarrier = async (message, context, preHandlerOutput) => {
  const prerequisiteMet = await checkPrerequisite(message)
  return {
    isPassing: prerequisiteMet,
    output: { ready: true },
  }
}
```

### Handler Spies

Handler spies enable testing of async message flows:

```typescript
// Enable in consumer/publisher options
{ handlerSpy: true }

// Wait for specific messages in tests
const result = await consumer.handlerSpy.waitForMessageWithId('msg-123', 'consumed', 5000)
expect(result.userId).toBe('user-456')
```

## Key Classes

### AbstractQueueService

Base class for all queue services. Provides:

- Message serialization/deserialization
- Schema validation
- Retry logic with exponential backoff
- Payload offloading support
- Message deduplication primitives

### MessageHandlerConfigBuilder

Fluent builder for configuring message handlers:

```typescript
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'

const handlers = new MessageHandlerConfigBuilder<
  SupportedMessages,
  ExecutionContext,
  PrehandlerOutput
>()
  .addConfig(Schema1, handler1)
  .addConfig(Schema2, handler2, {
    preHandlers: [preHandler1, preHandler2],
    preHandlerBarrier: barrierFn,
    messageLogFormatter: (msg) => ({ id: msg.id }),
  })
  .build()
```

#### Handler Configuration Options

The third parameter to `addConfig` accepts these options:

| Option | Type | Description |
|--------|------|-------------|
| `messageType` | `string` | Explicit message type for routing. Required when using custom resolver. |
| `messageLogFormatter` | `(message) => unknown` | Custom formatter for logging |
| `preHandlers` | `Prehandler[]` | Middleware functions run before the handler |
| `preHandlerBarrier` | `BarrierCallback` | Barrier function for out-of-order message handling |

#### Explicit Message Type

When using a custom resolver function (`messageTypeResolver: { resolver: fn }`), the message type cannot be automatically extracted from schemas at registration time. You must provide an explicit `messageType` for each handler:

```typescript
const handlers = new MessageHandlerConfigBuilder<SupportedMessages, Context>()
  .addConfig(
    STORAGE_OBJECT_SCHEMA,
    handleObjectCreated,
    { messageType: 'storage.object.created' }  // Required for custom resolver
  )
  .addConfig(
    STORAGE_DELETE_SCHEMA,
    handleObjectDeleted,
    { messageType: 'storage.object.deleted' }  // Required for custom resolver
  )
  .build()

const container = new HandlerContainer({
  messageHandlers: handlers,
  messageTypeResolver: {
    resolver: ({ messageAttributes }) => {
      // Map external event types to your internal types
      const eventType = messageAttributes?.eventType as string
      if (eventType === 'OBJECT_FINALIZE') return 'storage.object.created'
      if (eventType === 'OBJECT_DELETE') return 'storage.object.deleted'
      throw new Error(`Unknown event type: ${eventType}`)
    },
  },
})
```

**Priority for determining handler message type:**
1. Explicit `messageType` in handler options (highest priority)
2. Literal type from `messageTypeResolver: { literal: 'type' }`
3. Extract from schema's literal field using `messageTypePath`

If the message type cannot be determined, an error is thrown during container construction.

### HandlerContainer

Routes messages to appropriate handlers based on message type:

```typescript
import { HandlerContainer } from '@message-queue-toolkit/core'

const container = new HandlerContainer({
  messageHandlers: handlers,
  messageTypeField: 'type',
})

const handler = container.resolveHandler(message.type)
```

### MessageSchemaContainer

Manages Zod schemas and validates messages:

```typescript
import { MessageSchemaContainer } from '@message-queue-toolkit/core'

const container = new MessageSchemaContainer({
  messageSchemas: [Schema1, Schema2],
  messageTypeField: 'type',
})

const schema = container.resolveSchema(message.type)
```

### AbstractPublisherManager

Factory pattern for spawning publishers on demand:

```typescript
import { AbstractPublisherManager } from '@message-queue-toolkit/core'

// Automatically spawns publishers and fills metadata
await publisherManager.publish('user-events-topic', {
  type: 'user.created',
  userId: 'user-123',
})
```

### DomainEventEmitter

Event emitter for domain events:

```typescript
import { DomainEventEmitter } from '@message-queue-toolkit/core'

const emitter = new DomainEventEmitter()

emitter.on('user.created', async (event) => {
  console.log('User created:', event.userId)
})

await emitter.emit('user.created', { userId: 'user-123' })
```

## Utilities

### Error Classes

```typescript
import {
  MessageValidationError,
  MessageInvalidFormatError,
  DoNotProcessMessageError,
  RetryMessageLaterError,
} from '@message-queue-toolkit/core'

// Validation failed
throw new MessageValidationError(zodError.issues)

// Message format is invalid (cannot parse)
throw new MessageInvalidFormatError({ message: 'Invalid JSON' })

// Do not process this message (skip without retry)
throw new DoNotProcessMessageError({ message: 'Duplicate message' })

// Retry this message later
throw new RetryMessageLaterError({ message: 'Dependency not ready' })
```

### Message Deduplication

Interfaces for implementing deduplication stores:

```typescript
import type { MessageDeduplicationStore, ReleasableLock } from '@message-queue-toolkit/core'

// Implement custom deduplication store
class MyDeduplicationStore implements MessageDeduplicationStore {
  async keyExists(key: string): Promise<boolean> { /* ... */ }
  async setKey(key: string, ttlSeconds: number): Promise<void> { /* ... */ }
  async acquireLock(key: string, options: AcquireLockOptions): Promise<ReleasableLock> { /* ... */ }
}
```

### Payload Offloading

Interfaces for implementing payload stores:

```typescript
import type { PayloadStore, PayloadStoreConfig } from '@message-queue-toolkit/core'

// Implement custom payload store
class MyPayloadStore implements PayloadStore {
  async storePayload(payload: Buffer, messageId: string): Promise<PayloadRef> { /* ... */ }
  async retrievePayload(ref: PayloadRef): Promise<Buffer> { /* ... */ }
}
```

## API Reference

### Types

```typescript
// Handler result type
type HandlerResult = Either<'retryLater', 'success'>

// Pre-handler signature
type Prehandler<Message, Context, Output> = (
  message: Message,
  context: Context,
  output: Output,
  next: (result: PrehandlerResult) => void
) => void

// Barrier signature
type BarrierCallback<Message, Context, PrehandlerOutput, BarrierOutput> = (
  message: Message,
  context: Context,
  preHandlerOutput: PrehandlerOutput
) => Promise<BarrierResult<BarrierOutput>>

// Barrier result
type BarrierResult<Output> =
  | { isPassing: true; output: Output }
  | { isPassing: false; output?: never }

// Message type resolver context
type MessageTypeResolverContext = {
  messageData: unknown
  messageAttributes?: Record<string, unknown>
}

// Message type resolver function
type MessageTypeResolverFn = (context: MessageTypeResolverContext) => string

// Message type resolver configuration
type MessageTypeResolverConfig =
  | { messageTypePath: string }  // Extract from field at root of message data
  | { literal: string }          // Constant type for all messages
  | { resolver: MessageTypeResolverFn }  // Custom resolver function
```

### Utility Functions

```typescript
// Environment utilities
isProduction(): boolean
reloadConfig(): void

// Date utilities
isRetryDateExceeded(timestamp: string | Date, maxRetryDuration: number): boolean

// Message parsing
parseMessage<T>(data: unknown, schema: ZodSchema<T>): ParseMessageResult<T>

// Wait utilities
waitAndRetry<T>(fn: () => Promise<T>, options: WaitAndRetryOptions): Promise<T>

// Object utilities
objectMatches(obj: unknown, pattern: unknown): boolean
isShallowSubset(subset: object, superset: object): boolean
```

## Links

- [Main Repository](https://github.com/kibertoad/message-queue-toolkit)
- [SQS Package](https://www.npmjs.com/package/@message-queue-toolkit/sqs)
- [SNS Package](https://www.npmjs.com/package/@message-queue-toolkit/sns)
- [AMQP Package](https://www.npmjs.com/package/@message-queue-toolkit/amqp)
- [GCP Pub/Sub Package](https://www.npmjs.com/package/@message-queue-toolkit/gcp-pubsub)
- [Kafka Package](https://www.npmjs.com/package/@message-queue-toolkit/kafka)
