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
  - [NO_MESSAGE_TYPE_FIELD](#no_message_type_field)
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

The library supports flexible message type resolution through two configuration options:

#### Option 1: `messageTypeField` (Simple)

Use when the message type is a field at the root level of the message data:

```typescript
{
  messageTypeField: 'type',  // Extracts type from message.type
}
```

#### Option 2: `messageTypeResolver` (Flexible)

Use for more complex scenarios like extracting type from message attributes, nested fields, or when all messages are of the same type.

**Literal Type (Constant)**

When all messages in a queue/subscription are of the same type:

```typescript
import type { MessageTypeResolverConfig } from '@message-queue-toolkit/core'

const config: MessageTypeResolverConfig = {
  literal: 'order.created',  // All messages treated as this type
}
```

**Field Path**

Equivalent to `messageTypeField`, extracts from a root-level field:

```typescript
const config: MessageTypeResolverConfig = {
  messageTypePath: 'type',  // Same as messageTypeField: 'type'
}
```

**Custom Resolver Function**

For complex scenarios like Google Cloud Storage notifications via Pub/Sub where the event type is in message attributes:

```typescript
const config: MessageTypeResolverConfig = {
  resolver: ({ messageData, messageAttributes }) => {
    // Extract from Pub/Sub message attributes
    const eventType = messageAttributes?.eventType as string | undefined
    if (!eventType) {
      throw new Error('eventType attribute is required')
    }

    // Optionally map to internal event types
    if (eventType === 'OBJECT_FINALIZE') return 'storage.object.created'
    if (eventType === 'OBJECT_DELETE') return 'storage.object.deleted'
    return eventType
  },
}
```

**CloudEvents Format**

For CloudEvents binary mode where type might be in headers/attributes:

```typescript
const config: MessageTypeResolverConfig = {
  resolver: ({ messageData, messageAttributes }) => {
    // Check for CloudEvents binary mode (type in attributes)
    if (messageAttributes?.['ce-type']) {
      return messageAttributes['ce-type'] as string
    }
    // Fall back to type in message data
    const data = messageData as { type?: string }
    if (!data.type) {
      throw new Error('Message type not found')
    }
    return data.type
  },
}
```

**Important:** The resolver function must always return a valid string. If the type cannot be determined, either return a default type or throw an error with a descriptive message.

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

### NO_MESSAGE_TYPE_FIELD

Use this constant when your consumer should accept all message types without routing:

```typescript
import { NO_MESSAGE_TYPE_FIELD } from '@message-queue-toolkit/core'

// Consumer will use a single handler for all messages
{
  messageTypeField: NO_MESSAGE_TYPE_FIELD,
  handlers: new MessageHandlerConfigBuilder()
    .addConfig(PassthroughSchema, async (message) => {
      // Handles any message type
      return { result: 'success' }
    })
    .build(),
}
```

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
