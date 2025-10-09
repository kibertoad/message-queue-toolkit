# @message-queue-toolkit/pubsub

Google Cloud Pub/Sub adapter for message-queue-toolkit. Provides type-safe message publishing and consumption with automatic schema validation, payload offloading, and advanced features like deduplication and dead letter queues.

## Overview

This package provides a complete Pub/Sub implementation following the message-queue-toolkit architecture:
- **Type-safe message handling** with Zod schema validation
- **Publisher** for publishing messages to topics
- **Consumer** for consuming messages from subscriptions
- **Payload offloading** for messages exceeding 10MB (integrates with GCS)
- **Message deduplication** (publisher and consumer level)
- **Dead letter queue support**
- **Exponential backoff** with retry limits
- **Barrier pattern** for handling out-of-order messages
- **Pre-handlers** (middleware) for message preprocessing
- **Handler spies** for testing and observability

## Installation

```bash
npm install @message-queue-toolkit/pubsub @google-cloud/pubsub zod
```

## Architecture

Google Pub/Sub follows a strict topic/subscription model:

```
Publisher → Topic → Subscription → Consumer
```

**Key concepts:**
- **Topics**: Named resources to which messages are published
- **Subscriptions**: Named resources representing message streams from a topic
- ❌ You CANNOT publish directly to subscriptions
- ❌ You CANNOT consume directly from topics
- ✅ One topic can have multiple subscriptions (fan-out)
- ✅ One subscription per consumer (or consumer group)

## Prerequisites

- Google Cloud Platform account
- Pub/Sub API enabled
- Appropriate IAM permissions

## Basic Usage

### Publisher

```typescript
import { PubSub } from '@google-cloud/pubsub'
import { AbstractPubSubPublisher } from '@message-queue-toolkit/pubsub'
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
import { AbstractPubSubConsumer, MessageHandlerConfigBuilder } from '@message-queue-toolkit/pubsub'

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

### Topic Configuration

```typescript
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
}
```

### Subscription Configuration

```typescript
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
}
```

### Locator Config (Production)

Instead of creating resources, locate existing ones:

```typescript
locatorConfig: {
  topicName: 'existing-topic',
  subscriptionName: 'existing-subscription', // For consumers
}
```

## Advanced Features

### Payload Offloading (Messages > 10MB)

```typescript
import { Storage } from '@google-cloud/storage'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import { PUBSUB_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/pubsub'

const storage = new Storage({ projectId: 'my-project' })

class LargeMessagePublisher extends AbstractPubSubPublisher<MyMessage> {
  constructor() {
    super(dependencies, {
      creationConfig: {
        topic: { name: 'large-messages' },
      },
      messageSchemas: [MyMessageSchema],
      messageTypeField: 'type',
      payloadStoreConfig: {
        store: new GCSPayloadStore(
          { gcsStorage: storage },
          { bucketName: 'my-payload-bucket' }
        ),
        messageSizeThreshold: PUBSUB_MESSAGE_MAX_SIZE,
      },
    })
  }
}
```

Consumer automatically retrieves offloaded payloads - no special configuration needed!

### Message Deduplication

**Publisher deduplication** (prevent duplicate sends):

```typescript
new MyPublisher(dependencies, {
  // ...other options
  enablePublisherDeduplication: true,
  messageDeduplicationConfig: {
    store: redisStore,
    deduplicationIdField: 'id',
  },
})
```

**Consumer deduplication** (prevent duplicate processing):

```typescript
new MyConsumer(dependencies, options, {
  // ...other options
  enableConsumerDeduplication: true,
  messageDeduplicationConfig: {
    store: redisStore,
    deduplicationIdField: 'id',
    deduplicationLockTimeout: 20000,
  },
})
```

### Dead Letter Queue

```typescript
subscription: {
  name: 'my-subscription',
  options: {
    deadLetterPolicy: {
      deadLetterTopic: 'projects/my-project/topics/my-dlq',
      maxDeliveryAttempts: 5,
    },
  },
}
```

Messages that fail after 5 delivery attempts will be sent to the DLQ topic.

### Message Ordering

Enable ordered delivery of messages with the same ordering key:

```typescript
// Publisher
creationConfig: {
  topic: {
    name: 'ordered-events',
    options: {
      enableMessageOrdering: true,
    },
  },
}

// Publish with ordering key
await publisher.publish(message, {
  orderingKey: 'user-123', // All messages with this key are delivered in order
})

// Consumer
subscription: {
  options: {
    enableMessageOrdering: true,
  },
}
```

### Pre-handlers (Middleware)

Execute logic before the main handler:

```typescript
handlers: new MessageHandlerConfigBuilder<MyMessage, Context>()
  .addConfig(
    MyMessageSchema,
    async (message) => {
      // Main handler
      return { result: 'success' }
    },
    {
      preHandlers: [
        (message, context, output, next) => {
          // Pre-processing
          console.log('Pre-handler 1')
          output.timestamp = Date.now()
          next({ result: 'success' })
        },
        (message, context, output, next) => {
          // More pre-processing
          console.log('Pre-handler 2')
          next({ result: 'success' })
        },
      ],
    }
  )
  .build()
```

### Barrier Pattern (Out-of-Order Handling)

Delay processing until prerequisites are met:

```typescript
handlers: new MessageHandlerConfigBuilder<MyMessage, Context>()
  .addConfig(
    MyMessageSchema,
    async (message, context, outputs) => {
      // This only runs if barrier passes
      return { result: 'success' }
    },
    {
      preHandlerBarrier: async (message, context) => {
        const isReady = await checkPrerequisites(message)

        if (isReady) {
          return {
            isPassing: true,
            output: { prerequisiteData: 'some data' },
          }
        }

        // Message will be nacked and retried later
        return { isPassing: false }
      },
    }
  )
  .build()
```

### Consumer Flow Control

Control message throughput:

```typescript
consumerOverrides: {
  flowControl: {
    maxMessages: 100,      // Max concurrent messages
    maxBytes: 10 * 1024 * 1024, // Max bytes in memory
  },
  batching: {
    maxMessages: 10,       // Pull messages in batches
    maxMilliseconds: 100,  // Max wait time for batch
  },
}
```

### Retry Configuration

```typescript
{
  maxRetryDuration: 4 * 24 * 60 * 60, // 4 days (default)
}
```

Messages older than this will not be retried (sent to DLQ if configured).

## Multiple Message Types

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

## Testing

### With Emulator

```bash
# Start emulator (included in docker-compose)
docker compose up -d pubsub-emulator
```

```typescript
import { PubSub } from '@google-cloud/pubsub'

const pubSubClient = new PubSub({
  projectId: 'test-project',
  apiEndpoint: 'localhost:8085', // Emulator endpoint
})
```

### Using Handler Spies

```typescript
// Publisher
await publisher.publish(message)
const spyResult = await publisher.handlerSpy.waitForMessageWithId('123', 'published')
expect(spyResult.processingResult).toBe('published')

// Consumer
await publisher.publish(message)
const spyResult = await consumer.handlerSpy.waitForMessageWithId('123', 'consumed')
expect(spyResult.processingResult).toBe('consumed')
```

## Error Handling

### Handler Returns

```typescript
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

### Error Resolver

```typescript
import { PubSubConsumerErrorResolver } from '@message-queue-toolkit/pubsub'

const consumerErrorResolver = new PubSubConsumerErrorResolver()

// Or custom implementation
class CustomErrorResolver implements ErrorResolver {
  processError(error: Error): void {
    // Send to Sentry, log, etc.
    console.error('Consumer error:', error)
  }
}
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

## Integration with Other Packages

Works seamlessly with:
- `@message-queue-toolkit/gcs-payload-store` - Payload offloading
- `@message-queue-toolkit/redis-message-deduplication-store` - Deduplication
- `@message-queue-toolkit/schemas` - Event registry
- `@message-queue-toolkit/metrics` - Prometheus metrics

## License

MIT

## Contributing

Contributions are welcome! Please see the main [message-queue-toolkit repository](https://github.com/kibertoad/message-queue-toolkit) for contribution guidelines.
