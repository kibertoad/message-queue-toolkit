# KafkaJS

This library provides utilities for implementing Kafka consumers and publishers using the [KafkaJS](https://kafka.js.org/) client library.
While following the same patterns as other message broker implementations,
Kafka's unique characteristics require some specific adaptations in the publisher and consumer definitions.

> **_NOTE:_** Check [README.md](../../README.md) for transport-agnostic library documentation.

## Installation

```bash
npm install @message-queue-toolkit/kafkajs kafkajs
```

## Publishers

Use `AbstractKafkaPublisher` as a base class for publisher implementation.

```typescript
import { AbstractKafkaPublisher } from '@message-queue-toolkit/kafkajs'

export class MyPublisher extends AbstractKafkaPublisher<typeof MY_TOPICS_CONFIG> {
  constructor(dependencies: KafkaDependencies) {
    super(dependencies, {
      kafka: {
        brokers: ['localhost:9092'],
        clientId: 'my-app',
      },
      topicsConfig: MY_TOPICS_CONFIG,
      autocreateTopics: true,
    })
  }
}
```

See [test publisher](test/publisher/PermissionPublisher.ts) for an example of implementation.

## Consumers

Use `AbstractKafkaConsumer` as a base class for consumer implementation.

```typescript
import { AbstractKafkaConsumer, KafkaHandlerConfig, KafkaHandlerRoutingBuilder } from '@message-queue-toolkit/kafkajs'

export class MyConsumer extends AbstractKafkaConsumer<typeof MY_TOPICS_CONFIG, MyExecutionContext> {
  constructor(dependencies: KafkaConsumerDependencies) {
    super(
      dependencies,
      {
        kafka: {
          brokers: ['localhost:9092'],
          clientId: 'my-app',
        },
        groupId: 'my-consumer-group',
        batchProcessingEnabled: false,
        handlers: new KafkaHandlerRoutingBuilder<typeof MY_TOPICS_CONFIG, MyExecutionContext, false>()
          .addConfig('my-topic', new KafkaHandlerConfig(MY_SCHEMA, (message, context) => {
            // Handle message
          }))
          .build(),
      },
      executionContext,
    )
  }
}
```

See [test consumer](test/consumer/PermissionConsumer.ts) for an example of implementation.

## Batch Processing

Kafka supports batch processing for improved throughput. To enable it, set `batchProcessingEnabled` to `true` and configure `batchProcessingOptions`.

When batch processing is enabled, message handlers receive an array of messages instead of a single message.

### Configuration Options

- `batchSize` - Maximum number of messages per batch
- `timeoutMilliseconds` - Maximum time to wait for a batch to fill before processing

### How It Works

Messages are buffered per topic-partition combination. Batches are processed when either:
- The buffer reaches the configured `batchSize`
- The `timeoutMilliseconds` timeout is reached since the first message was added

After successful batch processing, the offset of the last message in the batch is committed.

See [test batch consumer](test/consumer/PermissionBatchConsumer.ts) for an example of implementation.

## Configuration

### KafkaConfig

```typescript
type KafkaConfig = {
  brokers: string[]           // List of Kafka broker addresses
  clientId: string            // Client identifier
  ssl?: boolean | TLSConfig   // SSL configuration
  sasl?: SASLOptions          // SASL authentication
  connectionTimeout?: number   // Connection timeout in ms
  requestTimeout?: number      // Request timeout in ms
  retry?: RetryOptions        // Retry configuration
}
```

### Publisher Options

- `topicsConfig` - Array of topic configurations with schemas
- `autocreateTopics` - Whether to auto-create topics (default: false)
- `producerConfig` - Additional KafkaJS producer configuration

### Consumer Options

- `groupId` - Consumer group ID (required)
- `handlers` - Handler routing configuration
- `batchProcessingEnabled` - Enable batch processing (default: false)
- `batchProcessingOptions` - Batch configuration (required if batch processing enabled)
- `autocreateTopics` - Whether to auto-create topics (default: false)
- `fromBeginning` - Start consuming from beginning of topic (default: false)
- `sessionTimeout` - Session timeout in ms
- `rebalanceTimeout` - Rebalance timeout in ms
- `heartbeatInterval` - Heartbeat interval in ms

## Error Handling and Retries

The consumer implements an in-memory retry mechanism with exponential backoff:
- Failed messages are retried up to 3 times
- Backoff delay: 2^(retry-1) seconds between retries
- After all retries are exhausted, the message is logged as an error

## Message Format

Messages are deserialized and passed to handlers with the following structure:

```typescript
type DeserializedMessage<MessageValue> = {
  topic: string
  partition: number
  key: string | null
  value: MessageValue
  headers: Record<string, string | undefined>
  offset: string
  timestamp: string
}
```

## Handler Routing

The `KafkaHandlerRoutingBuilder` provides a type-safe way to configure message handlers:

```typescript
const handlers = new KafkaHandlerRoutingBuilder<TopicsConfig, ExecutionContext, BatchEnabled>()
  .addConfig('topic-1', new KafkaHandlerConfig(SCHEMA_1, handler1))
  .addConfig('topic-2', new KafkaHandlerConfig(SCHEMA_2, handler2))
  .build()
```

Each handler config requires:
- A Zod schema for message validation
- A handler function that receives the validated message(s) and execution context

## Testing

Use the `handlerSpy` for testing message processing:

```typescript
// Wait for a specific message to be processed
const result = await consumer.handlerSpy.waitForMessageWithId('message-123', 'consumed')

// Check if a message was processed without waiting
const check = consumer.handlerSpy.checkForMessage({ type: 'my.event' })
```

## Differences from @message-queue-toolkit/kafka

This package uses [KafkaJS](https://kafka.js.org/) as the underlying Kafka client instead of [@platformatic/kafka](https://github.com/platformatic/kafka). Key differences:

| Feature | @message-queue-toolkit/kafka | @message-queue-toolkit/kafkajs |
|---------|------------------------------|--------------------------------|
| Broker config | `bootstrapBrokers` | `brokers` |
| Client library | @platformatic/kafka | kafkajs |
| Stream-based | Yes (uses Node.js streams) | No (callback-based) |
| Node.js requirement | >= 22.14.0 | >= 22.14.0 |

Choose this package if you:
- Are already using KafkaJS in your project
- Need compatibility with KafkaJS plugins and ecosystem
- Prefer the KafkaJS API style
