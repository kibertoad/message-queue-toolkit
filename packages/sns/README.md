# @message-queue-toolkit/sns

AWS SNS (Simple Notification Service) implementation for the message-queue-toolkit. Provides a robust, type-safe abstraction for publishing messages to SNS topics and consuming them via SQS queue subscriptions, with support for both standard and FIFO topics.

## Table of Contents

- [Installation](#installation)
- [Features](#features)
- [Core Concepts](#core-concepts)
- [Quick Start](#quick-start)
  - [Standard Topic Publisher](#standard-topic-publisher)
  - [SNS-to-SQS Consumer](#sns-to-sqs-consumer)
  - [FIFO Topic Publisher](#fifo-topic-publisher)
  - [FIFO Topic Consumer](#fifo-topic-consumer)
- [Configuration](#configuration)
  - [Topic Creation](#topic-creation)
  - [Topic Locator](#topic-locator)
  - [Publisher Options](#publisher-options)
  - [Consumer Options](#consumer-options)
- [SNS-Specific Features](#sns-specific-features)
  - [Topic Subscriptions](#topic-subscriptions)
  - [Fan-Out Pattern](#fan-out-pattern)
  - [Message Filtering](#message-filtering)
  - [Cross-Account Publishing](#cross-account-publishing)
- [Advanced Features](#advanced-features)
- [FIFO Topics](#fifo-topics)
  - [FIFO Topic Requirements](#fifo-topic-requirements)
  - [Message Groups](#message-groups)
  - [FIFO-Specific Configuration](#fifo-specific-configuration)
- [Testing](#testing)
- [API Reference](#api-reference)

## Installation

```bash
npm install @message-queue-toolkit/sns @message-queue-toolkit/sqs @message-queue-toolkit/core
```

**Peer Dependencies:**
- `@aws-sdk/client-sns` - AWS SDK for SNS
- `@aws-sdk/client-sqs` - AWS SDK for SQS (required for consumers)
- `@aws-sdk/client-sts` - AWS SDK for STS (for ARN resolution)
- `zod` - Schema validation

## Features

- ✅ **Type-safe message handling** with Zod schema validation
- ✅ **Standard and FIFO topic support**
- ✅ **Automatic topic and subscription creation**
- ✅ **Fan-out pattern** - publish once, consume from multiple queues
- ✅ **Message filtering** - subscribe to specific message types
- ✅ **Message deduplication** (publisher and consumer level)
- ✅ **Payload offloading** for large messages (S3 integration)
- ✅ **Automatic retry logic** with exponential backoff (inherited from SQS consumer)
- ✅ **Dead Letter Queue (DLQ)** support
- ✅ **Handler spies** for testing
- ✅ **Pre-handlers and barriers** for complex message processing
- ✅ **Cross-account and cross-region publishing**

## Core Concepts

### Publishers

SNS publishers send messages to **topics** (not queues). A topic is a logical access point that acts as a communication channel. Publishers are responsible for:
- Message validation against Zod schemas
- Automatic serialization
- Optional deduplication (preventing duplicate sends)
- Optional payload offloading (for messages > 256KB)
- FIFO-specific concerns (MessageGroupId, MessageDeduplicationId)

### Consumers

SNS consumers subscribe to topics via **SQS queues** (SNS-to-SQS pattern). This provides:
- **Decoupling** - topics don't know about subscribers
- **Fan-out** - one message published to multiple queues
- **Persistence** - messages queued until processed
- **Retry logic** - inherited from SQS consumer capabilities
- **Message filtering** - only receive relevant messages

Consumers are actually `AbstractSnsSqsConsumer` which extends `AbstractSqsConsumer` from the SQS package, inheriting all SQS consumer capabilities.

### Message Flow

```text
Publisher → SNS Topic → [Subscriptions] → SQS Queues → Consumers
                            ↓
                    (optional filtering)
```

**Key Difference from SQS:**
- **SQS**: Direct queue-to-queue communication (1:1)
- **SNS**: Pub/Sub pattern with fan-out (1:N)

### Message Schemas

Messages use the same schema requirements as SQS. Each message must have:
- A unique message type field (discriminator for routing) - configurable via `messageTypeResolver` (required)
- A message ID field (for tracking and deduplication) - configurable via `messageIdField` (default: `'id'`)
- A timestamp field (added automatically if missing) - configurable via `messageTimestampField` (default: `'timestamp'`)

See the [SQS README - Message Schemas section](../sqs/README.md#message-schemas) for full details.

## Quick Start

### Standard Topic Publisher

```typescript
import { AbstractSnsPublisher } from '@message-queue-toolkit/sns'
import { SNSClient } from '@aws-sdk/client-sns'
import { STSClient } from '@aws-sdk/client-sts'
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
class UserEventsPublisher extends AbstractSnsPublisher<SupportedMessages> {
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    super(
      {
        snsClient,
        stsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
      },
      {
        messageSchemas: [UserCreatedSchema, UserUpdatedSchema],
        messageTypeResolver: { messageTypePath: 'messageType' },
        creationConfig: {
          topic: {
            Name: 'user-events-topic',
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
const snsClient = new SNSClient({ region: 'us-east-1' })
const stsClient = new STSClient({ region: 'us-east-1' })
const publisher = new UserEventsPublisher(snsClient, stsClient)

await publisher.init()

// Publish to the topic - all subscribers will receive this message
await publisher.publish({
  id: '123',
  messageType: 'user.created',
  userId: 'user-456',
  email: 'user@example.com',
})

await publisher.close()
```

### SNS-to-SQS Consumer

Consumers subscribe to SNS topics via SQS queues:

```typescript
import { AbstractSnsSqsConsumer } from '@message-queue-toolkit/sns'
import { MessageHandlerConfigBuilder } from '@message-queue-toolkit/core'
import type { Either } from '@lokalise/node-core'

type ExecutionContext = {
  userService: UserService
}

class UserEventsConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext
> {
  constructor(
    snsClient: SNSClient,
    sqsClient: SQSClient,
    stsClient: STSClient,
    userService: UserService
  ) {
    super(
      {
        snsClient,
        sqsClient,
        stsClient,
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

        // Queue configuration (SQS queue that subscribes to SNS topic)
        creationConfig: {
          queue: {
            QueueName: 'user-events-consumer-queue',
          },
          topic: {
            Name: 'user-events-topic', // Must match publisher's topic name
          },
        },

        // Subscription configuration
        subscriptionConfig: {
          updateAttributesIfExists: false,
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
const consumer = new UserEventsConsumer(snsClient, sqsClient, stsClient, userService)
await consumer.start() // Creates queue, subscribes to topic, starts consuming

// Later, to stop
await consumer.close()
```

**What happens during `start()`:**
1. Creates SNS topic (if using `creationConfig`)
2. Creates SQS queue (if using `creationConfig`)
3. Subscribes queue to topic
4. Configures queue permissions to allow SNS to publish
5. Starts consuming messages from the queue

### FIFO Topic Publisher

FIFO topics guarantee message ordering and exactly-once delivery:

```typescript
class UserEventsFifoPublisher extends AbstractSnsPublisher<SupportedMessages> {
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    super(
      {
        snsClient,
        stsClient,
        logger: console,
        errorReporter: { report: (error) => console.error(error) },
      },
      {
        messageSchemas: [UserCreatedSchema, UserUpdatedSchema],
        messageTypeResolver: { messageTypePath: 'messageType' },
        fifoTopic: true, // Enable FIFO mode

        // Option 1: Use a field from the message as MessageGroupId
        messageGroupIdField: 'userId',

        // Option 2: Use a default MessageGroupId for all messages
        // defaultMessageGroupId: 'user-events',

        creationConfig: {
          topic: {
            Name: 'user-events-topic.fifo', // Must end with .fifo
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'false', // or 'true' for automatic deduplication
            },
          },
        },
      }
    )
  }
}

// Publishing to FIFO topic
const fifoPublisher = new UserEventsFifoPublisher(snsClient, stsClient)
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

### FIFO Topic Consumer

```typescript
class UserEventsFifoConsumer extends AbstractSnsSqsConsumer<
  SupportedMessages,
  ExecutionContext
> {
  constructor(
    snsClient: SNSClient,
    sqsClient: SQSClient,
    stsClient: STSClient,
    userService: UserService
  ) {
    super(
      {
        snsClient,
        sqsClient,
        stsClient,
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
        fifoQueue: true, // Enable FIFO mode for SQS queue
        messageTypeResolver: { messageTypePath: 'messageType' },
        handlers: new MessageHandlerConfigBuilder<SupportedMessages, ExecutionContext>()
          .addConfig(UserCreatedSchema, handleUserCreated)
          .addConfig(UserUpdatedSchema, handleUserUpdated)
          .build(),

        creationConfig: {
          queue: {
            QueueName: 'user-events-consumer-queue.fifo', // Must end with .fifo
            Attributes: {
              FifoQueue: 'true',
              ContentBasedDeduplication: 'false',
            },
          },
          topic: {
            Name: 'user-events-topic.fifo', // Must match publisher's FIFO topic
            Attributes: {
              FifoTopic: 'true',
              ContentBasedDeduplication: 'false',
            },
          },
        },

        subscriptionConfig: {
          updateAttributesIfExists: false,
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

### Topic Creation

When using `creationConfig`, the topic will be created automatically if it doesn't exist:

```typescript
{
  creationConfig: {
    topic: {
      Name: 'my-topic',
      Attributes: {
        // Standard Topic attributes
        DisplayName: 'My Notification Topic',

        // FIFO Topic attributes (only for .fifo topics)
        FifoTopic: 'true',                     // Must be 'true' for FIFO topics
        ContentBasedDeduplication: 'false',    // Automatic deduplication based on message body

        // Encryption
        KmsMasterKeyId: 'alias/aws/sns',       // KMS key for encryption

        // Delivery policy
        DeliveryPolicy: JSON.stringify({
          healthyRetryPolicy: {
            minDelayTarget: 20,
            maxDelayTarget: 20,
            numRetries: 3,
            numMaxDelayRetries: 0,
            numNoDelayRetries: 0,
            numMinDelayRetries: 0,
            backoffFunction: 'linear'
          }
        }),
      },
      Tags: [
        { Key: 'Environment', Value: 'production' },
        { Key: 'Team', Value: 'backend' },
      ],
    },

    queue: {
      QueueName: 'my-consumer-queue',
      // See SQS README for full queue configuration options
    },

    updateAttributesIfExists: true,  // Update attributes if topic/queue exists
    forceTagUpdate: false,           // Force tag update even if unchanged

    // Queue permissions for SNS publishing (automatically configured)
    queueUrlsWithSubscribePermissionsPrefix: 'https://sqs.us-east-1.amazonaws.com/123456789012/',
  },
}
```

### Topic Locator

When using `locatorConfig`, you connect to an existing topic without creating it:

```typescript
{
  locatorConfig: {
    // Option 1: By topic ARN
    topicArn: 'arn:aws:sns:us-east-1:123456789012:my-topic',

    // Option 2: By topic name (ARN will be resolved using STS)
    // topicName: 'my-topic',

    // Optional: Existing queue URL or name
    queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789012/my-queue',
    // or
    // queueName: 'my-queue',

    // Optional: Existing subscription ARN
    subscriptionArn: 'arn:aws:sns:us-east-1:123456789012:my-topic:uuid',
  },
}
```

### Publisher Options

```typescript
{
  // Required - Message Schema Configuration
  messageSchemas: [Schema1, Schema2],  // Array of Zod schemas
  messageTypeResolver: { messageTypePath: 'messageType' },     // Field containing message type discriminator

  // Topic Configuration (one of these required)
  creationConfig: { /* ... */ },       // Create topic if doesn't exist
  locatorConfig: { /* ... */ },        // Use existing topic

  // Optional - FIFO Configuration
  fifoTopic: false,                    // Set to true for FIFO topics
  messageGroupIdField: 'userId',       // Field to use as MessageGroupId
  defaultMessageGroupId: 'default',    // Default MessageGroupId if field not present

  // Optional - Message Field Configuration (same as SQS)
  messageIdField: 'id',                       // Default: 'id'
  messageTimestampField: 'timestamp',         // Default: 'timestamp'
  messageDeduplicationIdField: 'deduplicationId',     // Default: 'deduplicationId'
  messageDeduplicationOptionsField: 'deduplicationOptions', // Default: 'deduplicationOptions'

  // Optional - Features
  logMessages: false,                  // Log all published messages
  handlerSpy: true,                    // Enable handler spy for testing

  // Optional - Deduplication (same as SQS)
  enablePublisherDeduplication: false,
  messageDeduplicationConfig: { /* ... */ },

  // Optional - Payload Offloading (same as SQS)
  payloadStoreConfig: { /* ... */ },

  // Optional - Deletion
  deletionConfig: { /* ... */ },
}
```

See the [SQS README - Publisher Options section](../sqs/README.md#publisher-options) for full details on shared options.

### Consumer Options

SNS consumers use the same options as SQS consumers, plus SNS-specific subscription configuration:

```typescript
{
  // All SQS consumer options are supported
  // See SQS README for full consumer options

  // Required - Message Handling Configuration
  handlers: MessageHandlerConfigBuilder.build(),
  messageTypeResolver: { messageTypePath: 'messageType' },

  // Topic & Queue Configuration
  creationConfig: {
    topic: { /* SNS topic config */ },
    queue: { /* SQS queue config */ },
  },
  // or
  locatorConfig: {
    topicArn: 'arn:aws:sns:...',
    queueUrl: 'https://sqs...',
    subscriptionArn: 'arn:aws:sns:...',
  },

  // SNS-Specific - Subscription Configuration
  subscriptionConfig: {
    updateAttributesIfExists: false,  // Update subscription attributes if exists

    // Optional: Message filtering
    filterPolicy: {
      messageType: ['user.created', 'user.updated'],  // Only receive these types
    },

    // Optional: Raw message delivery (disable SNS envelope)
    rawMessageDelivery: false,

    // Optional: Redrive policy (DLQ for undeliverable messages)
    redrivePolicy: {
      deadLetterTargetArn: 'arn:aws:sqs:us-east-1:123456789012:my-dlq',
    },
  },

  // Optional - FIFO Configuration
  fifoQueue: false,

  // Optional - Other options inherited from SQS
  concurrentConsumersAmount: 1,
  maxRetryDuration: 345600,  // 4 days
  deadLetterQueue: { /* ... */ },
  consumerOverrides: { /* ... */ },
  // ... see SQS README for full list
}
```

See the [SQS README - Consumer Options section](../sqs/README.md#consumer-options) for full details on shared options.

## SNS-Specific Features

### Topic Subscriptions

SNS topics can have multiple subscribers. Each subscriber receives a copy of every message published to the topic:

```typescript
// Publisher publishes to one topic
class NotificationPublisher extends AbstractSnsPublisher<NotificationMessage> {
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    super(dependencies, {
      messageSchemas: [NotificationSchema],
      creationConfig: {
        topic: { Name: 'notifications' },
      },
    })
  }
}

// Multiple consumers subscribe to the same topic
class EmailConsumer extends AbstractSnsSqsConsumer<NotificationMessage, EmailContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(sendEmail),
      creationConfig: {
        queue: { QueueName: 'email-notifications' },
        topic: { Name: 'notifications' }, // Same topic
      },
    })
  }
}

class SMSConsumer extends AbstractSnsSqsConsumer<NotificationMessage, SMSContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(sendSMS),
      creationConfig: {
        queue: { QueueName: 'sms-notifications' },
        topic: { Name: 'notifications' }, // Same topic
      },
    })
  }
}

class PushConsumer extends AbstractSnsSqsConsumer<NotificationMessage, PushContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(sendPush),
      creationConfig: {
        queue: { QueueName: 'push-notifications' },
        topic: { Name: 'notifications' }, // Same topic
      },
    })
  }
}

// One publish reaches all three consumers
await publisher.publish({
  id: '123',
  messageType: 'notification.created',
  userId: 'user-456',
  message: 'Your order has shipped!',
})
// → Email sent
// → SMS sent
// → Push notification sent
```

### Fan-Out Pattern

The fan-out pattern enables broadcasting messages to multiple independent processing pipelines:

```typescript
// Single event publisher
class OrderEventPublisher extends AbstractSnsPublisher<OrderEvent> {
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    super(dependencies, {
      messageSchemas: [OrderCreatedSchema, OrderUpdatedSchema],
      creationConfig: {
        topic: { Name: 'order-events' },
      },
    })
  }
}

// Different services consume the same events independently

// Inventory service - updates stock levels
class InventoryConsumer extends AbstractSnsSqsConsumer<OrderEvent, InventoryContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildInventoryHandlers(),
      creationConfig: {
        queue: { QueueName: 'inventory-order-events' },
        topic: { Name: 'order-events' },
      },
    })
  }
}

// Analytics service - tracks order metrics
class AnalyticsConsumer extends AbstractSnsSqsConsumer<OrderEvent, AnalyticsContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildAnalyticsHandlers(),
      creationConfig: {
        queue: { QueueName: 'analytics-order-events' },
        topic: { Name: 'order-events' },
      },
    })
  }
}

// Shipping service - prepares shipments
class ShippingConsumer extends AbstractSnsSqsConsumer<OrderEvent, ShippingContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildShippingHandlers(),
      creationConfig: {
        queue: { QueueName: 'shipping-order-events' },
        topic: { Name: 'order-events' },
      },
    })
  }
}

// Benefits:
// - Each service processes independently
// - Failure in one doesn't affect others
// - Easy to add new consumers without changing publisher
// - Each consumer can have its own retry/DLQ configuration
```

### Message Filtering

Subscribers can filter messages to receive only specific types:

```typescript
class UserCreatedConsumer extends AbstractSnsSqsConsumer<UserEvent, UserContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(),
      creationConfig: {
        queue: { QueueName: 'user-created-processor' },
        topic: { Name: 'user-events' },
      },
      subscriptionConfig: {
        // Only receive user.created events
        filterPolicy: {
          messageType: ['user.created'],
        },
      },
    })
  }
}

class UserModificationConsumer extends AbstractSnsSqsConsumer<UserEvent, UserContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(),
      creationConfig: {
        queue: { QueueName: 'user-modification-processor' },
        topic: { Name: 'user-events' },
      },
      subscriptionConfig: {
        // Only receive update and delete events
        filterPolicy: {
          messageType: ['user.updated', 'user.deleted'],
        },
      },
    })
  }
}

// Advanced filtering with message attributes
class HighPriorityConsumer extends AbstractSnsSqsConsumer<OrderEvent, OrderContext> {
  constructor(deps) {
    super(deps, {
      handlers: buildHandlers(),
      creationConfig: {
        queue: { QueueName: 'high-priority-orders' },
        topic: { Name: 'order-events' },
      },
      subscriptionConfig: {
        filterPolicy: {
          messageType: ['order.created'],
          priority: ['high', 'critical'],  // Filters on message attribute
        },
      },
    })
  }
}
```

**Benefits:**
- Reduces unnecessary message processing
- Lowers SQS costs (fewer messages received)
- Filtering happens at SNS level (before queuing)
- Each subscriber can have different filters

### Cross-Account Publishing

SNS supports publishing from one AWS account to topics in another:

```typescript
// Account A - Publisher
class CrossAccountPublisher extends AbstractSnsPublisher<Message> {
  constructor(snsClient: SNSClient, stsClient: STSClient) {
    super(dependencies, {
      messageSchemas: [MessageSchema],
      locatorConfig: {
        // Topic in Account B
        topicArn: 'arn:aws:sns:us-east-1:222222222222:shared-topic',
      },
    })
  }
}

// Account B - Consumer (topic owner)
// Topic policy must allow Account A to publish:
{
  creationConfig: {
    topic: {
      Name: 'shared-topic',
      Attributes: {
        Policy: JSON.stringify({
          Version: '2012-10-17',
          Statement: [
            {
              Effect: 'Allow',
              Principal: {
                AWS: 'arn:aws:iam::111111111111:root', // Account A
              },
              Action: 'SNS:Publish',
              Resource: 'arn:aws:sns:us-east-1:222222222222:shared-topic',
            },
          ],
        }),
      },
    },
  },
}
```

## Advanced Features

SNS consumers inherit all advanced features from SQS consumers. See the SQS README for detailed documentation on:

- **[Custom Message Field Names](../sqs/README.md#custom-message-field-names)** - Adapt to existing schemas
- **[Dead Letter Queue (DLQ)](../sqs/README.md#dead-letter-queue-dlq)** - Handle permanently failing messages
- **[Message Retry Logic](../sqs/README.md#message-retry-logic)** - Exponential backoff and retry limits
- **[Message Deduplication](../sqs/README.md#message-deduplication)** - Publisher and consumer-level deduplication
- **[Payload Offloading](../sqs/README.md#payload-offloading)** - S3 storage for large messages
- **[Message Handlers](../sqs/README.md#message-handlers)** - Type-safe handler configuration
- **[Pre-handlers and Barriers](../sqs/README.md#pre-handlers-and-barriers)** - Middleware and message dependencies
- **[Handler Spies](../sqs/README.md#handler-spies)** - Testing async message flows

All these features work identically for SNS consumers since they extend the SQS consumer implementation.

## FIFO Topics

FIFO (First-In-First-Out) topics provide message ordering and exactly-once delivery, similar to FIFO queues.

### FIFO Topic Requirements

1. **Topic name must end with `.fifo`**
   ```typescript
   Name: 'my-topic.fifo'  // ✅ Valid
   Name: 'my-topic'       // ❌ Invalid for FIFO
   ```

2. **FifoTopic attribute must be 'true'**
   ```typescript
   Attributes: {
     FifoTopic: 'true',
   }
   ```

3. **Subscribed queues must also be FIFO**
   ```typescript
   creationConfig: {
     topic: {
       Name: 'events.fifo',
       Attributes: { FifoTopic: 'true' },
     },
     queue: {
       QueueName: 'consumer.fifo',  // Must be FIFO
       Attributes: { FifoQueue: 'true' },
     },
   }
   ```

4. **MessageGroupId required for all messages**
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

5. **DLQ must also be FIFO**
   ```typescript
   deadLetterQueue: {
     creationConfig: {
       queue: {
         QueueName: 'my-dlq.fifo',  // Must be FIFO
         Attributes: { FifoQueue: 'true' },
       },
     },
   }
   ```

### Message Groups

Message groups work the same way as in FIFO queues. See the [SQS README - Message Groups section](../sqs/README.md#message-groups) for detailed information on:
- Group assignment and parallelism
- Best practices for group design
- Balancing group sizes
- Sizing concurrent consumers

**SNS FIFO Fan-Out Example:**

```typescript
// FIFO publisher publishes to FIFO topic
const fifoPublisher = new OrderEventsFifoPublisher(snsClient, stsClient)
await fifoPublisher.publish({
  id: '123',
  messageType: 'order.created',
  customerId: 'customer-A',
  orderId: 'order-1',
}, {
  MessageGroupId: 'customer-A',  // All messages for customer-A ordered
})

// Multiple FIFO consumers subscribe to the same FIFO topic
const inventoryConsumer = new InventoryFifoConsumer(deps)
const analyticsConsumer = new AnalyticsFifoConsumer(deps)
const shippingConsumer = new ShippingFifoConsumer(deps)

// Each consumer receives messages in order within each group
// Different consumers can process different groups in parallel
```

### FIFO-Specific Configuration

```typescript
// Publisher
{
  fifoTopic: true,

  // Choose one or more:
  messageGroupIdField: 'userId',        // Use field from message
  defaultMessageGroupId: 'default',     // Fallback value
  // or provide in publish call
}

// Consumer
{
  fifoQueue: true,
  concurrentConsumersAmount: 3,  // Process 3 groups in parallel

  // Note: Retry behavior inherits from SQS FIFO queues
  // - No DelaySeconds support (AWS limitation)
  // - Messages retry immediately
  // - Order is preserved
}

// Topic Configuration
{
  creationConfig: {
    topic: {
      Name: 'my-topic.fifo',
      Attributes: {
        FifoTopic: 'true',

        // Optional: Automatic deduplication based on message body
        ContentBasedDeduplication: 'false',  // or 'true'
      },
    },
  },
}

// Queue Configuration
{
  creationConfig: {
    queue: {
      QueueName: 'my-queue.fifo',
      Attributes: {
        FifoQueue: 'true',

        // Optional: Queue-level deduplication settings
        ContentBasedDeduplication: 'false',
        DeduplicationScope: 'queue',  // or 'messageGroup'
        FifoThroughputLimit: 'perQueue',  // or 'perMessageGroupId'
      },
    },
  },
}
```

**FIFO Limitations:**
- **Throughput**: 3,000 messages/second per topic (or 300/second per message group)
- **No delays**: FIFO queues don't support `DelaySeconds`
- **Strict ordering**: Within a message group, messages are delivered in exact order
- **FIFO-to-FIFO only**: FIFO topics can only fan out to FIFO queues

See the [SQS README - FIFO Queues section](../sqs/README.md#fifo-queues) for comprehensive FIFO documentation.

## Testing

SNS testing works the same as SQS testing. Handler spies enable testing of async pub/sub flows:

```typescript
import { describe, it, expect, beforeEach, afterEach } from 'vitest'

describe('SNS Publisher and Consumer', () => {
  let publisher: UserEventsPublisher
  let consumer: UserEventsConsumer

  beforeEach(async () => {
    publisher = new UserEventsPublisher(snsClient, stsClient, { handlerSpy: true })
    consumer = new UserEventsConsumer(snsClient, sqsClient, stsClient, userService, {
      handlerSpy: true
    })

    await publisher.init()
    await consumer.start()
  })

  afterEach(async () => {
    await consumer.close()
    await publisher.close()
  })

  it('publishes to topic and consumes from subscribed queue', async () => {
    // Publish to SNS topic
    await publisher.publish({
      id: '123',
      messageType: 'user.created',
      userId: 'user-456',
      email: 'test@example.com',
    })

    // Wait for publisher spy
    await publisher.handlerSpy.waitForMessageWithId('123', 'published')

    // Wait for consumer spy
    const consumedMessage = await consumer.handlerSpy.waitForMessageWithId('123', 'consumed')

    expect(consumedMessage.userId).toBe('user-456')
    expect(userService.createUser).toHaveBeenCalledWith('user-456', 'test@example.com')
  })
})
```

See the [SQS README - Testing section](../sqs/README.md#testing) for comprehensive testing documentation including:
- Integration tests with LocalStack
- Unit tests with handler spies
- Testing indirect message publishing
- Complex workflow testing

## API Reference

### AbstractSnsPublisher

```typescript
class AbstractSnsPublisher<MessagePayloadType extends object> {
  constructor(
    dependencies: SNSDependencies,
    options: SNSPublisherOptions<MessagePayloadType>
  )

  async init(): Promise<void>
  async close(): Promise<void>

  async publish(
    message: MessagePayloadType,
    options?: SNSMessageOptions
  ): Promise<void>

  readonly handlerSpy: HandlerSpy<MessagePayloadType>
  readonly topicArn: string
}
```

### AbstractSnsSqsConsumer

```typescript
class AbstractSnsSqsConsumer<
  MessagePayloadType extends object,
  ExecutionContext,
  PrehandlerOutput = undefined
> extends AbstractSqsConsumer<...> {
  constructor(
    dependencies: SNSSQSConsumerDependencies,
    options: SNSSQSConsumerOptions<MessagePayloadType, ExecutionContext, PrehandlerOutput>,
    executionContext: ExecutionContext
  )

  async init(): Promise<void>
  async start(): Promise<void>
  async close(abort?: boolean): Promise<void>

  readonly handlerSpy: HandlerSpy<MessagePayloadType>
  readonly topicArn: string
  readonly subscriptionArn: string
  readonly queueUrl: string
  readonly queueName: string
}
```

### Types

```typescript
// Message options for publishing
type SNSMessageOptions = {
  MessageGroupId?: string         // Required for FIFO topics
  MessageDeduplicationId?: string // Optional for FIFO topics
}

// Dependencies
type SNSDependencies = {
  snsClient: SNSClient
  stsClient: STSClient
  logger: Logger
  errorReporter: ErrorReporter
  messageMetricsManager?: MessageMetricsManager
}

// Consumer dependencies (extends SNSDependencies and SQSDependencies)
type SNSSQSConsumerDependencies = SNSDependencies & SQSDependencies & {
  consumerErrorResolver: ErrorResolver
  transactionObservabilityManager: TransactionObservabilityManager
}

// Subscription options
type SNSSubscriptionOptions = {
  updateAttributesIfExists?: boolean
  filterPolicy?: Record<string, string[]>
  rawMessageDelivery?: boolean
  redrivePolicy?: {
    deadLetterTargetArn: string
  }
}
```

### Utility Functions

```typescript
// Topic validation
function isFifoTopicName(topicName: string): boolean
function validateFifoTopicName(topicName: string, isFifoTopic: boolean): void

// Topic operations
async function assertTopic(
  snsClient: SNSClient,
  stsClient: STSClient,
  topicOptions: CreateTopicCommandInput,
  extraParams?: ExtraSNSCreationParams
): Promise<string> // Returns topicArn

async function deleteTopic(
  snsClient: SNSClient,
  stsClient: STSClient,
  topicName: string
): Promise<void>

async function getTopicAttributes(
  snsClient: SNSClient,
  topicArn: string
): Promise<Either<'not_found', { attributes?: Record<string, string> }>>

// Subscription operations
async function subscribeToTopic(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string,
  options?: SNSSubscriptionOptions
): Promise<string> // Returns subscriptionArn

async function findSubscriptionByTopicAndQueue(
  snsClient: SNSClient,
  topicArn: string,
  queueArn: string
): Promise<Subscription | undefined>

// Message reading
function deserializeSNSMessage(
  message: SQSMessage
): Either<MessageInvalidFormatError, SNSMessageBody>

// Message size calculation (same as SQS)
function calculateOutgoingMessageSize(message: unknown): number
```

## License

MIT

## Contributing

Contributions are welcome! Please see the main repository for guidelines.

## Links

- [Main Repository](https://github.com/kibertoad/message-queue-toolkit)
- [Core Package](https://www.npmjs.com/package/@message-queue-toolkit/core)
- [SQS Package](https://www.npmjs.com/package/@message-queue-toolkit/sqs) - SNS consumers extend SQS consumers
- [AWS SNS Documentation](https://docs.aws.amazon.com/sns/)
- [FIFO Topic Documentation](https://docs.aws.amazon.com/sns/latest/dg/sns-fifo-topics.html)
- [SNS Message Filtering](https://docs.aws.amazon.com/sns/latest/dg/sns-message-filtering.html)
