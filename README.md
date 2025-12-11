# message-queue-toolkit ✉️

[![NPM Version](https://img.shields.io/npm/v/@message-queue-toolkit/core.svg)](https://www.npmjs.com/package/@message-queue-toolkit/core)
[![NPM Downloads](https://img.shields.io/npm/dm/@message-queue-toolkit/core.svg)](https://npmjs.org/package/@message-queue-toolkit/core)
[![Build Status](https://github.com/kibertoad/message-queue-toolkit/workflows/ci/badge.svg)](https://github.com/kibertoad/message-queue-toolkit/actions)

Useful utilities, interfaces and base classes for message queue handling.

## Overview

`message-queue-toolkit ` is an abstraction over several different queue systems, which implements common deserialization, validation and error handling logic. The library provides utilities, interfaces and base classes to build the support for any queue system you may need in your service.

It consists of the following submodules:

* `@message-queue-toolkit/core` - core library. It needs to be installed regardless of which queue system you are using.
* `@message-queue-toolkit/amqp` - AMQP 0-9-1 (Advanced Message Queuing Protocol), used e. g. by RabbitMQ
* `@message-queue-toolkit/sqs` - SQS (AWS Simple Queue Service)
* `@message-queue-toolkit/sns` - SNS (AWS Simple Notification Service)
* `@message-queue-toolkit/kafka` - Kafka (in development)

## Basic Usage

### Publishers

`message-queue-toolkit` provides base classes for implementing publishers for each of the supported protocols.
They implement the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `messageSchemas` – the `zod` schemas for all supported messages;
        * `messageTimestampField` - which field in the message contains the message creation date (by default it is `timestamp`). This field needs to be a `Date` object or ISO-8601 date string, if your message doesn't contain it the library will add one automatically to avoid infinite loops on consumer;
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for resolving the correct schema for validation. **Note:** It is not supported for Kafka publisher
        * `messageTypeResolver` - alternative to `messageTypeField` for flexible message type resolution. Supports three modes:
            * `{ messageTypePath: 'type' }` - extract type from a field (equivalent to `messageTypeField`)
            * `{ literal: 'my.message.type' }` - use a constant type for all messages
            * `{ resolver: ({ messageData, messageAttributes }) => 'resolved.type' }` - custom resolver function
          See `@message-queue-toolkit/core` README for detailed documentation and examples.
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`;
        * `policyConfig` - SQS only - configuration for queue access policies (see [SQS Policy Configuration](#sqs-policy-configuration) for more information);
        * `deletionConfig` - automatic cleanup of resources;
        * `handlerSpy` - allow awaiting certain messages to be published (see [Handler Spies](#handler-spies) for more information);
        * `logMessages` - add logs for processed messages.
        * `payloadStoreConfig` - configuration for payload offloading. This option enables the external storage of large message payloads to comply with message size limitations of the queue system. For more details on setting this up, see [Payload Offloading](#payload-offloading).
        * `messageDeduplicationConfig` - configuration for store-based message deduplication on publisher level. For more details on setting this up, see [Publisher-level store-based-message deduplication](#publisher-level-store-based-message-deduplication).
        * `enablePublisherDeduplication` - enable store-based publisher-level deduplication. For more details on setting this up, see [Publisher-level store-based-message deduplication](#publisher-level-store-based-message-deduplication).
        * `messageDeduplicationIdField` - which field in the message contains the deduplication id (by default it is `deduplicationId`). This field needs to be defined as `z.string` in the schema. For more details on setting this up, see [Publisher-level store-based-message deduplication](#publisher-level-store-based-message-deduplication).
        * `messageDeduplicationOptionsField` - which field in the message contains the deduplication options (by default it is `deduplicationOptions`). This field needs to have the below stricture.
            * `deduplicationWindowSeconds` - how many seconds the deduplication key should be kept in the store, i.e. how much time message should be prevented from being published again. This fields needs to be integer greater than 0. Default is 40 seconds.
* `init()`, prepare publisher for use (e. g. establish all necessary connections);
* `close()`, stop publisher use (e. g. disconnect);
* `publish()`, send a message to a queue or topic. It accepts the following parameters:
    * `message` – a message following one of the `zod` schemas, supported by the publisher;
    * `options` – a protocol-dependent set of message parameters. For more information please check documentation for options for each protocol: [AMQP](https://amqp-node.github.io/amqplib/channel_api.html#channel_sendToQueue), [SQS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/interfaces/sendmessagecommandinput.html) and [SNS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sns/interfaces/publishcommandinput.html).

> **_NOTE:_**  See [SqsPermissionPublisher.ts](./packages/sqs/test/publishers/SqsPermissionPublisher.ts) for a practical example.

#### PublisherManager

PublisherManager is a wrapper to automatically spawn publishers on demand and reduce the amount of boilerplate needed to write them, while guaranteeing type-safety and autocompletion. It also automatically fills metadata fields.

When instantiating the PublisherManager, you can define the following arguments:
* `publisherFactory`;
* `newPublisherOptions`;
* `publisherDependencies`;
* `metadataFiller`;
* `eventRegistry`;
* `metadataField`;
* `isAsync`.

Once your PublisherManager is ready, you have access to the following methods:

* `publish()` (SNS), sends a message to a topic. Attempts to publish to a non-existing topic will throw an error. It accepts the following parameters:
  * `topic` – the topic to publish to;
  * `message` – the message to be published;
  * `precedingEventMetadata` – (optional) ;
  * `messageOptions` – (optional) a protocol-dependent set of message parameters. For more information please check documentation for options for [AMQP](https://amqp-node.github.io/amqplib/channel_api.html#channel_sendToQueue).
* `publishSync()` (AMQP), sends a message to a queue or topic. Attempts to publish to  It accepts the following parameters:
  * `queue` or `topic` – the queue or topic to publish to;
  * `message` – the message to be published;
  * `precedingEventMetadata` – (optional) additional metadata that describes the context in which the message was created. If not provided, each field will resolve to its default value. Available fields are:
    * `schemaVersion` – message schema version, defaults to 1.0.0;
    * `originatedFrom` – service that initiated the entire workflow that led to this message, defaults to the service ID provided in `CommonMetadataFiller`;
    * `correlationId` – unique identifier passed to all events in the workflow chain, defaults to a randomly generated UUID.
  * `messageOptions` – (optional) a protocol-dependent set of message parameters. For more information please check documentation for options for [SNS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sns/interfaces/publishcommandinput.html).
* `handlerSpy()` – spy feature that allows waiting for the moment a particular event gets published (see [Handler Spies](#handler-spies) for more information);
* `injectPublisher()` – injects a new publisher for a given event;
* `injectEventDefinition()` – injects a new event definition.

### Consumers

`message-queue-toolkit` provides base classes for implementing consumers for each of the supported protocols.
They expose the following public methods:

Multi-schema consumers support multiple message types via handler configs. They expose the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `handlers` – configuration for handling each of the supported message types. See "Multi-schema handler definition" for more details;
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for routing the message to the correct handler; **Note:** It is not supported for Kafka consumer
        * `messageTypeResolver` - alternative to `messageTypeField` for flexible message type resolution. See Publishers section above for details.
        * `messageTimestampField` - which field in the message contains the message creation date (by default it is `timestamp`). This field needs to be a `Date` object or an ISO-8601 date string;
        * `maxRetryDuration` - how long (in seconds) the message should be retried due to the `retryLater` result before marking it as consumed (and sending to DLQ, if one is configured). This is used to avoid infinite loops. Default is 4 days;
        * `queueName`; (for SNS publishers this is a misnomer which actually refers to a topic name)
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
        * `subscriptionConfig` - SNS SQS consumer only - configuration for SNS -> SQS subscription to create, if one doesn't exist.
        * `policyConfig` - SQS only - configuration for queue access policies (see [SQS Policy Configuration](#sqs-policy-configuration) for more information);
        * `deletionConfig` - automatic cleanup of resources;
        * `consumerOverrides` – available only for SQS consumers;
        * `deadLetterQueue` - available only for SQS and SNS consumers (see [Dead Letter Queue](#dead-letter-queue) for more information);
        * `handlerSpy` - allow awaiting certain messages to be published (see [Handler Spies](#handler-spies) for more information);
        * `logMessages` - add logs for processed messages.
        * `payloadStoreConfig` - configuration for payload offloading. This option enables the external storage of large message payloads to comply with message size limitations of the queue system. For more details on setting this up, see [Payload Offloading](#payload-offloading).
        * `concurrentConsumersAmount` - configuration for specifying the number of concurrent consumers to create. Available only for SQS and SNS consumers
        * `messageDeduplicationConfig` - configuration for store-based message deduplication on consumer level. For more details on setting this up, see [Consumer-level store-based-message deduplication](#consumer-level-store-based-message-deduplication).
        * `enableConsumerDeduplication` - enable store-based consumer-level deduplication. For more details on setting this up, see [Consumer-level store-based-message deduplication](#consumer-level-store-based-message-deduplication).
        * `messageDeduplicationIdField` - which field in the message contains the deduplication id (by default it is `deduplicationId`). This field needs to be defined as `z.string` in the schema. For more details on setting this up, see [Consumer-level store-based-message deduplication](#consumer-level-store-based-message-deduplication).
        * `messageDeduplicationOptionsField` - which field in the message contains the deduplication options (by default it is `deduplicationOptions`). This field needs to have the below structure.
            * `deduplicationWindowSeconds` - how many seconds the deduplication key should be kept in the store, i.e. how much time message should be prevented from being processed again after it was successfully consumed. This fields needs to be integer greater than 0. Default is 40 seconds.
            * `lockTimeoutSeconds` - how many seconds the lock should be kept in the store, i.e. how much time message should be prevented from being processed by another consumer. If consumer doesn't crash, the lock is being constantly updated to prevent other consumers from processing the message. This fields needs to be integer greater than 0. Default is 20 seconds.
            * `acquireTimeoutSeconds` - how many seconds at most the consumer should wait for the lock to be acquired. If the lock is not acquired within this time, the message will be re-queued. This field needs to be integer greater than 0. Default is 20 seconds.
            * `refreshIntervalSeconds` - how often the lock should be refreshed. This field needs to be numeric greater than 0 (fractional values are allowed). Default is 10 second.
* `init()`, prepare consumer for use (e. g. establish all necessary connections);
* `close()`, stop listening for messages and disconnect;
* `start()`, which invokes `init()`.

> **_NOTE:_**  See [SqsPermissionConsumer.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.ts) for a practical example.


#### Handlers

You can define handlers for each of the supported messages in a type-safe way using the MessageHandlerConfigBuilder.

Here is an example:

```typescript
type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
    userService: UserService
}

export class SqsPermissionConsumer extends AbstractSqsConsumer<
    SupportedMessages,
    ExecutionContext,
    PreHandlerContext
> {
    constructor(
        dependencies: SQSConsumerDependencies,
        userService: UserService,
    ) {
        super(dependencies, {
            //
            // rest of configuration skipped
            //
            handlers: new MessageHandlerConfigBuilder<
                SupportedMessages,
                ExecutionContext,
                PreHandlerContext
            >()
                .addConfig(
                    PERMISSIONS_ADD_MESSAGE_SCHEMA,
                    async (message, context) => {
                        const users = await context.userService.getUsers()
                        //
                        // execute some domain logic here
                        //
                        return {
                            result: 'success',
                        }
                    },
                    {
                        preHandlerBarrier: async (message) => {
                            // do barrier check here
                            return true
                        }
                    },
                )
                .addConfig(PERMISSIONS_REMOVE_MESSAGE_SCHEMA, 
                    async (message, context) => {
                        const users = await context.userService.getUsers()
                        //
                        // execute some domain logic here
                        //
                      return {
                          result: 'success',
                      }
                })
                .build(),
        }, {
            userService,
        })
    }
}
```

#### Error Handling

When implementing a handler, you are expected to return an instance of `Either`, containing either an error `retryLater`, or result `success`. In case of `retryLater`, the abstract consumer is instructed to requeue the message. Otherwise, in case of success, the message is finally removed from the queue. If an error is thrown while processing the message, the abstract consumer will also requeue the message.

#### Schema Validation and Deserialization

Message deserialization is done within the abstract consumer _before_ processing the message. Deserialization is done for a message, given a message schema type. `zod` is the library used to declare and validate the message schema type.

If
* The message before deserialization is `null`
* Deserialization fails because of a syntax error or a validation error
* Deserialization returns an empty value

Then the message is automatically nacked without requeueing by the abstract consumer and processing fails.

> **_NOTE:_**  See [userConsumerSchemas.ts](./packages/sqs/test/consumers/userConsumerSchemas.ts) and [SqsPermissionsConsumer.spec.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.spec.ts) for a practical example.

### Barrier Pattern
The barrier pattern facilitates the out-of-order message handling by retrying the message later if the system is not yet in the proper state to be able to process that message (e. g. some prerequisite messages have not yet arrived).

To enable this pattern you should define `preHandlerBarrier` on your message handler in order to define the conditions for starting to process the message.
If the barrier method returns `false`, message will be returned into the queue for the later processing. If the barrier method returns `true`, message will be processed.

> **_NOTE:_**  See [SqsPermissionConsumer.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumer.ts) for a practical example.

### Dead Letter Queue
> **_NOTE:_**  DLQ is only available on SQS and SNS consumers, so the following is only relevant there.
    If you try to use it on AMQP at this point you will receive an error, AMQP support will come in a future version.

A dead letter queue is a queue where messages that cannot be processed successfully are stored. It serves as a holding area for messages that have encountered errors or exceptions during processing, allowing developers to review and handle them later.
To create a dead letter queue, you need to specify the `deadLetterQueue` parameter within options on the consumer configuration. The parameter should contain the following fields:
- `creationConfig`: configuration for the queue to create, if one does not exist. Should not be specified together with the `locatorConfig`
- `locatorConfig`: configuration for resolving existing queue. Should not be specified together with the `creationConfig`
- `redrivePolicy`: an object that contains the following fields:
  - `maxReceiveCount`: the number of times a message can be received before being moved to the DLQ.

> **_NOTE:_**  if a message is stuck returning retryLater, it will be moved to the DLQ after the `maxRetryDuration` is reached.

## Fan-out to Multiple Consumers

SQS queues are built in a way that every message is only consumed once, and then deleted. If you want to do fan-out to multiple consumers, you need SNS topic in the middle, which is then propagated to all the SQS queues that have subscribed.

> **_NOTE:_**  See [SnsPermissionPublisher.ts](./packages/sns/test/publishers/SnsPermissionPublisher.ts) and [SnsSqsPermissionConsumerMonoSchema.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumer.ts) for a practical example.

## Automatic Queue and Topic Creation

Both publishers and consumers accept a queue name and configuration as parameters. If the referenced queue (or SNS topic) does not exist at the moment the publisher or the consumer is instantiated, it is automatically created. Similarly, if the referenced topic does not exist during instantiation, it is also automatically created.

If you do not want to create a new queue/topic, you can set `queueLocator` field for `queueConfiguration`. In that case `message-queue-toolkit` will not attempt to create a new queue or topic, and instead throw an error if they don't already exist.

## SQS Policy Configuration

SQS queues can be configured with access policies to control who can send messages to and receive messages from the queue. The `policyConfig` parameter allows you to define these policies when creating or updating SQS queues.

### Policy Configuration Options

The `policyConfig` parameter accepts the following structure:

```typescript
type SQSPolicyConfig = {
  resource: string | typeof SQS_RESOURCE_ANY | typeof SQS_RESOURCE_CURRENT_QUEUE
  statements?: SQSPolicyStatement | SQSPolicyStatement[]
}

type SQSPolicyStatement = {
  Effect?: string        // "Allow" or "Deny" (default: "Allow")
  Principal?: string    // AWS principal ARN (default: "*")
  Action?: string[]     // SQS actions (default: ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"])
}
```

### Resource Types

The `resource` field supports three types of values:

- **`SQS_RESOURCE_CURRENT_QUEUE`**: Uses the ARN of the current queue being created
- **`SQS_RESOURCE_ANY`**: Uses the wildcard ARN `arn:aws:sqs:*:*:*`
- **Custom ARN**: Any specific SQS queue ARN (e.g., `arn:aws:sqs:us-east-1:123456789012:my-queue`)

### Usage Examples

#### Basic Policy Configuration

```typescript
import { SQS_RESOURCE_ANY } from "@message-queue-toolkit/sqs"

const publisher = new SqsPermissionPublisher(dependencies, {
  creationConfig: {
    queue: {
      QueueName: "my-queue",
      Attributes: {
        VisibilityTimeout: "30",
      },
    },
    policyConfig: {
      resource: SQS_RESOURCE_ANY,
      statements: {
        Effect: "Allow",
        Principal: "arn:aws:iam::123456789012:user/my-user",
        Action: ["sqs:SendMessage", "sqs:ReceiveMessage"],
      },
    },
  },
})
```

#### Multiple Policy Statements

```typescript
const consumer = new SqsPermissionConsumer(dependencies, {
  creationConfig: {
    queue: {
      QueueName: "my-queue",
    },
    policyConfig: {
      resource: SQS_RESOURCE_CURRENT_QUEUE,
      statements: [
        {
          Effect: "Allow",
          Principal: "arn:aws:iam::123456789012:role/my-role",
          Action: ["sqs:SendMessage", "sqs:ReceiveMessage"],
        },
        {
          Effect: "Deny",
          Principal: "arn:aws:iam::123456789012:user/blocked-user",
          Action: ["sqs:SendMessage"],
        },
      ],
    },
  },
})
```

#### Policy Updates

To update an existing queue's policy, use `updateAttributesIfExists: true`:

```typescript
const updatedPublisher = new SqsPermissionPublisher(dependencies, {
  creationConfig: {
    queue: {
      QueueName: "existing-queue",
    },
    policyConfig: {
      resource: SQS_RESOURCE_ANY,
      statements: {
        Effect: "Allow",
        Principal: "arn:aws:iam::123456789012:user/new-user",
        Action: ["sqs:SendMessage"],
      },
    },
    updateAttributesIfExists: true,
  },
  deletionConfig: {
    deleteIfExists: false,
  },
})
```

### Default Behavior

- If `policyConfig` is not provided or set to `undefined`, no policy is applied to the queue
- If `statements` is not provided, default values are used:
  - `Effect`: "Allow"
  - `Principal`: "*" (allows all AWS principals)
  - `Action`: ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"]

### Policy Structure

The generated policy follows the AWS IAM policy format:

```json
{
  "Version": "2012-10-17",
  "Resource": "arn:aws:sqs:*:*:*",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": "arn:aws:iam::123456789012:user/my-user"},
      "Action": ["sqs:SendMessage", "sqs:ReceiveMessage"]
    }
  ]
}
```

> **_NOTE:_**  See [SqsPermissionConsumer.spec.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.spec.ts) and [SqsPermissionPublisher.spec.ts](./packages/sqs/test/publishers/SqsPermissionPublisher.spec.ts) for practical examples of policy configuration tests.

## Handler Spies

In certain cases you want to await until certain publisher publishes a message, or a certain handler consumes a message. For that you can use handler spy functionality, built into `message-queue-toolkit` directly.

In order to enable this functionality, configure spyHandler on the publisher or consumer:

```ts
export class TestConsumerMultiSchema extends AbstractSqsConsumer<
    SupportedMessages,
    ExecutionContext
> {
    constructor(
        dependencies: SQSConsumerDependencies,
        userService: UserService,
    ) {
        super(dependencies, {
            //
            // rest of configuration skipped
            //
            handlerSpy: {
                bufferSize: 100, // how many processed messages should be retained in memory for spy lookup. Default is 100
                messageIdField: 'id', // which field within a message payload uniquely identifies it. Default is `id`
            },
        })
    }
}
```

Then you can use handler spies in your tests or production code to await certain events:

```ts
await myConsumer.handlerSpy.waitForMessageWithId('1', 'consumed')

await myConsumer.handlerSpy.waitForMessageWithId('1')

await myConsumer.handlerSpy.waitForMessageWithId('1')

await myConsumer.handlerSpy.waitForMessage({
    projectId: 1,
})

await myPublisher.handlerSpy.waitForMessage({
    userId: 1
}, 'published')
```

In case you do not want to await for message to be published, but want to merely check whether or not it was published by this moment, you can use `checkForMessage` method:

```ts
const notEmittedMessage = myPublisher.handlerSpy.checkForMessage({
    type: 'entity.created',
}) // this will resolve to undefined if such message wasn't published up to this moment
```

You can also check details of the message processing outcome:

```ts
const result = await myConsumer.handlerSpy.waitForMessageWithId('1')
expect(result.processingResult).toEqual({ status: 'consumed' })
```

## Payload Offloading
Payload offloading allows you to manage large message payloads by storing them in external storage, bypassing any message size restrictions imposed by queue systems.

### Configuration

1. **Install a payload store implementation (S3 in example):**
   ```bash
   npm install @message-queue-toolkit/s3-payload-store
   ```

2. **Configure your setup:**

    #### Single-Store Configuration
    ```typescript
    import { S3 } from '@aws-sdk/client-s3'
    import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
    import type { SinglePayloadStoreConfig } from '@message-queue-toolkit/core'
    import { SQS_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/sqs'
    
    const s3Client = new S3({
        region: 'your-s3-region',
        credentials: {
            accessKeyId: 'your-access-key-id',
            secretAccessKey: 'your-secret-access-key'
        }
    })
    
    const payloadStoreConfig: SinglePayloadStoreConfig = {
        store: new S3PayloadStore(
            { s3: s3Client },
            {
                bucketName: 'your-s3-bucket-name',
                keyPrefix: 'optional-key-prefix'
            }
        ),
        messageSizeThreshold: SQS_MESSAGE_MAX_SIZE,
        storeName: 'my-s3-store' // Required: identifies this store in message payloads
    }
    ```

    #### Multi-Store Configuration
    For scenarios requiring multiple payload stores (e.g., multi-region setups, migration between stores):
    ```typescript
    import type { MultiPayloadStoreConfig } from '@message-queue-toolkit/core'
    
    const payloadStoreConfig: MultiPayloadStoreConfig = {
        messageSizeThreshold: SQS_MESSAGE_MAX_SIZE,
        stores: {
            'store-us': new S3PayloadStore(deps, { bucketName: 'us-bucket' }),
            'store-eu': new S3PayloadStore(deps, { bucketName: 'eu-bucket' })
        },
        outgoingStore: 'store-eu',           // Required: store for publishing messages
        defaultIncomingStore: 'store-us'     // Optional: fallback for legacy messages
    }
    ```

    #### Using the Configuration
    ```typescript    
    export class MyConsumer extends AbstractSqsConsumer<> {
        constructor(
            // dependencies and options
        ) {
            super(dependencies, {
                // rest of the configuration
                payloadStoreConfig,
            })
        }
    }
    
    export class MyPublisher extends AbstractSqsPublisher<> {
        constructor(
            // dependencies and options
        ) {
            super(dependencies, {
                // rest of the configuration
                payloadStoreConfig,
            })
        }
    }
    ```
3. **Usage:**
   Messages larger than the `messageSizeThreshold` are automatically offloaded to the configured store (S3 bucket in example). 
   The queue manages only references to these payloads, ensuring compliance with size limits.
 
### Managing payload lifecycle
It's important to note that once messages are processed, their payloads **are not automatically deleted** from the external storage.
**The management and deletion of old payloads is the responsibility of the payload store itself.**

The primary reason for this is simplification. Managing payloads becomes complicated with fan-out, as tracking whether 
all consumers have read the message is challenging. 
Additionally, you'll likely need to establish an expiration policy for unconsumed or DLQ-rerouted messages since retaining them indefinitely is impractical.
Therefore, you will still need to implement some method to expire items.

For example, in the case of S3, you can set up a lifecycle policy to delete old payloads:
```json
{
  "Rules": [
    {
      "ID": "ExpireOldPayloads",
      "Prefix": "optional-key-prefix/",
      "Status": "Enabled",
      "Expiration": {
        "Days": 7
      }
    }
  ]
}
```

## Injectable dependencies

As mentioned above, some classes accept `dependencies` parameter, which allows to provide custom dependencies to control the behaviour of specific functionalities.

### MessageMetricsManager

It can be provided to `AbstractQueueService` and classes that extend it. 

It needs to implement the following methods:
- `registerProcessedMessage` - it's executed once message is processed. As a parameter it accepts message processing metadata with the following properties:
  - `messageId`
  - `messageType`
  - `processingResult`
    - `status`: can have one of the following values: `retryLater`, `consumed`, `published`, `error`
    - `skippedAsDuplicate`: is case of `consumed` status, it indicates whether the message was skipped as a duplicate
    - `errorReason`: in case of `error` status, it contains one of the following values: `invalidMessage`, `handlerError`, `retryLaterExceeded`
  - `message` - whole message object
  - `queueName` - name of the queue or topic on which message is consumed or published
  - `messageTimestamp` - the timestamp when the message was sent initially
  - `messageProcessingStartTimestamp` - the timestamp when the processing of the message started
  - `messageProcessingEndTimestamp` - the timestamp when the processing of the message finished
  - `messageDeduplicationId` - the deduplication id of the message, in case deduplication is enabled

See [@message-queue-toolkit/metrics](packages/metrics/README.md) for concrete implementations

## Store-based message deduplication

There are 2 types of store-based message deduplication: publisher-level and consumer-level.

### Publisher-level store-based message deduplication

Publisher-level store-based message deduplication is a mechanism that prevents the same message from being sent to the queue multiple times.
It is useful when you want to ensure that a message is published only once in a specified period of time, regardless of how many times it is sent.

The mechanism relies on:
1. a deduplication store, which is used to store deduplication keys for a certain period of time
2. a deduplication key, which uniquely identifies a message
3. a deduplication config, which contains details like a period of time during which a deduplication key is stored in the store (see [Publishers](#publishers) for more details of the options)

Note that in case of some queuing systems, such as standard SQS, publisher-level deduplication is not sufficient to guarantee that a message is **processed** only once.
This is because standard SQS has an at-least-once delivery guarantee, which means that a message can be delivered more than once.
In such cases, publisher-level deduplication should be combined with consumer-level one.

The keys stored in the deduplication stored are prefixed with `publisher:` to avoid conflicts with consumer-level deduplication keys in case they are both enabled and the same duplication store is used.

In case you would like to use SQS FIFO deduplication feature, store-based deduplication won't handle it for you.
Instead, you should either enable content-based deduplication on the queue or pass `MessageDeduplicationId` within message options when publishing a message.

#### Configuration

1. **Install a deduplication store implementation (Redis in example)**:
    ```bash
    npm install @message-queue-toolkit/redis-message-deduplication-store
    ```

2. **Configure your setup:**
    ```typescript
    import { Redis } from 'ioredis'
    import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'

    const redisClient = new Redis({
        // your redis configuration
    })

    // Create a new instance of RedisMessageDeduplicationStore
    messageDeduplicationStore = new RedisMessageDeduplicationStore({ redis: redisClient })

    // Configure messages publisher
    export class MyPublisher extends AbstractSqsPublisher<> {
        constructor(
            // dependencies and options
        ) {
            super(dependencies, {
                // rest of the configuration
                enablePublisherDeduplication: true,
                messageDeduplicationIdField: 'deduplicationId', // provide a field name in the message that contains unique deduplication id
                messageDeduplicationOptionsField: 'deduplicationOptions', // provide a field name in the message that contains deduplication options
                messageDeduplicationConfig: {
                    deduplicationStore: messageDeduplicationStore, // provide an instance of deduplication store
                },
            })
        }
    }

    // Create a publisher and publish a message
    const publisher = new MyPublisher(dependencies, options)
    await publisher.init()

    // Publish a message and deduplicate it for the next 60 seconds
    await publisher.publish({ 
        // rest of the message payload
        deduplicationId: 'unique-id',
        // the below options are optional. In case they are not provided or invalid, default values will be used
        deduplicationOptions: {
            deduplicationWindowSeconds: 60,
        },
    })
    // any subsequent call to publish with the same deduplicationId will be ignored for the next 60 seconds

   // You can also publish messages without deduplication, by simply omitting deduplicationId field
    await publisher.publish({ 
        // message payload
    })
    ```

### Consumer-level store-based message deduplication

Consumer-level store-based message deduplication is a mechanism that prevents the same message from being processed multiple times.
It is useful when you want to be sure that message is processed only once in a specified period of time, regardless of how many times it is received.

The mechanism relies on:
1. a deduplication store, which is used to store deduplication keys for a certain period of time.
2. a deduplication key, which uniquely identifies a message
3. a deduplication config (see [Consumers](#consumers) for more details of the options)

When a message is received, the consumer checks if the deduplication key exists in the store.

The consumer with store-based message deduplication enabled starts by checking if the deduplication key exists in the store.
If it does, message is considered as a duplicate and is ignored.
Otherwise, the consumer acquires an exclusive lock, which guarantees that only it can process the message.
In case of redis-based deduplication store, `redis-semaphore` is used which handles keeping lock alive when consumer is processing the message, and releasing it when processing is done.
Upon successful message processing, deduplication key is stored in the store for a given period of time to prevent processing the same message again.
In case lock is not acquired within the specified time, the message is re-queued.

The keys stored in the deduplication stored are prefixed with `consumer:` to avoid conflicts with publisher-level deduplication keys in case they are both enabled and the same duplication store is used.

In case you would like to use SQS FIFO deduplication feature, store-based deduplication won't handle it for you.
Instead, you should either enable content-based deduplication on the queue or pass `MessageDeduplicationId` within message options when publishing a message.

#### Configuration

1. **Install a deduplication store implementation (Redis in example)**:
    ```bash
    npm install @message-queue-toolkit/redis-message-deduplication-store
    ```

2. **Configure your setup:**
    ```typescript
    import { Redis } from 'ioredis'
    import { RedisMessageDeduplicationStore } from '@message-queue-toolkit/redis-message-deduplication-store'

    const redisClient = new Redis({
        // your redis configuration
    })

    // Create a new instance of RedisMessageDeduplicationStore
    messageDeduplicationStore = new RedisMessageDeduplicationStore({ redis: redisClient })

    export class MyConsumer extends AbstractSqsConsumer<> {
        constructor(
            // dependencies and options
        ) {
            super(dependencies, {
                enableConsumerDeduplication: true,
                messageDeduplicationIdField: 'deduplicationId', // provide a field name in the message that contains unique deduplication id
                messageDeduplicationOptionsField: 'deduplicationOptions', // provide a field name in the message that contains deduplication options
                messageDeduplicationConfig: {
                  deduplicationStore: messageDeduplicationStore, // provide an instance of deduplication store
                },
            })
        }
    }

    // Create a consumer and start listening for messages
    const consumer = new MyConsumer(dependencies, options)
    await consumer.init()

    // Publish a message, so that consumer can process it
    publisher.publish({
        // rest of the message payload
        deduplicationId: 'unique-id',
        // the below options are optional. In case they are not provided or invalid, default values will be used
        deduplicationOptions: {
            deduplicationWindowSeconds: 60,  
            lockTimeoutSeconds: 10,
            acquireTimeoutSeconds: 10,
            refreshIntervalSeconds: 5, 
        },
    })

   // You can also consume messages without deduplication, by simply ommitting deduplicationId field while publishing
    publisher.publish({ 
        // message payload
    })
    ```
