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

## Basic Usage

### Publishers

`message-queue-toolkit` provides base classes for implementing publishers for each of the supported protocols.
They implement the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `messageSchemas` – the `zod` schemas for all supported messages;
        * `messageTimestampField` - which field in the message contains the message creation date (by default it is `timestamp`). This field needs to be a `Date` object or ISO-8601 date string, if your message doesn't contain it the library will add one automatically to avoid infinite loops on consumer;
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for resolving the correct schema for validation
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`;
        * `deletionConfig` - automatic cleanup of resources;
        * `handlerSpy` - allow awaiting certain messages to be published (see [Handler Spies](#handler-spies) for more information);
        * `logMessages` - add logs for processed messages.
        * `payloadStoreConfig` - configuration for payload offloading. This option enables the external storage of large message payloads to comply with message size limitations of the queue system. For more details on setting this up, see [Payload Offloading](#payload-offloading).
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
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for routing the message to the correct handler;
        * `messageTimestampField` - which field in the message contains the message creation date (by default it is `timestamp`). This field needs to be a `Date` object or an ISO-8601 date string;
        * `maxRetryDuration` - how long (in seconds) the message should be retried due to the `retryLater` result before marking it as consumed (and sending to DLQ, if one is configured). This is used to avoid infinite loops. Default is 4 days;
        * `queueName`; (for SNS publishers this is a misnomer which actually refers to a topic name)
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
        * `subscriptionConfig` - SNS SQS consumer only - configuration for SNS -> SQS subscription to create, if one doesn't exist.
        * `deletionConfig` - automatic cleanup of resources;
        * `consumerOverrides` – available only for SQS consumers;
        * `deadLetterQueue` - available only for SQS and SNS consumers (see [Dead Letter Queue](#dead-letter-queue) for more information);
        * `handlerSpy` - allow awaiting certain messages to be published (see [Handler Spies](#handler-spies) for more information);
        * `logMessages` - add logs for processed messages.
        * `payloadStoreConfig` - configuration for payload offloading. This option enables the external storage of large message payloads to comply with message size limitations of the queue system. For more details on setting this up, see [Payload Offloading](#payload-offloading).
        * `concurrentConsumersAmount` - configuration for specifying the number of concurrent consumers to create. Available only for SQS and SNS consumers
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
expect(result.processingResult).toEqual('consumed')
```

## Payload Offloading
Payload offloading allows you to manage large message payloads by storing them in external storage, bypassing any message size restrictions imposed by queue systems.

### Configuration

1. **Install a payload store implementation (S3 in example):**
   ```bash
   npm install @message-queue-toolkit/s3-payload-store
   ```

2. **Configure your setup:**
    ```typescript
    import { S3 } from '@aws-sdk/client-s3'
    import { S3PayloadStore } from '@message-queue-toolkit/s3-payload-store'
    import { PayloadStoreConfig } from '@message-queue-toolkit/core'
    import { SQS_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/sqs'
    
    const s3Client = new S3({
        region: 'your-s3-region',
        credentials: {
            accessKeyId: 'your-access-key-id',
            secretAccessKey: 'your-secret-access-key'
        }
    })
    
    const payloadStoreConfig = {
        store: new S3PayloadStore(
            {s3: s3Client},
            {
                bucketName: 'your-s3-bucket-name',
                keyPrefix: 'optional-key-prefix'
            }
        ),
        messageSizeThreshold: SQS_MESSAGE_MAX_SIZE,
    }
    
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
  - `processingResult` - can have one of the following values: `retryLater`, `consumed`, `published`, `error`, `invalid_message`
  - `message` - whole message object
  - `queueName` - name of the queue or topic on which message is consumed or published
  - `messageProcessingMilliseconds` - message processing time in milliseconds

See [@message-queue-toolkit/metrics](packages/metrics/README.md) for concrete implementations
