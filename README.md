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

`message-queue-toolkit` provides base classes for implementing publishers for each of the supported protocol.
They implement the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `messageSchemas` – the `zod` schemas for all supported messages;
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for resolving the correct schema for validation
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
* `init()`, prepare publisher for use (e. g. establish all necessary connections);
* `close()`, stop publisher use (e. g. disconnect);
* `publish()`, send a message to a queue or topic. It accepts the following parameters:
    * `message` – a message following one of the `zod` schemas, supported by the publisher;
    * `options` – a protocol-dependent set of message parameters. For more information please check documentation for options for each protocol: [AMQP](https://amqp-node.github.io/amqplib/channel_api.html#channel_sendToQueue), [SQS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/interfaces/sendmessagecommandinput.html) and [SNS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sns/interfaces/publishcommandinput.html).

> **_NOTE:_**  See [SqsPermissionPublisher.ts](./packages/sqs/test/publishers/SqsPermissionPublisher.ts) for a practical example.

> **_NOTE:_**  Lazy loading is not supported for AMQP publishers.


### Consumers

`message-queue-toolkit` provides base classes for implementing consumers for each of the supported protocol.
They expose the following public methods:

Multi-schema consumers support multiple message types via handler configs. They expose the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `handlers` – configuration for handling each of the supported message types. See "Multi-schema handler definition" for more details;
        * `messageTypeField` - which field in the message describes the type of a message. This field needs to be defined as `z.literal` in the schema and is used for routing the message to the correct handler;
        * `queueName`; (for SNS publishers this is a misnomer which actually refers to a topic name)
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
        * `subscriptionConfig` - SNS SQS consumer only - configuration for SNS -> SQS subscription to create, if one doesn't exist.
        * `consumerOverrides` – available only for SQS consumers;
        * `deadLetterQueue` - available only for SQS and SNS consumers; please read the section below to understand how to use it.
        * `subscribedToTopic` – parameters for a topic to use during creation if it does not exist. Ignored if `queueLocator.subscriptionArn` is set. Available only for SNS consumers;
* `init()`, prepare consumer for use (e. g. establish all necessary connections);
* `close()`, stop listening for messages and disconnect;
* `start()`, which invokes `init()`.

> **_NOTE:_**  See [SqsPermissionConsumer.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.ts) for a practical example.


##### How to define a handler

You can define handlers for each of the supported messages in a type-safe way using the MessageHandlerConfigBuilder.

Here is an example:

```typescript
type SupportedMessages = PERMISSIONS_ADD_MESSAGE_TYPE | PERMISSIONS_REMOVE_MESSAGE_TYPE
type ExecutionContext = {
    userService: UserService
}

export class TestConsumerMultiSchema extends AbstractSqsConsumerMultiSchema<
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
            handlers: new MessageHandlerConfigBuilder<
                SupportedMessages,
                ExecutionContext
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

> **_NOTE:_**  See [userConsumerSchemas.ts](./packages/sqs/test/consumers/userConsumerSchemas.ts) and [SqsPermissionsConsumer.spec.ts](./packages/sqs/test/consumers/SqsPermissionsConsumer.spec.ts) for a practical example.

### Barrier pattern
The barrier pattern facilitates the out-of-order message handling by retrying the message later if the system is not yet in the proper state to be able to process that message (e. g. some prerequisite messages have not yet arrived).

To enable this pattern you should define `preHandlerBarrier` on your message handler in order to define the conditions for starting to process the message.
If the barrier method returns `false`, message will be returned into the queue for the later processing. If the barrier method returns `true`, message will be processed.

> **_NOTE:_**  See [SqsPermissionConsumer.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumer.ts) for a practical example.

### Dead Letter Queue
A dead letter queue is a queue where messages that cannot be processed successfully are stored. It serves as a holding area for messages that have encountered errors or exceptions during processing, allowing developers to review and handle them later.
To create a dead letter queue, you need to specify the `deadLetterQueue` parameter within options on the consumer configuration. The parameter should contain the following fields:
- `creationConfig`: configuration for queue to create, if one does not exist. Should not be specified together with the `locatorConfig`
- `locatorConfig`: configuration for resolving existing queue. Should not be specified together with the `creationConfig`
- `redrivePolicy`: an object that contains the following fields:
  - `maxReceiveCount`: the number of times a message can be received before being moved to the DLQ.

> **_NOTE:_**  DLQ is not available for AMQP consumers.

> **_NOTE:_**  DLQ could produce message duplication if used together with the barrier pattern (due to the 
    retry mechanism), although this is unlikely to happen in practice. 
    In a future release, we will implement deduplication to remove this risk but in the meantime, please be aware of this.

## Fan-out to Multiple Consumers

SQS queues are built in a way that every message is only consumed once, and then deleted. If you want to do fan-out to multiple consumers, you need SNS topic in the middle, which is then propagated to all the SQS queues that have subscribed.

> **_NOTE:_**  See [SnsPermissionPublisher.ts](./packages/sns/test/publishers/SnsPermissionPublisher.ts) and [SnsSqsPermissionConsumerMonoSchema.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumer.ts) for a practical example.

## Automatic Queue and Topic Creation

Both publishers and consumers accept a queue name and configuration as parameters. If the referenced queue (or SNS topic) does not exist at the moment the publisher or the consumer is instantiated, it is automatically created. Similarly, if the referenced topic does not exist during instantiation, it is also automatically created.

If you do not want to create a new queue/topic, you can set `queueLocator` field for `queueConfiguration`. In that case `message-queue-toolkit` will not attempt to create a new queue or topic, and instead throw an error if they don't already exist.

## Handler spies

In certain cases you want to await until certain publisher publishes a message, or a certain handler consumes a message. For that you can use handler spy functionality, built into message-queue-toolkit directly.

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

You can also check details of the message processing outcome:

```ts
const result = await myConsumer.handlerSpy.waitForMessageWithId('1')
expect(result.processingResult).toEqual('consumed')
```

## Automatic Reconnects (RabbitMQ)

`message-queue-toolkit` automatically reestablishes connections for all publishers and consumers via `AmqpConnectionManager` mechanism.

Example:

```ts
export const TEST_AMQP_CONFIG: AmqpConfig = {
    vhost: '',
    hostname: 'localhost',
    username: 'guest',
    password: 'guest',
    port: 5672,
    useTls: false,
}

const amqpConnectionManager = new AmqpConnectionManager(config, logger)
await amqpConnectionManager.init()

const publisher = new TestAmqpPublisher(
    { amqpConnectionManager },
    {
        /// other amqp options
    })
await publisher.init()

const consumer = new TestAmqpConsumer(
    { amqpConnectionManager },
    {
        /// other amqp options
    })
await consumer.start()

// break connection, to simulate unexpected disconnection in production
await (await amqpConnectionManager.getConnection()).close()

const message = { 
    // some test message
}

// This will fail, but will trigger reconnection within amqpConnectionManager
publisher.publish(message)
    
// eventually connection is reestablished and propagated across all the AMQP services that use same amqpConnectionManager 

// This will succeed and consumer, which also received new connection, will be able to consume it
publisher.publish(message)
```
