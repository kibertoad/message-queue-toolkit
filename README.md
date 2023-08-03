# message-queue-toolkit ✉️
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

`message-queue-toolkit` provides base classes for implementing publishers for each of the supported protocol. They implement the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `messageSchema` – the `zod` schema for the message;
        * `messageTypeField`;
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
* `init()`, which needs to be invoked before the publisher can be used;
* `close()`, which needs to be invoked when stopping the application;
* `publish()`, which accepts the following parameters:
    * `message` – a message following a `zod` schema;
    * `options` – a protocol-dependent set of message parameters. For more information please check documentation for options for each protocol: [AMQP](https://amqp-node.github.io/amqplib/channel_api.html#channel_sendToQueue), [SQS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/interfaces/sendmessagecommandinput.html) and [SNS](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sns/interfaces/publishcommandinput.html).

> **_NOTE:_**  See [SqsPermissionPublisherMonoSchema.ts](./packages/sqs/test/publishers/SqsPermissionPublisherMonoSchema.ts) for a practical example.

### Consumers

`message-queue-toolkit` provides base classes for implementing consumers for each of the supported protocol. They implement the following public methods:

* `constructor()`, which accepts the following parameters:
    * `dependencies` – a set of dependencies depending on the protocol;
    * `options`, composed by
        * `messageSchema` – the `zod` schema for the message;
        * `messageTypeField` - which field in the message is used for resolving the message type (for observability purposes);
        * `queueName`; (for SNS publishers this is a misnomer which actually refers to a topic name)
        * `locatorConfig` - configuration for resolving existing queue and/or topic. Should not be specified together with the `creationConfig`.
        * `creationConfig` - configuration for queue and/or topic to create, if one does not exist. Should not be specified together with the `locatorConfig`.
        * `subscriptionConfig` - SNS SQS consumer only - configuration for SNS -> SQS subscription to create, if one doesn't exist.
        * `consumerOverrides` – available only for SQS consumers;
        * `subscribedToTopic` – parameters for a topic to use during creation if it does not exist. Ignored if `queueLocator.subscriptionArn` is set. Available only for SNS consumers;
* `init()`, which needs to be invoked before the consumer can be used;
* `close()`, which needs to be invoked when stopping the application;
* `processMessage()`, which accepts as parameter a `message` following a `zod` schema and should be overridden with logic on what to do with the message;
* `start()`, which invokes `init()` and `processMessage()` and handles errors.

> **_NOTE:_**  See [SqsPermissionConsumerMonoSchema.ts](./packages/sqs/test/consumers/SqsPermissionConsumerMonoSchema.ts) for a practical example.

#### Error Handling

When implementing message handler in consumer (by overriding the `processMessage()` method), you are expected to return an instance of `Either`, containing either an error `retryLater`, or result `success`. In case of `retryLater`, the abstract consumer is instructed to requeue the message. Otherwise, in case of success, the message is finally removed from the queue. If an error is thrown while processing the message, the abstract consumer will also requeue the message. When overriding the `processMessage()` method, you should leverage the possible types to process the message as you need.

#### Schema Validation and Deserialization

Message deserialization is done within the abstract consumer _before_ processing the message. Deserialization is done for a message, given a message schema type. `zod` is the library used to declare and validate the message schema type.

If
* The message before deserialization is `null`
* Deserialization fails because of a syntax error or a validation error
* Deserialization returns an empty value

Then the message is automatically nacked without requeueing by the abstract consumer and processing fails.

> **_NOTE:_**  See [userConsumerSchemas.ts](./packages/sqs/test/consumers/userConsumerSchemas.ts) and [SqsPermissionsConsumerMonoSchema.spec.ts](./packages/sqs/test/consumers/SqsPermissionsConsumerMonoSchema.spec.ts) for a practical example.

## Fan-out to Multiple Consumers

SQS queues are built in a way that every message is only consumed once, and then deleted. If you want to do fan-out to multiple consumers, you need SNS topic in the middle, which is then propagated to all the SQS queues that have subscribed.

> **_NOTE:_**  See [SnsPermissionPublisher.ts](./packages/sns/test/publishers/SnsPermissionPublisherMonoSchema.ts) and [SnsSqsPermissionConsumerMonoSchema.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumerMonoSchema.ts) for a practical example.

## Automatic Queue and Topic Creation

Both publishers and consumers accept a queue name and configuration as parameters. If the referenced queue (or SNS topic) does not exist at the moment the publisher or the consumer is instantiated, it is automatically created. Similarly, if the referenced topic does not exist during instantiation, it is also automatically created.

If you do not want to create a new queue/topic, you can set `queueLocator` field for `queueConfiguration`. In that case `message-queue-toolkit` will not attempt to create a new queue or topic, and instead throw an error if they don't already exist.
