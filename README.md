# message-queue-toolkit ✉️
Useful utilities, interfaces and base classes for message queue handling.

## Overview

This is an abstraction to switch between different queue systems without having to implement your own deserialization, error handling, etc. The library provides utilities, interfaces and base classes to build the support for any queue system you may need in your service and already implements support for the following:

* AMQP 0-9-1 (Advanced Message Queuing Protocol), used e. g. by RabbitMQ
* SQS (Simple Queue Service)
* SNS (Simple Notification Service)

## Basic Usage

### Publishers

Build a publisher to send messages to a queue or a topic in a messaging system by creating a class that:

* Extends the abstract publisher class provided for the messaging system of choice
* Defines a queue name
* Defines a message schema
* Configures the queue

> **_NOTE:_**  See [SqsPermissionPublisher.ts](./packages/sqs/test/publishers/SqsPermissionPublisher.ts) for a practical example.

#### Consumers

Build a recipient of messages from a queue or a topic in a messaging system by creating a class that:

* Extends the abstract consumer class provided for the messaging system of choice
* Defines a queue name
* Defines a message schema
* Configures the queue
* Overrides the `processMessage()` method

> **_NOTE:_**  See [SqsPermissionConsumer.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.ts) for a practical example.

## Error Handling

`processMessage()` return response is `Either<'retryLater', 'success'>`, meaning that it could be an either one value or the other. In case of `retryLater`, the abstract consumer is instructed to requeue the message. Otherwise, in case of success, the message is finally removed from the queue. In case of any error, the abstract consumer will also requeue the message. When overriding the `processMessage()` method, you should leverage the possible types to process the message as you need.

## Schema Validation and Deserialization

Message deserialization is done within the abstract consumer _before_ processing the message. Deserialization is done for a message, given a message schema type. We use `zod` to define the message schema type.

If
* The message before deserialization is `null`
* Deserialization returns a `MessageInvalidFormatError` error for a syntax error
* Deserialization returns a `MessageValidationError` error for a `zod` schema validation error
* Deserialization returns an empty value

Then `abort` value is returned. In this case, the message is automatically nacked without requeueing by the abstract consumer and processing fails.

> **_NOTE:_**  See [userConsumerSchemas.ts](./packages/sqs/test/consumers/userConsumerSchemas.ts) and [SqsPermissionsConsumer.spec.ts](./packages/sqs/test/consumers/SqsPermissionsConsumer.spec.ts) for a practical example.

## Fan-out to Multiple Consumers

SQS queues are built in a way that every message is only consumed once, and then deleted. If you want to do fan-out to multiple consumers, you need SNS topic in the middle, which is then propagated to all the SQS queues that have subscribed.

> **_NOTE:_**  See [SnsPermissionPublisher.ts](./packages/sns/test/publishers/SnsPermissionPublisher.ts) and [SnsSqsPermissionConsumer.ts](./packages/sns/test/consumers/SnsSqsPermissionConsumer.ts) for a practical example.

## Automatic Queue and Topic Creation

The `init()` method in the abstract queue publishers and consumers calls the `assertQueue()` method. This allows to assert a queue into existence and if the queue does not exist, it will be created.

Similarly, the `init()` method in the abstract SNS publishers and consumers calls the `assertTopic()` method, which will also automaticlaly create a topic if it does not exist.
