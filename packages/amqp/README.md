# AMQP (Advanced Message Queuing Protocol)

The library provides support for both direct exchanges and topic exchanges.

> **_NOTE:_**  Check [README.md](../../README.md) for transport-agnostic library documentation.
>

## Publishers

Use `AbstractAmqpQueuePublisher` to implement direct exchange and `AbstractAmqpTopicPublisher` for topic exchange.

See [test publisher](test/publishers/AmqpPermissionPublisher.ts) for an example of implementation.

## Consumers

Use `AbstractAmqpQueueConsumer` to implement direct exchange and `AbstractAmqpTopicConsumer` for topic exchange.

See [test consumer](test/consumers/AmqpPermissionConsumer.ts) for an example of implementation.

## Automatic Reconnects

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
        // other amqp options
    })
await publisher.init()

const consumer = new TestAmqpConsumer(
    { amqpConnectionManager },
    {
        // other amqp options
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
