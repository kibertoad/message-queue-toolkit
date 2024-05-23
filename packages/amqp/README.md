# AMQP (Advanced Message Queuing Protocol)

## Publishers

The library provides support for:
- exchange publishers: publish a message to an exchange and uses the exchange and message routing key specified to define where the message goes;
- queue publishers, delivers a message to the specified queue, bypassing routing.

See [test publisher](test/publishers/AmqpPermissionPublisher.ts) for an example of implementation.

## Consumers

The library provides support for queue consumers. Exchange consumers to be implemented.
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
