# Upgrading Guide

## Upgrading from `17.0.0` to `18.0.0`

### Description of Breaking Changes
- `logger` injectable dependency interface was replaced by `CommonLogger` from `@lokalise/node-core` package, It is compatible with on `pino` logger (https://github.com/pinojs/pino)

### Migration steps
- If you are using `pino` logger, or any other logger compatible with `CommonLogger` interface, you don't need to do anything
- Otherwise, there are serveral properties that need to be provided compared to the old interface:
  - `level` - string defining configured minimal logging level
  - `isLevelEnabled` - utility method for determining if a given log level will write to the destination.
  - `child` - method for generating child logger instance with predefined properties
  - `silent` - method used for logging when `silent` level is selected

## Upgrading from `12.0.0` to `13.0.0`

### Description of Breaking Change
- The property `prehandlers` has been updated to `preHandler` for consumers.
- In the SQS and SNS consumer, `consumerOverrides` no longer permits the alteration of critical properties such as
 `sqs`, `queueUrl`, `handler`, and `handleMessageBatch` to prevent confusion and maintain proper functionality of 
 the consumer.
- The `consumerOverrides` in the SQS and SNS consumer no longer includes the option to define the `visibilityTimeout` 
 property, as it is now automatically determined by the consumer from the `creationConfig` or queue configuration in the
 case of `locatorConfig`.

### Migration Steps
#### preHandlers
If you currently utilize the `prehandlers` property in your consumer, it will be necessary to update it to `preHandler`.

#### ConsumerOverrides
If you are implementing the `consumerOverrides` property in your consumer, it is essential to eliminate the properties 
`sqs`, `queueUrl`, `handler`, `handleMessageBatch`, and `visibilityTimeout`.
- `sqs` should be included in the constructor dependencies
- `queueUrl` is managed automatically by the library and does not require manual specification
- `handleMessageBatch` is not supported by the library
- `visibilityTimeout` is automatically managed by the library and does not need explicit declaration
- For the `handler`, use the `handler` property in the constructor as demonstrated below
```typescript
export class MyConsumer extends AbstractAmqpConsumer<MyType, undefined> {
    public static QUEUE_NAME = 'my-queue-name'

    constructor(dependencies: AMQPConsumerDependencies) {
        super(
            dependencies,
            {
                creationConfig: {
                    queueName: AmqpPermissionConsumer.QUEUE_NAME,
                    queueOptions: { durable: true, autoDelete: false },
                },
                messageTypeField: 'messageType',
                handlers: new MessageHandlerConfigBuilder<SupportedEvents, ExecutionContext>()
                    .addConfig(
                        MY_MESSAGE_SCHEMA,
                        async (message) => {
                            // Your handling code
                            return {
                                result: 'success',
                            }
                        },
                    )
                    .build(),
            },
            undefined
        )
    }
}
```

## Upgrading from `11.0.0 `to `12.0.0`

### Description of Breaking Change
Multi consumers and publishers can accomplish the same tasks as mono ones, but they add extra layer of complexity by 
requiring features to be implemented in both.
As a result, we have decided to remove the mono ones to enhance maintainability.

### Migration Steps
#### Multi consumers and publishers
If you are using the multi consumer or consumer, you will only need to rename the class you are extending, and it should 
work as before.
- `AbstractSqsMultiConsumer` -> `AbstractSqsConsumer`
- `AbstractSqsMultiPublisher` -> `AbstractSqsPublisher`

#### Mono consumers and publishers
If you are using the mono consumer or publisher, they no longer exist, so you will need to adjust your code to use
the old named multi consumer or publisher (now called just consumer or publisher). Please check the guide below.

##### Publisher
1. Rename the class you are extending from `AbstractSqsPublisherMonoSchema` to `AbstractSqsPublisherSchema`.
2. replace the `messageSchema` property with `messageSchemas`, it is an array of `zod` schemas.
```typescript
// Old code
export class MyPublisher extends AbstractSqsPublisherMonoSchema<MyType> {
    public static QUEUE_NAME = 'my-queue-name'

    constructor(dependencies: SQSDependencies) {
        super(dependencies, {
            creationConfig: {
                queue: {
                    QueueName: SqsPermissionPublisherMonoSchema.QUEUE_NAME,
                },
            },
            handlerSpy: true,
            deletionConfig: {
                deleteIfExists: false,
            },
            logMessages: true,
            messageSchema: MY_MESSAGE_SCHEMA,
            messageTypeField: 'messageType',
        })
    }
}

// Updated code
export class MyPublisher extends AbstractSqsPublisher<MyType> {
    public static QUEUE_NAME = 'my-queue-name'

    constructor(dependencies: SQSDependencies) {
        super(dependencies, {
            creationConfig: {
                queue: {
                    QueueName: SqsPermissionPublisherMonoSchema.QUEUE_NAME,
                },
            },
            handlerSpy: true,
            deletionConfig: {
                deleteIfExists: false,
            },
            logMessages: true,
            messageSchemas: [MY_MESSAGE_SCHEMA],
            messageTypeField: 'messageType',
        })
    }
}
```

##### Consumer
1. Rename the class you are extending from `AbstractSqsConsumerMonoSchema` to `AbstractSqsConsumer`.
2. Remove the `messageSchema` property.
3. Define a handler (`handlers` property) for your message, specifying the `zod` schema (old `messageSchema`) and the
    method to handle the message (old `processMessage` method)
```typescript
// Old code
export class MyConsumer extends AbstractAmqpConsumerMonoSchema<MyType> {
    public static QUEUE_NAME = 'my-queue-name'

    constructor(dependencies: AMQPConsumerDependencies) {
        super(dependencies, {
            creationConfig: {
                queueName: AmqpPermissionConsumer.QUEUE_NAME,
                queueOptions: {
                    durable: true,
                    autoDelete: false,
                },
            },
            deletionConfig: {
                deleteIfExists: true,
            },
            messageSchema: MY_MESSAGE_SCHEMA,
            messageTypeField: 'messageType',
        })
    }

    override async processMessage(
        message: MyType,
    ): Promise<Either<'retryLater', 'success'>> {
        // Your handling code
        return { result: 'success' }
    }
}

// Updated code
export class MyConsumer extends AbstractAmqpConsumer<MyType, undefined> {
    public static QUEUE_NAME = 'my-queue-name'

    constructor(dependencies: AMQPConsumerDependencies) {
        super(
            dependencies,
            {
                creationConfig: {
                    queueName: AmqpPermissionConsumer.QUEUE_NAME,
                    queueOptions: {
                        durable: true,
                        autoDelete: false,
                    },
                },
                deletionConfig: {
                    deleteIfExists: true,
                },
                messageTypeField: 'messageType',
                handlers: new MessageHandlerConfigBuilder<SupportedEvents, ExecutionContext>()
                    .addConfig(
                        MY_MESSAGE_SCHEMA,
                        async (message) => {
                            // Your handling code
                            return {
                                result: 'success',
                            }
                        },
                    )
                    .build(),
            },
            undefined
        )
    }
}
```
> **_NOTE:_** on this example code we are omitting the barrier pattern (`preHandlerBarrier`) and pre handlers (`preHandlers`)
to simplify the example. If you are using them, please check [SqsPermissionConsumer.ts](./packages/sqs/test/consumers/SqsPermissionConsumer.ts)
to see how to update your code.
