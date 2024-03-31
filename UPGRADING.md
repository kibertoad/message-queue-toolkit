# Upgrading Guide

We have introduced the following breaking changes on version `12.0.0`, please follow the steps below to update your code
from the previous version to the new one.

## Breaking Changes

### Description of Breaking Change
Multiple consumers and publishers can accomplish the same tasks as single ones, but they add layer of complexity by 
requiring features to be implemented in both.
As a result, we have decided to remove the single ones to enhance maintainability.

### Migration Steps
#### Multi consumers and publishers
If you are using the multi consumer or consumer, you will only need to rename the class you are extending, and it should 
work as before.
- `AbstractSqsMultiConsumer` -> `AbstractSqsConsumer`
- `AbstractSqsMultiPublisher` -> `AbstractSqsPublisher`

#### Mono consumers and publishers
If you are using the mono consumer or publisher, they no longer exists, so you will need to transform your code to use
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
