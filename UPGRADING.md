# Upgrading Guide

## Upgrading </br> `core` `19.0.0` -> `20.0.0` </br> `sqs` `19.0.0` -> `20.0.0` </br> `sns` `20.0.0` -> `21.0.0` </br> `amqp` `18.0.0` -> `19.0.0` </br> `metrics` `2.0.0` -> `3.0.0`

### Description of Breaking Changes

- The `processingResult` from the core has been updated to an object format. It is utilized within spies and metrics and
 now contains the following properties:
  - `status`: This property can have one of the following values: `retryLater`, `consumed`, `published`, or `error`.
  - `skippedAsDuplicate`: For messages with a `status` of `consumed`, this property indicates whether the message was 
   skipped due to being a duplicate.
  - `errorReason`: When `status` is `error`, this property provides specific reasons and can be one of the following: 
   `invalidMessage`, `handlerError`, or `retryLaterExceeded`.
- Additionally, the `metrics` library has undergone class renaming to support future tool integrations and simplify the
 process of adding new Prometheus custom metrics by extending `PrometheusMessageMetric`.

### Migration Steps
- If your project utilizes `processingResult` within spies or metrics, you must update it to align with the new structure. 
- Follow these renaming changes in the `metrics` library:
  - `MessageProcessingMultiMetrics` is now `MessageMultiMetricManager`
  - `MessageLifetimeMetric` is now `PrometheusMessageLifetimeMetric`
  - `MessageProcessingTimeMetric` is now `PrometheusMessageTimeMetric`


## Upgrading </br> `core` `18.0.0` -> `19.0.0` </br> `sqs` `18.0.0` -> `19.0.0` </br> `sns` `19.0.0` -> `20.0.0` </br> `amqp` `17.0.0` -> `18.0.0`

### Description of Breaking Changes
- In `AbstractQueueService`:
  - `handleMessageProcessed` method signature has changed. Parameters are now passed as an object, and 2 additional properties were added:
    - `messageProcessingStartTimestamp` - timestamp in milliseconds used to calculate message processing time
    - `queueName` - name of the queue or topic on which message is consumed or published
  - `resolveProcessedMessageMetadata` was made private

- `ProcessedMessageMetadata` type used in `MessageMetricsManager` has changed:
  - `messageProcessingMilliseconds` property was removed
  - the following properties were added:
    - `messageTimestamp`
    - `messageProcessingStartTimestamp`
    - `messageProcessingEndTimestamp`

- Because of the change above, `@message-queue-toolkit/metrics@1.0.0` is not compatible with the new version.

### Migration steps
- If you are using custom implementation of `MessageMetricsManager`, you may need to adjust it to the new properties in `ProcessedMessageMetadata`

- If you are extending `AbstractQueueService` and calling `handleMessageProcessed` manually, you need to adjust it to the new signature, e.g.:
  - from:  
    ```typescript
    this.handleMessageProcessed(message, 'consumed', messageProcessingStartTimestamp, queueName)
    ```
  - to:
    ```typescript
    this.handleMessageProcessed({ message, processingResult: 'consumed', messageProcessingStartTimestamp, queueName })
    ```

- If you are using features from `@message-queue-toolkit/metrics@1.0.0`, upgrade to `@message-queue-toolkit/metrics@2.0.0` and follow the migration guide below.

## Upgrading `metrics` from `1.0.0 `to `2.0.0`

### Description of Breaking Changes
- `MessageProcessingTimePrometheusMetric` class was replaced by 2 classes:
    - `MessageProcessingTimeMetric` - registers elapsed time from start to the end of processing
    - `MessageLifetimeMetric` - registers elapsed time from message creation to the end of processing

### Migration steps
- If you are using `MessageProcessingTimePrometheusMetric`, replace it with one of the metrics mentioned above, depending on what do you want to measure.
  **Note:** if you want to keep current measurements, use `MessageLifetimeMetric`.
  If you want to use both metrics, use `MessageProcessingMultiMetrics` (see: [README.md](packages/metrics/README.md))

## Upgrading </br> `core` `17.0.0` -> `18.0.0` </br> `sqs` `17.0.0` -> `18.0.0` </br> `sns` `18.0.0` -> `19.0.0` </br> `amqp` `16.0.0` -> `17.0.0`    

### Description of Breaking Changes
- `logger` injectable dependency interface was replaced by `CommonLogger` from `@lokalise/node-core` package. It is compatible with `pino` logger (https://github.com/pinojs/pino)

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
