# Kafka

This library provides utilities for implementing Kafka consumers and publishers.
While following the same patterns as other message broker implementations,
Kafka's unique characteristics require some specific adaptations in the publisher and consumer definitions.

> **_NOTE:_** Check [README.md](../../README.md) for transport-agnostic library documentation.

## Publishers

Use `AbstractKafkaPublisher` as a base class for publisher implementation.

See [test publisher](test/publisher/PermissionPublisher.ts) for an example of implementation.

## Consumers

Use `AbstractKafkaConsumer` as a base class for consumer implementation.

See [test consumer](test/consumer/PermissionConsumer.ts) for an example of implementation.

## Batch Processing

Kafka supports batch processing for improved throughput. To enable it, set `batchProcessingEnabled` to `true` and configure `batchProcessingOptions`.

When batch processing is enabled, message handlers receive an array of messages instead of a single message.

### Configuration Options

- `batchSize` - Maximum number of messages per batch
- `timeoutMilliseconds` - Maximum time to wait for a batch to fill before processing

### How It Works

Messages are buffered per topic-partition combination. Batches are processed when either:
- The buffer reaches the configured `batchSize`
- The `timeoutMilliseconds` timeout is reached since the first message was added

After successful batch processing, the offset of the last message in the batch is committed.

See [test batch consumer](test/consumer/PermissionBatchConsumer.ts) for an example of implementation.
