# @message-queue-toolkit/gcs-payload-store

Google Cloud Storage-based payload store implementation for message-queue-toolkit. Enables offloading large message payloads to GCS to comply with message size limitations in queue systems.

## Overview

This package provides a GCS-based implementation of the `PayloadStore` interface, allowing you to automatically store large message payloads in Google Cloud Storage while keeping only a reference pointer in the actual message.

This is particularly useful when:
- Message payloads exceed queue system limits (e.g., 256 KB for SQS, 10 MB for Pub/Sub)
- You want to reduce message processing costs by offloading large data
- You need to handle variable-sized payloads efficiently

## Installation

```bash
npm install @message-queue-toolkit/gcs-payload-store @google-cloud/storage
```

## Prerequisites

- Google Cloud Platform account
- GCS bucket for payload storage
- Appropriate IAM permissions for GCS access

## Basic Usage

### Configuration

```typescript
import { Storage } from '@google-cloud/storage'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import { AbstractSqsPublisher } from '@message-queue-toolkit/sqs'

const storage = new Storage({
  projectId: 'my-project',
  keyFilename: '/path/to/credentials.json',
})

class MyPublisher extends AbstractSqsPublisher<MyMessageType> {
  constructor() {
    super(dependencies, {
      // ... other options
      payloadStoreConfig: {
        store: new GCSPayloadStore(
          { gcsStorage: storage },
          {
            bucketName: 'my-payload-bucket',
            keyPrefix: 'message-payloads', // optional
          }
        ),
        messageSizeThreshold: 256 * 1024, // 256 KB
      },
    })
  }
}
```

### Using the Helper Function

```typescript
import { resolvePayloadStoreConfig } from '@message-queue-toolkit/gcs-payload-store'

const payloadStoreConfig = resolvePayloadStoreConfig(
  { gcsStorage: storage },
  {
    gcsPayloadOffloadingBucket: 'my-payload-bucket',
    messageSizeThreshold: 256 * 1024,
  }
)

// Returns undefined if bucket not configured
// Throws error if storage client not provided
```

## Configuration Options

### GCSPayloadStoreConfiguration

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `bucketName` | `string` | ✅ | GCS bucket name for storing payloads |
| `keyPrefix` | `string` | ❌ | Optional prefix for all stored keys (useful for organizing payloads) |

### PayloadStoreConfig

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `store` | `PayloadStore` | ✅ | Instance of `GCSPayloadStore` |
| `messageSizeThreshold` | `number` | ✅ | Size threshold in bytes - payloads exceeding this will be offloaded |
| `serializer` | `PayloadSerializer` | ❌ | Custom payload serializer (defaults to JSON) |

## How It Works

### Publishing Flow

1. **Message size check**: When publishing, the toolkit calculates message size
2. **Offload decision**: If size exceeds `messageSizeThreshold`, payload is offloaded
3. **Store in GCS**: Payload is serialized and stored in GCS with a UUID key
4. **Publish pointer**: Only a small pointer object is sent through the queue:
   ```typescript
   {
     offloadedPayloadPointer: "prefix/uuid-key",
     offloadedPayloadSize: 1234567,
     // ... message metadata (id, type, timestamp, etc.)
   }
   ```

### Consumption Flow

1. **Detect pointer**: Consumer detects the offloaded payload pointer
2. **Retrieve from GCS**: Payload is retrieved from GCS using the pointer
3. **Deserialize**: Payload is deserialized back to original format
4. **Process normally**: Message handler receives the full payload

## Lifecycle Management

**Important**: Payloads are **not automatically deleted** after message processing.

### Why Not Auto-Delete?

1. **Fan-out complexity**: With multiple consumers, tracking when all have processed is difficult
2. **DLQ scenarios**: Messages sent to dead letter queues still reference payloads
3. **Retry scenarios**: Failed messages may be retried and need the payload

### Recommended Approach: GCS Lifecycle Policies

Set up GCS lifecycle rules to automatically delete old payloads:

```bash
# Using gcloud CLI
gcloud storage buckets update gs://my-payload-bucket \
  --lifecycle-file=lifecycle.json
```

**lifecycle.json**:
```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {
          "type": "Delete"
        },
        "condition": {
          "age": 7,
          "matchesPrefix": ["message-payloads/"]
        }
      }
    ]
  }
}
```

This deletes payloads older than 7 days, regardless of whether they've been consumed.

## Testing with Emulator

### Using fake-gcs-server

The package includes support for testing with the GCS emulator:

```bash
# Start emulator (included in docker-compose)
docker compose up -d gcs-emulator
```

**Test configuration**:
```typescript
import { Storage } from '@google-cloud/storage'

const storage = new Storage({
  projectId: 'test-project',
  apiEndpoint: 'http://127.0.0.1:4443',
})

const store = new GCSPayloadStore(
  { gcsStorage: storage },
  { bucketName: 'test-bucket' }
)
```

## API Reference

### GCSPayloadStore

#### Constructor

```typescript
new GCSPayloadStore(
  dependencies: GCSPayloadStoreDependencies,
  config: GCSPayloadStoreConfiguration
)
```

#### Methods

**`storePayload(payload: SerializedPayload): Promise<string>`**

Stores a payload in GCS and returns a unique key.

- **Parameters:**
  - `payload.value`: `string | Readable` - The payload data
  - `payload.size`: `number` - Size in bytes
- **Returns:** Promise resolving to the storage key

**`retrievePayload(key: string): Promise<Readable | null>`**

Retrieves a previously stored payload.

- **Parameters:**
  - `key`: The storage key returned by `storePayload`
- **Returns:** Promise resolving to a Readable stream, or `null` if not found

**`deletePayload(key: string): Promise<void>`**

Deletes a payload from storage.

- **Parameters:**
  - `key`: The storage key
- **Returns:** Promise that resolves when deletion is complete

## Integration Examples

### With SQS

```typescript
import { Storage } from '@google-cloud/storage'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import { AbstractSqsPublisher, SQS_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/sqs'

const storage = new Storage({ projectId: 'my-project' })

class LargeMessagePublisher extends AbstractSqsPublisher<MyMessage> {
  constructor(dependencies) {
    super(dependencies, {
      creationConfig: {
        queue: { QueueName: 'large-messages' },
      },
      messageSchemas: [MY_MESSAGE_SCHEMA],
      messageTypeField: 'type',
      payloadStoreConfig: {
        store: new GCSPayloadStore(
          { gcsStorage: storage },
          { bucketName: 'sqs-large-payloads' }
        ),
        messageSizeThreshold: SQS_MESSAGE_MAX_SIZE,
      },
    })
  }
}
```

### With Pub/Sub

```typescript
import { Storage } from '@google-cloud/storage'
import { GCSPayloadStore } from '@message-queue-toolkit/gcs-payload-store'
import { AbstractPubSubPublisher, PUBSUB_MESSAGE_MAX_SIZE } from '@message-queue-toolkit/gcp-pubsub'

const storage = new Storage({ projectId: 'my-project' })

class PubSubLargeMessagePublisher extends AbstractPubSubPublisher<MyMessage> {
  constructor(dependencies) {
    super(dependencies, {
      creationConfig: {
        topic: { name: 'large-events' },
      },
      messageSchemas: [MY_MESSAGE_SCHEMA],
      messageTypeField: 'type',
      payloadStoreConfig: {
        store: new GCSPayloadStore(
          { gcsStorage: storage },
          { bucketName: 'pubsub-large-payloads', keyPrefix: 'events' }
        ),
        messageSizeThreshold: PUBSUB_MESSAGE_MAX_SIZE,
      },
    })
  }
}
```

## Error Handling

The GCSPayloadStore handles errors gracefully:

- **Not found**: Returns `null` instead of throwing
- **Permission errors**: Thrown as-is for proper handling
- **Network errors**: Thrown as-is for retry logic

```typescript
try {
  const payload = await store.retrievePayload('some-key')
  if (payload === null) {
    // Payload not found - handle gracefully
  }
} catch (error) {
  // Permission or network error - log and alert
}
```

## Best Practices

1. **Set appropriate thresholds**: Use queue-specific limits (e.g., `SQS_MESSAGE_MAX_SIZE`)
2. **Use key prefixes**: Organize payloads by message type or tenant
3. **Configure lifecycle policies**: Always set up automatic cleanup
4. **Monitor storage costs**: Track bucket size and set up alerts
5. **Use IAM roles**: Prefer IAM roles over service account keys in production
6. **Test with emulator**: Use fake-gcs-server for local development

## Troubleshooting

### Payloads not being deleted

Set up GCS lifecycle policies. The store intentionally does not auto-delete to handle fan-out and retry scenarios.

### Authentication errors

Ensure your Storage client has proper credentials:
```typescript
const storage = new Storage({
  projectId: 'my-project',
  keyFilename: '/path/to/service-account.json',
})
```

### Bucket not found errors

Ensure the bucket exists before using:
```bash
gsutil mb gs://my-payload-bucket
```

Or create programmatically:
```typescript
await storage.createBucket('my-payload-bucket')
```

## License

MIT

## Contributing

Contributions are welcome! Please see the main [message-queue-toolkit repository](https://github.com/kibertoad/message-queue-toolkit) for contribution guidelines.
