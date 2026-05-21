# @message-queue-toolkit/codec

Message compression codec implementations for [message-queue-toolkit](https://github.com/kibertoad/message-queue-toolkit).

This package provides the concrete codec implementations (e.g. zstd) used by the SQS and SNS adapters. The codec interfaces and types (`MessageCodecEnum`, `MessageCodecHandler`, `CodecEnvelope`) live in `@message-queue-toolkit/core`.

## Installation

```sh
npm install @message-queue-toolkit/codec @message-queue-toolkit/core
```

> **Requirements:** Node.js >=22.15.0 (uses the built-in `zlib` zstd support).

## Usage

Codec options are typically set on the publisher/consumer constructor in the SQS or SNS adapter packages. You do not need to interact with this package directly unless you are building a custom adapter.

### How compression works during publish

When `codec` is set on a publisher, compression happens **exactly once** at the start of `publish()`, before any other processing:

1. The message JSON is compressed to a raw `Buffer`.
2. If a payload store is configured **and** the compressed size exceeds `messageSizeThreshold`, the compressed bytes are stored in S3 and only a lightweight pointer is sent. The codec name is recorded in `payloadRef.codec` so the consumer can decompress after retrieval.
3. If the compressed size fits within the threshold (or no store is configured), the message is sent inline as a self-describing codec envelope.

The payload is never compressed twice. The same compressed `Buffer` from step 1 is either uploaded to S3 or wrapped in the envelope — whichever path is taken.

### Compress / decompress a message body

```typescript
import { compressMessageBody, decompressMessageBody } from '@message-queue-toolkit/codec'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

// Compress (returns a JSON string containing the codec envelope)
const compressed = await compressMessageBody(JSON.stringify(payload), MessageCodecEnum.ZSTD)

// Decompress (parses the envelope and returns the original object)
const original = await decompressMessageBody(JSON.parse(compressed))
```

### Build a codec envelope from already-compressed bytes

When you have pre-compressed bytes (e.g., from `resolveCodecHandler(codec).compress(...)`) and want to produce the envelope string without compressing again:

```typescript
import { buildCodecEnvelope, resolveCodecHandler } from '@message-queue-toolkit/codec'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

const handler = resolveCodecHandler(MessageCodecEnum.ZSTD)
const compressed: Buffer = await handler.compress(Buffer.from(JSON.stringify(payload), 'utf8'))

// Build envelope without a second compression pass
const envelopeString = buildCodecEnvelope(compressed, MessageCodecEnum.ZSTD)
// → '{"__mqtCodec":"zstd","__mqtData":"<base64>"}'
```

### Custom codec handler

```typescript
import type { MessageCodecHandler } from '@message-queue-toolkit/core'

class MyCodecHandler implements MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer> { /* ... */ }
  decompress(data: Buffer): Promise<Buffer> { /* ... */ }
}
```

## Codec envelope format

Compressed messages are wrapped in a self-describing JSON envelope:

```json
{
  "__mqtCodec": "zstd",
  "__mqtData": "<base64-encoded compressed bytes>"
}
```

Consumers auto-detect this envelope and decompress transparently, even if the `codec` option is not set on the consumer.

## License

MIT
