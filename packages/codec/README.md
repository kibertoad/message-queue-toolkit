# @message-queue-toolkit/codec

Message compression codec implementations for [message-queue-toolkit](https://github.com/kibertoad/message-queue-toolkit).

This package provides the concrete codec implementations (e.g. zstd) used by the SQS and SNS adapters. The codec interfaces and types (`MessageCodecEnum`, `MessageCodecHandler`, `CodecEnvelope`) live in `@message-queue-toolkit/core`.

## Installation

```sh
npm install @message-queue-toolkit/codec @message-queue-toolkit/core
```

> **Requirements:** Node.js 22+ (uses the built-in `zlib` zstd support).

## Usage

Codec options are typically set on the publisher/consumer constructor in the SQS or SNS adapter packages. You do not need to interact with this package directly unless you are building a custom adapter.

### Compress / decompress a message body

```typescript
import { compressMessageBody, decompressMessageBody } from '@message-queue-toolkit/codec'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

// Compress (returns a JSON string containing the codec envelope)
const compressed = await compressMessageBody(JSON.stringify(payload), MessageCodecEnum.ZSTD)

// Decompress (parses the envelope and returns the original object)
const original = await decompressMessageBody(JSON.parse(compressed))
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
  "__codec": "zstd",
  "__data": "<base64-encoded compressed bytes>"
}
```

Consumers auto-detect this envelope and decompress transparently, even if the `codec` option is not set on the consumer.

## License

MIT
