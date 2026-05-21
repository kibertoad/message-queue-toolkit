import { promisify } from 'node:util'
import zlib from 'node:zlib'
import type { CodecEnvelope, MessageCodec, MessageCodecHandler } from '@message-queue-toolkit/core'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

if (typeof zlib.zstdCompress !== 'function' || typeof zlib.zstdDecompress !== 'function') {
  throw new Error(
    'zlib.zstdCompress and zlib.zstdDecompress are not available in this Node.js version. ' +
      '@message-queue-toolkit/codec requires Node.js >=22.15.0 or >=23.8.0.',
  )
}

const zstdCompress = promisify(zlib.zstdCompress)
const zstdDecompress = promisify(zlib.zstdDecompress)

export class ZstdCodecHandler implements MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer> {
    return zstdCompress(data)
  }

  decompress(data: Buffer): Promise<Buffer> {
    return zstdDecompress(data)
  }
}

const ZSTD_HANDLER = new ZstdCodecHandler()

export function resolveCodecHandler(codec: MessageCodec): MessageCodecHandler {
  if (codec === MessageCodecEnum.ZSTD) return ZSTD_HANDLER
  throw new Error(`Unsupported codec: ${codec}`)
}

export async function compressMessageBody(jsonBody: string, codec: MessageCodec): Promise<string> {
  const handler = resolveCodecHandler(codec)
  const compressed = await handler.compress(Buffer.from(jsonBody, 'utf8'))
  return buildCodecEnvelope(compressed, codec)
}

/**
 * Wraps an already-compressed buffer in a codec envelope string.
 * Use this when you have pre-compressed bytes and want to avoid compressing twice.
 *
 * Uses string concatenation instead of JSON.stringify to avoid allocating an
 * intermediate object — the base64 string and the envelope string are the only
 * two allocations on the inline path.
 */
export function buildCodecEnvelope(compressed: Buffer, codec: MessageCodec): string {
  return '{"__mqtCodec":"' + codec + '","__mqtData":"' + compressed.toString('base64') + '"}'
}

export async function decompressMessageBody(envelope: CodecEnvelope): Promise<unknown> {
  const handler = resolveCodecHandler(envelope.__mqtCodec)
  const compressed = Buffer.from(envelope.__mqtData, 'base64')
  const decompressed = await handler.decompress(compressed)
  return JSON.parse(decompressed.toString('utf8'))
}
