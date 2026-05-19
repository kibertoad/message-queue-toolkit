import { promisify } from 'node:util'
import zlib from 'node:zlib'
import type { CodecEnvelope, MessageCodec, MessageCodecHandler } from '@message-queue-toolkit/core'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

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
  const envelope: CodecEnvelope = {
    __codec: codec,
    __data: compressed.toString('base64'),
  }
  return JSON.stringify(envelope)
}

export async function decompressMessageBody(envelope: CodecEnvelope): Promise<unknown> {
  const handler = resolveCodecHandler(envelope.__codec)
  const compressed = Buffer.from(envelope.__data, 'base64')
  const decompressed = await handler.decompress(compressed)
  return JSON.parse(decompressed.toString('utf8'))
}
