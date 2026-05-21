import type { Transform } from 'node:stream'
import { promisify } from 'node:util'
import zlib from 'node:zlib'
import type {
  CodecEnvelope,
  MessageCodecHandler,
  MessageCodecRegistration,
} from '@message-queue-toolkit/core'
import { MessageCodecEnum } from '@message-queue-toolkit/core'

/**
 * Validates that a string is properly-padded base64 before passing it to Buffer.from.
 * Buffer.from(str, 'base64') silently ignores non-base64 characters, so without this
 * check a malformed __mqtData field produces garbage bytes and a confusing codec error
 * instead of a clear "invalid envelope" message.
 */
const BASE64_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})?$/

const ZSTD_UNSUPPORTED_MSG =
  'zlib.zstdCompress and zlib.zstdDecompress are not available in this Node.js version. ' +
  '@message-queue-toolkit/codec requires Node.js >=22.15.0 or >=23.8.0.'

// Resolved lazily — undefined on Node versions that lack zstd support.
const zstdCompress =
  typeof zlib.zstdCompress === 'function' ? promisify(zlib.zstdCompress) : undefined
const zstdDecompress =
  typeof zlib.zstdDecompress === 'function' ? promisify(zlib.zstdDecompress) : undefined

export class ZstdCodecHandler implements MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer> {
    if (!zstdCompress) throw new Error(ZSTD_UNSUPPORTED_MSG)
    return zstdCompress(data)
  }

  decompress(data: Buffer): Promise<Buffer> {
    if (!zstdDecompress) throw new Error(ZSTD_UNSUPPORTED_MSG)
    return zstdDecompress(data)
  }

  createCompressStream(): Transform {
    if (typeof zlib.createZstdCompress !== 'function') throw new Error(ZSTD_UNSUPPORTED_MSG)
    return zlib.createZstdCompress()
  }
}

const ZSTD_HANDLER = new ZstdCodecHandler()

/**
 * Returns the name string that will be written into the `__mqtCodec` field of every envelope.
 */
export function getCodecName(codec: MessageCodecRegistration): string {
  return typeof codec === 'string' ? codec : codec.name
}

/**
 * Resolves the {@link MessageCodecHandler} for the given codec registration.
 *
 * - String form (`MessageCodec`): returns the built-in handler for that codec.
 * - Object form (`{ name, handler }`): returns the provided handler directly.
 */
export function resolveCodecHandler(codec: MessageCodecRegistration): MessageCodecHandler {
  if (typeof codec === 'object') return codec.handler
  if (codec === MessageCodecEnum.ZSTD) return ZSTD_HANDLER
  throw new Error(`Unsupported codec: ${codec}`)
}

export async function compressMessageBody(
  jsonBody: string,
  codec: MessageCodecRegistration,
): Promise<string> {
  const handler = resolveCodecHandler(codec)
  const compressed = await handler.compress(Buffer.from(jsonBody, 'utf8'))
  return buildCodecEnvelope(compressed, getCodecName(codec))
}

/**
 * Wraps an already-compressed buffer in a codec envelope string.
 * Use this when you have pre-compressed bytes and want to avoid compressing twice.
 *
 * Uses string concatenation instead of JSON.stringify to avoid allocating an
 * intermediate object — the base64 string and the envelope string are the only
 * two allocations on the inline path.
 */
export function buildCodecEnvelope(compressed: Buffer, codecName: string): string {
  return '{"__mqtCodec":"' + codecName + '","__mqtData":"' + compressed.toString('base64') + '"}'
}

export async function decompressMessageBody(envelope: CodecEnvelope): Promise<unknown> {
  if (!BASE64_RE.test(envelope.__mqtData)) {
    throw new Error(`Codec envelope __mqtData is not valid base64 (codec: ${envelope.__mqtCodec})`)
  }
  const handler = resolveCodecHandler(envelope.__mqtCodec as MessageCodecRegistration)
  const compressed = Buffer.from(envelope.__mqtData, 'base64')
  const decompressed = await handler.decompress(compressed)
  return JSON.parse(decompressed.toString('utf8'))
}
