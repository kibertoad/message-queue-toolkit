import type { Transform } from 'node:stream'
import { promisify } from 'node:util'
import zlib from 'node:zlib'
import type {
  CodecEnvelope,
  MessageCodecHandler,
  MessageCodecRegistration,
} from './messageCodec.ts'
import { BASE64_RE, MessageCodecEnum } from './messageCodec.ts'

const ZSTD_UNSUPPORTED_MSG =
  'zlib.zstdCompress and zlib.zstdDecompress are not available in this Node.js version. ' +
  'Message compression requires Node.js >=22.15.0 or >=23.8.0.'

// Resolved lazily — undefined on Node versions that lack zstd support.
// Keeping these lazy means importing core never throws on older Node; only an
// actual compress/decompress call does, and only when zstd is genuinely used.
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
 * Allowed characters for a custom codec name: ASCII letters, digits, hyphens, underscores.
 * This keeps the name JSON-safe without escaping and makes it a recognisable identifier.
 */
const SAFE_CODEC_NAME_RE = /^[A-Za-z0-9_-]+$/

/**
 * Returns the name string that will be written into the `__mqtCodec` field of every envelope.
 * Throws for custom (object-form) registrations whose name contains characters that would
 * produce invalid JSON when interpolated raw into the envelope string.
 */
export function getCodecName(codec: MessageCodecRegistration): string {
  if (typeof codec === 'object') {
    if (!SAFE_CODEC_NAME_RE.test(codec.name)) {
      throw new Error(
        `Invalid codec name "${codec.name}": only ASCII letters, digits, hyphens, and underscores are allowed`,
      )
    }
    return codec.name
  }
  return codec
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
 *
 * `codecName` must already be a JSON-safe identifier (see {@link getCodecName},
 * which is enforced for every registration before it reaches this function).
 */
export function buildCodecEnvelope(compressed: Buffer, codecName: string): string {
  return '{"__mqtCodec":"' + codecName + '","__mqtData":"' + compressed.toString('base64') + '"}'
}

/**
 * Decompresses a codec envelope produced by {@link compressMessageBody} or
 * {@link buildCodecEnvelope} and returns the original parsed JSON value.
 *
 * **Built-in codecs only.** This utility resolves the handler via
 * {@link resolveCodecHandler}, which only recognises built-in codec names
 * (e.g. `MessageCodecEnum.ZSTD`). Calling it with a custom-codec envelope
 * (where `__mqtCodec` is a user-chosen name) will throw "Unsupported codec".
 * Consumer-side decoding of custom codecs is handled automatically via the
 * consumer's codec registry; this function is intended for one-off, built-in use cases.
 */
export async function decompressMessageBody(envelope: CodecEnvelope): Promise<unknown> {
  if (!BASE64_RE.test(envelope.__mqtData)) {
    throw new Error(`Codec envelope __mqtData is not valid base64 (codec: ${envelope.__mqtCodec})`)
  }
  const handler = resolveCodecHandler(envelope.__mqtCodec as MessageCodecRegistration)
  const compressed = Buffer.from(envelope.__mqtData, 'base64')
  const decompressed = await handler.decompress(compressed)
  return JSON.parse(decompressed.toString('utf8'))
}
