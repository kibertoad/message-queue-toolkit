import type { Transform } from 'node:stream'
import { promisify } from 'node:util'
import zlib from 'node:zlib'
import type { MessageCodecHandler, MessageCodecRegistration } from './messageCodec.ts'
import { MessageCodecEnum } from './messageCodec.ts'

const ZSTD_UNSUPPORTED_MSG =
  'zlib.zstdCompress and zlib.zstdDecompress are not available in this Node.js version. ' +
  'Message compression requires Node.js >=22.15.0 or >=23.8.0.'

/**
 * Default upper bound on the decompressed size of a single message, in bytes (100 MiB).
 *
 * Protects consumers from decompression-bomb inputs: a tiny compressed envelope can
 * otherwise expand to gigabytes of highly-repetitive data and exhaust process memory.
 * 100 MiB is far above any realistic queue message (SQS/SNS cap bodies at 256 KiB, and
 * even offloaded payloads are typically single-digit MiB) while still bounding the blast
 * radius of a malicious or corrupt frame. Override via the {@link ZstdCodecHandler}
 * constructor if you legitimately handle larger messages.
 */
export const DEFAULT_MAX_DECOMPRESSED_BYTES = 100 * 1024 * 1024

// Resolved lazily — undefined on Node versions that lack zstd support.
// Keeping these lazy means importing core never throws on older Node; only an
// actual compress/decompress call does, and only when zstd is genuinely used.
const zstdCompress =
  typeof zlib.zstdCompress === 'function' ? promisify(zlib.zstdCompress) : undefined
const zstdDecompress =
  typeof zlib.zstdDecompress === 'function' ? promisify(zlib.zstdDecompress) : undefined

export class ZstdCodecHandler implements MessageCodecHandler {
  private readonly maxDecompressedBytes: number

  /**
   * @param maxDecompressedBytes upper bound on a single decompressed message, in bytes.
   *   Defaults to {@link DEFAULT_MAX_DECOMPRESSED_BYTES} (100 MiB). Decompression of an
   *   input that would exceed this limit is rejected before the full payload is buffered.
   */
  constructor(maxDecompressedBytes: number = DEFAULT_MAX_DECOMPRESSED_BYTES) {
    this.maxDecompressedBytes = maxDecompressedBytes
  }

  compress(data: Buffer): Promise<Buffer> {
    if (!zstdCompress) throw new Error(ZSTD_UNSUPPORTED_MSG)
    return zstdCompress(data)
  }

  decompress(data: Buffer): Promise<Buffer> {
    if (!zstdDecompress) throw new Error(ZSTD_UNSUPPORTED_MSG)
    // maxOutputLength caps the decompressed size: zstdDecompress rejects with a
    // RangeError once the limit is exceeded, guarding against decompression bombs.
    return zstdDecompress(data, { maxOutputLength: this.maxDecompressedBytes })
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

/**
 * Wraps an already-compressed buffer in a codec envelope string.
 * Use this when you have pre-compressed bytes and want to avoid compressing twice.
 *
 * `preservedFields`, when provided, are emitted as plaintext siblings of the codec
 * fields (`{ ...preserved, __mqtCodec, __mqtData }`). Publishers use this to keep
 * identity/routing fields (`id`, `type`, …) visible on the wire so broker-side
 * filtering (e.g. SNS body-scoped FilterPolicy) still works on compressed messages —
 * the same fields an offloaded-payload pointer carries. The codec fields are written
 * last, so a colliding preserved key can never corrupt the envelope; consumers ignore
 * the preserved siblings and decode `__mqtData` only.
 *
 * Without `preservedFields` the fast path uses string concatenation instead of
 * JSON.stringify, avoiding an intermediate object — the base64 string and the
 * envelope string are the only two allocations on the inline path.
 *
 * `codecName` must already be a JSON-safe identifier (see {@link getCodecName},
 * which is enforced for every registration before it reaches this function).
 */
export function buildCodecEnvelope(
  compressed: Buffer,
  codecName: string,
  preservedFields?: Record<string, unknown>,
): string {
  const data = compressed.toString('base64')
  if (!preservedFields || Object.keys(preservedFields).length === 0) {
    return `{"__mqtCodec":"${codecName}","__mqtData":"${data}"}`
  }
  // Preserved fields present: a single JSON.stringify handles all value escaping.
  // Codec fields are listed last so they always win over any colliding preserved key.
  return JSON.stringify({ ...preservedFields, __mqtCodec: codecName, __mqtData: data })
}
