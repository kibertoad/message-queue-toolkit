type ObjectValues<T> = T[keyof T]

/**
 * Supported message compression codecs.
 *
 * Use the enum values instead of raw strings so that adding a new codec in
 * the future is a single-place change and consumers benefit from IDE
 * auto-complete.
 *
 * @example
 * new MyPublisher(deps, { codec: MessageCodecEnum.ZSTD })
 */
export const MessageCodecEnum = {
  /** zstd compression via Node.js built-in `zlib` (requires Node.js >=22.15.0). */
  ZSTD: 'zstd',
} as const
export type MessageCodec = ObjectValues<typeof MessageCodecEnum>

const CODEC_FIELD = '__mqtCodec'
const DATA_FIELD = '__mqtData'

export type CodecEnvelope = {
  // string (not MessageCodec) to accommodate user-supplied codec names.
  [CODEC_FIELD]: string
  [DATA_FIELD]: string
}

/**
 * Low-level interface for a compression codec.
 *
 * Implement this interface to plug in a custom compression algorithm.
 * The built-in implementation (`ZstdCodecHandler` in `@message-queue-toolkit/codec`)
 * uses Node.js built-in `zlib` zstd support.
 *
 * All three methods are required:
 * - `compress` / `decompress` are used for the inline (non-offloaded) publish path.
 * - `createCompressStream` is used by the streaming offload path to pipe serialized
 *   JSON directly through compression into the payload store without buffering the
 *   full payload in memory.
 */
export interface MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer>
  decompress(data: Buffer): Promise<Buffer>
  /** Returns a Transform stream that compresses its input using this codec. */
  createCompressStream(): import('node:stream').Transform
}

/**
 * Passed to the `codec` option to select a compression codec.
 *
 * - **String form** (`MessageCodec`): selects one of the built-in codecs
 *   (e.g. `MessageCodecEnum.ZSTD`).
 * - **Object form** (`{ name, handler }`): plugs in a custom
 *   `MessageCodecHandler` implementation under a user-chosen name.  The name
 *   is written into the `__mqtCodec` field of every envelope so the consumer
 *   can identify and route to the correct handler.
 *
 * @example Built-in zstd
 * new MyPublisher(deps, { codec: MessageCodecEnum.ZSTD })
 *
 * @example Custom codec
 * import { LZ4Handler } from './lz4Handler.ts'
 * const codec = { name: 'lz4', handler: new LZ4Handler() }
 * new MyPublisher(deps, { codec })
 * new MyConsumer(deps, { codec })  // same registration on the consumer side
 */
export type MessageCodecRegistration = MessageCodec | { name: string; handler: MessageCodecHandler }

/**
 * Base64 pattern: groups of 4 chars from the alphabet, with at most 2 trailing `=` pads.
 * An empty string (compressed payload of 0 bytes) is also valid.
 */
const BASE64_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})?$/

/** Built once at module load — avoids a fresh array allocation on every hot-path call. */
export const KNOWN_CODECS: ReadonlySet<string> = new Set(Object.values(MessageCodecEnum))

/**
 * Returns true when `value` is a codec envelope that the consumer should decompress.
 *
 * Pass `knownCodecs` to restrict auto-detection to the codecs your consumer is
 * configured to handle (built from the `codec` option).  Defaults to the built-in
 * codec set — backwards-compatible for consumers that don't configure a codec.
 */
export function isCodecEnvelope(
  value: unknown,
  knownCodecs: ReadonlySet<string> = KNOWN_CODECS,
): value is CodecEnvelope {
  const record = value as Record<string, unknown>
  return (
    typeof value === 'object' &&
    value !== null &&
    // Exact two-key shape — extra fields mean this is a real message, not an envelope.
    Object.keys(value).length === 2 &&
    CODEC_FIELD in value &&
    DATA_FIELD in value &&
    knownCodecs.has(record[CODEC_FIELD] as string) &&
    typeof record[DATA_FIELD] === 'string' &&
    // Validate __mqtData is a properly-padded base64 string before handing it to the codec.
    BASE64_RE.test(record[DATA_FIELD] as string)
  )
}
