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
 * The built-in implementation (`ZstdCodecHandler`, exported from
 * `@message-queue-toolkit/core`) uses Node.js built-in `zlib` zstd support.
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
 * new MyConsumer(deps, { codecs: [codec] })  // register the same codec on the consumer
 */
export type MessageCodecRegistration = MessageCodec | { name: string; handler: MessageCodecHandler }

/**
 * Base64 pattern: groups of 4 chars from the alphabet, with at most 2 trailing `=` pads.
 * An empty string (compressed payload of 0 bytes) is also valid.
 * Exported so codec implementations can reuse it without duplicating the regex.
 */
export const BASE64_RE =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{4})?$/

/** Built once at module load — avoids a fresh array allocation on every hot-path call. */
export const KNOWN_CODECS: ReadonlySet<string> = new Set(Object.values(MessageCodecEnum))

/**
 * Structural check: returns true when `value` has the shape of a codec envelope —
 * a non-empty string `__mqtCodec` and a base64 `__mqtData` — **regardless of whether
 * the named codec is one this consumer can decode**.
 *
 * Detection is **presence-based**: extra sibling fields are allowed, because publishers
 * copy identity/routing fields (`id`, `type`, …) alongside the codec fields so
 * broker-side filtering (e.g. SNS body-scoped FilterPolicy) keeps working on compressed
 * messages. This mirrors how an offloaded-payload pointer is recognised by its
 * `payloadRef` shape, not by an exact object shape.
 *
 * Consumers use this (rather than {@link isCodecEnvelope}) so an envelope for an
 * unregistered codec can be told apart from an ordinary message and surfaced as a
 * misconfiguration instead of being validated as an incomplete skeleton. The cheap
 * `in` checks run first, so a non-envelope value returns without allocating anything.
 */
export function hasCodecEnvelopeShape(value: unknown): value is CodecEnvelope {
  if (typeof value !== 'object' || value === null) return false
  const record = value as Record<string, unknown>
  return (
    CODEC_FIELD in record &&
    DATA_FIELD in record &&
    typeof record[CODEC_FIELD] === 'string' &&
    (record[CODEC_FIELD] as string).length > 0 &&
    typeof record[DATA_FIELD] === 'string' &&
    // Validate __mqtData is a properly-padded base64 string before handing it to the codec.
    BASE64_RE.test(record[DATA_FIELD] as string)
  )
}

/**
 * Returns true when `value` is a codec envelope **for a codec in `knownCodecs`** — i.e.
 * one this consumer can actually decode. Combines the structural
 * {@link hasCodecEnvelopeShape} check with a codec-name lookup.
 *
 * Pass `knownCodecs` to restrict the match to the codecs your consumer is configured to
 * handle. Defaults to the built-in codec set — backwards-compatible for consumers that
 * don't configure a codec.
 */
export function isCodecEnvelope(
  value: unknown,
  knownCodecs: ReadonlySet<string> = KNOWN_CODECS,
): value is CodecEnvelope {
  return hasCodecEnvelopeShape(value) && knownCodecs.has((value as CodecEnvelope).__mqtCodec)
}
