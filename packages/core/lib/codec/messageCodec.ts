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
  /** zstd compression via Node.js built-in `zlib` (requires Node.js 22+). */
  ZSTD: 'zstd',
} as const
export type MessageCodec = ObjectValues<typeof MessageCodecEnum>

const CODEC_FIELD = '__codec'
const DATA_FIELD = '__data'

export type CodecEnvelope = {
  [CODEC_FIELD]: MessageCodec
  [DATA_FIELD]: string
}

/**
 * Low-level interface for a compression codec.
 *
 * Implement this interface to plug in a custom compression algorithm.
 * The built-in implementation (`ZstdCodecHandler` in `@message-queue-toolkit/sqs`)
 * uses Node.js built-in `zlib` zstd support.
 */
export interface MessageCodecHandler {
  compress(data: Buffer): Promise<Buffer>
  decompress(data: Buffer): Promise<Buffer>
}

export function isCodecEnvelope(value: unknown): value is CodecEnvelope {
  const record = value as Record<string, unknown>
  return (
    typeof value === 'object' &&
    value !== null &&
    CODEC_FIELD in value &&
    DATA_FIELD in value &&
    (Object.values(MessageCodecEnum) as string[]).includes(record[CODEC_FIELD] as string) &&
    typeof record[DATA_FIELD] === 'string'
  )
}
