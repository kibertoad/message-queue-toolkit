export const SUPPORTED_CODECS = ['zstd'] as const
export type MessageCodec = (typeof SUPPORTED_CODECS)[number]

const CODEC_FIELD = '__codec'
const DATA_FIELD = '__data'

export type CodecEnvelope = {
  [CODEC_FIELD]: MessageCodec
  [DATA_FIELD]: string
}

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
    SUPPORTED_CODECS.includes(record[CODEC_FIELD] as MessageCodec) &&
    typeof record[DATA_FIELD] === 'string'
  )
}
