import { compress, decompress } from '@mongodb-js/zstd'

export const SUPPORTED_CODECS = ['zstd'] as const
export type MessageCodec = (typeof SUPPORTED_CODECS)[number]

const CODEC_FIELD = '__codec'
const DATA_FIELD = '__data'

export type CodecEnvelope = {
  [CODEC_FIELD]: MessageCodec
  [DATA_FIELD]: string
}

export function isCodecEnvelope(value: unknown): value is CodecEnvelope {
  return (
    typeof value === 'object' &&
    value !== null &&
    CODEC_FIELD in value &&
    DATA_FIELD in value &&
    SUPPORTED_CODECS.includes((value as Record<string, unknown>)[CODEC_FIELD] as MessageCodec)
  )
}

export async function compressMessageBody(jsonBody: string, codec: MessageCodec): Promise<string> {
  if (codec === 'zstd') {
    const compressed = await compress(Buffer.from(jsonBody, 'utf8'))
    const envelope: CodecEnvelope = {
      [CODEC_FIELD]: codec,
      [DATA_FIELD]: compressed.toString('base64'),
    }
    return JSON.stringify(envelope)
  }
  throw new Error(`Unsupported codec: ${codec}`)
}

export async function decompressMessageBody(envelope: CodecEnvelope): Promise<unknown> {
  if (envelope[CODEC_FIELD] === 'zstd') {
    const compressed = Buffer.from(envelope[DATA_FIELD], 'base64')
    const decompressed = await decompress(compressed)
    return JSON.parse(decompressed.toString('utf8'))
  }
  throw new Error(`Unsupported codec: ${envelope[CODEC_FIELD]}`)
}
