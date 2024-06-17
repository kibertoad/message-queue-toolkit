import type { Readable } from 'node:stream'

export async function streamToString(stream: Readable): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of stream) {
    if (!Buffer.isBuffer(chunk) && typeof chunk !== 'string') {
      continue
    }
    chunks.push(!Buffer.isBuffer(chunk) ? Buffer.from(chunk, 'utf8') : chunk)
  }
  return Buffer.concat(chunks).toString('utf8')
}
