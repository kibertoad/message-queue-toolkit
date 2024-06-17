import type { Readable } from 'node:stream'

export async function streamWithKnownSizeToString(stream: Readable, size: number): Promise<string> {
  const buffer = Buffer.alloc(size)
  let offset = 0

  for await (const chunk of stream) {
    if (typeof chunk !== 'string' && !Buffer.isBuffer(chunk)) {
      continue
    }

    const chunkBuffer = !Buffer.isBuffer(chunk) ? Buffer.from(chunk, 'utf8') : chunk
    chunkBuffer.copy(buffer, offset)
    offset += chunkBuffer.length
  }

  return buffer.toString('utf8', 0, offset)
}
