import type { Readable } from 'node:stream'

export async function streamWithKnownSizeToBuffer(stream: Readable, size: number): Promise<Buffer> {
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

  // Copy only when the stream delivered fewer bytes than expected so the
  // full backing allocation is not retained via a shared-memory view.
  return offset === size ? buffer : Buffer.from(buffer.subarray(0, offset))
}

export async function streamWithKnownSizeToString(stream: Readable, size: number): Promise<string> {
  const buffer = await streamWithKnownSizeToBuffer(stream, size)
  return buffer.toString('utf8')
}
