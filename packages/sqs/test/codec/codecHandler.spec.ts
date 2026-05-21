import { decompressMessageBody } from '@message-queue-toolkit/codec'
import { MessageCodecEnum } from '@message-queue-toolkit/core'
import { describe, expect, it } from 'vitest'

describe('decompressMessageBody', () => {
  it('throws a descriptive error when __mqtData is not valid base64', async () => {
    await expect(
      decompressMessageBody({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'not-base64!!!' }),
    ).rejects.toThrow('Codec envelope __mqtData is not valid base64 (codec: zstd)')
  })

  it('throws a descriptive error for base64 with incorrect padding', async () => {
    // Valid base64 characters but wrong padding — Buffer.from would silently accept this
    // and produce garbage bytes; the guard must catch it before the codec is invoked.
    await expect(
      decompressMessageBody({ __mqtCodec: MessageCodecEnum.ZSTD, __mqtData: 'abc' }),
    ).rejects.toThrow('Codec envelope __mqtData is not valid base64 (codec: zstd)')
  })
})
